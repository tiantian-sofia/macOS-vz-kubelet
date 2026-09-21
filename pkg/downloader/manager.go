package downloader

import (
	"context"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/agoda-com/macOS-vz-kubelet/pkg/event"
	"github.com/agoda-com/macOS-vz-kubelet/pkg/resource"
	"github.com/agoda-com/macOS-vz-kubelet/pkg/vm/config"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Manager manages the download of OCI images.
type Manager struct {
	eventRecorder event.EventRecorder
	cachePath     string

	downloads *xsync.Map[downloadKey, *state]

	// runDownload performs the actual pull. It is a field so tests can substitute it.
	runDownload func(ctx context.Context, params Params, eventRecorder event.EventRecorder) (config.MacPlatformConfigurationOptions, error)
	// onSubscribed, when set, is invoked after a subscriber joined an in-flight
	// download. Test seam only.
	onSubscribed func(st *state)
}

// downloadKey identifies a dedup bucket. Two requests share an in-flight download
// only when they are interchangeable: same reference, same cache policy and the
// same registry credentials. In particular an imagePullPolicy: Always request must
// never merge with a cached pull, and pulls authenticated with different
// imagePullSecrets (even for the same registry) stay isolated.
type downloadKey struct {
	ref            string
	ignoreExisting bool
	creds          resource.RegistryCredentials
}

// state contains the state of a download operation.
//
// Lifecycle: a state is fully initialized (span, cancel func, done channel) before
// it is published in Manager.downloads, so subscribers never observe nil fields.
// Subscribers join while the state is not closed. When the last subscriber leaves,
// the state is marked closed, removed from the map (identity-checked) and its
// download context is canceled. A subscriber leaving while others stay never
// affects the shared download.
type state struct {
	mu sync.Mutex
	// subscribers is the number of callers currently joined; guarded by mu.
	subscribers int
	// closed marks the state retired by its last subscriber; guarded by mu.
	// A closed state must not accept new subscribers and is (being) removed from
	// the map.
	closed bool

	done       chan struct{}
	span       oteltrace.Span
	cancelFunc context.CancelFunc

	config   config.MacPlatformConfigurationOptions
	duration time.Duration
	err      error
}

// NewManager creates a new DownloadManager.
func NewManager(eventRecorder event.EventRecorder, cachePath string) *Manager {
	return &Manager{
		eventRecorder: eventRecorder,
		cachePath:     cachePath,
		downloads:     xsync.NewMap[downloadKey, *state](),
		runDownload:   Download,
	}
}

// Download ensures that a download operation for the given ref, pull policy and
// credentials runs at most once concurrently: callers with an identical
// (ref, ignoreExisting, creds) tuple subscribe to the same in-flight operation and
// all receive its result.
//
// Cancellation is scoped per subscriber: canceling one caller's context makes that
// caller return context.Canceled but leaves the shared download untouched for the
// remaining subscribers. The underlying pull is only canceled when every
// subscriber has gone. A caller whose context is already canceled still counts as a
// subscriber only if it successfully joins; otherwise it is rejected with
// context.Canceled before any pull is started.
//
// Parameters:
//   - ctx: The context controlling the lifecycle of the subscriber's interest in the download.
//   - ref: A unique identifier for the resource being downloaded.
//   - ignoreExisting: A flag indicating whether to force a re-download, even if the resource
//     is already cached.
//   - creds: Registry credentials used for the pull; requests with different credentials
//     are never merged.
//
// Returns:
// - config.MacPlatformConfigurationOptions: The result of the download if successful.
// - error: Any error that occurred during the download, or if the subscriber's context is canceled.
func (m *Manager) Download(ctx context.Context, ref string, ignoreExisting bool, creds resource.RegistryCredentials) (cfg config.MacPlatformConfigurationOptions, d time.Duration, err error) {
	ctx, span := trace.StartSpan(ctx, "Manager.Download")
	ctx = span.WithFields(ctx, log.Fields{
		"ref":            ref,
		"ignoreExisting": ignoreExisting,
	})
	defer func() {
		_ = span.WithField(ctx, "duration", d)
		span.SetStatus(err)
		span.End()
	}()
	logger := log.G(ctx)
	logger.Infof("Requesting to subscribe to download %q", ref)

	// A caller that is already gone must neither start a pull nor subscribe to one:
	// if it raced in as the owner it would cancel the shared download immediately.
	if err := ctx.Err(); err != nil {
		return cfg, d, err
	}

	key := downloadKey{ref: ref, ignoreExisting: ignoreExisting, creds: creds}
	st, owner := m.join(ctx, key)

	defer m.unsubscribe(logger, st, key, owner, ref)

	// Link the download span to the subscriber's span. The span is guaranteed to be
	// initialized because the state is fully built before being published.
	oteltrace.SpanFromContext(ctx).AddLink(oteltrace.Link{SpanContext: st.span.SpanContext()})

	select {
	case <-ctx.Done():
		return cfg, d, ctx.Err()
	case <-st.done:
		return st.config, st.duration, st.err
	}
}

// join attaches the caller to an in-flight download for key, or starts a new one.
// The returned state is always live (not closed) and has the caller registered as a
// subscriber. owner reports whether this caller started the download.
func (m *Manager) join(ctx context.Context, key downloadKey) (*state, bool) {
	for {
		// Fast path: an active state already exists.
		if existing, ok := m.downloads.Load(key); ok {
			if st := trySubscribe(existing, m.onSubscribed); st != nil {
				return st, false
			}
			// The state was retired by its last subscriber right as we looked it up.
			// Its removal from the map is concurrent; wait for the (being canceled)
			// download to wind down and retry instead of busy-spinning.
			<-existing.done
			continue
		}

		// Slow path: build a fully initialized state before publishing it.
		// Subscribers can therefore never observe a nil span or cancel func.
		candidate, downloadCtx := m.newState(ctx)
		actual, loaded := m.downloads.LoadOrStore(key, candidate)
		if !loaded {
			logger := log.G(ctx)
			logger.Infof("Initiating download %q per request", key.ref)

			// Performing download in a go routine to keep listening for context
			// cancellation. startDownload closes the span and cancels its context
			// when the download finishes.
			// nolint:contextcheck
			go m.startDownload(downloadCtx, candidate, key.ref, key.ignoreExisting, key.creds)
			return candidate, true
		}

		// Lost the race: discard the candidate's unused context and span.
		candidate.discard()

		if st := trySubscribe(actual, m.onSubscribed); st != nil {
			return st, false
		}
		<-actual.done
	}
}

// unsubscribe removes the caller from the state. When the last subscriber leaves,
// the state is retired: marked closed, identity-checked out of the map, and the
// underlying download is canceled. owner indicates whether the caller started the
// download and is used only for logging.
func (m *Manager) unsubscribe(logger log.Logger, st *state, key downloadKey, owner bool, ref string) {
	st.mu.Lock()
	st.subscribers--
	last := st.subscribers == 0
	if last {
		st.closed = true
	}
	st.mu.Unlock()

	if !last {
		return
	}

	// Remove only while the map still holds this exact state. A different state that
	// may have been created for the same key concurrently must never be touched.
	m.downloads.Compute(key, func(current *state, loaded bool) (*state, xsync.ComputeOp) {
		if loaded && current == st {
			return nil, xsync.DeleteOp
		}
		return current, xsync.CancelOp
	})

	// Cancel the pull after retiring the state: any request arriving from now on
	// cannot subscribe to it and starts a fresh download instead.
	st.cancelFunc()
	if owner {
		logger.Infof("No more subscribers left for %q, cleaning up...", ref)
	}
}

// trySubscribe registers a subscriber on st while it is still live. It returns the
// state on success or nil if the state was already retired, in which case the caller
// must retry.
func trySubscribe(st *state, onSubscribed func(*state)) *state {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return nil
	}
	st.subscribers++
	if onSubscribed != nil {
		onSubscribed(st)
	}
	return st
}

// newState builds a download state whose span, cancel func and done channel are all
// ready before the state enters the map.
func (m *Manager) newState(ctx context.Context) (*state, context.Context) {
	// Use a background context to manage the underlying download.
	downloadCtx, cancel := context.WithCancel(context.Background())

	// Create a new span for the download operation and link it to the parent span.
	// The detached span is ended in startDownload or discard.
	name := "Manager.startDownload"
	link := oteltrace.LinkFromContext(ctx)
	// nolint: spancheck
	downloadCtx, span := otel.Tracer(name).Start(downloadCtx, name, oteltrace.WithLinks(link))
	downloadCtx = log.WithLogger(downloadCtx, log.G(ctx).WithField("method", name))

	// Propagate the object reference to the download context.
	// TODO: OCI doesnt have to report to kubernetes directly, remove this eventually
	if objRef, ok := event.GetObjectRef(ctx); ok {
		downloadCtx = event.WithObjectRef(downloadCtx, *objRef)
	}

	return &state{
		subscribers: 1,
		done:        make(chan struct{}),
		span:        span,
		cancelFunc:  cancel,
	}, downloadCtx
}

// discard releases the resources of a state that never started because it lost the
// publication race.
func (st *state) discard() {
	st.cancelFunc()
	st.span.End()
}

// startDownload starts the download operation and manages the state of the download.
func (m *Manager) startDownload(ctx context.Context, state *state, ref string, ignoreExisting bool, creds resource.RegistryCredentials) {
	defer func() {
		close(state.done)
		state.cancelFunc()

		// Set Span status and end it
		if state.err == nil {
			state.span.SetStatus(codes.Ok, "")
		} else {
			state.span.SetStatus(codes.Error, state.err.Error())
		}
		state.span.End()
	}()
	state.span.SetAttributes(attribute.String("ref", ref), attribute.Bool("ignoreExisting", ignoreExisting))
	logger := log.G(ctx)

	logger.Infof("Starting download for %q", ref)
	startTime := time.Now()
	state.config, state.err = m.runDownload(ctx, Params{
		Ref:             ref,
		StorePath:       m.cachePath,
		IgnoreExisiting: ignoreExisting,
		Credentials:     creds,
	}, m.eventRecorder)

	state.duration = time.Since(startTime)
	logger.Debugf("Download for %q completed in %v", ref, state.duration)

	if ctx.Err() != nil {
		// prioritize the context error
		state.err = ctx.Err()
	}
}
