package downloader

import (
	"context"
	"sync"
	"time"

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

// downloadKey identifies a set of pull requests that may share a single in-flight
// download. Two requests coalesce only when every component matches:
//   - ref: the OCI image reference,
//   - ignoreExisting: the resolved pull policy. A forced (Always) pull never shares
//     with a cache-using (IfNotPresent) pull, otherwise the Always subscriber could
//     observe a stale cached image,
//   - credentials: the registry authentication material. Requests authenticated with
//     different credentials never share a download, so one pod's pull secret can never
//     be used to fetch another pod's image.
type downloadKey struct {
	ref            string
	ignoreExisting bool
	credentials    resource.RegistryCredentials
}

// Manager manages the download of OCI images.
type Manager struct {
	eventRecorder event.EventRecorder
	cachePath     string

	// mu guards downloads. Lock ordering: Manager.mu must always be acquired before
	// state.mu; a goroutine holding state.mu never takes Manager.mu.
	mu        sync.Mutex
	downloads map[downloadKey]*state

	// downloadFunc performs the actual pull. It defaults to Download and is a field
	// solely so tests can drive the manager deterministically without a real registry.
	downloadFunc func(ctx context.Context, params Params, eventRecorder event.EventRecorder) (config.MacPlatformConfigurationOptions, error)
}

// state contains the state of a single in-flight download operation. All mutable fields
// are guarded by mu. The lifecycle is:
//
//  1. A request finds no entry for its key and registers a new state, becoming the owner
//     that starts the download goroutine.
//  2. Further requests subscribe (subscribers++) and wait on done.
//  3. A subscriber whose context is canceled simply unsubscribes; the download keeps
//     running while at least one subscriber remains.
//  4. When the last subscriber leaves while the download is still running, the download
//     context is canceled and the map entry is removed.
//  5. When the download finishes, the map entry is removed so later requests start a
//     fresh pull (a forced pull cannot be served from a completed one); subscribers
//     already waiting on done still receive the result.
//
// Every map mutation and every subscribers change happens under Manager.mu (and
// state.mu), so registration and cleanup can never interleave into subscribing to an
// entry that is being torn down or into orphaning a freshly inserted entry.
type state struct {
	mu          sync.Mutex
	subscribers int
	finished    bool

	// cancel stops the underlying download. It is created while the state is registered
	// (before Manager.mu is released), so it is never nil when a subscriber leaves.
	downloadCtx context.Context
	cancel      context.CancelFunc

	done chan struct{} // closed once config/duration/err are populated
	span oteltrace.Span

	config   config.MacPlatformConfigurationOptions
	duration time.Duration
	err      error
}

// NewManager creates a new DownloadManager.
func NewManager(eventRecorder event.EventRecorder, cachePath string) *Manager {
	return &Manager{
		eventRecorder: eventRecorder,
		cachePath:     cachePath,
		downloads:     make(map[downloadKey]*state),
		downloadFunc:  Download,
	}
}

// Download coalesces concurrent pulls of the same image. Pulls are only shared when the
// reference, resolved pull policy (ignoreExisting) and registry credentials are
// identical. Subscribers receive the shared download's result; canceling one
// subscriber's context never affects the others, while the download is stopped once no
// subscriber remains interested in it.
//
// Parameters:
//   - ctx: The context controlling the lifecycle of this subscriber's interest.
//   - ref: A unique identifier for the resource being downloaded.
//   - ignoreExisting: A flag indicating whether to force a re-download, even if the
//     resource is already cached.
//   - creds: Registry credentials to authenticate the pull with.
//
// Returns:
// - config.MacPlatformConfigurationOptions: The result of the download if successful.
// - error: Any error that occurred during the download, or if the subscriber's context
// was canceled.
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

	key := downloadKey{ref: ref, ignoreExisting: ignoreExisting, credentials: creds}

	st, isOwner := m.register(key)
	if isOwner {
		logger.Infof("Initiating download %q per request", ref)
		m.startDownload(ctx, st, key)
	} else {
		logger.Infof("Joining in-flight download %q", ref)
	}

	// Link the subscriber's span to the download span. The span is set synchronously by
	// the owner before the download goroutine is launched; for a subscriber that races
	// ahead of the owner it simply records an empty link.
	oteltrace.SpanFromContext(ctx).AddLink(oteltrace.Link{SpanContext: st.spanContext()})

	select {
	case <-ctx.Done():
		m.unsubscribe(key, st)
		return cfg, d, context.Canceled
	case <-st.done:
		m.unsubscribe(key, st)
		return st.snapshot()
	}
}

// register joins the state for key, creating it when the caller is the first subscriber.
func (m *Manager) register(key downloadKey) (*state, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if st, ok := m.downloads[key]; ok {
		st.mu.Lock()
		st.subscribers++
		st.mu.Unlock()
		return st, false
	}

	downloadCtx, cancel := context.WithCancel(context.Background())
	st := &state{
		subscribers: 1,
		done:        make(chan struct{}),
		downloadCtx: downloadCtx,
		cancel:      cancel,
	}
	m.downloads[key] = st
	return st, true
}

// unsubscribe decrements the subscriber count. When the departing subscriber is the last
// one and the download has not finished yet, the download is canceled and its map entry
// removed so that subsequent requests start a fresh pull.
func (m *Manager) unsubscribe(key downloadKey, st *state) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	st.subscribers--
	if st.subscribers > 0 || st.finished {
		return
	}

	if current := m.downloads[key]; current == st {
		delete(m.downloads, key)
	}
	st.cancel()
}

// snapshot returns the published result of the download. Only valid after done closes.
func (s *state) snapshot() (config.MacPlatformConfigurationOptions, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config, s.duration, s.err
}

// spanContext returns the trace span context associated with the download.
func (s *state) spanContext() oteltrace.SpanContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.span == nil {
		return oteltrace.SpanContext{}
	}
	return s.span.SpanContext()
}

// startDownload prepares the download's detached span synchronously and runs the actual
// pull in a goroutine. The cancel function already exists on the state (created during
// registration), so every subscriber can safely unsubscribe at any point.
func (m *Manager) startDownload(parentCtx context.Context, st *state, key downloadKey) {
	name := "Manager.startDownload"
	link := oteltrace.LinkFromContext(parentCtx)
	// nolint: spancheck
	ctx, span := otel.Tracer(name).Start(st.downloadCtx, name,
		oteltrace.WithLinks(link),
		oteltrace.WithAttributes(
			attribute.String("ref", key.ref),
			attribute.Bool("ignoreExisting", key.ignoreExisting),
		),
	)
	ctx = log.WithLogger(ctx, log.G(parentCtx).WithField("method", name))

	// Propagate the object reference to the download context
	// TODO: OCI doesnt have to report to kubernetes directly, remove this eventually
	if objRef, ok := event.GetObjectRef(parentCtx); ok {
		ctx = event.WithObjectRef(ctx, *objRef)
	}

	st.mu.Lock()
	st.span = span
	st.mu.Unlock()

	// startDownload manages its own background context and cancels it when done.
	// nolint: contextcheck
	go m.runDownload(ctx, st, key)
}

// runDownload executes the pull and publishes its result to all subscribers.
func (m *Manager) runDownload(ctx context.Context, st *state, key downloadKey) {
	logger := log.G(ctx)
	logger.Infof("Starting download for %q", key.ref)

	startTime := time.Now()
	cfg, err := m.downloadFunc(ctx, Params{
		Ref:             key.ref,
		StorePath:       m.cachePath,
		IgnoreExisiting: key.ignoreExisting,
		Credentials:     key.credentials,
	}, m.eventRecorder)
	duration := time.Since(startTime)

	if ctx.Err() != nil {
		// Prioritize the context error: a canceled download must not look successful
		// even if the pull returned a result.
		err = ctx.Err()
	}

	// Publish the result and detach the entry atomically with respect to registration:
	// a request that arrives after this lock section starts a new pull, while every
	// subscriber already attached still receives this result via done.
	m.mu.Lock()
	st.mu.Lock()
	st.config = cfg
	st.duration = duration
	st.err = err
	st.finished = true
	if current := m.downloads[key]; current == st {
		delete(m.downloads, key)
	}
	close(st.done)
	st.mu.Unlock()
	m.mu.Unlock()

	logger.Debugf("Download for %q completed in %v", key.ref, duration)

	if err == nil {
		st.span.SetStatus(codes.Ok, "")
	} else {
		st.span.SetStatus(codes.Error, err.Error())
	}
	st.span.End()
	st.cancel()
}
