package downloader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agoda-com/macOS-vz-kubelet/pkg/event"
	"github.com/agoda-com/macOS-vz-kubelet/pkg/resource"
	"github.com/agoda-com/macOS-vz-kubelet/pkg/vm/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// activeCall is one invocation of the injected download function.
type activeCall struct {
	params Params
	ctx    context.Context
}

// pullHarness replaces the real pull: invocations register themselves under a
// condition variable, then block until their context is canceled or the gate is
// opened.
type pullHarness struct {
	gate     chan struct{}
	closeOne sync.Once

	mu    sync.Mutex
	cond  *sync.Cond
	calls []*activeCall

	cfg config.MacPlatformConfigurationOptions
}

func newPullHarness() *pullHarness {
	h := &pullHarness{
		gate: make(chan struct{}),
		cfg: config.MacPlatformConfigurationOptions{
			BlockStoragePath:      "/cache/blk",
			AuxiliaryStoragePath:  "/cache/aux",
			HardwareModelData:     "hw",
			MachineIdentifierData: "mid",
		},
	}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *pullHarness) run(ctx context.Context, params Params, _ event.EventRecorder) (config.MacPlatformConfigurationOptions, error) {
	call := &activeCall{params: params, ctx: ctx}
	h.mu.Lock()
	h.calls = append(h.calls, call)
	h.cond.Broadcast()
	h.mu.Unlock()

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-h.gate:
	}
	if err != nil {
		return config.MacPlatformConfigurationOptions{}, err
	}
	return h.cfg, nil
}

func (h *pullHarness) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

// waitStarted blocks until at least n pulls have started and returns them.
func (h *pullHarness) waitStarted(t *testing.T, n int) []*activeCall {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	h.mu.Lock()
	defer h.mu.Unlock()
	for len(h.calls) < n {
		timer := time.NewTimer(time.Until(deadline))
		go func() {
			select {
			case <-timer.C:
				h.cond.Broadcast()
			}
		}()
		h.cond.Wait()
		timer.Stop()
		if len(h.calls) < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %d pulls to start (got %d)", n, len(h.calls))
			}
		}
	}
	out := make([]*activeCall, n)
	copy(out, h.calls[:n])
	return out
}

// closeGate unblocks all pulls waiting on the gate; safe to call repeatedly.
func (h *pullHarness) closeGate() {
	h.closeOne.Do(func() { close(h.gate) })
}

func newTestManager(t *testing.T) (*Manager, *pullHarness) {
	t.Helper()
	h := newPullHarness()
	mgr := NewManager(event.LogEventRecorder{}, t.TempDir())
	mgr.runDownload = h.run
	t.Cleanup(h.closeGate)
	return mgr, h
}

// TestDownloadCoalescesConcurrentRequests pins the core dedup contract: N concurrent
// requests for the same ref/creds/policy trigger exactly one real pull, and every
// subscriber receives its result.
func TestDownloadCoalescesConcurrentRequests(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)

	const total = 5
	creds := resource.RegistryCredentials{Server: "registry.example", Username: "alice", Password: "secret"}
	ref := "registry.example/image:tag"
	var joined atomic.Int32
	mgr.onSubscribed = func(*state) { joined.Add(1) }

	type result struct {
		cfg config.MacPlatformConfigurationOptions
		err error
	}
	results := make(chan result, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, _, err := mgr.Download(context.Background(), ref, false, creds)
			results <- result{cfg: cfg, err: err}
		}()
	}

	calls := h.waitStarted(t, 1)
	require.Equal(t, creds, calls[0].params.Credentials)
	require.False(t, calls[0].params.IgnoreExisiting)
	require.Eventually(t, func() bool { return joined.Load() == total-1 },
		5*time.Second, 2*time.Millisecond, "all subscribers must join the shared pull")
	require.Equal(t, 1, h.callCount(), "concurrent identical requests must start exactly one pull")

	h.closeGate()
	wg.Wait()
	close(results)

	for res := range results {
		require.NoError(t, res.err)
		require.Equal(t, h.cfg, res.cfg, "every subscriber receives the shared download result")
	}
}

// TestCancelOneSubscriberDoesNotAffectOthers reproduces the production incident: the
// first pod is deleted mid-pull while a second pod is subscribed to the same image.
// The leaving subscriber gets context.Canceled, but the shared pull keeps running and
// the remaining subscriber still gets the result.
func TestCancelOneSubscriberDoesNotAffectOthers(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)
	ref := "registry.example/image:tag"
	var joined atomic.Int32
	mgr.onSubscribed = func(*state) { joined.Add(1) }

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)

	errs := make(chan error, 2)
	go func() {
		_, _, err := mgr.Download(ctx1, ref, false, resource.RegistryCredentials{})
		errs <- err
	}()
	go func() {
		_, _, err := mgr.Download(ctx2, ref, false, resource.RegistryCredentials{})
		errs <- err
	}()

	calls := h.waitStarted(t, 1)
	require.Eventually(t, func() bool { return joined.Load() == 1 },
		5*time.Second, 2*time.Millisecond, "second subscriber must join the shared pull")

	cancel1()
	select {
	case err := <-errs:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("leaving subscriber did not return after cancellation")
	}

	// Give the teardown path a chance to wrongly propagate the cancel.
	time.Sleep(150 * time.Millisecond)
	require.NoError(t, calls[0].ctx.Err(),
		"one subscriber leaving must not cancel the shared in-flight pull")
	require.Equal(t, 1, h.callCount())

	h.closeGate()
	select {
	case err := <-errs:
		require.NoError(t, err, "remaining subscriber must complete the shared pull")
	case <-time.After(5 * time.Second):
		t.Fatal("remaining subscriber did not return after pull completion")
	}
}

// TestLastSubscriberLeavingCancelsPull proves cancellation still works when nobody is
// interested anymore, and that a later request starts a fresh pull rather than
// reusing the retired state.
func TestLastSubscriberLeavingCancelsPull(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)
	ref := "registry.example/image:tag"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := mgr.Download(ctx, ref, false, resource.RegistryCredentials{})
		done <- err
	}()

	calls := h.waitStarted(t, 1)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("last subscriber did not return after cancellation")
	}
	require.Eventually(t, func() bool { return calls[0].ctx.Err() != nil },
		5*time.Second, 2*time.Millisecond,
		"the pull context must be canceled with the last subscriber")
	require.ErrorIs(t, calls[0].ctx.Err(), context.Canceled)

	// Gate opens so the fresh pull completes immediately.
	h.closeGate()

	cfg, _, err := mgr.Download(context.Background(), ref, false, resource.RegistryCredentials{})
	require.NoError(t, err)
	require.Equal(t, h.cfg, cfg)
	require.Equal(t, 2, h.callCount(),
		"a request after the state retired must start a fresh pull, not reuse it")
}

// TestDownloadsSeparateByCredentials pins the cross-namespace isolation fix: the same
// ref pulled with different imagePullSecrets starts separate pulls, so A's registry
// credential can never authenticate B's pull.
func TestDownloadsSeparateByCredentials(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)
	ref := "registry.example/image:tag"

	credsA := resource.RegistryCredentials{Server: "registry.example", Username: "a", Password: "pa"}
	credsB := resource.RegistryCredentials{Server: "registry.example", Username: "b", Password: "pb"}

	errs := make(chan error, 2)
	go func() {
		_, _, err := mgr.Download(context.Background(), ref, false, credsA)
		errs <- err
	}()
	go func() {
		_, _, err := mgr.Download(context.Background(), ref, false, credsB)
		errs <- err
	}()

	calls := h.waitStarted(t, 2)
	seen := map[string]resource.RegistryCredentials{}
	for _, call := range calls {
		seen[call.params.Credentials.Username] = call.params.Credentials
	}
	require.Equal(t, credsA, seen["a"])
	require.Equal(t, credsB, seen["b"])

	h.closeGate()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("download did not return after gate opened")
		}
	}
}

// TestDownloadsSeparateByPullPolicy pins the imagePullPolicy: Always fix: a forced
// pull must never merge with an in-flight cached pull and silently serve the old
// image.
func TestDownloadsSeparateByPullPolicy(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)
	ref := "registry.example/image:tag"

	errs := make(chan error, 2)
	go func() {
		_, _, err := mgr.Download(context.Background(), ref, false, resource.RegistryCredentials{})
		errs <- err
	}()
	go func() {
		_, _, err := mgr.Download(context.Background(), ref, true, resource.RegistryCredentials{})
		errs <- err
	}()

	calls := h.waitStarted(t, 2)
	flags := map[bool]int{}
	for _, call := range calls {
		flags[call.params.IgnoreExisiting]++
	}
	require.Equal(t, 1, flags[false], "one cached pull expected")
	require.Equal(t, 1, flags[true], "one forced (imagePullPolicy: Always) pull expected")

	h.closeGate()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("download did not return after gate opened")
		}
	}
}

// TestAlreadyCanceledContextNeverStartsPull guards against a pre-canceled caller
// racing in as the owner, starting a pull and canceling it on the spot.
func TestAlreadyCanceledContextNeverStartsPull(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := mgr.Download(ctx, "registry.example/image:tag", false, resource.RegistryCredentials{})
	require.ErrorIs(t, err, context.Canceled)

	time.Sleep(100 * time.Millisecond)
	require.Zero(t, h.callCount(), "a pre-canceled caller must not start a pull")
}

// TestDownloadJoinLeaveStress is the regression hammer for the nil-pointer panic
// (span/cancelFunc observed before once.Do initialized them) and the subscriber
// counter teardown race. Run with -race.
func TestDownloadJoinLeaveStress(t *testing.T) {
	t.Parallel()
	mgr, h := newTestManager(t)

	const goroutines = 40
	const iterations = 25
	const credentialBuckets = 4

	var wg sync.WaitGroup
	var panics atomic.Int32
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
					t.Errorf("panic in download path: %v", r)
				}
			}()
			for i := 0; i < iterations; i++ {
				bucket := (g + i) % credentialBuckets
				ctx, cancel := context.WithCancel(context.Background())
				err := make(chan error, 1)
				go func() {
					_, _, e := mgr.Download(ctx, "registry.example/image:tag",
						i%3 == 0,
						resource.RegistryCredentials{
							Username: string(rune('a' + bucket)),
							Password: "p",
						})
					err <- e
				}()
				// Roughly a quarter of subscribers leave early; the rest ride to
				// completion once the gate opens.
				if (g+i)%4 == 0 {
					cancel()
				}
				select {
				case e := <-err:
					if e != nil && !errors.Is(e, context.Canceled) {
						t.Errorf("unexpected download error: %v", e)
					}
				case <-time.After(5 * time.Second):
					t.Errorf("download goroutine never returned")
				}
				cancel()
			}
		}(g)
	}

	// Let join/leave churn accumulate, then release everything still in flight.
	time.AfterFunc(100*time.Millisecond, h.closeGate)
	wg.Wait()

	assert.Zero(t, panics.Load(), "no nil-pointer panics under concurrent join/leave churn")
}
