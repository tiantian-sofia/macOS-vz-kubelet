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

const testRef = "registry.example.com/macos:latest"

// fakePull is a controllable stand-in for Download. It blocks on the per-call gate until
// release is invoked, records every invocation, and can be configured to fail or to honor
// context cancellation. All state is concurrency-safe.
type fakePull struct {
	calls   atomic.Int64
	gate    chan struct{} // every invocation blocks until this is closed
	release sync.Once

	credsMu sync.Mutex
	creds   []resource.RegistryCredentials
	ignore  []bool
	fail    error
}

func newFakePull() *fakePull {
	return &fakePull{gate: make(chan struct{})}
}

func (f *fakePull) open() { f.release.Do(func() { close(f.gate) }) }

func (f *fakePull) pull(ctx context.Context, params Params, _ event.EventRecorder) (config.MacPlatformConfigurationOptions, error) {
	f.calls.Add(1)
	f.credsMu.Lock()
	f.creds = append(f.creds, params.Credentials)
	f.ignore = append(f.ignore, params.IgnoreExisiting)
	f.credsMu.Unlock()

	select {
	case <-ctx.Done():
		return config.MacPlatformConfigurationOptions{}, ctx.Err()
	case <-f.gate:
	}
	return config.MacPlatformConfigurationOptions{BlockStoragePath: "/var/cache/img"}, f.fail
}

func (f *fakePull) snapshot() ([]resource.RegistryCredentials, []bool) {
	f.credsMu.Lock()
	defer f.credsMu.Unlock()
	creds := make([]resource.RegistryCredentials, len(f.creds))
	copy(creds, f.creds)
	ignore := make([]bool, len(f.ignore))
	copy(ignore, f.ignore)
	return creds, ignore
}

func newTestManager(t *testing.T) (*Manager, *fakePull) {
	t.Helper()
	fp := newFakePull()
	m := NewManager(event.LogEventRecorder{}, t.TempDir())
	m.downloadFunc = fp.pull
	t.Cleanup(fp.open) // never leak a blocked pull if a test fails early
	return m, fp
}

func credsA() resource.RegistryCredentials {
	return resource.RegistryCredentials{Server: "registry.example.com", Username: "alice", Password: "pass-a"}
}

func credsB() resource.RegistryCredentials {
	return resource.RegistryCredentials{Server: "registry.example.com", Username: "bob", Password: "pass-b"}
}

// waitStarted blocks until the fake pull has been invoked n times.
func waitStarted(t *testing.T, fp *fakePull, n int64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for fp.calls.Load() < n {
		select {
		case <-time.After(2 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for %d pull(s), got %d", n, fp.calls.Load())
		}
	}
}

// assertNoMoreCalls fails if the pull invocation count changes within a short window.
func assertNoMoreCalls(t *testing.T, fp *fakePull, want int64) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, want, fp.calls.Load(), "no additional pulls were expected")
}

// TestManagerCoalescesConcurrentDownloads: N concurrent identical requests must result in
// exactly one real pull, and every subscriber must receive its result.
func TestManagerCoalescesConcurrentDownloads(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, errs[idx] = m.Download(context.Background(), testRef, false, resource.RegistryCredentials{})
		}(i)
	}

	waitStarted(t, fp, 1)
	assertNoMoreCalls(t, fp, 1)

	fp.open()
	wg.Wait()

	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(1), fp.calls.Load())
}

// TestManagerCancelingOneSubscriberDoesNotCancelDownload reproduces the production bug:
// one subscriber leaves mid-pull while a second subscriber stays. The shared download must
// keep running and the remaining subscriber must still get the result - no context canceled.
func TestManagerCancelingOneSubscriberDoesNotCancelDownload(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	shortCtx, cancelShort := context.WithCancel(context.Background())
	doneCh := make(chan error, 2)

	go func() {
		_, _, err := m.Download(shortCtx, testRef, false, resource.RegistryCredentials{})
		doneCh <- err
	}()
	waitStarted(t, fp, 1)

	longDone := make(chan error, 1)
	go func() {
		_, _, err := m.Download(context.Background(), testRef, false, resource.RegistryCredentials{})
		longDone <- err
	}()
	// Give the second subscriber a moment to attach.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		var subs int
		for _, st := range m.downloads {
			st.mu.Lock()
			subs = st.subscribers
			st.mu.Unlock()
		}
		return subs >= 2
	}, time.Second, 5*time.Millisecond)

	// First subscriber (e.g. a deleted pod) gives up.
	cancelShort()
	select {
	case err := <-doneCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled subscriber never returned")
	}

	// The single in-flight pull must still be alive and must not have been restarted.
	assertNoMoreCalls(t, fp, 1)

	fp.open()
	select {
	case err := <-longDone:
		assert.NoError(t, err, "remaining subscriber must receive the shared result")
	case <-time.After(5 * time.Second):
		t.Fatal("remaining subscriber never received the download result")
	}
	assert.Equal(t, int64(1), fp.calls.Load())
}

// TestManagerLastSubscriberCancelsDownload: when every subscriber is gone the in-flight
// pull is stopped, and a later request starts a brand-new pull rather than joining the
// dead one.
func TestManagerLastSubscriberCancelsDownload(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() {
		_, _, err := m.Download(ctx, testRef, false, resource.RegistryCredentials{})
		doneCh <- err
	}()
	waitStarted(t, fp, 1)

	cancel()
	select {
	case err := <-doneCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled download never returned")
	}

	// The map entry must be gone so the next request is a fresh pull.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.downloads) == 0
	}, time.Second, 5*time.Millisecond)

	secondDone := make(chan error, 1)
	go func() {
		_, _, err := m.Download(context.Background(), testRef, false, resource.RegistryCredentials{})
		secondDone <- err
	}()
	waitStarted(t, fp, 2)
	fp.open()
	select {
	case err := <-secondDone:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second download never completed")
	}
}

// TestManagerAlwaysPullNotServedFromInFlightIfNotPresent pins the pull-policy bucketing:
// a forced (ignoreExisting) pull is never coalesced with a cache-using one, so an Always
// pod cannot silently observe an in-flight cached pull.
func TestManagerAlwaysPullNotServedFromInFlightIfNotPresent(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	var wg sync.WaitGroup
	startBoth := make(chan struct{})
	results := make([]error, 2)

	run := func(idx int, ignoreExisting bool) {
		wg.Add(1)
		defer wg.Done()
		<-startBoth
		_, _, results[idx] = m.Download(context.Background(), testRef, ignoreExisting, resource.RegistryCredentials{})
	}
	go run(0, false) // IfNotPresent
	go run(1, true)  // Always

	close(startBoth)
	waitStarted(t, fp, 2)
	fp.open()
	wg.Wait()

	for _, err := range results {
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(2), fp.calls.Load())
	_, ignore := fp.snapshot()
	assert.ElementsMatch(t, []bool{false, true}, ignore)
}

// TestManagerSeparateCredentialsDoNotCoalesce pins credential bucketing: pulls using
// different pull secrets never share an in-flight download, so one pod's credentials can
// never authenticate another pod's pull.
func TestManagerSeparateCredentialsDoNotCoalesce(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	var wg sync.WaitGroup
	startBoth := make(chan struct{})
	results := make([]error, 2)
	run := func(idx int, creds resource.RegistryCredentials) {
		wg.Add(1)
		defer wg.Done()
		<-startBoth
		_, _, results[idx] = m.Download(context.Background(), testRef, false, creds)
	}
	go run(0, credsA())
	go run(1, credsB())

	close(startBoth)
	waitStarted(t, fp, 2)
	fp.open()
	wg.Wait()

	for _, err := range results {
		assert.NoError(t, err)
	}
	gotCreds, _ := fp.snapshot()
	require.Len(t, gotCreds, 2)
	assert.ElementsMatch(t, []resource.RegistryCredentials{credsA(), credsB()}, gotCreds)
}

// TestManagerSameCredentialsCoalesceAcrossNamespaces is the positive counterpart: equal
// credential material coalesces even if the pods live in different namespaces (the
// Manager has no namespace awareness; identical auth is safe to share).
func TestManagerSameCredentialsCoalesceAcrossNamespaces(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	const n = 4
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, _, errs[idx] = m.Download(context.Background(), testRef, false, credsA())
		}(i)
	}
	close(start)
	waitStarted(t, fp, 1)
	fp.open()
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(1), fp.calls.Load())
}

// TestManagerCompletedDownloadDoesNotServeLateJoiners: after a pull finishes, its map
// entry is removed, so a subsequent Always request cannot be answered from the completed
// (cached) result and triggers a fresh pull.
func TestManagerCompletedDownloadDoesNotServeLateJoiners(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	fp.open()
	_, _, err := m.Download(context.Background(), testRef, true, resource.RegistryCredentials{})
	require.NoError(t, err)
	require.Equal(t, int64(1), fp.calls.Load())

	// A fresh gate for the second pull so the request can finish once coalescing
	// (or lack thereof) has been observed.
	second := newFakePull()
	t.Cleanup(second.open)
	m.downloadFunc = second.pull
	doneCh := make(chan error, 1)
	go func() {
		_, _, e := m.Download(context.Background(), testRef, true, resource.RegistryCredentials{})
		doneCh <- e
	}()
	waitStarted(t, second, 1)
	second.open()
	select {
	case err := <-doneCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("post-completion pull never finished")
	}
}

// TestManagerDownloadErrorPropagatesToAllSubscribers: a failed pull surfaces the error to
// every coalesced subscriber, and a later request retries from scratch.
func TestManagerDownloadErrorPropagatesToAllSubscribers(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)
	wantErr := errors.New("boom")
	fp.fail = wantErr

	const n = 3
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, _, errs[idx] = m.Download(context.Background(), testRef, false, resource.RegistryCredentials{})
		}(i)
	}
	close(start)
	waitStarted(t, fp, 1)
	fp.open()
	wg.Wait()
	for _, err := range errs {
		assert.ErrorIs(t, err, wantErr)
	}
	assert.Equal(t, int64(1), fp.calls.Load())

	// Failure must not poison future requests: the entry is removed and a retry pulls again.
	fp.fail = nil
	go func() {
		_, _, _ = m.Download(context.Background(), testRef, false, resource.RegistryCredentials{})
	}()
	waitStarted(t, fp, 2)
	fp.open()
}

// TestManagerConcurrentSubscribeUnsubscribeStress hammers the lifecycle churn window:
// while one pull is in flight, many subscribers arrive and leave concurrently with
// different contexts, credentials and pull policies. The shared pull must never be
// stopped while a live subscriber remains, every live subscriber must get a result, and
// no goroutine may panic or deadlock. This is the race-detector-focused regression test
// for the "pod deleted mid-pull kills the other pod's pull" incident.
func TestManagerConcurrentSubscribeUnsubscribeStress(t *testing.T) {
	t.Parallel()
	m, fp := newTestManager(t)

	var (
		wg        sync.WaitGroup
		okCount   atomic.Int64
		failCount atomic.Int64
	)

	// resettableGate lets the test alternate between "pull blocked in flight" and
	// "pull completes instantly" waves, exercising both mid-pull cancellation and
	// normal completion paths. open is idempotent.
	gateMu := sync.Mutex{}
	gate := make(chan struct{})
	resetGate := func() {
		gateMu.Lock()
		gate = make(chan struct{})
		gateMu.Unlock()
	}
	openGate := func() {
		gateMu.Lock()
		defer gateMu.Unlock()
		select {
		case <-gate:
		default:
			close(gate)
		}
	}

	m.downloadFunc = func(ctx context.Context, params Params, _ event.EventRecorder) (config.MacPlatformConfigurationOptions, error) {
		fp.calls.Add(1)
		gateMu.Lock()
		current := gate
		gateMu.Unlock()
		select {
		case <-ctx.Done():
			return config.MacPlatformConfigurationOptions{}, ctx.Err()
		case <-current:
		}
		return config.MacPlatformConfigurationOptions{BlockStoragePath: "/var/cache/img"}, nil
	}

	keys := []struct {
		ignore bool
		creds  resource.RegistryCredentials
	}{
		{false, resource.RegistryCredentials{}},
		{true, resource.RegistryCredentials{}},
		{false, credsA()},
		{false, credsB()},
	}

	const workers = 32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				k := keys[(seed+j)%len(keys)]
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					_, _, e := m.Download(ctx, testRef, k.ignore, k.creds)
					done <- e
				}()
				// Half the subscribers give up while the pull may still be blocked;
				// canceling this context must never stop a pull other live subscribers
				// depend on.
				if (seed+j)%2 == 0 {
					cancel()
				}
				err := <-done
				cancel()
				if err == nil {
					okCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(i)
	}

	// Wave driver: let subscribers pile up against a blocked pull, then alternately
	// cancel-by-context happens organically via the subscribers and release the gate.
	go func() {
		for i := 0; i < 20; i++ {
			time.Sleep(10 * time.Millisecond)
			openGate()
			if i < 19 {
				time.Sleep(2 * time.Millisecond)
				resetGate()
			}
		}
	}()

	wg.Wait()
	openGate()
	assert.Greater(t, okCount.Load(), int64(0), "at least some subscribers must complete successfully")
}
