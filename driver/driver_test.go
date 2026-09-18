package driver

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

var testNow = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// recorder is one fake per seam, all logging into one ordered call list so a
// test can assert the exact sequence a pass performed.
type recorder struct {
	mu    sync.Mutex
	calls []string

	entries      map[Key]sessionstore.CatalogEntry
	catalogErr   error
	registration map[Key]sessionstore.HostRegistrationEntry
	registryErr  error
	acquireErr   error
	ensureErr    error
	ensured      []sessionstore.PlacementIntent
	acquired     []sessionstore.AcquireReconciliationClaimRequest
	released     []sessionstore.ReleaseReconciliationClaimRequest
}

func (r *recorder) log(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recorder) GetCatalogEntry(_ context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	r.log("catalog")
	if r.catalogErr != nil {
		return sessionstore.CatalogEntry{}, r.catalogErr
	}
	entry, ok := r.entries[Key{req.TenantID, req.SessionID}]
	if !ok {
		return sessionstore.CatalogEntry{}, &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound}
	}
	return entry, nil
}

func (r *recorder) GetHostRegistration(_ context.Context, req sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error) {
	r.log("registry")
	if r.registryErr != nil {
		return sessionstore.HostRegistrationEntry{}, r.registryErr
	}
	entry, ok := r.registration[Key{req.TenantID, req.SessionID}]
	if !ok {
		return sessionstore.HostRegistrationEntry{}, &sessionstore.RegistryError{Code: sessionstore.RegistryErrorNotFound}
	}
	return entry, nil
}

func (r *recorder) AcquireReconciliationClaim(_ context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	r.log("acquire")
	r.acquired = append(r.acquired, req)
	return sessionstore.ReconciliationClaimEntry{}, r.acquireErr
}

func (r *recorder) ReleaseReconciliationClaim(_ context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	r.log("release")
	r.released = append(r.released, req)
	return sessionstore.ReconciliationClaimEntry{}, nil
}

func (r *recorder) EnsureWorkload(_ context.Context, intent sessionstore.PlacementIntent) error {
	r.log("ensure")
	r.ensured = append(r.ensured, intent)
	return r.ensureErr
}

func (r *recorder) ObserveWorkload(context.Context, sessionstore.PlacementIntent) (sessionwire.HostLinkRegistryObservation, bool, error) {
	r.log("observe")
	return sessionwire.HostLinkRegistryObservation{}, false, errors.New("the driver must not observe in D2.1")
}

func (r *recorder) RequestDrain(context.Context, sessionstore.PlacementIntent) (sessionwire.HostLinkDrainObservation, error) {
	r.log("drain")
	return sessionwire.HostLinkDrainObservation{}, errors.New("the driver must not drain in D2.1")
}

func (r *recorder) DeleteWorkload(context.Context, sessionstore.PlacementIntent) error {
	r.log("delete")
	return errors.New("the driver must never delete")
}

var key = Key{TenantID: "tenant-acme", SessionID: "session-0001"}

func dedicatedRecord() sessionstore.CatalogRecord {
	return sessionstore.CatalogRecord{
		TenantID:               key.TenantID,
		SessionID:              key.SessionID,
		AgentID:                "agent-coder",
		RuntimeCompatibilityID: "runtime-2026-09",
		CreatedAt:              testNow.Add(-time.Hour),
		LastActiveAt:           testNow.Add(-time.Minute),
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementDedicated,
		DesiredIdempotencyKey:  "placement-key",
		DesiredGeneration:      4,
		DesiredWorkload:        sessionstore.DesiredWorkload{PayloadVersion: "test/v1", Payload: []byte(`{"x":1}`)},
	}
}

func newRecorder(record sessionstore.CatalogRecord) *recorder {
	return &recorder{entries: map[Key]sessionstore.CatalogEntry{key: {Record: record, Revision: 3}}}
}

func testConfig(t *testing.T, r *recorder, keys ...Key) Config {
	t.Helper()
	if len(keys) == 0 {
		keys = []Key{key}
	}
	source, err := NewFixedSource(keys, 4)
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		Source:         source,
		Catalog:        r,
		Registry:       r,
		Claims:         r,
		Workloads:      r,
		Clock:          fixedClock{testNow},
		HolderID:       "controller-replica-a",
		ClaimTTL:       30 * time.Second,
		ItemTimeout:    10 * time.Second,
		MaxKeysPerPass: 4,
		Interval:       time.Second,
	}
}

func pass(t *testing.T, r *recorder, keys ...Key) PassReport {
	t.Helper()
	d, err := New(testConfig(t, r, keys...))
	if err != nil {
		t.Fatal(err)
	}
	report, err := d.Pass(context.Background())
	if err != nil {
		t.Fatalf("Pass: %v", err)
	}
	return report
}

func TestPassEnsuresADedicatedSessionUnderAClaim(t *testing.T) {
	record := dedicatedRecord()
	r := newRecorder(record)

	report := pass(t, r)

	if want := []string{"catalog", "registry", "acquire", "ensure", "release"}; !slices.Equal(r.calls, want) {
		t.Fatalf("calls = %v, want exactly %v", r.calls, want)
	}
	intent, err := record.PlacementIntent()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.ensured) != 1 || !reflect.DeepEqual(r.ensured[0], intent) {
		t.Fatalf("ensured = %+v, want exactly the record's own PlacementIntent %+v", r.ensured, intent)
	}
	wantAcquire := sessionstore.AcquireReconciliationClaimRequest{
		TenantID: key.TenantID, SessionID: key.SessionID, HolderID: "controller-replica-a", ExpiresAt: testNow.Add(30 * time.Second),
	}
	if len(r.acquired) != 1 || r.acquired[0] != wantAcquire {
		t.Fatalf("acquire = %+v, want exactly %+v", r.acquired, wantAcquire)
	}
	wantRelease := sessionstore.ReleaseReconciliationClaimRequest{TenantID: key.TenantID, SessionID: key.SessionID, HolderID: "controller-replica-a"}
	if len(r.released) != 1 || r.released[0] != wantRelease {
		t.Fatalf("release = %+v, want exactly %+v", r.released, wantRelease)
	}
	if want := (PassReport{Items: []ItemResult{{Key: key, Outcome: OutcomeEnsured, Generation: 4}}}); !reflect.DeepEqual(report, want) {
		t.Fatalf("report = %+v, want exactly %+v", report, want)
	}
}

func TestPassOutcomes(t *testing.T) {
	liveRoute := sessionstore.HostRegistrationEntry{Registration: sessionstore.HostRegistration{
		TenantID: key.TenantID, SessionID: key.SessionID, LeaseEpoch: 2,
		ObservedAt: testNow.Add(-time.Second), ExpiresAt: testNow.Add(20 * time.Second),
		Route: &sessionstore.HostRoute{HostID: "host-a", HostGeneration: 1},
	}}
	lapsedRoute := liveRoute
	lapsedRoute.Registration.ExpiresAt = testNow
	cases := []struct {
		name      string
		setup     func(*recorder)
		outcome   Outcome
		calls     []string
		wantErrIs error
	}{
		{name: "pooled", setup: func(r *recorder) {
			e := r.entries[key]
			e.Record.DesiredPlacement = sessionwire.HostPlacementPooled
			e.Record.DesiredWorkload = sessionstore.DesiredWorkload{}
			r.entries[key] = e
		}, outcome: OutcomeNotDedicated, calls: []string{"catalog"}},
		{name: "stopped", setup: func(r *recorder) {
			e := r.entries[key]
			e.Record.State = sessionwire.SessionStateStopped
			r.entries[key] = e
		}, outcome: OutcomeEnded, calls: []string{"catalog"}},
		{name: "unknown state", setup: func(r *recorder) {
			e := r.entries[key]
			e.Record.State = "hibernating"
			r.entries[key] = e
		}, outcome: OutcomeFailed, calls: []string{"catalog"}, wantErrIs: ErrUnknownState},
		{name: "catalog failure", setup: func(r *recorder) {
			r.catalogErr = &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend}
		}, outcome: OutcomeFailed, calls: []string{"catalog"}},
		{name: "live owner", setup: func(r *recorder) {
			r.registration = map[Key]sessionstore.HostRegistrationEntry{key: liveRoute}
		}, outcome: OutcomeOwned, calls: []string{"catalog", "registry"}},
		{name: "route past expiry is no owner", setup: func(r *recorder) {
			r.registration = map[Key]sessionstore.HostRegistrationEntry{key: lapsedRoute}
		}, outcome: OutcomeEnsured, calls: []string{"catalog", "registry", "acquire", "ensure", "release"}},
		{name: "expired registration is no owner", setup: func(r *recorder) {
			r.registryErr = &sessionstore.RegistryError{Code: sessionstore.RegistryErrorExpired}
		}, outcome: OutcomeEnsured, calls: []string{"catalog", "registry", "acquire", "ensure", "release"}},
		{name: "released registration is no owner", setup: func(r *recorder) {
			r.registryErr = &sessionstore.RegistryError{Code: sessionstore.RegistryErrorReleased}
		}, outcome: OutcomeEnsured, calls: []string{"catalog", "registry", "acquire", "ensure", "release"}},
		{name: "registry backend failure", setup: func(r *recorder) {
			r.registryErr = &sessionstore.RegistryError{Code: sessionstore.RegistryErrorBackend}
		}, outcome: OutcomeFailed, calls: []string{"catalog", "registry"}},
		{name: "claim held by another replica", setup: func(r *recorder) {
			r.acquireErr = &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorHeld, ExpiresAt: testNow.Add(time.Minute)}
		}, outcome: OutcomeDeferred, calls: []string{"catalog", "registry", "acquire"}},
		{name: "claim backend failure", setup: func(r *recorder) {
			r.acquireErr = &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorBackend}
		}, outcome: OutcomeFailed, calls: []string{"catalog", "registry", "acquire"}},
		{name: "ensure failure still releases", setup: func(r *recorder) {
			r.ensureErr = errors.New("adapter refused")
		}, outcome: OutcomeFailed, calls: []string{"catalog", "registry", "acquire", "ensure", "release"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRecorder(dedicatedRecord())
			tc.setup(r)
			report := pass(t, r)
			if !slices.Equal(r.calls, tc.calls) {
				t.Fatalf("calls = %v, want exactly %v", r.calls, tc.calls)
			}
			if len(report.Items) != 1 {
				t.Fatalf("items = %d, want 1", len(report.Items))
			}
			item := report.Items[0]
			if item.Outcome != tc.outcome || item.Key != key {
				t.Fatalf("item = %+v, want outcome %q", item, tc.outcome)
			}
			if (item.Err != nil) != (tc.outcome == OutcomeFailed) {
				t.Fatalf("item err = %v for outcome %q", item.Err, item.Outcome)
			}
			if tc.wantErrIs != nil && !errors.Is(item.Err, tc.wantErrIs) {
				t.Fatalf("item err = %v, want %v", item.Err, tc.wantErrIs)
			}
		})
	}
}

func TestPassIsolatesItems(t *testing.T) {
	other := Key{TenantID: "tenant-acme", SessionID: "session-missing"}
	r := newRecorder(dedicatedRecord())
	report := pass(t, r, other, key)
	if len(report.Items) != 2 || report.Items[0].Outcome != OutcomeFailed || report.Items[1].Outcome != OutcomeEnsured {
		t.Fatalf("report = %+v, want [failed, ensured]", report)
	}
}

func TestPassRefusesAnUnboundedSource(t *testing.T) {
	r := newRecorder(dedicatedRecord())
	cfg := testConfig(t, r)
	cfg.Source = sourceFunc(func(context.Context) ([]Key, error) {
		return []Key{key, {TenantID: "t", SessionID: "1"}, {TenantID: "t", SessionID: "2"}, {TenantID: "t", SessionID: "3"}, {TenantID: "t", SessionID: "4"}}, nil
	})
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	report, err := d.Pass(context.Background())
	if !errors.Is(err, ErrTooMuchWork) || len(report.Items) != 0 || len(r.calls) != 0 {
		t.Fatalf("Pass = (%+v, %v) with calls %v, want (empty, ErrTooMuchWork) and no calls", report, err, r.calls)
	}
}

type sourceFunc func(context.Context) ([]Key, error)

func (f sourceFunc) Keys(ctx context.Context) ([]Key, error) { return f(ctx) }

func TestFixedSourceValidation(t *testing.T) {
	for name, keys := range map[string][]Key{
		"empty":         nil,
		"duplicate":     {key, key},
		"empty tenant":  {{SessionID: "s"}},
		"empty session": {{TenantID: "t"}},
		"over bound":    {{"t", "1"}, {"t", "2"}, {"t", "3"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewFixedSource(keys, 2); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("err = %v, want ErrInvalidConfig", err)
			}
		})
	}
	source, err := NewFixedSource([]Key{key}, 2)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	got, err := source.Keys(context.Background())
	if err != nil || !slices.Equal(got, []Key{key}) {
		t.Fatalf("Keys = %v, %v", got, err)
	}
	got[0].SessionID = "mutated"
	again, _ := source.Keys(context.Background())
	if !slices.Equal(again, []Key{key}) {
		t.Fatalf("Keys returned shared storage")
	}
}

func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	r := newRecorder(dedicatedRecord())
	for name, mutate := range map[string]func(*Config){
		"nil source":            func(c *Config) { c.Source = nil },
		"nil catalog":           func(c *Config) { c.Catalog = nil },
		"nil registry":          func(c *Config) { c.Registry = nil },
		"nil claims":            func(c *Config) { c.Claims = nil },
		"nil workloads":         func(c *Config) { c.Workloads = nil },
		"nil clock":             func(c *Config) { c.Clock = nil },
		"empty holder":          func(c *Config) { c.HolderID = "" },
		"zero claim ttl":        func(c *Config) { c.ClaimTTL = 0 },
		"claim ttl over store":  func(c *Config) { c.ClaimTTL = sessionstore.MaxReconciliationClaimTTL + time.Second },
		"zero item timeout":     func(c *Config) { c.ItemTimeout = 0 },
		"item outlives claim":   func(c *Config) { c.ItemTimeout = c.ClaimTTL + time.Second },
		"zero max keys":         func(c *Config) { c.MaxKeysPerPass = 0 },
		"max keys over ceiling": func(c *Config) { c.MaxKeysPerPass = MaxKeysPerPassCeiling + 1 },
		"zero interval":         func(c *Config) { c.Interval = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t, r)
			mutate(&cfg)
			if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("err = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if _, err := New(testConfig(t, r)); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func TestRunPassesUntilCancelled(t *testing.T) {
	r := newRecorder(dedicatedRecord())
	cfg := testConfig(t, r)
	cfg.Interval = time.Millisecond
	passes := make(chan PassReport, 64)
	cfg.OnPass = func(report PassReport, err error) {
		if err == nil {
			select {
			case passes <- report:
			default:
			}
		}
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	for range 2 {
		select {
		case <-passes:
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not pass twice")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
	for _, call := range r.calls {
		if call == "delete" || call == "drain" || call == "observe" {
			t.Fatalf("Run called %s", call)
		}
	}
}
