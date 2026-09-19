// Package driver is the controller's bounded, durable work loop.
//
// # Why it exists
//
// Factory v0.2.0 composes a placement reconciler but nothing in Factory calls
// it, and the reconciler is internal. An adapter with nobody driving it places
// nothing; a Pod that happens to be Ready places nothing either. This package
// is the explicit driver: on every pass it takes a BOUNDED set of session keys
// from a WorkSource and, for each one, decides from DURABLE records alone
// whether to ensure that session's dedicated workload.
//
// # Where authority comes from
//
//   - Desire is the SessionStore catalog record Factory authored. The driver
//     never writes desired state and never invents an intent: the intent it
//     hands the adapter is the record's own PlacementIntent projection, read
//     in the same pass as the record's State.
//   - "Somebody already serves it" is the epoch-fenced Host registry. A live
//     route means a Host holds the session's lease, and reconciliation is for
//     sessions NO Host owns, so the driver does nothing. Pod readiness is not
//     consulted here at all.
//   - Duplicate work between replicas (and Factory) is suppressed by the
//     durable, expiring reconciliation claim. The claim licenses nothing -- it
//     is not a fence -- which is why the adapter's own adoption checks and
//     deterministic naming still hold on their own.
//
// # Drain before delete
//
// A workload the desire no longer names -- deletion desire (a dedicated
// placement naming no workload) or a newer generation -- is drained over the
// controller's own HostLink client, fenced, deleted by UID and recorded, in
// that order, before anything new is created; teardown.go states the order and
// the termination-kind table. Deletion is never how a drain starts.
package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/controller/workload"
)

var (
	// ErrInvalidConfig reports a configuration New or NewFixedSource refuses.
	ErrInvalidConfig = errors.New("driver: invalid configuration")
	// ErrTooMuchWork reports a WorkSource that returned more keys than one
	// pass may take. The pass does nothing rather than truncating silently.
	ErrTooMuchWork = errors.New("driver: work source exceeded the per-pass bound")
	// ErrUnknownState reports a session state this driver does not know how
	// to act on. Core says an unfamiliar state is one a consumer cannot
	// actively control, so no workload is ensured for it.
	ErrUnknownState = errors.New("driver: session is in a state this driver does not recognise")
)

// MaxKeysPerPassCeiling bounds MaxKeysPerPass.
const MaxKeysPerPassCeiling = 256

// Key names one session.
type Key struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// WorkSource names the sessions one pass considers. It must return at most
// the configured bound; the durable records behind each key, not the source,
// decide what is done.
type WorkSource interface {
	Keys(ctx context.Context) ([]Key, error)
}

// FixedSource is an operator-configured, immutable set of session keys.
//
// It is the work source D2.1 shipped because SessionStore (v0.10.0, and still
// v0.11.0) publishes no
// cross-tenant enumeration of sessions desiring dedicated placement: the only
// released catalog listing is per tenant. Each key's authority is still
// durable -- every pass re-reads the catalog, the registry and the claim.
type FixedSource struct{ keys []Key }

// NewFixedSource validates keys: at least one, at most limit, each a valid
// Core identity, no duplicates.
func NewFixedSource(keys []Key, limit int) (*FixedSource, error) {
	if len(keys) == 0 || limit < 1 || len(keys) > limit {
		return nil, fmt.Errorf("%w: a fixed source names 1..%d sessions", ErrInvalidConfig, limit)
	}
	seen := make(map[Key]bool, len(keys))
	for _, k := range keys {
		if k.TenantID.Validate() != nil || k.SessionID.Validate() != nil {
			return nil, fmt.Errorf("%w: a fixed source key is not a valid tenant/session identity", ErrInvalidConfig)
		}
		if seen[k] {
			return nil, fmt.Errorf("%w: a fixed source names one session twice", ErrInvalidConfig)
		}
		seen[k] = true
	}
	return &FixedSource{keys: append([]Key(nil), keys...)}, nil
}

// Keys returns a copy of the configured keys.
func (s *FixedSource) Keys(context.Context) ([]Key, error) {
	return append([]Key(nil), s.keys...), nil
}

// Catalog reads the durable desired state.
type Catalog interface {
	GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
}

// Registry reads the epoch-fenced Host registry and writes the controller's
// one registry write: the ClearHostRegistration fence before a delete.
type Registry interface {
	GetHostRegistration(ctx context.Context, req sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error)
	ClearHostRegistration(ctx context.Context, req sessionstore.ClearHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error)
}

// Terminations records how a generation's workload ended.
type Terminations interface {
	RecordPlacementTermination(ctx context.Context, req sessionstore.RecordPlacementTerminationRequest) (sessionstore.PlacementTerminationEntry, bool, error)
}

// Drainer is the controller's own HostLink drain client (package hostlink).
type Drainer interface {
	StartDrain(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error)
	DrainStatus(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error)
}

// Claims is SessionStore's reconciliation claim.
type Claims interface {
	AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
	ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// Workloads is the platform surface the driver acts through.
//
// It is NOT factory.WorkloadController: the driver never calls that seam's
// RequestDrain, ObserveWorkload or DeleteWorkload, and the binary does not
// link Factory to name it. EnsureWorkload creates; everything else serves the
// drain-before-delete state machine, which names workloads the platform
// REPORTS (ListWorkloads) rather than ones it re-derives from an intent --
// an older generation's workload may have been created under a runtime
// compatibility the current intent no longer names.
type Workloads interface {
	EnsureWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error
	ListWorkloads(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) ([]workload.Workload, error)
	MarkDrain(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload, d workload.Drain) (workload.Workload, error)
	MarkDecision(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload, d workload.Decision) (workload.Workload, error)
	ClearMarks(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload) (workload.Workload, error)
	Terminate(ctx context.Context, w workload.Workload) error
	Release(ctx context.Context, w workload.Workload) error
}

// Clock is the time seam.
type Clock interface{ Now() time.Time }

// Config is one driver's configuration. Every member but Logger and OnPass
// is required.
type Config struct {
	Source       WorkSource
	Catalog      Catalog
	Registry     Registry
	Claims       Claims
	Workloads    Workloads
	Terminations Terminations
	Drainer      Drainer
	Clock        Clock

	// HolderID names this replica in claims. Stable per process.
	HolderID string
	// ClaimTTL is how long one item's claim suppresses other reconcilers.
	ClaimTTL time.Duration
	// ItemTimeout bounds one item's work. It may not exceed ClaimTTL, so the
	// work cannot outlive the claim it runs under.
	ItemTimeout time.Duration
	// MaxKeysPerPass bounds one pass.
	MaxKeysPerPass int
	// Interval is the time between passes.
	Interval time.Duration
	// DrainTimeout is how long after a drain begins the controller waits for
	// the Host to report it complete before forcing the workload's end. It
	// must be at least the Host's own drain bound (the kubernetes adapter's
	// DrainCeiling), or the controller would force a drain the Host is still
	// entitled to be running.
	DrainTimeout time.Duration

	// Logger receives per-item failures. Nil discards.
	Logger *slog.Logger
	// OnPass, when set, is called after every pass Run makes.
	OnPass func(PassReport, error)
}

// Outcome is what one item's reconciliation did.
type Outcome string

const (
	// OutcomeEnsured: the adapter's EnsureWorkload returned nil under this
	// replica's claim. It says a workload exists or was created -- NOT that a
	// Host serves the session.
	OutcomeEnsured Outcome = "ensured"
	// OutcomeNotDedicated: the record does not desire dedicated placement.
	OutcomeNotDedicated Outcome = "not_dedicated"
	// OutcomeEnded: the session is stopped; no workload is ensured for it.
	OutcomeEnded Outcome = "ended"
	// OutcomeOwned: a live registry route exists; a Host holds the session.
	OutcomeOwned Outcome = "owned"
	// OutcomeDeferred: another reconciler holds the claim.
	OutcomeDeferred Outcome = "deferred"
	// OutcomeFailed: see ItemResult.Err.
	OutcomeFailed Outcome = "failed"
	// OutcomeTearingDown: a workload the desire no longer names still exists;
	// this pass advanced its drain-before-delete sequence (or waited on it),
	// and nothing was created.
	OutcomeTearingDown Outcome = "tearing_down"
	// OutcomeDeleted: the desire names no workload and none remains.
	OutcomeDeleted Outcome = "deleted"
)

// ItemResult is one key's result.
type ItemResult struct {
	Key        Key
	Outcome    Outcome
	Generation uint64
	Err        error
}

// PassReport is one pass's results, in source order.
type PassReport struct {
	Items []ItemResult
}

// Driver runs passes.
type Driver struct{ cfg Config }

// FieldError names the Config member a check refused. It unwraps to
// ErrInvalidConfig.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string {
	return ErrInvalidConfig.Error() + ": " + e.Field + " " + e.Reason
}

// Unwrap classifies the error as ErrInvalidConfig.
func (e *FieldError) Unwrap() error { return ErrInvalidConfig }

// CheckConfig runs every check on cfg's VALUES and none on its seams. It is
// what New runs, exported so a caller can refuse a bad value before opening
// anything the seams need.
func CheckConfig(cfg Config) error {
	switch {
	case cfg.HolderID == "" || len(cfg.HolderID) > sessionwire.MaxIDBytes || !utf8.ValidString(cfg.HolderID):
		return &FieldError{Field: "HolderID", Reason: fmt.Sprintf("must be 1..%d bytes of UTF-8", sessionwire.MaxIDBytes)}
	case cfg.ClaimTTL <= 0 || cfg.ClaimTTL > sessionstore.MaxReconciliationClaimTTL:
		return &FieldError{Field: "ClaimTTL", Reason: fmt.Sprintf("must be in (0, %v]", sessionstore.MaxReconciliationClaimTTL)}
	case cfg.ItemTimeout <= 0 || cfg.ItemTimeout > cfg.ClaimTTL:
		return &FieldError{Field: "ItemTimeout", Reason: "must be in (0, ClaimTTL]"}
	case cfg.MaxKeysPerPass < 1 || cfg.MaxKeysPerPass > MaxKeysPerPassCeiling:
		return &FieldError{Field: "MaxKeysPerPass", Reason: fmt.Sprintf("must be 1..%d", MaxKeysPerPassCeiling)}
	case cfg.Interval <= 0:
		return &FieldError{Field: "Interval", Reason: "must be positive"}
	case cfg.DrainTimeout <= 0:
		return &FieldError{Field: "DrainTimeout", Reason: "must be positive"}
	}
	return nil
}

// New validates cfg.
func New(cfg Config) (*Driver, error) {
	if cfg.Source == nil || cfg.Catalog == nil || cfg.Registry == nil || cfg.Claims == nil || cfg.Workloads == nil ||
		cfg.Terminations == nil || cfg.Drainer == nil || cfg.Clock == nil {
		return nil, fmt.Errorf("%w: a required seam is nil", ErrInvalidConfig)
	}
	if err := CheckConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Driver{cfg: cfg}, nil
}

// Run passes every Interval until ctx ends, and returns ctx's error.
func (d *Driver) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		report, err := d.Pass(ctx)
		if err != nil {
			d.cfg.Logger.Error("controller pass failed", "err", err)
		}
		for _, item := range report.Items {
			if item.Err != nil {
				// Keys are the operator's own configuration; errors are the
				// redacted typed errors of the store and adapter.
				d.cfg.Logger.Warn("controller item failed", "tenant", item.Key.TenantID, "session", item.Key.SessionID, "err", item.Err)
			}
		}
		if d.cfg.OnPass != nil {
			d.cfg.OnPass(report, err)
		}
		timer.Reset(d.cfg.Interval)
	}
}

// Pass reconciles each key the source names, once. One item's failure does
// not stop the others.
func (d *Driver) Pass(ctx context.Context) (PassReport, error) {
	keys, err := d.cfg.Source.Keys(ctx)
	if err != nil {
		return PassReport{}, fmt.Errorf("driver: read work source: %w", err)
	}
	if len(keys) > d.cfg.MaxKeysPerPass {
		return PassReport{}, ErrTooMuchWork
	}
	report := PassReport{Items: make([]ItemResult, 0, len(keys))}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		itemCtx, cancel := context.WithTimeout(ctx, d.cfg.ItemTimeout)
		report.Items = append(report.Items, d.item(itemCtx, key))
		cancel()
	}
	return report, nil
}

func (d *Driver) item(ctx context.Context, key Key) ItemResult {
	// The first look is before the claim, so a pooled, stopped or owned
	// session costs no claim write.
	view, result, done := d.look(ctx, key)
	if done {
		return result
	}
	if view.owned() {
		result.Outcome = OutcomeOwned
		return result
	}

	_, err := d.cfg.Claims.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
		TenantID: key.TenantID, SessionID: key.SessionID,
		HolderID: d.cfg.HolderID, ExpiresAt: d.cfg.Clock.Now().Add(d.cfg.ClaimTTL),
	})
	if err != nil {
		var reconcileErr *sessionstore.ReconcileError
		if errors.As(err, &reconcileErr) && reconcileErr.Code == sessionstore.ReconcileErrorHeld {
			result.Outcome = OutcomeDeferred
			return result
		}
		result.Outcome, result.Err = OutcomeFailed, fmt.Errorf("driver: acquire reconciliation claim: %w", err)
		return result
	}
	// The claim is released when the item ends -- EXCEPT after an Ensure that
	// failed (quality review P2). A failed Ensure's outcome is unknown: a
	// create sent before a timeout or cancellation may still commit on the
	// API server. Releasing then would let another reconciler take the claim,
	// read a newer desire, and drain or create against a workload set that is
	// about to change under it. So a failed Ensure keeps the claim and lets
	// it lapse at its TTL, which bounds how long a late create can land
	// unseen. A definite refusal costs the same delay, which is the price of
	// not classifying every adapter error as definite or not. Release stays
	// best effort otherwise, like Factory's own reconciler: a claim left to
	// lapse costs a delayed takeover, and a release error must not replace
	// the item's real outcome.
	release := true
	defer func() {
		if !release {
			return
		}
		_, _ = d.cfg.Claims.ReleaseReconciliationClaim(context.WithoutCancel(ctx), sessionstore.ReleaseReconciliationClaimRequest{
			TenantID: key.TenantID, SessionID: key.SessionID, HolderID: d.cfg.HolderID,
		})
	}()

	// Look AGAIN under the claim. Desire read before the claim may already be
	// stale: a desired-generation write landing between that read and the
	// claim would otherwise act on an obsolete generation (quality review
	// P1). This narrows the window to the claim-held interval; it does not
	// close it, because SessionStore does not check the claim on desired-state
	// writes.
	view, result, done = d.look(ctx, key)
	if done {
		return result
	}
	if view.owned() {
		result.Outcome = OutcomeOwned
		return result
	}

	// Every workload the platform holds for the session, whatever generation
	// it was created for. A workload the desire no longer names is torn down
	// -- drain, fence, delete, record -- BEFORE anything is created, lowest
	// generation first, so terminations are recorded in generation order and
	// a replacement never runs beside the Host it replaces.
	workloads, err := d.cfg.Workloads.ListWorkloads(ctx, key.TenantID, key.SessionID)
	if err != nil {
		result.Outcome, result.Err = OutcomeFailed, fmt.Errorf("driver: list workloads: %w", err)
		return result
	}
	var ending []workload.Workload
	for _, w := range workloads {
		switch {
		case w.Generation > view.record.DesiredGeneration:
			// The durable desire never moves backwards; a workload for a
			// generation it has not issued is not one this pass can judge.
			result.Outcome, result.Err = OutcomeFailed, fmt.Errorf("%w: generation %d above desired %d", ErrNewerWorkload, w.Generation, view.record.DesiredGeneration)
			return result
		case w.Generation < view.record.DesiredGeneration:
			ending = append(ending, w)
		case w.Terminal || w.Terminating || w.Decision != nil:
			// The desired generation's own workload has ended (G10: a crashed
			// Host leaves a terminal Pod under RestartPolicy Never) or is
			// already being ended; it is torn down and then recreated.
			ending = append(ending, w)
		}
	}
	if len(ending) > 0 {
		for _, w := range ending {
			finished, err := d.teardown(ctx, key, view, w)
			if err != nil {
				result.Outcome, result.Err = OutcomeFailed, err
				return result
			}
			if !finished {
				break
			}
		}
		// Even a workload whose teardown finished may still exist, held by
		// the kubelet while its container stops; nothing is created until a
		// later pass lists none.
		result.Outcome = OutcomeTearingDown
		return result
	}
	if view.record.DesiredWorkload.PayloadVersion == "" && len(view.record.DesiredWorkload.Payload) == 0 {
		// A dedicated placement naming no workload is deletion desire, and
		// nothing is left.
		result.Outcome = OutcomeDeleted
		return result
	}
	intent, err := view.record.PlacementIntent()
	if err != nil {
		result.Outcome, result.Err = OutcomeFailed, fmt.Errorf("driver: project placement intent: %w", err)
		return result
	}
	if err := d.cfg.Workloads.EnsureWorkload(ctx, intent); err != nil {
		release = false
		result.Outcome, result.Err = OutcomeFailed, err
		return result
	}
	result.Outcome = OutcomeEnsured
	return result
}

// ErrNewerWorkload reports a workload for a generation above the desire.
var ErrNewerWorkload = errors.New("driver: a workload exists for a generation the desire has not issued")

// view is one look at a session's durable state.
type view struct {
	record sessionstore.CatalogRecord
	// live is the session's registration if it is a live route at the
	// driver's clock, and nil if it is absent, released or expired.
	live *sessionstore.HostRegistration
}

// owned reports a live route held by a Host this driver must stand back for:
// any live route EXCEPT a dedicated one for a generation older than the
// desire, whose Host is exactly what the teardown state machine drains.
func (v view) owned() bool {
	if v.live == nil {
		return false
	}
	route := v.live.Route
	return route.Placement != sessionwire.HostPlacementDedicated || route.HostGeneration >= v.record.DesiredGeneration
}

// look reads the durable record and the registry and decides whether the
// session is one this driver acts on. done reports that result is final.
func (d *Driver) look(ctx context.Context, key Key) (view, ItemResult, bool) {
	result := ItemResult{Key: key}
	fail := func(err error) (view, ItemResult, bool) {
		result.Outcome, result.Err = OutcomeFailed, err
		return view{}, result, true
	}
	entry, err := d.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: key.TenantID, SessionID: key.SessionID})
	if err != nil {
		return fail(fmt.Errorf("driver: read catalog: %w", err))
	}
	record := entry.Record
	result.Generation = record.DesiredGeneration
	if record.DesiredPlacement != sessionwire.HostPlacementDedicated {
		result.Outcome = OutcomeNotDedicated
		return view{}, result, true
	}
	switch record.State {
	case sessionwire.SessionStateStopped:
		result.Outcome = OutcomeEnded
		return view{}, result, true
	case sessionwire.SessionStateRunning, sessionwire.SessionStateWaitingOnGate, sessionwire.SessionStateSuspended,
		sessionwire.SessionStateRestoring, sessionwire.SessionStateIdle, sessionwire.SessionStateFailed,
		sessionwire.SessionStateInterrupted:
		// A failed or interrupted session is still one a Host may restore, so
		// its workload is ensured; only stopped ends it.
	default:
		return fail(ErrUnknownState)
	}
	live, err := d.liveRegistration(ctx, key)
	if err != nil {
		return fail(err)
	}
	return view{record: record, live: live}, result, false
}

// liveRegistration returns the session's registration if it is a live route,
// nil if there is none to route to.
func (d *Driver) liveRegistration(ctx context.Context, key Key) (*sessionstore.HostRegistration, error) {
	entry, err := d.cfg.Registry.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: key.TenantID, SessionID: key.SessionID})
	if err != nil {
		var registryErr *sessionstore.RegistryError
		if errors.As(err, &registryErr) {
			switch registryErr.Code {
			case sessionstore.RegistryErrorNotFound, sessionstore.RegistryErrorExpired, sessionstore.RegistryErrorReleased:
				return nil, nil
			}
		}
		return nil, fmt.Errorf("driver: read host registration: %w", err)
	}
	if entry.Registration.Route == nil || !d.cfg.Clock.Now().Before(entry.Registration.ExpiresAt) {
		return nil, nil
	}
	registration := entry.Registration
	return &registration, nil
}
