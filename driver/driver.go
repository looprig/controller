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
// # What it never does
//
// It never deletes, never drains and never observes. Deletion must follow a
// completed Host drain, and that ordering is task D2.2's.
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
// It is the work source D2.1 ships because SessionStore v0.10.0 publishes no
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

// Registry reads the epoch-fenced Host registry.
type Registry interface {
	GetHostRegistration(ctx context.Context, req sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error)
}

// Claims is SessionStore's reconciliation claim.
type Claims interface {
	AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
	ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// Ensurer is the ONE WorkloadController operation the driver may call.
//
// It is deliberately narrower than factory.WorkloadController, which the
// kubernetes adapter implements: a driver holding only EnsureWorkload cannot
// drain, observe or delete by construction rather than by convention, and the
// controller binary does not link Factory's server to name one interface.
type Ensurer interface {
	EnsureWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error
}

// Clock is the time seam.
type Clock interface{ Now() time.Time }

// Config is one driver's configuration. Every member but Logger and OnPass
// is required.
type Config struct {
	Source    WorkSource
	Catalog   Catalog
	Registry  Registry
	Claims    Claims
	Workloads Ensurer
	Clock     Clock

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
	}
	return nil
}

// New validates cfg.
func New(cfg Config) (*Driver, error) {
	if cfg.Source == nil || cfg.Catalog == nil || cfg.Registry == nil || cfg.Claims == nil || cfg.Workloads == nil || cfg.Clock == nil {
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
	record, result, done := d.look(ctx, key)
	if done {
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
	// Best effort, like Factory's own reconciler: a claim left to lapse costs
	// a delayed takeover, and a release error must not replace the item's
	// real outcome.
	//
	// BOOKED FOR D2.2 (quality review P2): this release also runs when
	// EnsureWorkload's outcome is UNKNOWN -- a timeout or cancellation after
	// the create request left. The API server may still commit that create
	// after the claim is released; another reconciler can then take the claim,
	// read a newer generation, find no Pod and create one, leaving two
	// generations' Pods for one session. The lease keeps that safe, and the
	// adapter then refuses with GenerationConflictError, but only D2.2's
	// drain-and-delete clears it. The fix owed there is to NOT release after
	// an unknown-outcome Ensure and let the claim lapse at its TTL instead.
	defer func() {
		_, _ = d.cfg.Claims.ReleaseReconciliationClaim(context.WithoutCancel(ctx), sessionstore.ReleaseReconciliationClaimRequest{
			TenantID: key.TenantID, SessionID: key.SessionID, HolderID: d.cfg.HolderID,
		})
	}()

	// Look AGAIN under the claim. Desire read before the claim may already be
	// stale: a desired-generation write landing between that read and the
	// claim would otherwise create a Pod for an obsolete generation, which
	// then blocks the current one (quality review P1). This narrows the
	// window to the claim-held interval; it does not close it, because
	// SessionStore does not check the claim on desired-state writes.
	record, result, done = d.look(ctx, key)
	if done {
		return result
	}
	intent, err := record.PlacementIntent()
	if err != nil {
		result.Outcome, result.Err = OutcomeFailed, fmt.Errorf("driver: project placement intent: %w", err)
		return result
	}
	if err := d.cfg.Workloads.EnsureWorkload(ctx, intent); err != nil {
		result.Outcome, result.Err = OutcomeFailed, err
		return result
	}
	result.Outcome = OutcomeEnsured
	return result
}

// look reads the durable record and the registry and decides whether the
// session needs its workload ensured. done reports that result is final.
func (d *Driver) look(ctx context.Context, key Key) (sessionstore.CatalogRecord, ItemResult, bool) {
	result := ItemResult{Key: key}
	fail := func(err error) (sessionstore.CatalogRecord, ItemResult, bool) {
		result.Outcome, result.Err = OutcomeFailed, err
		return sessionstore.CatalogRecord{}, result, true
	}
	entry, err := d.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: key.TenantID, SessionID: key.SessionID})
	if err != nil {
		return fail(fmt.Errorf("driver: read catalog: %w", err))
	}
	record := entry.Record
	result.Generation = record.DesiredGeneration
	if record.DesiredPlacement != sessionwire.HostPlacementDedicated {
		result.Outcome = OutcomeNotDedicated
		return record, result, true
	}
	switch record.State {
	case sessionwire.SessionStateStopped:
		result.Outcome = OutcomeEnded
		return record, result, true
	case sessionwire.SessionStateRunning, sessionwire.SessionStateWaitingOnGate, sessionwire.SessionStateSuspended,
		sessionwire.SessionStateRestoring, sessionwire.SessionStateIdle, sessionwire.SessionStateFailed,
		sessionwire.SessionStateInterrupted:
		// A failed or interrupted session is still one a Host may restore, so
		// its workload is ensured; only stopped ends it.
	default:
		return fail(ErrUnknownState)
	}
	owned, err := d.owned(ctx, key)
	if err != nil {
		return fail(err)
	}
	if owned {
		result.Outcome = OutcomeOwned
		return record, result, true
	}
	return record, result, false
}

// owned reports whether a live registry route exists for the session.
func (d *Driver) owned(ctx context.Context, key Key) (bool, error) {
	entry, err := d.cfg.Registry.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: key.TenantID, SessionID: key.SessionID})
	if err != nil {
		var registryErr *sessionstore.RegistryError
		if errors.As(err, &registryErr) {
			switch registryErr.Code {
			case sessionstore.RegistryErrorNotFound, sessionstore.RegistryErrorExpired, sessionstore.RegistryErrorReleased:
				return false, nil
			}
		}
		return false, fmt.Errorf("driver: read host registration: %w", err)
	}
	return entry.Registration.Route != nil && d.cfg.Clock.Now().Before(entry.Registration.ExpiresAt), nil
}
