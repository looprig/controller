# looprig/controller

`controller` is an optional, separate product repository for Looprig's
Kubernetes workload controller. It is not required by the embeddable Factory:
applications may continue to embed and compose Factory directly. Factory does
not depend on this repository, and no Kubernetes client package reaches a
Factory consumer through it.

## Status

Tasks D2.1 ("reconcile one fixed-session Host workload") and D2.2
("enforce drain-before-delete") are implemented and tested **against
client-go's fake clientset (with the API-server behaviours the controller
relies on added in `internal/fakeapi`), an in-memory SessionStore, and — for
the drain client — a released Host v0.2.1 in a private harness**. The
controller has **never run against a real cluster**. The disposable-namespace
acceptance (D3.1) has not been granted or run. No version of this module is
tagged.

What exists:

- `kubernetes/`: an implementation of Factory v0.2.0's `WorkloadController`
  over **direct Pods**, one Pod per dedicated session's desired generation,
  plus the platform half of drain-before-delete (`teardown.go`).
  - Pod names and identifying labels are SHA-256 digests; annotations carry
    only the generation, the payload version and a spec hash — and, during a
    teardown, the controller's own drain and termination marks. The
    derivations are pinned by golden values in tests, because existing Pods are
    found by them. **Hashing does not hide identities from Pod readers:** the
    session ID appears raw in the Pod's environment (`HOST_FIXED_SESSION_ID`),
    readable by anyone who can read Pods in the namespace. The tenant appears
    nowhere in the Pod (see "Endpoint note").
  - The workload payload is a strictly decoded, versioned document
    (`looprig.controller/kubernetes-pod/v1`): a digest-pinned image, resources,
    a bounded workspace, **credential references** resolved through the
    controller's allowlist to Secret names, and an allowlist of Host tuning
    variables whose values must parse as positive durations or counts.
    Decoding refuses unknown, repeated and case-variant member names (a
    token-level pre-pass; `encoding/json` alone matches names
    case-insensitively and lets the last duplicate win). **Except for the image
    reference** — a free-form, digest-pinned string of up to 512 bytes authored
    by the deployment's launch template — there is no field through which an
    inline secret, a command payload or a token can enter a Pod spec. The
    ServiceAccount token is not mounted.
  - The Host is configured dedicated, capacity one, with the session's fixed
    SessionID, a HostID/generation derived from the intent, and **no Host-wide
    tenant configuration** (owner decision H8).
  - Adoption is strict: an existing Pod is adopted only if every ownership and
    identity label, the absence of owner references and the recorded spec hash
    match. Anything else fails closed and is never updated.
  - `HOST_INTERNAL_ENDPOINT` is a **bare base** (`ws://<pod>.<subdomain>.<namespace>.svc:<port>`),
    and the session tenant's address is derived from it with Core's
    `HostLinkEndpoint` before any cluster call; a tenant Core refuses (`.`,
    `..`, any containing `/`, or one whose derived address exceeds Core's
    length limit) fails there, so no Pod is created whose Host could never
    start or be dialled for its session.
  - `EnsureWorkload` never deletes. If another generation's workload exists
    for the session it returns `GenerationConflictError` and creates nothing.
  - Every Pod carries the finalizer `controller.looprig.dev/record-termination` and a
    `terminationGracePeriodSeconds` of `DrainCeiling + CommitMargin` (see
    "Failure backstop" below).
  - Factory's `RequestDrain` seam returns `ErrDrainNotImplemented`: the
    controller drains through its driver, never through that seam.
- `hostlink/`: the controller's **own** HostLink drain client, built strictly
  from Core — Core's connect codecs, the tenant's address derived from the
  Host's base with `HostLinkEndpoint`, `hostlink.drain` / `hostlink.drain_status`,
  the drain records, the `centrifuge-json` WebSocket subprotocol, and the
  capability gate (`Supports`: a Host that does not advertise a method is never
  sent it). Its bytes are pinned against Core's own fixtures
  (`contract_test.go`). It presents the controller's **distinct** service token.
- `driver/`: the bounded, durable work loop. Each pass reads a bounded,
  operator-configured set of session keys; for each, it reads the SessionStore
  catalog record (Factory-authored desire and state), stands back if the
  epoch-fenced Host registry shows a live owner of the desired generation,
  takes SessionStore's reconciliation claim, **reads the record and registry
  again under the claim**, lists the session's workloads, **tears down every
  workload the desire no longer names** (drain before delete, below), and only
  then calls `EnsureWorkload` with the record's own `PlacementIntent` — or,
  for deletion desire, creates nothing. The claim is **not released after an
  `EnsureWorkload` that failed** (its create may still land); it lapses at its
  TTL. The claim is not a fence (SessionStore does not check it on
  desired-state writes); the session lease keeps a race safe.
- `workload/`: the platform-neutral view the driver and the adapter share.
- `cmd/controller`: the executable. It reads its configuration from the
  environment, with **no defaults**, and refuses to start on any missing or
  malformed variable. The binary built from this repository **also refuses to
  start once configured**, because this module composes no storage backend:
  a product supplies a `Bootstrap` (the SessionStore storage composite) and
  calls `Run`, as with Host's generic binary.
- `deploy/`: a namespace-only `ServiceAccount`/`Role`/`RoleBinding`
  (`pods`: `create`, `delete`, `get`, `list`, `update` — exactly the calls the
  adapter makes, enforced by test; `update` is metadata only) and the
  deployment-owned headless Service that gives each Host Pod a stable DNS name.
  The Service **publishes not-ready addresses**, because a draining Host is
  not ready and must stay resolvable for `drain_status`; Factory routes by the
  Host registry, never by DNS. Nothing here applies them anywhere.

## Drain before delete

Deletion desire is a dedicated placement whose desired workload is empty (no
new SessionStore API is needed). A workload the desire no longer names — after
deletion desire, or a newer generation — ends in this order:

1. the drain RPC over the controller's HostLink client, gated on the Host
   advertising `hostlink.drain`, to Core's `HostLinkEndpoint(base, tenant)` for
   the session's tenant, from the base the adapter renders for that Pod (never
   one read from the registry);
2. `drained` observed for **this** workload: the observation names the Pod's
   HostID and host generation, and the lease epoch is the one of the live
   registry route naming the same pair;
3. the termination **Kind decided from the controller's own observations and
   persisted on the Pod**, before anything destructive;
4. the `ClearHostRegistration(epoch)` fence;
5. the Pod deleted, preconditioned on its UID (the finalizer holds the object);
6. `RecordPlacementTermination` under the **workload's** generation (never the
   deletion desire's), then the finalizer released.

| Controller observation | Recorded |
|---|---|
| Pod deleted by someone else (terminating, no decision of ours) | forced `platform_deleted` |
| Pod terminal (Host exited) | forced `workload_terminated` |
| no live route names the Pod and no drain was begun | forced `drain_refused`, epoch 0 |
| Host reported `drained` for this Pod | graceful |
| Host does not advertise the drain | forced `drain_refused` |
| Host still `draining` at the drain timeout | forced `drain_timeout` |
| a refusal or no answer, and the route no longer names the Pod | forced `drain_refused` |
| a refusal or no answer at the drain timeout | forced `drain_refused` |

`runtime_unavailable` is ambiguous and is never a failure: the controller
re-observes the registry. If `hostlink.drain` is refused and the route that named
the Pod has just gone, the Host may have finished in between, so the next pass
asks `drain_status` instead of recording a forced outcome.

**A later owner and an old decision.** Immediately before every delete the
controller reads the registry again. If a live route names this Pod at an epoch
**above** the decision's — its own Host took the session again after the
decision, including after an epoch-0 decision or after the fence — the decision
is dropped, nothing is deleted, and the next pass decides afresh (a fence
refused by a later epoch naming the Pod does the same). **The exact guarantee:**
an old decision never deletes a Pod whose Host the controller has *observed*
holding a later lease at its last registry read before the delete. A
registration landing between that read and the API server applying the delete
is not seen; that Host is then stopped by the Pod deletion's SIGTERM drain
(bounded by `HOST_DRAIN_GRACE`, inside the grace period). The fence cannot close
that window — the registry refuses only *lower* epochs — and neither SessionStore
nor the Kubernetes API offers a fence checked at delete time. Once the
controller's own delete has been sent, a later lease no longer re-opens the
decision: what was decided is recorded.

Terminations are recorded in generation order; a
`superseded` (or, for a recreated incarnation of one generation, `mismatch`)
answer is terminal — logged, and the workload still released. A restarted
controller reads the persisted decision back rather than re-deriving it. A
crashed Host's terminal Pod whose route is gone is deleted without a drain and
its generation recreated (D2.1's G10).

### Failure backstop

Pod deletion is the **failure backstop only**: it never starts a normal drain.
When it reaches a live Host, the kubelet's SIGTERM starts the Host's own drain,
bounded by the Host's `HOST_DRAIN_GRACE`. The controller renders
`terminationGracePeriodSeconds = ceil(DrainCeiling + CommitMargin)`, requires
`CommitMargin >= 5s`, and refuses a payload whose `HOST_DRAIN_GRACE` exceeds
`DrainCeiling`, so the drain ceiling is always strictly shorter than the grace
period. There is no `preStop` hook: the Host drains on SIGTERM, and a hook
would only spend the grace period before that signal. **Host obligation:** a
Host finishes its drain within `HOST_DRAIN_GRACE` and its post-drain commit
within `CommitMargin`; the controller cannot observe Host internals. The
controller waits `DrainCeiling + CommitMargin` for an RPC drain before forcing
it.

The drain RPC's reply is awaited for at most 10s (`hostlink.Config.RPCTimeout`,
also the transport's read timeout); a later reply counts as no answer.

### The termination finalizer: cost and removal

Every Host Pod carries `controller.looprig.dev/record-termination`. It holds the
Pod object after a delete until the controller has recorded how the workload
ended. The controller never strips it on shutdown — a rolling update or crash
is not an uninstall, and stripping it would discard exactly the
crash-between-delete-and-record safety it buys.

- **While the controller is absent, deleted Host Pods stay `Terminating`**
  (their containers still stop), and **deleting the namespace hangs** until the
  finalizer is removed.
- **To remove it** (uninstall, or an abandoned namespace): scale the controller
  to zero first, so it cannot race the removal, then strip the finalizer from
  every Pod it manages:

  ```
  kubectl -n <namespace> scale deployment/<controller> --replicas=0
  kubectl -n <namespace> get pods -l app.kubernetes.io/managed-by=looprig-controller -o name \
    | xargs -I{} kubectl -n <namespace> patch {} --type=json \
        -p '[{"op":"remove","path":"/metadata/finalizers"}]'
  ```

  (The patch removes the whole finalizer list; the controller's Pods carry no
  other finalizer unless one was added by hand.) Terminations in flight lose
  their audit row.
- **A Pod whose finalizer was stripped is invisible to the controller once it
  is gone:** the controller finds workloads by listing Pods, so no termination
  is recorded for that generation, any registry route naming it lapses at its
  expiry instead of being fenced, and the next pass either recreates the
  generation or reports the session deleted. This is an accepted operator
  override.

## What a ready Pod means

**A Ready Pod is only a placement candidate.** Readiness is the Host's
`/readyz` ("accepting"); it says nothing about which process holds the
session's lease. `ObserveWorkload` reports a workload only when the
epoch-fenced Host registry record in SessionStore is live, dedicated, and names
exactly the HostID, host generation, agent and runtime compatibility the
adapter configured. Kubernetes readiness never replaces the session lease or
the registry record, and neither the adapter nor a Ready Pod establishes
placement authority.

## What is not done

- Integration in a real namespace (D3.1).
- A durable work source. `CONTROLLER_SESSIONS` is **operator configuration,
  not a durable listing**: a session Factory creates later is invisible until
  an operator adds it and restarts the controller. Each listed key is re-read
  against durable records every pass. SessionStore (v0.11.0 included) has no
  cross-tenant index of sessions desiring dedicated placement; one is owed
  before this controller can be described as placing arbitrary dedicated
  sessions.
- A stopped session's workload, and a dedicated workload whose session has
  moved to pooled placement, are not torn down: the driver acts only on
  dedicated desire of a live session. An older workload beside a session a
  live Host owns is not listed while that Host owns it.
- A storage backend: none is pinned here.

## Endpoint note

Host v0.3.0 treats `HOST_INTERNAL_ENDPOINT` as a **base**: it refuses a base
carrying a path, advertises the base unchanged, and serves each tenant at Core
v0.10.0's `sessionwire/v1.HostLinkEndpoint(base, tenant)`
(`base + /hostlink/ + PathEscape(tenant)`). The adapter therefore renders
`ws://<pod>.<subdomain>.<namespace>.svc:<port>` with no path, and the
controller's drain client derives the session tenant's address from it before
dialling; a verbatim dial of a base gets 404. The tenant is routing chosen per
link by the dialler, so it appears in no Pod field, name, label or annotation
(H8).

**Compatibility:** a Pod rendered by this controller runs a Host that only a
**deriving** Factory (one on Core ≥ v0.10.0) can reach; a Factory that dials
the advertised endpoint verbatim gets 404. Deploy this controller only with
such a Factory and with host ≥ v0.3.0 images (a v0.2.1 Host configured with a
bare base also serves the derived paths).

## Configuration (`cmd/controller`)

| Variable | Meaning |
|---|---|
| `CONTROLLER_NAMESPACE` | the one namespace Pods are created in |
| `CONTROLLER_ID` | this controller deployment's identity (owner label digest) |
| `CONTROLLER_REPLICA_ID` | this replica's name; the claim holder is this plus a random per-process suffix, so replicas sharing a value still hold distinct claims. Set it per replica (e.g. the Pod name via the downward API) for diagnostics |
| `CONTROLLER_HOST_SUBDOMAIN` | the headless Service name |
| `CONTROLLER_HOST_PORT` | the Host's HostLink port |
| `CONTROLLER_CREDENTIALS` | allowlist, `ref=secret-name,...` |
| `CONTROLLER_SESSIONS` | JSON `[{"tenant_id":…,"session_id":…}]` |
| `CONTROLLER_INTERVAL` | time between passes |
| `CONTROLLER_CLAIM_TTL` | reconciliation claim TTL (≤ 5m) |
| `CONTROLLER_ITEM_TIMEOUT` | per-session bound (≤ claim TTL) |
| `CONTROLLER_DRAIN_CEILING` | the longest Host drain allowed; a payload's `HOST_DRAIN_GRACE` may not exceed it |
| `CONTROLLER_COMMIT_MARGIN` | the Host's post-drain commit budget (≥ 5s); grace period = ceiling + margin |
| `CONTROLLER_HOSTLINK_TOKEN_FILE` | path of the controller's **own** HostLink service token (e.g. a mounted Secret), read on every dial; distinct from Factory's, so a product verifier can scope and revoke it |

## Verification

```
GOWORK=off GOTOOLCHAIN=go1.26.8 make check
```
