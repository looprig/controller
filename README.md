# looprig/controller

`controller` is an optional, separate product repository for Looprig's
Kubernetes workload controller. It is not required by the embeddable Factory:
applications may continue to embed and compose Factory directly. Factory does
not depend on this repository, and no Kubernetes client package reaches a
Factory consumer through it.

## Status

Task D2.1 ("reconcile one fixed-session Host workload") is implemented and
tested **against client-go's fake clientset and an in-memory SessionStore
only**. It has **never run against a real cluster**. The disposable-namespace
acceptance (D3.1) has not been granted or run. No version of this module is
tagged.

What exists:

- `kubernetes/`: an implementation of Factory v0.2.0's `WorkloadController`
  over **direct Pods**, one Pod per dedicated session's desired generation.
  - Pod names and identifying labels are SHA-256 digests; annotations carry
    only the generation, the payload version and a spec hash. The derivations
    are pinned by golden values in tests, because existing Pods are found by
    them. **Hashing does not hide identities from Pod readers:** the tenant and
    session IDs appear raw in the Pod's environment (`HOST_INTERNAL_ENDPOINT`,
    `HOST_FIXED_SESSION_ID`), readable by anyone who can read Pods in the
    namespace.
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
    match. Anything else fails closed and is never updated. A Pod failing an
    **ownership or identity** check is also never deleted; an owned Pod whose
    only mismatch is the spec hash **is** deleted by `DeleteWorkload`, which
    judges identity alone so that deletion does not depend on today's payload.
  - The rendered HostLink endpoint is validated with Core's own validator
    before any cluster call, and tenants `.`, `..` and any containing `/` are
    refused, so no Pod is created whose Host could never start or be dialled.
  - `EnsureWorkload` never deletes. If another generation's workload exists
    for the session it returns `GenerationConflictError` and creates nothing.
  - `DeleteWorkload` is preconditioned on the observed Pod UID.
  - `RequestDrain` returns `ErrDrainNotImplemented`. It is not faked.
- `driver/`: the bounded, durable work loop. Each pass reads a bounded,
  operator-configured set of session keys; for each, it reads the SessionStore
  catalog record (Factory-authored desire and state), stands back if the
  epoch-fenced Host registry shows a live owner, takes SessionStore's
  reconciliation claim, **reads the record and registry again under the
  claim**, and calls `EnsureWorkload` with that record's own `PlacementIntent`.
  It never drains, observes or deletes. The claim suppresses duplicate work
  between reconcilers that honour it; it is **not a fence** (SessionStore does
  not check it on desired-state writes), so two Pods for two generations of
  one session remain possible in a race. The session lease keeps that safe;
  clearing the older Pod needs D2.2.
- `cmd/controller`: the executable. It reads its configuration from the
  environment, with **no defaults**, and refuses to start on any missing or
  malformed variable. The binary built from this repository **also refuses to
  start once configured**, because this module composes no storage backend:
  a product supplies a `Bootstrap` (the SessionStore storage composite) and
  calls `Run`, as with Host's generic binary.
- `deploy/`: a namespace-only `ServiceAccount`/`Role`/`RoleBinding`
  (`pods`: `create`, `delete`, `get`, `list` — exactly the calls the adapter
  makes, enforced by test) and the deployment-owned headless Service that gives
  each Host Pod a stable DNS name. Nothing here applies them anywhere.

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

- A crashed Host leaves a `Failed` Pod (`RestartPolicy: Never`); every later
  `EnsureWorkload` answers `ErrWorkloadTerminated`, so that session's
  generation is stranded until D2.2 (drain and delete) or an operator deletes
  the Pod.
- After an `EnsureWorkload` whose outcome is unknown (timeout or cancellation
  after the create was sent), the driver still releases its claim; a late
  server-side create can then coexist with another replica's Pod for a newer
  generation. Owed to D2.2: do not release after an unknown outcome.

- Drain-before-delete (D2.2): no drain RPC, no deletion ordering, no preStop
  or termination-grace policy, no forced-termination outcome. Note for D2.2:
  `deploy/hosts-service.yaml` does not publish not-ready addresses, and a
  draining Host reports not ready, so its DNS record disappears while it
  drains.
- Integration in a real namespace (D3.1).
- A durable work source. `CONTROLLER_SESSIONS` is **operator configuration,
  not a durable listing**: a session Factory creates later is invisible until
  an operator adds it and restarts the controller. Each listed key is re-read
  against durable records every pass. SessionStore v0.10.0 has no cross-tenant
  index of sessions desiring dedicated placement; one is owed before this
  controller can be described as placing arbitrary dedicated sessions.
- A storage backend: none is pinned here.

## Endpoint note

The released Host serves HostLink only at `/hostlink/<tenant>` and Factory dials
the advertised endpoint verbatim, so the adapter advertises
`ws://<pod>.<subdomain>.<namespace>.svc:<port>/hostlink/<tenant>`. The tenant
therefore appears in exactly one Pod field, `HOST_INTERNAL_ENDPOINT`, as
routing for the authenticated link. It is not a Host tenant gate and appears in
no name, label or annotation.

One advertised endpoint reaches exactly one tenant's HostLink path. That is
correct for these dedicated, single-session Hosts, but it is a contract gap for
pooled multi-tenant Hosts, and it belongs to Core, Host and Factory, not to
this adapter.

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

## Verification

```
GOWORK=off GOTOOLCHAIN=go1.26.8 make check
```
