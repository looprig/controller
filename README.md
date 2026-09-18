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
    only the generation, the payload version and a spec hash.
  - The workload payload is a strictly decoded, versioned document
    (`looprig.controller/kubernetes-pod/v1`): a digest-pinned image, resources,
    a bounded workspace, **credential references** resolved through the
    controller's allowlist to Secret names, and an allowlist of Host tuning
    variables whose values must parse as durations or counts. There is no
    field through which an inline secret, a command payload or a token can
    enter a Pod spec. The ServiceAccount token is not mounted.
  - The Host is configured dedicated, capacity one, with the session's fixed
    SessionID, a HostID/generation derived from the intent, and **no Host-wide
    tenant configuration** (owner decision H8).
  - Adoption is strict: an existing Pod is adopted only if every ownership and
    identity label, the absence of owner references and the recorded spec hash
    match. Anything else fails closed and is never updated or deleted.
  - `EnsureWorkload` never deletes. If another generation's workload exists
    for the session it returns `GenerationConflictError` and creates nothing.
  - `DeleteWorkload` is preconditioned on the observed Pod UID.
  - `RequestDrain` returns `ErrDrainNotImplemented`. It is not faked.
- `driver/`: the bounded, durable work loop. Each pass reads a bounded,
  operator-configured set of session keys; for each, it reads the SessionStore
  catalog record (Factory-authored desire and state), stands back if the
  epoch-fenced Host registry shows a live owner, takes SessionStore's durable
  reconciliation claim, and calls `EnsureWorkload` with the record's own
  `PlacementIntent`. It never drains, observes or deletes.
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

- Drain-before-delete (D2.2): no drain RPC, no deletion ordering, no preStop
  or termination-grace policy, no forced-termination outcome.
- Integration in a real namespace (D3.1).
- A cross-tenant durable work source: SessionStore v0.10.0 publishes no listing
  of sessions desiring dedicated placement, so the work set is the configured
  `CONTROLLER_SESSIONS` list, re-read against durable records every pass.
- A storage backend: none is pinned here.

## Endpoint note

The released Host serves HostLink only at `/hostlink/<tenant>` and Factory dials
the advertised endpoint verbatim, so the adapter advertises
`ws://<pod>.<subdomain>.<namespace>.svc:<port>/hostlink/<tenant>`. The tenant
therefore appears in exactly one Pod field, `HOST_INTERNAL_ENDPOINT`, as
routing for the authenticated link. It is not a Host tenant gate and appears in
no name, label or annotation.

## Configuration (`cmd/controller`)

| Variable | Meaning |
|---|---|
| `CONTROLLER_NAMESPACE` | the one namespace Pods are created in |
| `CONTROLLER_ID` | this controller deployment's identity (owner label digest) |
| `CONTROLLER_REPLICA_ID` | this replica's reconciliation-claim holder |
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
