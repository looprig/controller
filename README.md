# looprig/controller

`controller` is an optional, separate product repository for a future Looprig
controller. It is not required by the embeddable Factory: applications may
continue to embed and compose Factory directly.

## Status

This repository is a foundation scaffold only. It has no operational adapter,
command, cluster client, or success-shaped stub. There are no fake operational
claims in this module.

The eventual D2 surface is intentionally constrained:

- authentication is trusted and injected by the product boundary; this module
  does not create an authentication system;
- direct Pods are eventual work, not an implemented runtime path;
- permissions are namespace-only;
- useful operation requires product Host bootstrap;
- the future integration depends on Factory's public exports, which remain
  unreleased and pending, so this module has no Factory dependency pin.

No Kubernetes API calls, container changes, or cluster changes belong in this
foundation scaffold.
