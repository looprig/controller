// Package controller is the root of Looprig's optional Kubernetes workload
// controller, a repository separate from the embeddable Factory.
//
// The platform adapter is package kubernetes (direct Pods behind Factory's
// WorkloadController seam), the durable work loop is package driver, the
// controller's own HostLink drain client is package hostlink, and the
// executable is cmd/controller. The executable refuses to start without a
// product-supplied storage bootstrap. A workload the desire no longer names is
// drained over HostLink before it is deleted, and how it ended is recorded in
// SessionStore. Real-namespace acceptance (task D3.1) is not implemented; a
// Ready Pod is a placement candidate only, never placement authority. See
// README.md.
package controller
