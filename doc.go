// Package controller is the root of Looprig's optional Kubernetes workload
// controller, a repository separate from the embeddable Factory.
//
// The platform adapter is package kubernetes (direct Pods behind Factory's
// WorkloadController seam), the durable work loop is package driver, and the
// executable is cmd/controller. The executable refuses to start without a
// product-supplied storage bootstrap. Drain-before-delete (task D2.2) and
// real-namespace acceptance (task D3.1) are not implemented; a Ready Pod is a
// placement candidate only, never placement authority. See README.md.
package controller
