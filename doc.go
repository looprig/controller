// Package controller is reserved for Looprig's optional controller product.
//
// This repository is currently a foundation scaffold; it has no operational
// adapter yet. The eventual controller remains separate from the embeddable
// Factory, trusts authentication injected by its product boundary, and limits
// permissions to namespaces. Direct Pods support is eventual and requires
// product Host bootstrap. Factory's public exports remain unreleased and are
// intentionally not pinned here.
package controller
