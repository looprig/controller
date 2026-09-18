package kubernetes

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// Platform identity.
//
// EVERY PLATFORM NAME AND EVERY IDENTIFYING LABEL VALUE IS A DIGEST. Tenant,
// session, agent and runtime identifiers are Core strings whose alphabet is not
// Kubernetes' and whose content is tenant data; a Pod name or label is visible
// to anyone who can list Pods in the namespace. Only the digest reaches those
// fields. The Host itself is told its fixed SessionID in its environment,
// because that is configuration it cannot run without -- but never its tenant
// (owner decision H8: a Host has no fixed TenantID), and never in a name.
//
// These derivations are the ADAPTER'S OWN identity, domain-separated from any
// other derivation in the workspace. Factory derives a similar name internally
// for its own correlation; that one is unexported and is deliberately not
// reproduced here, so neither module can silently start depending on the other's
// framing.

const (
	// LabelManagedBy marks every Pod this adapter creates.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// ManagedByValue is LabelManagedBy's value.
	ManagedByValue = "looprig-controller"
	// LabelOwner is the digest of the configured ControllerID, so two
	// controller deployments sharing a namespace cannot adopt each other's Pods.
	LabelOwner = "controller.looprig.dev/owner"
	// LabelSession is the digest of (tenant, session). It is generation
	// independent: it is how another generation's workload for the same
	// session is found.
	LabelSession = "controller.looprig.dev/session"
	// LabelWorkload repeats the Pod name, so a selector can name one workload.
	LabelWorkload = "controller.looprig.dev/workload"
	// LabelGeneration is the desired generation the Pod was created for. A
	// decimal integer is not tenant data.
	LabelGeneration = "controller.looprig.dev/generation"

	// AnnotationGeneration is the minimum diagnostic identity: which desired
	// revision this Pod realises.
	AnnotationGeneration = "controller.looprig.dev/generation"
	// AnnotationPayloadVersion names the payload schema the Pod was rendered from.
	AnnotationPayloadVersion = "controller.looprig.dev/payload-version"
	// AnnotationSpecHash is the SHA-256 of the adapter's rendered Pod spec. An
	// existing Pod whose hash differs is refused rather than adopted.
	AnnotationSpecHash = "controller.looprig.dev/spec-sha256"

	namePrefix = "lrh-"
	// digestBytes keeps 224 bits of SHA-256: 56 hex characters, which with the
	// prefix is a 60-byte DNS label (limit 63) and a 56-byte label value
	// (limit 63).
	digestBytes = 28

	domainWorkload = "looprig.controller/kubernetes/workload/v1"
	domainSession  = "looprig.controller/kubernetes/session/v1"
	domainOwner    = "looprig.controller/kubernetes/owner/v1"
)

// WorkloadName is the deterministic Pod name for one desired revision of one
// session: a digest over tenant, session, agent, runtime compatibility and
// generation, length-prefixed so no field boundary can be shifted to produce an
// alternate preimage. A restart derives the same name; a new generation derives
// a different one.
func WorkloadName(intent sessionstore.PlacementIntent) string {
	d := sha256.New()
	writeField(d, []byte(domainWorkload))
	writeField(d, []byte(intent.TenantID))
	writeField(d, []byte(intent.SessionID))
	writeField(d, []byte(intent.AgentID))
	writeField(d, []byte(intent.RuntimeCompatibilityID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], intent.Generation)
	writeField(d, generation[:])
	return namePrefix + hex.EncodeToString(d.Sum(nil)[:digestBytes])
}

// HostID is the Host identity the adapter configures into the Pod. It is the
// workload name, so a registry route naming it can only have been published by
// a process started from this adapter's Pod for this exact desired revision.
func HostID(intent sessionstore.PlacementIntent) sessionwire.HostID {
	return sessionwire.HostID(WorkloadName(intent))
}

// SessionHash is the generation-independent digest of one session.
func SessionHash(tenant sessionwire.TenantID, session sessionwire.SessionID) string {
	d := sha256.New()
	writeField(d, []byte(domainSession))
	writeField(d, []byte(tenant))
	writeField(d, []byte(session))
	return hex.EncodeToString(d.Sum(nil)[:digestBytes])
}

func ownerHash(controllerID string) string {
	d := sha256.New()
	writeField(d, []byte(domainOwner))
	writeField(d, []byte(controllerID))
	return hex.EncodeToString(d.Sum(nil)[:digestBytes])
}

func writeField(d hash.Hash, field []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(field)))
	_, _ = d.Write(length[:])
	_, _ = d.Write(field)
}
