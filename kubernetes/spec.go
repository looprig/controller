package kubernetes

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/looprig/controller/internal/strictjson"
)

// PayloadVersionV1 is the only DesiredWorkload.PayloadVersion this adapter
// renders. A payload naming anything else is refused as unsupported rather
// than guessed at: the version is what lets a reader say "I do not understand
// this" instead of misreading it.
const PayloadVersionV1 = "looprig.controller/kubernetes-pod/v1"

// MaxPayloadBytes bounds a payload before it is decoded.
const MaxPayloadBytes = 16 << 10

const (
	maxCredentials   = 8
	maxImageBytes    = 512
	maxSettingBytes  = 32
	containerName    = "host"
	workspaceVolume  = "workspace"
	workspaceMount   = "/workspace"
	credentialPrefix = "cred-"
	credentialRoot   = "/var/run/looprig/credentials/"
	hostLinkPath     = "/hostlink/"
	portName         = "hostlink"
	readinessPath    = "/readyz"
)

// PayloadV1 is the constrained, versioned workload payload a deployment's
// dedicated LaunchTemplate carries (Factory stores it opaquely and never parses
// it). It is decoded STRICTLY: a token-level pre-pass (internal/strictjson)
// refuses any member name that is not exactly, case-sensitively, one of the
// names below and any repeated member name at any depth; the typed decode then
// refuses unknown fields and trailing data. There is therefore no field through
// which an inline secret, an arbitrary environment variable, a command, a
// volume or a security override could arrive -- the image reference is the one
// free-form string, and it must be digest-pinned.
//
// Everything that identifies or fences the Host -- HOST_ID, HOST_GENERATION,
// HOST_INTERNAL_ENDPOINT, HOST_PLACEMENT, HOST_CAPACITY, HOST_ISOLATION_CLASS,
// HOST_FIXED_SESSION_ID, HOST_LISTEN_ADDRESS -- is ADAPTER-OWNED and derived
// from the intent; a payload naming any of them is refused.
type PayloadV1 struct {
	// Image must be pinned by digest (…@sha256:<64 hex>). A tag can move under
	// a Pod the adapter has already adopted, and the spec hash could not see it.
	Image string `json:"image"`

	// Resources are applied as both requests and limits.
	Resources Resources `json:"resources"`

	// Workspace is the session workspace: an emptyDir bounded by SizeLimit.
	Workspace Workspace `json:"workspace"`

	// Credentials are REFERENCE NAMES resolved through the controller's
	// configured allowlist to Secrets in the namespace, mounted read-only. The
	// payload cannot name a Secret directly and cannot carry a value.
	Credentials []string `json:"credentials"`

	// HostSettings are the Host command's tuning variables (host v0.2.x
	// cmd/host). Only the listed names are accepted and every value must parse
	// as the listed kind -- a duration or a count -- so no value can carry a
	// token or a payload.
	HostSettings map[string]string `json:"host_settings"`
}

// Resources are Kubernetes quantities, each required and positive.
type Resources struct {
	CPU              string `json:"cpu"`
	Memory           string `json:"memory"`
	EphemeralStorage string `json:"ephemeral_storage"`
}

// Workspace bounds the session workspace volume.
type Workspace struct {
	SizeLimit string `json:"size_limit"`
}

type settingKind int

const (
	kindDuration settingKind = iota + 1
	kindCount
)

// requiredSettings are every variable host v0.2.x cmd/host requires that the
// adapter does not own. A payload missing one is refused here, rather than
// producing a Pod that can only crash.
var requiredSettings = map[string]settingKind{
	"HOST_WARM_TTL":              kindDuration,
	"HOST_REGISTRY_HEARTBEAT":    kindDuration,
	"HOST_REGISTRY_EXPIRY":       kindDuration,
	"HOST_CLAIM_TTL":             kindDuration,
	"HOST_APPLY_DEADLINE":        kindDuration,
	"HOST_RECONCILE_INTERVAL":    kindDuration,
	"HOST_COMMAND_QUEUE_SIZE":    kindCount,
	"HOST_RECONCILE_BATCH":       kindCount,
	"HOST_MAX_BINDINGS_PER_LINK": kindCount,
	"HOST_MAX_BINDINGS":          kindCount,
	"HOST_MAX_TENANT_LINKS":      kindCount,
	"HOST_DRAIN_GRACE":           kindDuration,
	"HOST_DRAIN_IDLE_BOUNDARY":   kindDuration,
	"HOST_DRAIN_PUBLISH_BOUND":   kindDuration,
	"HOST_COMPATIBILITY_TIMEOUT": kindDuration,
	"HOST_WORK_POLL":             kindDuration,
}

// pairedSettings are optional, but only together (cmd/host refuses a ping
// without a pong).
var pairedSettings = [2]string{"HOST_PING_INTERVAL", "HOST_PONG_TIMEOUT"}

// payloadSchema is PayloadV1's exact member names, for the strict pre-pass.
var payloadSchema = strictjson.Object{
	"image":         nil,
	"resources":     strictjson.Object{"cpu": nil, "memory": nil, "ephemeral_storage": nil},
	"workspace":     strictjson.Object{"size_limit": nil},
	"credentials":   strictjson.Array{},
	"host_settings": strictjson.Map{},
}

var imagePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._\-/:]*[a-z0-9])?@sha256:[0-9a-f]{64}$`)

// decodePayload refuses everything it does not positively recognise. Error
// text names the offending member and never echoes a value, so a refused
// payload that did carry a secret does not put it in a log.
//
// The payload VERSION is checked once, by validateIntent, which every caller
// of this function has already run (desired -> identity -> validateIntent).
func decodePayload(workload sessionstore.DesiredWorkload, allowlist map[string]string) (PayloadV1, error) {
	if len(workload.Payload) == 0 || len(workload.Payload) > MaxPayloadBytes {
		return PayloadV1{}, fmt.Errorf("%w: payload size must be 1..%d bytes", ErrInvalidPayload, MaxPayloadBytes)
	}
	if err := strictjson.Check(workload.Payload, payloadSchema); err != nil {
		return PayloadV1{}, fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(workload.Payload))
	decoder.DisallowUnknownFields()
	var payload PayloadV1
	if err := decoder.Decode(&payload); err != nil {
		return PayloadV1{}, fmt.Errorf("%w: payload does not decode as %s", ErrInvalidPayload, PayloadVersionV1)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return PayloadV1{}, fmt.Errorf("%w: payload carries trailing data", ErrInvalidPayload)
	}

	if len(payload.Image) > maxImageBytes || !imagePattern.MatchString(payload.Image) {
		return PayloadV1{}, fmt.Errorf("%w: image must be pinned by sha256 digest", ErrInvalidPayload)
	}
	for member, value := range map[string]string{
		"resources.cpu":               payload.Resources.CPU,
		"resources.memory":            payload.Resources.Memory,
		"resources.ephemeral_storage": payload.Resources.EphemeralStorage,
		"workspace.size_limit":        payload.Workspace.SizeLimit,
	} {
		if _, err := positiveQuantity(value); err != nil {
			return PayloadV1{}, fmt.Errorf("%w: %s must be a positive quantity", ErrInvalidPayload, member)
		}
	}

	if len(payload.Credentials) > maxCredentials {
		return PayloadV1{}, fmt.Errorf("%w: at most %d credential references", ErrInvalidPayload, maxCredentials)
	}
	seen := make(map[string]bool, len(payload.Credentials))
	for _, ref := range payload.Credentials {
		if seen[ref] {
			return PayloadV1{}, fmt.Errorf("%w: duplicate credential reference", ErrInvalidPayload)
		}
		seen[ref] = true
		if _, allowed := allowlist[ref]; !allowed {
			return PayloadV1{}, fmt.Errorf("%w: a credential reference is not in the controller's allowlist", ErrCredentialNotAllowed)
		}
	}

	for name, value := range payload.HostSettings {
		kind, required := requiredSettings[name]
		if !required && (name == pairedSettings[0] || name == pairedSettings[1]) {
			kind = kindDuration
		} else if !required {
			return PayloadV1{}, fmt.Errorf("%w: %w", ErrInvalidPayload, ErrSettingNotAllowed)
		}
		if !validSetting(kind, value) {
			return PayloadV1{}, fmt.Errorf("%w: host_settings %s has an invalid value", ErrInvalidPayload, name)
		}
	}
	for name := range requiredSettings {
		if _, ok := payload.HostSettings[name]; !ok {
			return PayloadV1{}, fmt.Errorf("%w: host_settings is missing %s", ErrInvalidPayload, name)
		}
	}
	_, ping := payload.HostSettings[pairedSettings[0]]
	_, pong := payload.HostSettings[pairedSettings[1]]
	if ping != pong {
		return PayloadV1{}, fmt.Errorf("%w: %s and %s are set together or not at all", ErrInvalidPayload, pairedSettings[0], pairedSettings[1])
	}
	return payload, nil
}

func validSetting(kind settingKind, value string) bool {
	if value == "" || len(value) > maxSettingBytes {
		return false
	}
	switch kind {
	case kindDuration:
		d, err := time.ParseDuration(value)
		return err == nil && d > 0
	case kindCount:
		n, err := strconv.ParseUint(value, 10, 31)
		return err == nil && n > 0
	}
	return false
}

func positiveQuantity(value string) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, err
	}
	if q.Sign() <= 0 {
		return resource.Quantity{}, errors.New("not positive")
	}
	return q, nil
}

// validateIntent refuses an intent this adapter must not act on. It runs before
// any cluster call on every operation.
func validateIntent(intent sessionstore.PlacementIntent) error {
	if intent.Placement != sessionwire.HostPlacementDedicated {
		return fmt.Errorf("%w: placement is not dedicated", ErrInvalidIntent)
	}
	if intent.Generation == 0 {
		return fmt.Errorf("%w: generation zero names no desired revision", ErrInvalidIntent)
	}
	if err := intent.TenantID.Validate(); err != nil {
		return fmt.Errorf("%w: tenant_id", ErrInvalidIntent)
	}
	// The released Host takes the tenant from exactly one path segment and
	// refuses a path with a further segment, so a tenant containing '/' is one
	// no Host this adapter starts could ever be dialled for.
	// "." and ".." are legal Core tenants, but http.ServeMux path-cleans
	// /hostlink/. and /hostlink/.. and answers a redirect, so the Host could
	// never be dialled for them either.
	if tenant := string(intent.TenantID); strings.Contains(tenant, "/") || tenant == "." || tenant == ".." {
		return fmt.Errorf("%w: tenant_id cannot be a HostLink path segment", ErrInvalidIntent)
	}
	if err := intent.SessionID.Validate(); err != nil {
		return fmt.Errorf("%w: session_id", ErrInvalidIntent)
	}
	if err := intent.AgentID.Validate(); err != nil {
		return fmt.Errorf("%w: agent_id", ErrInvalidIntent)
	}
	if intent.RuntimeCompatibilityID == "" {
		return fmt.Errorf("%w: runtime_compatibility_id", ErrInvalidIntent)
	}
	if intent.Workload.PayloadVersion != PayloadVersionV1 {
		return fmt.Errorf("%w: payload version is not %s", ErrUnsupportedPayload, PayloadVersionV1)
	}
	return nil
}

// endpoint is the HostLink address the Host advertises and Factory dials.
//
// The host part is the Pod's stable DNS name under the deployment's headless
// Service (hostname + subdomain). The path is /hostlink/<tenant>: the released
// Host serves HostLink only there and Factory dials the advertised endpoint
// verbatim, so an endpoint without it is one no Factory could connect to. The
// tenant here is ROUTING for the authenticated link -- the Host still accepts a
// link for any tenant path its verifier admits -- and is not a Host-wide tenant
// configuration (owner decision H8).
func (c *Controller) endpoint(name string, tenant sessionwire.TenantID) string {
	return "ws://" + name + "." + c.cfg.HostSubdomain + "." + c.cfg.Namespace + ".svc:" +
		strconv.Itoa(int(c.cfg.HostPort)) + hostLinkPath + url.PathEscape(string(tenant))
}

// identity renders only the name and identity labels an intent's Pod carries.
// It is what DeleteWorkload judges by: deleting an owned Pod must not depend on
// the payload still being renderable under today's configuration.
func (c *Controller) identity(intent sessionstore.PlacementIntent) (*corev1.Pod, error) {
	if err := validateIntent(intent); err != nil {
		return nil, err
	}
	name := WorkloadName(intent)
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: c.cfg.Namespace,
		Labels: map[string]string{
			LabelManagedBy:  ManagedByValue,
			LabelOwner:      c.owner,
			LabelSession:    SessionHash(intent.TenantID, intent.SessionID),
			LabelWorkload:   name,
			LabelGeneration: strconv.FormatUint(intent.Generation, 10),
		},
	}}, nil
}

// desired renders the complete Pod this adapter would create for an intent.
func (c *Controller) desired(intent sessionstore.PlacementIntent) (*corev1.Pod, error) {
	pod, err := c.identity(intent)
	if err != nil {
		return nil, err
	}
	payload, err := decodePayload(intent.Workload, c.cfg.Credentials)
	if err != nil {
		return nil, err
	}
	name := pod.Name
	generation := pod.Labels[LabelGeneration]
	// The Host refuses to START with an endpoint Core rejects (its options
	// validate it), and under RestartPolicy Never that Pod would be Failed for
	// good. A tenant long enough, or escaping to enough bytes, overflows Core's
	// 256-byte limit, so the rendered endpoint is validated here -- before any
	// cluster call -- with Core's own validator rather than a restated rule.
	endpoint := c.endpoint(name, intent.TenantID)
	if err := sessionwire.InternalEndpoint(endpoint).Validate(); err != nil {
		return nil, fmt.Errorf("%w: the HostLink endpoint for this tenant is not a valid Core endpoint", ErrInvalidIntent)
	}
	port := c.cfg.HostPort

	adapterEnv := map[string]string{
		"HOST_ID":                string(HostID(intent)),
		"HOST_GENERATION":        generation,
		"HOST_INTERNAL_ENDPOINT": endpoint,
		"HOST_ISOLATION_CLASS":   string(sessionwire.HostIsolationClassTenantExclusive),
		"HOST_PLACEMENT":         string(sessionwire.HostPlacementDedicated),
		"HOST_CAPACITY":          "1",
		"HOST_FIXED_SESSION_ID":  string(intent.SessionID),
		"HOST_LISTEN_ADDRESS":    ":" + strconv.Itoa(int(port)),
	}
	env := make([]corev1.EnvVar, 0, len(adapterEnv)+len(payload.HostSettings))
	for k, v := range adapterEnv {
		env = append(env, corev1.EnvVar{Name: k, Value: v})
	}
	for k, v := range payload.HostSettings {
		env = append(env, corev1.EnvVar{Name: k, Value: v})
	}
	slices.SortFunc(env, func(a, b corev1.EnvVar) int { return strings.Compare(a.Name, b.Name) })

	resources := corev1.ResourceList{}
	for name, value := range map[corev1.ResourceName]string{
		corev1.ResourceCPU:              payload.Resources.CPU,
		corev1.ResourceMemory:           payload.Resources.Memory,
		corev1.ResourceEphemeralStorage: payload.Resources.EphemeralStorage,
	} {
		q, err := positiveQuantity(value)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidPayload, name)
		}
		resources[name] = q
	}
	workspaceLimit, err := positiveQuantity(payload.Workspace.SizeLimit)
	if err != nil {
		return nil, fmt.Errorf("%w: workspace.size_limit", ErrInvalidPayload)
	}

	volumes := []corev1.Volume{{
		Name:         workspaceVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &workspaceLimit}},
	}}
	mounts := []corev1.VolumeMount{{Name: workspaceVolume, MountPath: workspaceMount}}
	refs := slices.Clone(payload.Credentials)
	slices.Sort(refs)
	for _, ref := range refs {
		volumes = append(volumes, corev1.Volume{
			Name: credentialPrefix + ref,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  c.cfg.Credentials[ref],
				DefaultMode: ptrTo[int32](0o440),
				Optional:    ptrTo(false),
			}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: credentialPrefix + ref, MountPath: credentialRoot + ref, ReadOnly: true})
	}

	spec := corev1.PodSpec{
		Hostname:  name,
		Subdomain: c.cfg.HostSubdomain,
		// A Host process is one incarnation. Never restarting it in place is
		// what makes (HOST_ID, HOST_GENERATION) name at most one running
		// process: a replacement needs a new Pod object, and the API server
		// keeps a name unique until the old object is gone.
		RestartPolicy: corev1.RestartPolicyNever,
		// The Host needs no Kubernetes API access; a mounted ServiceAccount
		// token would be an auth token in the Pod for nothing.
		AutomountServiceAccountToken: ptrTo(false),
		// Service links inject one variable per Service in the namespace --
		// environment this adapter did not choose.
		EnableServiceLinks: ptrTo(false),
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:   ptrTo(true),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Containers: []corev1.Container{{
			Name:            containerName,
			Image:           payload.Image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports:           []corev1.ContainerPort{{Name: portName, ContainerPort: port, Protocol: corev1.ProtocolTCP}},
			Env:             env,
			Resources:       corev1.ResourceRequirements{Requests: resources, Limits: resources.DeepCopy()},
			VolumeMounts:    mounts,
			// The released Host answers /readyz 200 only while ACCEPTING. That
			// makes Ready mean "this process accepts sessions" -- still only a
			// placement candidate, never ownership. No liveness probe: with
			// RestartPolicy Never a liveness kill ends the incarnation, and
			// when a Host may be stopped is D2.2's drain ordering to decide.
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
					Path: readinessPath, Port: intstr.FromString(portName), Scheme: corev1.URISchemeHTTP,
				}},
				PeriodSeconds:    5,
				FailureThreshold: 3,
				SuccessThreshold: 1,
				TimeoutSeconds:   2,
			},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptrTo(false),
				ReadOnlyRootFilesystem:   ptrTo(true),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
		}},
		Volumes: volumes,
	}
	specHash, err := hashSpec(spec)
	if err != nil {
		return nil, err
	}
	pod.Annotations = map[string]string{
		AnnotationGeneration:     generation,
		AnnotationPayloadVersion: PayloadVersionV1,
		AnnotationSpecHash:       specHash,
	}
	pod.Spec = spec
	return pod, nil
}

// hashSpec is the SHA-256 of the adapter's own rendering. It is recorded at
// create and compared on adoption; it is never recomputed from a live Pod,
// whose spec the API server has defaulted.
func hashSpec(spec corev1.PodSpec) (string, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("kubernetes: hash pod spec: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validDNSLabel(value string) bool { return len(validation.IsDNS1123Label(value)) == 0 }

func validDNSSubdomain(value string) bool { return len(validation.IsDNS1123Subdomain(value)) == 0 }

func ptrTo[T any](v T) *T { return &v }
