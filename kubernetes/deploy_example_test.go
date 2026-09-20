package kubernetes

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/scheme"
)

func exampleDeployment(t *testing.T, raw []byte) *appsv1.Deployment {
	t.Helper()
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
	if err != nil {
		t.Fatalf("decode controller Deployment: %v", err)
	}
	deployment, ok := obj.(*appsv1.Deployment)
	if !ok {
		t.Fatalf("example is %T, want Deployment", obj)
	}
	return deployment
}

func checkDedicatedDeployment(d *appsv1.Deployment) string {
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 1 || d.Spec.Template.Spec.ServiceAccountName != "looprig-controller" {
		return "controller replicas or ServiceAccount"
	}
	if len(d.Spec.Template.Spec.Containers) != 1 || d.Spec.Template.Spec.HostNetwork {
		return "controller Pod surface"
	}
	c := d.Spec.Template.Spec.Containers[0]
	if !imagePattern.MatchString(c.Image) || len(c.Resources.Requests) != 3 || len(c.Resources.Limits) != 3 {
		return "controller image or resources"
	}
	for name, request := range c.Resources.Requests {
		limit, ok := c.Resources.Limits[name]
		if !ok || request.Sign() <= 0 || limit.Sign() <= 0 || request.Cmp(limit) > 0 {
			return "controller resource bounds"
		}
	}
	if len(c.Ports) != 0 || len(c.VolumeMounts) != 1 || !c.VolumeMounts[0].ReadOnly ||
		len(d.Spec.Template.Spec.Volumes) != 1 || d.Spec.Template.Spec.Volumes[0].Secret == nil ||
		d.Spec.Template.Spec.Volumes[0].Secret.SecretName != "controller-hostlink-token" {
		return "public port or credential mount"
	}
	// The controller itself needs its namespace-scoped API token; the rendered
	// Host Pod below must not mount one.
	env := map[string]string{}
	allowedEnv := map[string]bool{
		"CONTROLLER_NAMESPACE": true, "CONTROLLER_ID": true, "CONTROLLER_REPLICA_ID": true,
		"CONTROLLER_HOST_SUBDOMAIN": true, "CONTROLLER_HOST_PORT": true,
		"CONTROLLER_CREDENTIALS": true, "CONTROLLER_SESSIONS": true,
		"CONTROLLER_INTERVAL": true, "CONTROLLER_CLAIM_TTL": true,
		"CONTROLLER_ITEM_TIMEOUT": true, "CONTROLLER_DRAIN_CEILING": true,
		"CONTROLLER_COMMIT_MARGIN": true, "CONTROLLER_HOSTLINK_TOKEN_FILE": true,
	}
	for _, e := range c.Env {
		if !allowedEnv[e.Name] || e.ValueFrom != nil && (e.ValueFrom.FieldRef == nil || (e.Name != "CONTROLLER_NAMESPACE" && e.Name != "CONTROLLER_REPLICA_ID")) ||
			strings.Contains(strings.ToLower(e.Name), "secret") || strings.Contains(strings.ToLower(e.Name), "token") && e.Name != "CONTROLLER_HOSTLINK_TOKEN_FILE" {
			return "inline credential environment"
		}
		if _, duplicate := env[e.Name]; duplicate {
			return "duplicate controller environment"
		}
		env[e.Name] = e.Value
	}
	if len(env) != len(allowedEnv) {
		return "missing controller environment"
	}
	if env["CONTROLLER_DRAIN_CEILING"] != "60s" || env["CONTROLLER_COMMIT_MARGIN"] != "10s" ||
		env["CONTROLLER_HOSTLINK_TOKEN_FILE"] != "/var/run/looprig/controller-hostlink/token" ||
		env["CONTROLLER_HOST_SUBDOMAIN"] != "looprig-hosts" || env["CONTROLLER_HOST_PORT"] != "7443" ||
		env["CONTROLLER_CREDENTIALS"] != "session-store=shared-session-store,hostlink-auth=hostlink-service-identity" {
		return "controller configuration"
	}
	if c.Lifecycle != nil {
		return "preStop or other lifecycle hook"
	}
	return ""
}

func checkDedicatedPayload(raw []byte) string {
	workload := sessionstore.DesiredWorkload{PayloadVersion: PayloadVersionV1, Payload: raw}
	allowlist := map[string]string{"session-store": "shared-session-store", "hostlink-auth": "hostlink-service-identity"}
	payload, err := decodePayload(workload, allowlist)
	if err != nil {
		return "strict payload"
	}
	if payload.HostSettings["HOST_COMMAND_QUEUE_SIZE"] != "64" || payload.HostSettings["HOST_DRAIN_GRACE"] != "60s" ||
		!slices.Equal(payload.Credentials, []string{"session-store", "hostlink-auth"}) {
		return "queue, drain, or credential references"
	}
	c := &Controller{cfg: Config{Namespace: "looprig-dedicated", HostSubdomain: "looprig-hosts", HostPort: 7443,
		Credentials: allowlist, DrainCeiling: 60 * time.Second, CommitMargin: 10 * time.Second}}
	intent := sessionstore.PlacementIntent{TenantID: "tenant-acme", SessionID: "session-0001", AgentID: "agent-coder",
		RuntimeCompatibilityID: "runtime-2026-09", Placement: sessionwire.HostPlacementDedicated,
		Workload: workload, Generation: 1}
	pod, err := c.desired(intent)
	if err != nil {
		return "payload render"
	}
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != 70 ||
		pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken ||
		len(pod.Spec.Containers) != 1 || len(pod.Spec.Containers[0].Resources.Limits) != 3 ||
		len(pod.Spec.Containers[0].Resources.Requests) != 3 || pod.Spec.Containers[0].Lifecycle != nil {
		return "rendered Host bounds"
	}
	if len(pod.Spec.Volumes) != 3 || pod.Spec.Volumes[0].EmptyDir == nil || pod.Spec.Volumes[0].EmptyDir.SizeLimit == nil {
		return "rendered workspace or Secret references"
	}
	secretNames := []string{}
	for _, volume := range pod.Spec.Volumes[1:] {
		if volume.Secret == nil || volume.Secret.Optional == nil || *volume.Secret.Optional {
			return "rendered credential Secret"
		}
		secretNames = append(secretNames, volume.Secret.SecretName)
	}
	slices.Sort(secretNames)
	if !slices.Equal(secretNames, []string{"hostlink-service-identity", "shared-session-store"}) {
		return "rendered credential allowlist"
	}
	hostEnv := map[string]string{}
	for _, e := range pod.Spec.Containers[0].Env {
		hostEnv[e.Name] = e.Value
	}
	if hostEnv["HOST_PLACEMENT"] != string(sessionwire.HostPlacementDedicated) ||
		hostEnv["HOST_CAPACITY"] != "1" || hostEnv["HOST_FIXED_SESSION_ID"] != "session-0001" {
		return "rendered fixed-session Host identity"
	}
	return ""
}

func TestDedicatedDeploymentExample(t *testing.T) {
	raw, err := os.ReadFile("../deploy/controller-deployment.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("\n---\n")) || bytes.Contains(raw, []byte("kind: Secret")) || bytes.Contains(raw, []byte("kind: Service")) {
		t.Fatal("example must contain only its controller Deployment")
	}
	d := exampleDeployment(t, raw)
	if problem := checkDedicatedDeployment(d); problem != "" {
		t.Fatal(problem)
	}
	mutations := map[string]func(*appsv1.Deployment){
		"public port": func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 7443}}
		},
		"broad RBAC":     func(d *appsv1.Deployment) { d.Spec.Template.Spec.ServiceAccountName = "default" },
		"missing limits": func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers[0].Resources.Limits = nil },
		"zero resources": func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("0")
		},
		"inline token": func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Env = append(d.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "CONTROLLER_HOSTLINK_TOKEN", Value: "inline"})
		},
		"unsafe timing": func(d *appsv1.Deployment) {
			for i := range d.Spec.Template.Spec.Containers[0].Env {
				if d.Spec.Template.Spec.Containers[0].Env[i].Name == "CONTROLLER_COMMIT_MARGIN" {
					d.Spec.Template.Spec.Containers[0].Env[i].Value = "0s"
				}
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			copy := d.DeepCopy()
			mutate(copy)
			if checkDedicatedDeployment(copy) == "" {
				t.Fatal("unsafe mutation accepted")
			}
		})
	}
}

func TestDedicatedHostServiceStaysInternalDuringDrain(t *testing.T) {
	raw, err := os.ReadFile("../deploy/hosts-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, ok := obj.(*corev1.Service)
	if !ok {
		t.Fatalf("host discovery object = %T, want Service", obj)
	}
	check := func(s *corev1.Service) bool {
		return s.Name == "looprig-hosts" && s.Spec.ClusterIP == corev1.ClusterIPNone &&
			s.Spec.Type == "" && len(s.Spec.ExternalIPs) == 0 && s.Spec.PublishNotReadyAddresses &&
			len(s.Spec.Ports) == 1 && s.Spec.Ports[0].Port == 7443 && s.Spec.Ports[0].NodePort == 0
	}
	if !check(service) {
		t.Fatal("headless HostLink Service became public or dropped not-ready addresses")
	}
	for name, mutate := range map[string]func(*corev1.Service){
		"public NodePort": func(s *corev1.Service) { s.Spec.Type = corev1.ServiceTypeNodePort },
		"external IP":     func(s *corev1.Service) { s.Spec.ExternalIPs = []string{"192.0.2.10"} },
		"lost drain DNS":  func(s *corev1.Service) { s.Spec.PublishNotReadyAddresses = false },
	} {
		t.Run(name, func(t *testing.T) {
			copy := service.DeepCopy()
			mutate(copy)
			if check(copy) {
				t.Fatal("unsafe HostLink Service mutation accepted")
			}
		})
	}
}

func TestDedicatedPayloadExample(t *testing.T) {
	raw, err := os.ReadFile("../deploy/dedicated-host-payload.json")
	if err != nil {
		t.Fatal(err)
	}
	if problem := checkDedicatedPayload(raw); problem != "" {
		t.Fatal(problem)
	}
	mutations := map[string][]byte{
		"inline secret":   bytes.Replace(raw, []byte(`"image":`), []byte(`"token":"inline","image":`), 1),
		"tag image":       bytes.Replace(raw, []byte(`@sha256:0000000000000000000000000000000000000000000000000000000000000000`), []byte(`:latest`), 1),
		"unbounded queue": bytes.Replace(raw, []byte(`"HOST_COMMAND_QUEUE_SIZE": "64"`), []byte(`"HOST_COMMAND_QUEUE_SIZE": "999999999"`), 1),
		"unsafe drain":    bytes.Replace(raw, []byte(`"HOST_DRAIN_GRACE": "60s"`), []byte(`"HOST_DRAIN_GRACE": "71s"`), 1),
	}
	for name, changed := range mutations {
		t.Run(name, func(t *testing.T) {
			if bytes.Equal(raw, changed) {
				t.Fatal("mutation did not change fixture")
			}
			if checkDedicatedPayload(changed) == "" {
				t.Fatal("unsafe mutation accepted")
			}
		})
	}
}
