package hostlink

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// The client's framing is pinned against CORE'S OWN FIXTURES, read from the
// Core module this build resolves -- not against copies, and not against
// constants: a fixture built from the value under test pins nothing. Each
// fixture below is a byte-exact statement of what a released Host sends or
// expects.

// coreFixture reads one of Core's sessionwire/v1 fixtures from the module this
// build resolves.
func coreFixture(t *testing.T, name string) []byte {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/looprig/core").Output()
	if err != nil {
		t.Fatalf("locate the core module: %v", err)
	}
	dir := strings.TrimSpace(string(out))
	raw, err := os.ReadFile(filepath.Join(dir, "sessionwire", "v1", "testdata", "fixtures", name))
	if err != nil {
		t.Fatalf("read core fixture %s: %v", name, err)
	}
	return bytes.TrimSpace(raw)
}

// fixtureRequest is the request Core's hostlink_drain_request.json encodes.
func fixtureRequest() sessionwire.HostLinkDrainRequest {
	return sessionwire.HostLinkDrainRequest{
		Version: sessionwire.CurrentWireVersion, HostID: "host-1", HostGeneration: 7,
		IdempotencyKey: "drain-1", TenantID: "tenant-1", SessionID: "session-1",
	}
}

func TestConnectDataIsCoresBareNegotiationRequest(t *testing.T) {
	got, err := ConnectRequest()
	if err != nil {
		t.Fatal(err)
	}
	if want := coreFixture(t, "version_negotiation_request.json"); !bytes.Equal(got, want) {
		t.Fatalf("connect data = %s, want Core's fixture %s", got, want)
	}
}

func TestDrainExchangeMatchesCoreFixturesOnTheWire(t *testing.T) {
	for _, tc := range []struct {
		name   string
		call   func(*Client, context.Context, sessionwire.InternalEndpoint, sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error)
		method string
	}{
		{"drain", (*Client).StartDrain, "hostlink.drain"},
		{"drain_status", (*Client).DrainStatus, "hostlink.drain_status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := newStandIn(t, coreFixture(t, "version_negotiation_response_hostlink_methods.json"),
				coreFixture(t, "hostlink_drain_observation.json"))
			got, err := tc.call(newTestClient(t), context.Background(), host.endpoint("tenant-1"), fixtureRequest())
			if err != nil {
				t.Fatalf("%s: %v", tc.method, err)
			}
			want := sessionwire.HostLinkDrainObservation{
				HostID: "host-1", HostGeneration: 7, DrainGeneration: 9,
				State: sessionwire.HostLinkDrainStateDraining, TenantID: "tenant-1", SessionID: "session-1",
			}
			if got != want {
				t.Fatalf("observation = %+v, want %+v", got, want)
			}
			protocols, paths, tokens, connect, methods, bodies := host.recorded()
			if !slices.Equal(protocols, []string{"centrifuge-json"}) {
				t.Fatalf("upgrade subprotocol headers = %q, want exactly one centrifuge-json", protocols)
			}
			if !slices.Equal(paths, []string{"/hostlink/tenant-1"}) {
				t.Fatalf("upgrade paths = %q, want /hostlink/tenant-1", paths)
			}
			if !slices.Equal(tokens, []string{"controller-service-token"}) {
				t.Fatalf("connect tokens = %q, want the controller's own service token once", tokens)
			}
			if len(connect) != 1 || !bytes.Equal(connect[0], coreFixture(t, "version_negotiation_request.json")) {
				t.Fatalf("connect data = %q, want Core's bare negotiation request", connect)
			}
			// Method names pinned as LITERALS and the body as Core's fixture.
			if !slices.Equal(methods, []string{tc.method}) {
				t.Fatalf("rpc methods = %q, want exactly [%s]", methods, tc.method)
			}
			if !bytes.Equal(bodies[0], coreFixture(t, "hostlink_drain_request.json")) {
				t.Fatalf("rpc body = %s, want Core's fixture %s", bodies[0], coreFixture(t, "hostlink_drain_request.json"))
			}
		})
	}
}

func TestAHostThatDoesNotAdvertiseDrainIsNeverSentOne(t *testing.T) {
	// Core's plain negotiation reply: version 1 and NO hostlink_methods -- a
	// v0.1.0-shaped Host, which would answer hostlink.drain from its channel
	// arm with runtime_unavailable.
	host := newStandIn(t, coreFixture(t, "version_negotiation_response.json"), coreFixture(t, "hostlink_drain_observation.json"))
	for _, call := range []func(*Client, context.Context, sessionwire.InternalEndpoint, sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error){
		(*Client).StartDrain, (*Client).DrainStatus,
	} {
		_, err := call(newTestClient(t), context.Background(), host.endpoint("tenant-1"), fixtureRequest())
		if !errors.Is(err, ErrNotAdvertised) {
			t.Fatalf("err = %v, want ErrNotAdvertised", err)
		}
	}
	if _, _, _, connect, methods, _ := host.recorded(); len(methods) != 0 || len(connect) != 2 {
		t.Fatalf("methods sent = %q over %d connects; want none sent after two negotiations", methods, len(connect))
	}
}

func TestADrainAdvertisedWithoutItsStatusIsGatedPerMethod(t *testing.T) {
	reply := []byte(`{"hostlink_methods":["hostlink.drain"],"version":1}`)
	host := newStandIn(t, reply, coreFixture(t, "hostlink_drain_observation.json"))
	if _, err := newTestClient(t).StartDrain(context.Background(), host.endpoint("tenant-1"), fixtureRequest()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := newTestClient(t).DrainStatus(context.Background(), host.endpoint("tenant-1"), fixtureRequest()); !errors.Is(err, ErrNotAdvertised) {
		t.Fatalf("drain_status err = %v, want ErrNotAdvertised", err)
	}
	if _, _, _, _, methods, _ := host.recorded(); !slices.Equal(methods, []string{"hostlink.drain"}) {
		t.Fatalf("methods = %q", methods)
	}
}

func TestACoreRefusalIsARefusalNotAnObservation(t *testing.T) {
	host := newStandIn(t, coreFixture(t, "version_negotiation_response_hostlink_methods.json"),
		coreFixture(t, "hostlink_error_epoch_mismatch.json"))
	_, err := newTestClient(t).StartDrain(context.Background(), host.endpoint("tenant-1"), fixtureRequest())
	var refusal *RefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v, want a *RefusalError", err)
	}
	if refusal.Method != "hostlink.drain" || refusal.Refusal.Code != sessionwire.HostLinkErrorEpochMismatch ||
		refusal.Refusal.CurrentLeaseEpoch != 4 {
		t.Fatalf("refusal = %+v, want epoch_mismatch with current epoch 4", refusal)
	}
}

func TestRepliesThatAreNotCoreRecordsAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		connect []byte
		reply   []byte
		want    error
	}{
		{"wrapped connect reply (B8)", []byte(`{"version_negotiation":{"version":1}}`), nil, ErrUnsupportedProtocol},
		{"empty connect reply", nil, nil, ErrUnsupportedProtocol},
		{"empty rpc reply", nil, []byte{}, ErrUnreadableReply},
		{"unreadable rpc reply", nil, []byte(`{"state":"drained"}`), ErrUnreadableReply},
		{"observation of another host", nil, []byte(`{"drain_generation":9,"host_generation":7,"host_id":"host-2","session_id":"session-1","state":"drained","tenant_id":"tenant-1"}`), ErrForeignObservation},
		{"observation of another host generation", nil, []byte(`{"drain_generation":9,"host_generation":8,"host_id":"host-1","session_id":"session-1","state":"drained","tenant_id":"tenant-1"}`), ErrForeignObservation},
		{"observation of another session", nil, []byte(`{"drain_generation":9,"host_generation":7,"host_id":"host-1","session_id":"session-2","state":"drained","tenant_id":"tenant-1"}`), ErrForeignObservation},
		{"observation of another tenant", nil, []byte(`{"drain_generation":9,"host_generation":7,"host_id":"host-1","session_id":"session-1","state":"drained","tenant_id":"tenant-2"}`), ErrForeignObservation},
		{"observation of the whole host", nil, []byte(`{"drain_generation":9,"host_generation":7,"host_id":"host-1","state":"drained"}`), ErrForeignObservation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connect := tc.connect
			if connect == nil && tc.want != ErrUnsupportedProtocol {
				connect = coreFixture(t, "version_negotiation_response_hostlink_methods.json")
			}
			host := newStandIn(t, connect, tc.reply)
			_, err := newTestClient(t).StartDrain(context.Background(), host.endpoint("tenant-1"), fixtureRequest())
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAnUpgradeWithoutTheSubprotocolIsWhatTheHostRefuses(t *testing.T) {
	// Control for the header: the stand-in gates exactly as a released Host,
	// so a request without it is answered 400 before any framing runs.
	host := newStandIn(t, coreFixture(t, "version_negotiation_response_hostlink_methods.json"), nil)
	resp, err := http_get(host.server.URL + "/hostlink/tenant-1")
	if err != nil {
		t.Fatal(err)
	}
	if resp != 400 {
		t.Fatalf("status without the subprotocol = %d, want 400", resp)
	}
}

func TestRequestsAreValidatedBeforeAnyDial(t *testing.T) {
	host := newStandIn(t, coreFixture(t, "version_negotiation_response_hostlink_methods.json"), nil)
	whole := fixtureRequest()
	whole.TenantID, whole.SessionID = "", ""
	noKey := fixtureRequest()
	noKey.IdempotencyKey = ""
	for name, tc := range map[string]struct {
		endpoint sessionwire.InternalEndpoint
		req      sessionwire.HostLinkDrainRequest
	}{
		"whole-host scope":   {host.endpoint("tenant-1"), whole},
		"no idempotency key": {host.endpoint("tenant-1"), noKey},
		"invalid endpoint":   {"http://not-a-websocket/hostlink/tenant-1", fixtureRequest()},
	} {
		if _, err := newTestClient(t).StartDrain(context.Background(), tc.endpoint, tc.req); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: err = %v, want ErrInvalidRequest", name, err)
		}
	}
	if protocols, _, _, _, _, _ := host.recorded(); len(protocols) != 0 {
		t.Fatalf("an invalid request reached the Host (%d upgrades)", len(protocols))
	}
}

func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no token":     {Version: "v", DialTimeout: 1},
		"no version":   {Token: fixedToken("x"), DialTimeout: 1},
		"zero timeout": {Token: fixedToken("x"), Version: "v"},
	} {
		if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: err = %v, want ErrInvalidConfig", name, err)
		}
	}
	if _, err := New(Config{Token: fixedToken("x"), Version: "v", DialTimeout: 1}); err != nil {
		t.Fatalf("valid control refused: %v", err)
	}
}

func TestAServerErrorIsAnRPCFailureNotARefusal(t *testing.T) {
	host := newStandIn(t, coreFixture(t, "version_negotiation_response_hostlink_methods.json"), nil)
	host.rpcErr = centrifugeInternal
	_, err := newTestClient(t).StartDrain(context.Background(), host.endpoint("tenant-1"), fixtureRequest())
	var refusal *RefusalError
	if !errors.Is(err, ErrRPCFailed) || errors.As(err, &refusal) {
		t.Fatalf("err = %v, want ErrRPCFailed and no refusal", err)
	}
}

func TestADialThatNeverSettlesFailsWithinItsBound(t *testing.T) {
	// Nothing listens on this endpoint's port once the server is closed.
	host := newStandIn(t, coreFixture(t, "version_negotiation_response_hostlink_methods.json"), nil)
	endpoint := host.endpoint("tenant-1")
	host.server.Close()
	c, err := New(Config{Token: fixedToken("x"), Version: "v", DialTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.StartDrain(context.Background(), endpoint, fixtureRequest())
	if !errors.Is(err, ErrDialFailed) {
		t.Fatalf("err = %v, want ErrDialFailed", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("dial took %v, want it bounded by DialTimeout", elapsed)
	}
}

var centrifugeInternal = centrifuge.ErrorInternal
