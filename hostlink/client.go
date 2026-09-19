// Package hostlink is the controller's own HostLink drain client, built
// strictly from Core's released framing.
//
// # Why the controller owns one
//
// Factory's HostLink client is internal to Factory, and Factory's
// WorkloadController.RequestDrain is a seam a controller IMPLEMENTS and Factory
// never calls, so a drain RPC exported by Factory would invert that seam. The
// controller therefore speaks to a dedicated Host itself. It shares no code
// with Factory's client; what the two share is Core, which owns the connect
// framing (EncodeHostLinkConnectRequest / DecodeHostLinkConnectReply), the
// method names (HostLinkMethodDrain, HostLinkMethodDrainStatus), the drain
// records and the capability signal (VersionNegotiationResponse.Supports).
// B6 and B8 were framing drift between two ends that each restated the
// framing; this package restates none of it, and contract_test.go pins its
// bytes against Core's own fixtures.
//
// # What one exchange is
//
// Each call dials the Host, negotiates, checks the Host ADVERTISES the method,
// sends exactly one RPC, decodes the reply and closes. There is no pooled link
// and no reconnect: a drain is a handful of calls per workload, and a one-shot
// exchange cannot carry a capability set or a transport state from one Host
// incarnation into the next.
//
// # Identity
//
// The Host authenticates a link per connection, for the tenant named in the
// PATH (/hostlink/<tenant>), with the connect token. Nothing distinguishes a
// controller from any other bearer of a token the product's verifier accepts
// for that tenant, so the controller presents its OWN service token (TokenSource)
// -- distinct from Factory's -- which the product's verifier can scope to the
// tenants the controller drains and revoke on its own. This package adds no
// authentication feature; the verifier is the product's.
package hostlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Subprotocol is the WebSocket subprotocol a Host requires on the upgrade.
//
// A released Host answers an upgrade that does not name it with HTTP 400
// before any framing runs (host v0.2.1 selectsJSONProtocol), and
// centrifuge-go's JSON client names no subprotocol on its own. Its absence is
// how every v0.1.x Factory failed to reach a Host (B8).
const Subprotocol = "centrifuge-json"

// ClientName is the connection label presented to a Host. It is a diagnostic
// and carries no authority.
const ClientName = "looprig-controller"

var (
	// ErrInvalidConfig reports a Config New refuses.
	ErrInvalidConfig = errors.New("hostlink: invalid drain client configuration")
	// ErrInvalidRequest reports a drain request or endpoint Core refuses. It is
	// refused before any dial.
	ErrInvalidRequest = errors.New("hostlink: invalid drain request")
	// ErrDialFailed reports a connection that did not settle: a refused or
	// failed upgrade, a handshake timeout, or a terminal close from the Host.
	// Nothing was sent.
	ErrDialFailed = errors.New("hostlink: dial failed")
	// ErrUnsupportedProtocol reports a Host whose connect reply is absent,
	// unreadable, or names another wire version. Nothing was sent.
	ErrUnsupportedProtocol = errors.New("hostlink: host does not speak this wire version")
	// ErrNotAdvertised reports a Host whose connect reply does not list the
	// method. It is decided from Core's capability signal alone, before any
	// RPC: a Host that does not advertise a drain is never sent one.
	ErrNotAdvertised = errors.New("hostlink: host does not advertise the method")
	// ErrRPCFailed reports an RPC the transport or the Host failed without a
	// Core reply (for example a server error). The Host may or may not have
	// acted.
	ErrRPCFailed = errors.New("hostlink: rpc failed")
	// ErrUnreadableReply reports a reply that is neither a Core drain
	// observation nor a Core HostLink refusal.
	ErrUnreadableReply = errors.New("hostlink: reply is neither a drain observation nor a refusal")
	// ErrForeignObservation reports a valid observation that is not about the
	// Host incarnation and scope the request named. It is never reported as a
	// drain state: acting on another Host's drain would delete this one.
	ErrForeignObservation = errors.New("hostlink: drain observation names another host or scope")
)

// RefusalError is a Host's typed Core refusal of a drain RPC.
//
// A refusal is ambiguous by construction: runtime_unavailable, the code a
// released Host answers every drain refusal with, means equally "this Host no
// longer holds the session", "this Host is another version", and "this link's
// tenant is not the holder". A caller must re-observe durable state rather
// than read it as a failure or as a completion.
type RefusalError struct {
	Method  string
	Refusal sessionwire.HostLinkError
}

func (e *RefusalError) Error() string {
	return fmt.Sprintf("hostlink: %s refused: %s", e.Method, e.Refusal.Code)
}

// TokenSource supplies the controller's HostLink service token. It is read on
// every dial, so a rotated or revoked token takes effect at the next exchange.
type TokenSource interface {
	ServiceToken(ctx context.Context) (string, error)
}

// Config is one client's composition.
type Config struct {
	// Token is the controller's own service token. Required.
	Token TokenSource
	// Version is this build's identity reported to a Host. Required.
	Version string
	// DialTimeout bounds the upgrade plus the negotiation. Required, positive.
	DialTimeout time.Duration
	// NetDialContext, when set, replaces the TCP dialer. It is the transport's
	// own seam; the endpoint's host name is still what the upgrade names.
	NetDialContext func(ctx context.Context, network, addr string) (net.Conn, error)
}

// Client performs one-shot drain exchanges.
type Client struct{ cfg Config }

// New validates cfg.
func New(cfg Config) (*Client, error) {
	switch {
	case cfg.Token == nil:
		return nil, fmt.Errorf("%w: Token is nil", ErrInvalidConfig)
	case cfg.Version == "":
		return nil, fmt.Errorf("%w: Version is empty", ErrInvalidConfig)
	case cfg.DialTimeout <= 0:
		return nil, fmt.Errorf("%w: DialTimeout must be positive", ErrInvalidConfig)
	}
	return &Client{cfg: cfg}, nil
}

// StartDrain asks the Host to begin (or re-report) its drain for the request's
// scope and returns the Host's acknowledgement.
//
// An acknowledgement is a statement about INITIATION: the state it carries is
// the Host's state at that instant -- draining while it works, drained once it
// has finished -- and nothing about completion may be inferred from it being
// returned.
func (c *Client) StartDrain(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	return c.exchange(ctx, endpoint, sessionwire.HostLinkMethodDrain, req)
}

// DrainStatus observes the drain the request's scope names and starts
// nothing. A Host that has begun no drain for the scope refuses.
func (c *Client) DrainStatus(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	return c.exchange(ctx, endpoint, sessionwire.HostLinkMethodDrainStatus, req)
}

// ConnectRequest is the connect Data this client sends: Core's bare version
// negotiation request for the one wire version this build speaks.
func ConnectRequest() ([]byte, error) {
	return sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
}

func (c *Client) exchange(ctx context.Context, endpoint sessionwire.InternalEndpoint, method string, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	var none sessionwire.HostLinkDrainObservation
	if err := endpoint.Validate(); err != nil {
		return none, fmt.Errorf("%w: endpoint: %w", ErrInvalidRequest, err)
	}
	// A drain names one dedicated session. The released Host refuses a
	// whole-Host drain over a link (R-1), and a request with no scope would be
	// one this client could never match an observation against.
	if req.TenantID == "" || req.SessionID == "" {
		return none, fmt.Errorf("%w: a drain must name its tenant and session", ErrInvalidRequest)
	}
	// Core's MarshalJSON validates first, so a record the Host's strict
	// decoder would refuse never reaches the wire.
	body, err := json.Marshal(req)
	if err != nil {
		return none, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	connect, err := ConnectRequest()
	if err != nil {
		return none, fmt.Errorf("%w: connect data: %w", ErrInvalidRequest, err)
	}

	conn, negotiated, err := c.dial(ctx, endpoint, connect)
	if err != nil {
		return none, err
	}
	defer conn.Close()

	if !negotiated.Supports(method) {
		return none, fmt.Errorf("%w: %s", ErrNotAdvertised, method)
	}
	reply, err := rpc(ctx, conn, method, body)
	if err != nil {
		return none, fmt.Errorf("%w: %s: %w", ErrRPCFailed, method, err)
	}
	return DecodeDrainReply(method, req, reply)
}

// DecodeDrainReply reads one drain RPC reply.
//
// A released Host answers a drain RPC with a body on BOTH outcomes: the Core
// HostLinkDrainObservation on acknowledgement and the Core HostLinkError on
// refusal. Core's strict decoders keep them apart -- each requires a member
// the other refuses as unknown -- so the observation is tried first and the
// refusal second, and a body that is neither is refused rather than guessed
// at. An observation must then name exactly the Host incarnation and scope the
// request did.
func DecodeDrainReply(method string, req sessionwire.HostLinkDrainRequest, reply []byte) (sessionwire.HostLinkDrainObservation, error) {
	var none sessionwire.HostLinkDrainObservation
	if len(reply) == 0 {
		return none, fmt.Errorf("%w: %s: empty body", ErrUnreadableReply, method)
	}
	var observation sessionwire.HostLinkDrainObservation
	if err := json.Unmarshal(reply, &observation); err == nil {
		if observation.HostID != req.HostID || observation.HostGeneration != req.HostGeneration ||
			observation.TenantID != req.TenantID || observation.SessionID != req.SessionID {
			return none, fmt.Errorf("%w: %s", ErrForeignObservation, method)
		}
		return observation, nil
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(reply, &refusal); err == nil {
		return none, &RefusalError{Method: method, Refusal: refusal}
	}
	return none, fmt.Errorf("%w: %s", ErrUnreadableReply, method)
}

// dial opens one connection and returns once the handshake has SETTLED, with
// the Host's negotiated reply.
func (c *Client) dial(ctx context.Context, endpoint sessionwire.InternalEndpoint, connect []byte) (*centrifugego.Client, sessionwire.VersionNegotiationResponse, error) {
	var none sessionwire.VersionNegotiationResponse
	settled := make(chan error, 1)
	var once sync.Once
	settle := func(err error) { once.Do(func() { settled <- err }) }
	var mu sync.Mutex
	var negotiated sessionwire.VersionNegotiationResponse
	var lastErr error

	client := centrifugego.NewJsonClient(string(endpoint), centrifugego.Config{
		// GetToken is the only credential path and Token stays empty: the
		// client consults GetToken only when Token is empty
		// (centrifuge-go@v0.12.0 client.go:1183).
		GetToken: func(centrifugego.ConnectionTokenEvent) (string, error) {
			return c.cfg.Token.ServiceToken(ctx)
		},
		Data:              connect,
		Header:            http.Header{"Sec-WebSocket-Protocol": {Subprotocol}},
		Name:              ClientName,
		Version:           c.cfg.Version,
		HandshakeTimeout:  c.cfg.DialTimeout,
		NetDialContext:    c.cfg.NetDialContext,
		MinReconnectDelay: c.cfg.DialTimeout,
		MaxReconnectDelay: c.cfg.DialTimeout,
		EnableCompression: false,
		LogLevel:          centrifugego.LogLevelNone,
	})
	client.OnConnected(func(e centrifugego.ConnectedEvent) {
		reply, err := verifyNegotiation(e.Data)
		if err == nil {
			mu.Lock()
			negotiated = reply
			mu.Unlock()
		}
		settle(err)
	})
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) {
		settle(fmt.Errorf("%w: host closed the connection: %d %s", ErrDialFailed, e.Code, e.Reason))
	})
	client.OnError(func(e centrifugego.ErrorEvent) {
		mu.Lock()
		lastErr = e.Error
		mu.Unlock()
	})

	if err := client.Connect(); err != nil {
		client.Close()
		return nil, none, fmt.Errorf("%w: %w", ErrDialFailed, err)
	}
	timer := time.NewTimer(c.cfg.DialTimeout)
	defer timer.Stop()
	var err error
	select {
	case err = <-settled:
	case <-ctx.Done():
		err = fmt.Errorf("%w: %w", ErrDialFailed, ctx.Err())
	case <-timer.C:
		mu.Lock()
		cause := lastErr
		mu.Unlock()
		err = fmt.Errorf("%w: handshake did not settle within %v (last transport error: %v)", ErrDialFailed, c.cfg.DialTimeout, cause)
	}
	if err != nil {
		client.Close()
		return nil, none, err
	}
	mu.Lock()
	defer mu.Unlock()
	return client, negotiated, nil
}

// verifyNegotiation reads the Host's connect reply with Core's decoder. An
// absent reply, a wrapped one, and one naming another version are all refused:
// none of them establishes what a drain record would mean on this link.
func verifyNegotiation(data []byte) (sessionwire.VersionNegotiationResponse, error) {
	if len(data) == 0 {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("%w: host sent no version selection", ErrUnsupportedProtocol)
	}
	reply, err := sessionwire.DecodeHostLinkConnectReply(data)
	if err != nil {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("%w: %v", ErrUnsupportedProtocol, err)
	}
	if reply.Version != sessionwire.CurrentWireVersion {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("%w: host selected %d", ErrUnsupportedProtocol, reply.Version)
	}
	return reply, nil
}

type rpcOutcome struct {
	reply centrifugego.RPCResult
	err   error
}

// rpc runs client.RPC on its own goroutine and waits for its result or ctx.
// centrifuge-go v0.12.0 can run an RPC's completion callback twice during a
// connection teardown, and the second one blocks on RPC's result channel; run
// here, that costs one goroutine rather than the caller.
func rpc(ctx context.Context, client *centrifugego.Client, method string, body []byte) ([]byte, error) {
	outcome := make(chan rpcOutcome, 1)
	go func() {
		reply, err := client.RPC(ctx, method, body)
		outcome <- rpcOutcome{reply: reply, err: err}
	}()
	select {
	case out := <-outcome:
		return out.reply.Data, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
