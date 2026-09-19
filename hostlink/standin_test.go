package hostlink

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// standIn is a HostLink server built from the REAL Centrifuge server a Host
// runs (centrifuge v0.38.0), gated exactly as a released Host gates: an
// upgrade that does not name the centrifuge-json subprotocol is answered HTTP
// 400 before the handler runs (host v0.2.1 selectsJSONProtocol). Its replies
// are bytes a case chooses -- in the contract test, Core's own fixtures -- so
// what is asserted is the CLIENT's framing, not a restatement of it.
type standIn struct {
	t      *testing.T
	server *httptest.Server
	node   *centrifuge.Node

	mu sync.Mutex
	// connectReply is the Data the connect reply carries.
	connectReply []byte
	// rpcReply answers every RPC; rpcErr, when set, fails it instead.
	rpcReply []byte
	rpcErr   error
	// hold, when set, blocks every connect until it is closed: the upgrade
	// completes and the connect reply never comes.
	hold chan struct{}
	// Recorded.
	protocols   []string
	paths       []string
	tokens      []string
	connectData [][]byte
	methods     []string
	bodies      [][]byte
}

func newStandIn(t *testing.T, connectReply, rpcReply []byte) *standIn {
	t.Helper()
	s := &standIn{t: t, connectReply: connectReply, rpcReply: rpcReply}
	node, err := centrifuge.New(centrifuge.Config{LogLevel: centrifuge.LogLevelNone})
	if err != nil {
		t.Fatal(err)
	}
	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		if s.hold != nil {
			select {
			case <-s.hold:
			case <-ctx.Done():
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.tokens = append(s.tokens, event.Token)
		s.connectData = append(s.connectData, append([]byte(nil), event.Data...))
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "controller"},
			Data:        s.connectReply,
		}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnRPC(func(event centrifuge.RPCEvent, callback centrifuge.RPCCallback) {
			s.mu.Lock()
			s.methods = append(s.methods, event.Method)
			s.bodies = append(s.bodies, append([]byte(nil), event.Data...))
			reply, replyErr := s.rpcReply, s.rpcErr
			s.mu.Unlock()
			if replyErr != nil {
				callback(centrifuge.RPCReply{}, replyErr)
				return
			}
			callback(centrifuge.RPCReply{Data: reply}, nil)
		})
	})
	if err := node.Run(); err != nil {
		t.Fatal(err)
	}
	ws := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{CheckOrigin: func(*http.Request) bool { return true }})
	s.node = node
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.protocols = append(s.protocols, strings.Join(r.Header.Values("Sec-WebSocket-Protocol"), "|"))
		s.paths = append(s.paths, r.URL.EscapedPath())
		s.mu.Unlock()
		if strings.TrimSpace(r.Header.Get("Sec-WebSocket-Protocol")) != "centrifuge-json" {
			http.Error(w, "HostLink requires the JSON protocol", http.StatusBadRequest)
			return
		}
		ws.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		s.server.CloseClientConnections()
		s.server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = node.Shutdown(ctx)
	})
	return s
}

// endpoint is this stand-in's HostLink address for tenant.
func (s *standIn) endpoint(tenant string) sessionwire.InternalEndpoint {
	return sessionwire.InternalEndpoint("ws://" + strings.TrimPrefix(s.server.URL, "http://") + "/hostlink/" + tenant)
}

func (s *standIn) recorded() (protocols, paths, tokens []string, connect [][]byte, methods []string, bodies [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.protocols...), append([]string(nil), s.paths...), append([]string(nil), s.tokens...),
		append([][]byte(nil), s.connectData...), append([]string(nil), s.methods...), append([][]byte(nil), s.bodies...)
}

type fixedToken string

func (f fixedToken) ServiceToken(context.Context) (string, error) { return string(f), nil }

func newTestClient(t *testing.T) *Client {
	t.Helper()
	c, err := New(Config{Token: fixedToken("controller-service-token"), Version: "test", DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// http_get performs a plain GET (no upgrade headers) and returns the status.
func http_get(url string) (int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}
