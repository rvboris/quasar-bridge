package bridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// upstreamServer accepts one or more websocket sessions and records every
// binary message. closeNow kills the current connection abruptly (RST-like).
type fakeUpstream struct {
	server   *httptest.Server
	mu       sync.Mutex
	conns    []*websocket.Conn
	messages [][]byte
	accepts  int
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	f := &fakeUpstream{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.accepts++
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		defer c.CloseNow()
		for {
			_, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			f.mu.Lock()
			f.messages = append(f.messages, data)
			f.mu.Unlock()
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeUpstream) killFirst(t *testing.T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		c.CloseNow()
		t.Log("killed upstream connection")
		return
	}
}

func (f *fakeUpstream) accepted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.accepts
}

func (f *fakeUpstream) messagesFrom(n int) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]byte
	if n < len(f.messages) {
		out = append(out, f.messages[n:]...)
	}
	return out
}

// TestProxyRedialReplaysState: the upstream dies mid-session; the proxy
// must re-dial and forward subsequent station frames to the new upstream
// without dropping the station's leg.
func TestProxyRedialReplaysState(t *testing.T) {
	up := newFakeUpstream(t)
	proxy := &Proxy{Upstream: "ws" + stringsTrimPrefix(up.server.URL), Redial: true,
		NewSession: func(ctx context.Context, u, d Sender) Session {
			return &redialCounterSession{up: u, ctx: ctx}
		}}
	srv := httptest.NewServer(proxy)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, _, err := websocket.Dial(ctx, "ws"+stringsTrimPrefix(srv.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.CloseNow()

	if err := st.Write(ctx, websocket.MessageBinary, []byte("frame-1")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(up.messagesFrom(0)) >= 1 })

	up.killFirst(t)

	// The station leg must survive; further frames go to the re-dialed
	// upstream, along with the session's replay marker.
	if err := st.Write(ctx, websocket.MessageBinary, []byte("frame-2")); err != nil {
		t.Fatalf("station leg died after upstream kill: %v", err)
	}
	waitFor(t, func() bool { return up.accepted() >= 2 })
	var sawFrame2, sawReplay bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !(sawFrame2 && sawReplay) {
		for _, m := range up.messagesFrom(1) {
			if string(m) == "frame-2" {
				sawFrame2 = true
			}
			if string(m) == "replayed" {
				sawReplay = true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawFrame2 || !sawReplay {
		t.Fatalf("post-redial delivery: frame2=%v replay=%v", sawFrame2, sawReplay)
	}
}

type redialCounterSession struct {
	up      Sender
	ctx     context.Context
	redials int
}

func (s *redialCounterSession) Client(ctx context.Context, k websocket.MessageType, data []byte) error {
	return s.up(ctx, k, data)
}
func (s *redialCounterSession) Server(ctx context.Context, k websocket.MessageType, data []byte) error {
	return nil
}
func (s *redialCounterSession) Close() {}
func (s *redialCounterSession) UpstreamRedialed(ctx context.Context) {
	s.redials++
	// Replay marker frame through the (already swapped) upstream Sender.
	_ = s.up(ctx, websocket.MessageBinary, []byte("replayed"))
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func stringsTrimPrefix(s string) string {
	if len(s) > 4 && s[:4] == "http" {
		return s[4:]
	}
	return s
}
