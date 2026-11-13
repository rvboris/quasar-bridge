package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"quasarbridge/internal/journal"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type Proxy struct {
	Journal       *journal.Writer
	Subscriptions *SubscriptionMonitor
	Upstream      string
	Observer      func(direction string, kind websocket.MessageType, data []byte)
	NewSession    func(context.Context, Sender, Sender) Session
	// Redial enables transparent upstream re-dial: when the upstream leg dies
	// while the station is still connected, the proxy dials a fresh upstream
	// and lets the session replay its state instead of tearing both legs down
	// (the firmware's "no internet" reconnect storm).
	Redial         bool
	RedialAttempts int
}

type Sender func(context.Context, websocket.MessageType, []byte) error
type Session interface {
	Client(context.Context, websocket.MessageType, []byte) error
	Server(context.Context, websocket.MessageType, []byte) error
	Close()
}

// UpstreamRedialer is an optional Session capability: the proxy calls it on
// every successful transparent re-dial so the session can re-establish its
// upstream state (replay pending requests, re-issue synthesis, ...).
type UpstreamRedialer interface {
	UpstreamRedialed(ctx context.Context)
}

// upstreamPool owns the current upstream connection. The stored Sender
// survives re-dials: while the upstream is down, senders block (bounded by
// their context) until a fresh connection is swapped in, so a momentary
// upstream death never tears down the station's leg.
type upstreamPool struct {
	mu      sync.Mutex
	conn    *websocket.Conn
	aliveCh chan struct{} // closed while conn != nil
}

func newUpstreamPool(c *websocket.Conn) *upstreamPool {
	ch := make(chan struct{})
	close(ch)
	return &upstreamPool{conn: c, aliveCh: ch}
}

// sendWait bounds how long a sender waits for a re-dial before failing.
// It must exceed the proxy's full re-dial backoff budget.
var sendWait = 20 * time.Second

func (u *upstreamPool) send(ctx context.Context, k websocket.MessageType, data []byte) error {
	deadline := time.Now().Add(sendWait)
	for {
		u.mu.Lock()
		c := u.conn
		var alive chan struct{}
		if c == nil {
			alive = u.aliveCh
		}
		u.mu.Unlock()
		if c == nil {
			timer := time.NewTimer(time.Until(deadline))
			select {
			case <-alive:
				timer.Stop()
				continue
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				return errors.New("upstream disconnected")
			}
		}
		err := c.Write(ctx, k, data)
		if err == nil {
			return nil
		}
		// The write raced a re-dial swap: retry on the fresh conn. This
		// also covers conn already marked dead (nil): the sender then
		// blocks on the alive channel instead of dropping the frame —
		// the error path must only fire when this exact dead conn is
		// still current (no re-dial machinery running).
		u.mu.Lock()
		replaced := u.conn != c
		u.mu.Unlock()
		if replaced {
			continue
		}
		return err
	}
}

func (u *upstreamPool) current() *websocket.Conn {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.conn
}

// markDead drops the connection and unblocks the alive channel so senders
// start waiting for the re-dial instead of erroring.
func (u *upstreamPool) markDead() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.conn != nil {
		u.conn.CloseNow()
		u.conn = nil
		u.aliveCh = make(chan struct{})
	}
}

func (u *upstreamPool) swap(c *websocket.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.conn != nil && u.conn != c {
		u.conn.CloseNow()
	}
	u.conn = c
	close(u.aliveCh)
}

func (u *upstreamPool) close() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.conn != nil {
		// Best effort close frame; Close blocks until the peer answers or
		// the underlying read deadline fires, so it cannot hang forever.
		_ = u.conn.Close(websocket.StatusGoingAway, "")
		u.conn.CloseNow()
		u.conn = nil
	}
	select {
	case <-u.aliveCh:
	default:
		close(u.aliveCh)
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u, err := url.Parse(p.Upstream)
	if err != nil || u.Host == "" {
		http.Error(w, "invalid upstream", http.StatusBadGateway)
		return
	}
	if r.URL.RawQuery != "" {
		if u.RawQuery != "" {
			u.RawQuery += "&"
		}
		u.RawQuery += r.URL.RawQuery
	}
	headers := r.Header.Clone()
	for _, key := range []string{"Connection", "Upgrade", "Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Extensions", "Sec-WebSocket-Protocol"} {
		headers.Del(key)
	}
	var protocols []string
	for value := range strings.SplitSeq(r.Header.Get("Sec-WebSocket-Protocol"), ",") {
		if value = strings.TrimSpace(value); value != "" {
			protocols = append(protocols, value)
		}
	}
	dial := func(ctx context.Context) (*websocket.Conn, error) {
		dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
		up, _, err := websocket.Dial(dialCtx, u.String(), &websocket.DialOptions{HTTPHeader: headers, Subprotocols: protocols})
		dialCancel()
		if err != nil {
			slog.Info("dbg_dial_fail", "err", err.Error(), "upstream", u.Host)
		}
		return up, err
	}
	up, err := dial(r.Context())
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	pool := newUpstreamPool(up)
	// Close the upstream WebSocket with a proper close frame: an abrupt TCP
	// tear leaves the UniProxy session half-open server-side, and Yandex then
	// RSTs every fresh session from this device for ~10-20 s (the
	// "no internet" reconnect storm after interrupting an answer).
	defer pool.close()
	var selected []string
	if proto := up.Subprotocol(); proto != "" {
		selected = []string{proto}
	}
	down, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: selected, OriginPatterns: []string{u.Hostname()}})
	if err != nil {
		return
	}
	defer down.CloseNow()
	down.SetReadLimit(8 << 20)

	upSend := Sender(pool.send)
	up.SetReadLimit(8 << 20)

	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()
	var session Session
	var trace *conversation
	downSend := Sender(down.Write)
	if dir := osGetenvDumpDir(); dir != "" {
		dump := newFrameDump(dir, "down")
		defer dump.close()
		inner := downSend
		downSend = func(ctx context.Context, k websocket.MessageType, data []byte) error {
			if err := inner(ctx, k, data); err != nil {
				return err
			}
			dump.write(data)
			return nil
		}
	}
	if p.Journal != nil {
		trace = newConversation(p.Journal)
		if p.NewSession != nil {
			trace.initialRoute = ""
		}
		inner := downSend
		downSend = func(ctx context.Context, k websocket.MessageType, data []byte) error {
			if err := inner(ctx, k, data); err != nil {
				return err
			}
			trace.server(data, true)
			return nil
		}
	}
	if p.NewSession != nil {
		session = p.NewSession(ctx, upSend, downSend)
		if n, ok := session.(*Native); ok && trace != nil {
			n.OnRoute = trace.route
		}
	}
	if p.Subscriptions != nil {
		session = p.Subscriptions.NewSession(upSend, downSend, session)
	}
	if session != nil {
		defer session.Close()
	}

	attempts := p.RedialAttempts
	if attempts <= 0 {
		attempts = 5
	}
	copyClient := func() {
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				slog.Info("dbg_copy_panic", "direction", "client", "panic", fmt.Sprint(r))
			}
		}()
		for {
			kind, data, err := down.Read(ctx)
			if err != nil {
				slog.Info("dbg_copy_end", "direction", "client", "reader", "src", "err", err.Error(), "ctx_err", ctx.Err() != nil)
				return
			}
			if trace != nil {
				trace.client(data)
			}
			if p.Observer != nil {
				p.Observer("client", kind, data)
			}
			if session != nil {
				if err = session.Client(ctx, kind, data); err != nil {
					slog.Info("dbg_copy_end", "direction", "client", "reader", "handler", "err", err.Error())
					return
				}
				continue
			}
			if err := upSend(ctx, kind, data); err != nil {
				slog.Info("dbg_copy_end", "direction", "client", "reader", "send", "err", err.Error())
				return
			}
		}
	}
	copyServer := func() {
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				slog.Info("dbg_copy_panic", "direction", "server", "panic", fmt.Sprint(r))
			}
		}()
		for attempt := 0; ; {
			for {
				c := pool.current()
				if c == nil {
					break // upstream marked dead; fall through to re-dial
				}
				kind, data, err := c.Read(ctx)
				if err != nil {
					slog.Info("dbg_copy_end", "direction", "server", "reader", "src", "err", err.Error(), "ctx_err", ctx.Err() != nil)
					break
				}
				if trace != nil {
					trace.server(data, false)
				}
				if p.Observer != nil {
					p.Observer("server", kind, data)
				}
				if session != nil {
					if err = session.Server(ctx, kind, data); err != nil {
						slog.Info("dbg_copy_end", "direction", "server", "reader", "handler", "err", err.Error())
						return
					}
					continue
				}
				if err := downSend(ctx, kind, data); err != nil {
					slog.Info("dbg_copy_end", "direction", "server", "reader", "send", "err", err.Error())
					return
				}
			}
			// Upstream read failed. If the station leg or the session is
			// already dead, follow it down; otherwise re-dial transparently.
			// Mark dead FIRST so senders block on the swap instead of
			// erroring and tearing the session apart.
			if ctx.Err() != nil || !p.Redial || attempt >= attempts {
				pool.markDead()
				return
			}
			pool.markDead()
			attempt++
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			nu, err := dial(ctx)
			if err != nil {
				continue
			}
			pool.swap(nu)
			slog.Info("upstream_redial", "attempt", attempt)
			if rd, ok := session.(UpstreamRedialer); ok {
				rd.UpstreamRedialed(ctx)
			}
		}
	}
	var wg sync.WaitGroup
	wg.Go(copyClient)
	wg.Go(copyServer)
	wg.Wait()
}
