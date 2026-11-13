package bridge

import (
	"context"
	"testing"

	"github.com/coder/websocket"
)

func TestTeardownReleasesSynthClaim(t *testing.T) {
	n := NewNative(context.Background(),
		func(context.Context, websocket.MessageType, []byte) error { return nil },
		func(context.Context, websocket.MessageType, []byte) error { return nil },
		nil)
	pctx, cancel := context.WithCancel(context.Background())
	p := &nativeRequest{id: "req", ctx: pctx, cancel: cancel}
	n.mu.Lock()
	n.requests[p.id] = p
	p.curSynthDone = make(chan struct{})
	done := p.curSynthDone
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p})
	n.mu.Unlock()

	n.mu.Lock()
	n.teardownLocked(p, true)
	n.mu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.pendingSynth) != 0 {
		t.Fatalf("pendingSynth not released: %d", len(n.pendingSynth))
	}
	if n.requests[p.id] != nil {
		t.Fatal("request not removed")
	}
	select {
	case <-done:
	default:
		t.Fatal("synth waiters not released")
	}
	if err := pctx.Err(); err == nil {
		t.Fatal("context not canceled")
	}
}

func TestTeardownSoftKeepsRegistration(t *testing.T) {
	n := NewNative(context.Background(),
		func(context.Context, websocket.MessageType, []byte) error { return nil },
		func(context.Context, websocket.MessageType, []byte) error { return nil },
		nil)
	pctx, cancel := context.WithCancel(context.Background())
	p := &nativeRequest{id: "req", ctx: pctx, cancel: cancel}
	n.mu.Lock()
	n.requests[p.id] = p
	p.curSynthDone = make(chan struct{})
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p})
	n.mu.Unlock()

	n.mu.Lock()
	n.teardownLocked(p, false)
	n.mu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.requests[p.id] != p {
		t.Fatal("soft teardown must keep the request registered")
	}
	if len(n.pendingSynth) != 0 {
		t.Fatalf("soft teardown must still release synth claims: %d", len(n.pendingSynth))
	}
}
