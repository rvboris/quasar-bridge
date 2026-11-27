package bridge

import (
	"context"
	"testing"

	"github.com/coder/websocket"
)

type redialRecorderSession struct{ called int }

func (r *redialRecorderSession) Close() {}

func (r *redialRecorderSession) Client(ctx context.Context, k websocket.MessageType, data []byte) error {
	return nil
}

func (r *redialRecorderSession) Server(ctx context.Context, k websocket.MessageType, data []byte) error {
	return nil
}

func (r *redialRecorderSession) UpstreamRedialed(ctx context.Context) { r.called++ }

// The proxy reaches the wrapped Native through this type assertion; if
// subscriptionSession does not forward UpstreamRedialer, the re-dial replay
// (SynchronizeStateRequest first, then pending requests) never runs.
func TestSubscriptionSessionForwardsUpstreamRedialer(t *testing.T) {
	delegate := &redialRecorderSession{}
	m := &SubscriptionMonitor{}
	s := m.NewSession(nil, nil, delegate)
	rd, ok := s.(UpstreamRedialer)
	if !ok {
		t.Fatal("subscriptionSession does not implement UpstreamRedialer")
	}
	rd.UpstreamRedialed(t.Context())
	if delegate.called != 1 {
		t.Fatalf("delegate UpstreamRedialed calls = %d, want 1", delegate.called)
	}
}
