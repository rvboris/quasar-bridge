package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"

	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	syncstate "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/synchronize_state_request"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
)

// The station's SynchronizeStateRequest must be cached and replayed first on
// upstream re-dial: without it the cloud loses the device binding and music
// commands fail with error_class "other" until the bridge restarts.
func TestRedialReplaysSynchronizeStateFirst(t *testing.T) {
	sync := aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
		Header: &header.TEventHeader{MessageId: "sync-1"},
		Event:  &client.TClientMessage_TEvent_SynchronizeStateRequest{SynchronizeStateRequest: &syncstate.TSynchronizeStateRequest{}},
	}}})
	sent := make(chan []byte, 8)
	n := NewNative(t.Context(), func(_ context.Context, _ websocket.MessageType, b []byte) error {
		select {
		case sent <- b:
		default:
		}
		return nil
	}, func(context.Context, websocket.MessageType, []byte) error { return nil }, nil)
	t.Cleanup(n.Close)
	pending := newMetricsRequest(n, "redial-sync-req")
	pending.cloud = true
	pending.input = aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
		Header: &header.TEventHeader{MessageId: "redial-sync-req"},
		Event:  &client.TClientMessage_TEvent_TextInput{TextInput: nil},
	}}})

	if err := n.Client(t.Context(), websocket.MessageBinary, sync); err != nil {
		t.Fatal(err)
	}
	// The cached frame is also forwarded normally by Client; drain that copy.
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("SynchronizeStateRequest was not forwarded by Client")
	}
	n.UpstreamRedialed(t.Context())

	select {
	case got := <-sent:
		if string(got) != string(sync) {
			t.Fatalf("first replayed frame is not SynchronizeStateRequest: % x", got[:min(len(got), 16)])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SynchronizeStateRequest was not replayed after re-dial")
	}
	// The gated request replay follows, not precedes, the sync frame.
	select {
	case got := <-sent:
		if string(got) == string(sync) {
			t.Fatal("sync frame sent twice")
		}
		if string(got) != string(pending.input) {
			t.Fatalf("unexpected second frame: % x", got[:min(len(got), 16)])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending request was not replayed after sync")
	}
}
