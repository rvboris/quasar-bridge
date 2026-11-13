package bridge

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	directive "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/directives"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	response "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_response"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
	"google.golang.org/protobuf/encoding/protowire"
	"quasarbridge/internal/wire"
)

type streamingFallbackResolver struct {
	deltas []string
	answer string
}

func (r streamingFallbackResolver) Resolve(context.Context, string) (Resolution, error) {
	return Resolution{Route: "yandex"}, nil
}
func (r streamingFallbackResolver) Fallback(context.Context, string) (Resolution, error) {
	return Resolution{Route: "llm", Answer: r.answer}, nil
}
func (r streamingFallbackResolver) FallbackStream(ctx context.Context, query string, emit func(string)) (Resolution, error) {
	for _, d := range r.deltas {
		emit(d)
	}
	return Resolution{Route: "llm", Answer: r.answer}, nil
}

// TestNativeRefusalStreamsChunksThroughDeferredParts checks the opt-in
// streaming refusal path: chunk emission opens the deferred stream exactly
// once, the final deferred part carries only the remaining text and the
// concatenated spoken text equals the full answer.
func TestNativeRefusalStreamsChunksThroughDeferredParts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resolver := streamingFallbackResolver{
		deltas: []string{"Первая часть. ", "Вторая часть. "},
		answer: "Первая часть. Вторая часть. Финал.",
	}
	down := make(chan wire.Frame, 32)
	up := make(chan wire.Frame, 32)
	var streamMu sync.Mutex
	nextStream := uint32(100)
	var n *Native
	sent := make(chan struct{}, 1)
	n = NewNative(ctx, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		f := wire.Decode(b, wire.ClientToServer)
		if f.Kind() == "TtsGenerate" {
			// Answer each synthesis request with opus speech so audio chaining
			// and progress completion proceed.
			id := f.Client.GetEvent().GetHeader().GetMessageId()
			streamMu.Lock()
			nextStream++
			stream := nextStream
			streamMu.Unlock()
			feedSpeech(t, n, id, stream, false)
		} else {
			select {
			case sent <- struct{}{}:
			default:
			}
		}
		up <- f
		return nil
	}, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		down <- wire.Decode(b, wire.ServerToClient)
		return nil
	}, resolver)
	defer n.Close()
	n.StreamAnswers = true
	n.ProgressAfter = 0 // disabled: the streamed chunks themselves open the stream
	input := aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: "stream-refusal"}, Event: &client.TClientMessage_TEvent_TextInput{TextInput: &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Text: new("запрос")}}}}}}})
	if err := n.Client(ctx, websocket.MessageBinary, input); err != nil {
		t.Fatal(err)
	}
	// The request only becomes a cloud refusal candidate after routing handed
	// it upstream.
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("input never reached the cloud")
	}
	refusal := aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: "stream-refusal"}, Event: &server.TServerMessage_TEvent_AliceResponse{AliceResponse: &response.TAliceResponseProto{Response: &response.TAliceResponseProto_TResponse{Directives: []*directive.TDirective{{Type: "client_action", Name: "tts_play_placeholder"}}}, VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{OutputSpeech: &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new("Я не могу ответить на этот вопрос.")}}}}}}})
	if err := n.Server(ctx, websocket.MessageBinary, refusal); err != nil {
		t.Fatal(err)
	}

	next := func() wire.Frame {
		t.Helper()
		select {
		case f := <-down:
			return f
		case <-time.After(2 * time.Second):
			t.Fatal("missing downstream frame")
			return wire.Frame{}
		}
	}
	envelopes := 0
	var parts []string
	var voices []string
	for {
		f := next()
		switch f.Kind() {
		case "InputStartAck", "opaque", "TtsSpeak", "StreamControl":
			// Transport/audio plumbing; not part of the text contract.
		case "AliceResponse":
			if !f.Server.GetEvent().GetAliceResponse().GetResponse().GetIsStreaming() {
				t.Fatal("unexpected non-streaming answer envelope")
			}
			envelopes++
		case "DeferredAliceResponse":
			part := f.Server.GetEvent().GetDeferredAliceResponse()
			parts = append(parts, part.GetBaseResponse().GetText())
			if unknown := part.GetBaseResponse().ProtoReflect().GetUnknown(); len(unknown) > 0 {
				num, _, count := protowire.ConsumeTag(unknown)
				if num == 2 {
					if voice, n := protowire.ConsumeString(unknown[count:]); n > 0 {
						voices = append(voices, voice)
					}
				}
			}
			if part.GetIsLast() {
				if envelopes != 1 {
					t.Fatalf("deferred envelope opened %d times", envelopes)
				}
				for i, p := range parts[:len(parts)-1] {
					if p == "" && i > 0 {
						t.Fatalf("empty intermediate part %d", i)
					}
				}
				if got := strings.Join(parts, ""); got != resolver.answer {
					t.Fatalf("concatenated parts=%q want %q", got, resolver.answer)
				}
				if got := strings.Join(voices, ""); got != resolver.answer {
					t.Fatalf("concatenated voices=%q want %q", got, resolver.answer)
				}
				return
			}
		default:
			t.Fatalf("unexpected frame %s", f.Kind())
		}
	}
}

// TestNativeRefusalWithoutStreamingKeepsSingleAnswer verifies the legacy
// refusal path when StreamAnswers is off.
func TestNativeRefusalWithoutStreamingKeepsSingleAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	resolver := streamingFallbackResolver{answer: "Ответ после отказа"}
	down := make(chan wire.Frame, 16)
	sent := make(chan struct{}, 1)
	n := NewNative(ctx, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		select {
		case sent <- struct{}{}:
		default:
		}
		return nil
	}, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		down <- wire.Decode(b, wire.ServerToClient)
		return nil
	}, resolver)
	defer n.Close()
	input := aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: "plain-refusal"}, Event: &client.TClientMessage_TEvent_TextInput{TextInput: &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Text: new("запрос")}}}}}}})
	if err := n.Client(ctx, websocket.MessageBinary, input); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("input never reached the cloud")
	}
	refusal := aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: "plain-refusal"}, Event: &server.TServerMessage_TEvent_AliceResponse{AliceResponse: &response.TAliceResponseProto{Response: &response.TAliceResponseProto_TResponse{Directives: []*directive.TDirective{{Type: "client_action", Name: "tts_play_placeholder"}}}, VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{OutputSpeech: &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new("Я не могу ответить на этот вопрос.")}}}}}}})
	if err := n.Server(ctx, websocket.MessageBinary, refusal); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case f := <-down:
			if f.Kind() == "DeferredAliceResponse" {
				t.Fatal("unstreamed refusal used deferred parts")
			}
			if f.Kind() == "AliceResponse" && f.Server.GetEvent().GetAliceResponse().GetVoiceResponse().GetOutputSpeech().GetText() == "Ответ после отказа" {
				return
			}
		case <-ctx.Done():
			t.Fatal("no fallback answer")
		}
	}
}
