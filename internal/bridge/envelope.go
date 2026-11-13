package bridge

// Envelope builders: pure frame constructors with no locks and no I/O.
// Each takes its inputs explicitly and returns the marshaled AAPI frame;
// the caller owns synchronization and sending. Tests can construct and
// inspect frames directly (see envelope_test.go).

import (
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	directive "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/directives"
	response "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_response"
	cancellation "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/cancel_request"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	continuation "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/continue_request"
	ack "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/input_start_ack"
	stream "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/stream_control"
	tts "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/tts_generate"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
	deferred "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/streaming"
	"google.golang.org/protobuf/proto"

	"quasarbridge/internal/wire"
)

// cancelFrame cancels the upstream request with the given message id.
func cancelFrame(requestMessageID string) ([]byte, error) {
	m := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
		Header: &header.TEventHeader{MessageId: newID()},
		Event: &client.TClientMessage_TEvent_CancelRequest{CancelRequest: &cancellation.TCancelRequest{
			RequestMessageId: requestMessageID,
			Reason:           &cancellation.TCancelRequest_LocalExecution{LocalExecution: &cancellation.TCancelRequest_TReasonLocalExecution{}},
		}},
	}}}
	return (wire.Frame{Client: m, Prefix: []byte("AAPI")}).Marshal()
}

// continueFrame asks the upstream to keep the request's session open. The
// header is cloned from the original request so dialog attributes survive.
func continueFrame(h *header.TEventHeader, requestMessageID string) ([]byte, error) {
	h = proto.Clone(h).(*header.TEventHeader)
	h.MessageId = newID()
	h.RefMessageId = ""
	h.StreamId = nil
	m := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
		Header: h,
		Event:  &client.TClientMessage_TEvent_ContinueRequest{ContinueRequest: &continuation.TContinueRequest{RequestMessageId: requestMessageID}},
	}}}
	return (wire.Frame{Client: m, Prefix: []byte("AAPI")}).Marshal()
}

// ackFrame acknowledges the start of input for the referenced request.
func ackFrame(refMessageID string) ([]byte, error) {
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: newID(), RefMessageId: refMessageID},
		Event: &server.TServerMessage_TEvent_InputStartAck{InputStartAck: &ack.TInputStartAck{
			RequestStartTime: uint64(time.Now().UnixMilli()),
		}},
	}}}
	return (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
}

// ttsGenerateFrame requests Opus TTS for text under the given message id.
func ttsGenerateFrame(messageID, text string) ([]byte, error) {
	m := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
		Header: &header.TEventHeader{MessageId: messageID},
		Event: &client.TClientMessage_TEvent_TtsGenerate{TtsGenerate: &tts.TTtsGenerate{
			Text: new(text), Lang: new("ru-RU"), Format: new("audio/opus"),
		}},
	}}}
	return (wire.Frame{Client: m, Prefix: []byte("AAPI")}).Marshal()
}

// streamCloseFrame closes a raw audio stream toward the station.
func streamCloseFrame(messageID string, streamID uint32) ([]byte, error) {
	m := &server.TServerMessage{Message: &server.TServerMessage_StreamControl{StreamControl: &stream.TStreamControl{
		MessageId: messageID,
		StreamId:  int64(streamID),
		Action:    &stream.TStreamControl_Close{Close: &stream.TStreamControl_TActionClose{}},
	}}}
	return (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
}

// replyArgs carries everything needed to build one final voice reply.
type replyArgs struct {
	refMessageID string // station request id the reply references
	requestID    string // application request id (falls back to refMessageID)
	dialogID     string
	sequence     *uint32
	text         string // displayed answer
	speech       string // spoken answer (empty when silent)
	shouldListen bool
	silent       bool
	directives   []*directive.TDirective
}

// replyFrame builds the final (non-streaming) AliceResponse envelope.
func replyFrame(a replyArgs) ([]byte, error) {
	if a.silent {
		a.shouldListen = false
	}
	answer := &response.TAliceResponseProto{
		Header: &response.TAliceResponseProto_THeader{
			RequestId: new(a.requestID), ResponseId: new(newID()),
			DialogId: new(a.dialogID), SequenceNumber: a.sequence,
		},
		VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{
			OutputSpeech:     &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new(a.text)},
			HasVoiceResponse: true, ShouldListen: a.shouldListen,
		},
		Response: &response.TAliceResponseProto_TResponse{Cards: []*response.TAliceResponseProto_TResponse_TCard{{
			Type: "simple_text", Text: new(a.text), CardId: new(newID()),
		}}},
	}
	if a.silent {
		answer.VoiceResponse.HasVoiceResponse = false
		answer.VoiceResponse.OutputSpeech.Text = new("")
	}
	answer.Response.Directives = a.directives
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: newID(), RefMessageId: a.refMessageID},
		Event:  &server.TServerMessage_TEvent_AliceResponse{AliceResponse: answer},
	}}}
	return (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
}

// streamStartArgs carries what the deferred-stream opener needs.
type streamStartArgs struct {
	refMessageID string
	requestID    string
	dialogID     string
	sequence     *uint32
	responseID   string
	shouldListen bool
}

// streamStartFrame opens a deferred streaming answer (empty speech, one
// tts_play_placeholder directive). Station schedules post-speech listening
// from this envelope.
func streamStartFrame(a streamStartArgs) ([]byte, error) {
	r := &response.TAliceResponseProto{
		Header: &response.TAliceResponseProto_THeader{
			RequestId: new(a.requestID), ResponseId: new(a.responseID),
			DialogId: new(a.dialogID), SequenceNumber: a.sequence,
		},
		VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{
			HasVoiceResponse: true,
			OutputSpeech:     &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new("")},
		},
		Response: &response.TAliceResponseProto_TResponse{
			IsStreaming: true,
			Directives:  []*directive.TDirective{{Type: "client_action", Name: "tts_play_placeholder"}},
		},
	}
	r.VoiceResponse.ShouldListen = a.shouldListen
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: newID(), RefMessageId: a.refMessageID},
		Event:  &server.TServerMessage_TEvent_AliceResponse{AliceResponse: r},
	}}}
	return (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
}

// deferredPartArgs carries what one deferred delta part needs.
type deferredPartArgs struct {
	refMessageID string
	requestID    string // application request id
	partial      uint32
	last         bool
	text         string
	voice        string // spoken delta; written to reserved field 2 for old SDKs
	shouldListen bool
	directives   []*directive.TDirective
}

// deferredPartFrame builds one non-final or final deferred response part.
func deferredPartFrame(a deferredPartArgs) ([]byte, error) {
	base := &deferred.TDeferedAliceResponseProto_TBaseResponse{ShouldListen: a.shouldListen}
	if a.text != "" {
		base.Text = new(a.text)
	}
	base.Directives = a.directives
	// Older Station SDKs still consume reserved field 2 (Voice), separately
	// from Text. Observed cloud responses send speech deltas here.
	unknown := protowire.AppendTag(nil, 2, protowire.BytesType)
	unknown = protowire.AppendString(unknown, a.voice)
	base.ProtoReflect().SetUnknown(unknown)
	r := &deferred.TDeferedAliceResponseProto{
		RequestId: a.requestID, ResponsePartialNum: a.partial, IsLast: a.last,
		TDeferredResponse: &deferred.TDeferedAliceResponseProto_BaseResponse{BaseResponse: base},
	}
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: newID(), RefMessageId: a.refMessageID},
		Event:  &server.TServerMessage_TEvent_DeferredAliceResponse{DeferredAliceResponse: r},
	}}}
	return (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
}
