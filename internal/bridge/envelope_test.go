package bridge

import (
	"strings"
	"testing"

	wire "quasarbridge/internal/wire"
)

func TestDeferredPartFrameVoiceField(t *testing.T) {
	b, err := deferredPartFrame(deferredPartArgs{
		refMessageID: "req", requestID: "app", partial: 3, last: false,
		text: "привет", voice: "привет",
	})
	if err != nil {
		t.Fatal(err)
	}
	f := wire.Decode(b, wire.ServerToClient)
	if f.Server == nil {
		t.Fatal("no server message")
	}
	r := f.Server.GetEvent().GetDeferredAliceResponse()
	if r == nil || r.GetIsLast() || r.GetResponsePartialNum() != 3 {
		t.Fatalf("unexpected deferred part: %+v", r)
	}
	base := r.GetBaseResponse()
	if base.GetText() != "привет" {
		t.Fatalf("text = %q", base.GetText())
	}
	// Reserved field 2 (Voice) must carry the spoken delta for old SDKs.
	if !strings.Contains(base.String(), "привет") || !strings.Contains(string(base.ProtoReflect().GetUnknown()), "привет") {
		t.Fatalf("reserved voice field missing")
	}
}

func TestReplyFrameSilentStripsSpeech(t *testing.T) {
	b, err := replyFrame(replyArgs{
		refMessageID: "req", requestID: "app", text: "ответ", speech: "ответ",
		shouldListen: true, silent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := wire.Decode(b, wire.ServerToClient)
	a := f.Server.GetEvent().GetAliceResponse()
	if a.GetVoiceResponse().GetHasVoiceResponse() {
		t.Fatal("silent reply must not have voice response")
	}
	if a.GetVoiceResponse().GetShouldListen() {
		t.Fatal("silent reply must not listen")
	}
	if a.GetResponse().GetCards()[0].GetText() != "ответ" {
		t.Fatalf("card text = %q", a.GetResponse().GetCards()[0].GetText())
	}
}

func TestStreamStartFrameShape(t *testing.T) {
	b, err := streamStartFrame(streamStartArgs{
		refMessageID: "req", requestID: "app", responseID: "resp", shouldListen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := wire.Decode(b, wire.ServerToClient)
	a := f.Server.GetEvent().GetAliceResponse()
	if !a.GetResponse().GetIsStreaming() {
		t.Fatal("stream start must set IsStreaming")
	}
	if got := a.GetResponse().GetDirectives(); len(got) != 1 || got[0].GetName() != "tts_play_placeholder" {
		t.Fatalf("directives = %+v", got)
	}
	if !a.GetVoiceResponse().GetShouldListen() {
		t.Fatal("shouldListen lost")
	}
}
