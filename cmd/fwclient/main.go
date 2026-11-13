// fwclient is a firmware-emulating test client for quasar-bridge: it opens a
// uniproxy WebSocket, sends one TextInput request and prints a timeline of the
// server frames it receives. Used to verify progressive (streamed) deferred
// answer parts end-to-end against a live bridge.
package main

import (
	"context"
	"flag"
	"fmt"
	"encoding/binary"
	"os"
	"time"

	"github.com/coder/websocket"

	app "github.com/skrashevich/quasar-go-genproto/proto/alice/library/client/protos"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"

	"quasarbridge/internal/wire"
)

func main() {
	addr := flag.String("addr", "ws://127.0.0.1:8443", "bridge websocket url")
	text := flag.String("text", "Расскажи подробно, чем отличается рекурсия от итерации.", "request text")
	wait := flag.Duration("wait", 90*time.Second, "how long to read frames")
	dump := flag.String("dump", "", "write every received frame to this file (4-byte BE length + payload)")
	drain := flag.Duration("drain", 10*time.Second, "how long to keep reading frames after the last deferred part")
	interruptAfter := flag.Int("interrupt-after", 0, "send a second request after N deferred parts (simulates barge-in)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()
	c, _, err := websocket.Dial(ctx, *addr, nil)
	if err != nil {
		fmt.Println("DIAL_ERROR", err)
		os.Exit(1)
	}
	defer c.CloseNow()

	id := fmt.Sprintf("fwclient-%d", time.Now().UnixNano())
	msg := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
		Header: &header.TEventHeader{MessageId: id},
		Event: &client.TClientMessage_TEvent_TextInput{TextInput: &request.TAliceRequestProto{
			Application: &app.TClientInfoProto{DeviceId: new("fwclient-device")},
			Request:     &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Text: new(*text)}},
		}},
	}}}
	b, err := (wire.Frame{Client: msg, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		fmt.Println("MARSHAL_ERROR", err)
		os.Exit(1)
	}
	start := time.Now()
	if err := c.Write(ctx, websocket.MessageBinary, b); err != nil {
		fmt.Println("WRITE_ERROR", err)
		os.Exit(1)
	}
	fmt.Printf("t+0ms SENT TextInput %q id=%s\n", *text, id)

	parts, last, done := 0, 0, false
	interrupted := false
	sendSecond := func() {
		interrupted = true
		id2 := fmt.Sprintf("fwclient-%d", time.Now().UnixNano())
		msg2 := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{
			Header: &header.TEventHeader{MessageId: id2},
			Event: &client.TClientMessage_TEvent_TextInput{TextInput: &request.TAliceRequestProto{
				Application: &app.TClientInfoProto{DeviceId: new("fwclient-device")},
				Request:     &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Text: new("Который час?")}},
			}},
		}}}
		b2, err := (wire.Frame{Client: msg2, Prefix: []byte("AAPI")}).Marshal()
		if err == nil {
			err = c.Write(ctx, websocket.MessageBinary, b2)
		}
		fmt.Printf("t+%dms INTERRUPT sent err=%v\n", time.Since(start).Milliseconds(), err)
	}
	var dumpFile *os.File
	if *dump != "" {
		dumpFile, err = os.Create(*dump)
		if err != nil {
			fmt.Println("DUMP_ERROR", err)
			os.Exit(1)
		}
		defer dumpFile.Close()
	}
	for {
		mt, data, err := c.Read(ctx)
		if err != nil {
			break
		}
		_ = mt
		if dumpFile != nil {
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
			dumpFile.Write(hdr[:])
			dumpFile.Write(data)
		}
		f := wire.Decode(data, wire.ServerToClient)
		kind := f.Kind()
		switch kind {
		case "DeferredAliceResponse":
			r := f.Server.GetEvent().GetDeferredAliceResponse()
			txt := r.GetBaseResponse().GetText()
			fmt.Printf("t+%dms PART n=%d last=%v text=%q\n", time.Since(start).Milliseconds(), int(r.GetResponsePartialNum()), r.GetIsLast(), txt)
			if txt != "" {
				parts++
			}
			if *interruptAfter > 0 && !interrupted && parts >= *interruptAfter {
				sendSecond()
			}
			if r.GetIsLast() {
				last = int(time.Since(start).Milliseconds())
				done = true
			}
		case "TtsGenerate", "TtsSpeak", "InputStartAck", "StreamControl":
			// expected companions; keep the timeline terse
		default:
			if len(data) > 8 {
				fmt.Printf("t+%dms FRAME kind=%s bytes=%d\n", time.Since(start).Milliseconds(), kind, len(data))
			}
		}
		if done {
			// Keep draining trailing TTS/EOS frames; the final chunk's
			// synthesis can easily exceed a couple of seconds.
			dctx, dcancel := context.WithTimeout(context.Background(), *drain)
			defer dcancel()
			for {
				if _, data, err := c.Read(dctx); err != nil {
					break
				} else {
					if dumpFile != nil {
						var hdr [4]byte
						binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
						dumpFile.Write(hdr[:])
						dumpFile.Write(data)
					}
				}
			}
			break
		}
	}
	if done {
		fmt.Printf("RESULT ok parts=%d first_to_last_ms=%d\n", parts, last)
		os.Exit(0)
	}
	fmt.Println("RESULT incomplete (no last deferred part within deadline)")
	os.Exit(2)
}
