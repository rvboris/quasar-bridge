package bridge

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	response "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_response"
	asrcommon "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result/common"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	"google.golang.org/protobuf/proto"

	"quasarbridge/internal/dialogue"
	"quasarbridge/internal/reply"
	"quasarbridge/internal/sound"
	"quasarbridge/internal/wire"
)

type Resolution struct {
	Route  string
	Answer string
	speech string // Optional TTS text with markup, separate from the displayed answer.
}
type Resolver interface {
	Resolve(context.Context, string) (Resolution, error)
}
type RoutedResolver interface {
	Decide(context.Context, string) RouteDecision
	ResolveDecision(context.Context, string, RouteDecision) (Resolution, error)
}
type FallbackResolver interface {
	Fallback(context.Context, string) (Resolution, error)
}

type nativeRequest struct {
	audioInput                      []byte
	audioInputDone                  bool
	inputWhisper                    bool
	streaming                       bool
	listenOnStreamStart             bool
	partial                         uint32
	responseID                      string
	final                           bool
	aggregateSeen                   bool
	aggregateFinalSeen              bool
	header                          *header.TEventHeader
	isText                          bool
	ttsStarted                      bool
	gated                           bool
	input                           []byte
	id                              string
	request                         *request.TAliceRequestProto
	text                            string
	route                           string
	ctx                             context.Context
	cancel                          context.CancelFunc
	started, canceled, local, cloud bool
	arrived                         time.Time // request arrival, for lifetime metrics
	continuation                    []byte
	ttsID                           string
	progressTTS                     string
	markedTTS                       bool // <speaker> can produce chained Opus streams.
	rawStreams                      []uint32
	curSynthDone                    chan struct{} // closed when the current TTS stream's source ends
	audio                           oggSpeech
	flow                            speechFlow
	audioStream                     uint32
	finalSynth                      bool
	finalPart                       string // last deferred part held until joined audio reaches EOS
	progressAudioDone               chan struct{}
	soundRequest                    *sound.Request
	soundDirective                  *soundDirective
	soundSealed                     bool
	expires                         *time.Timer
	synths                          int // TtsGenerate requests issued (guarded by n.mu)
	emitMu                          sync.Mutex // Serializes deferredPart/Synthesize between progress keepalives and streamed chunks.
	// KNOWN LIMITATION: emitChunk holds emitMu across synthesizeClaim ->
	// awaitSynth, which can block for up to synthWaitLimit (15s). The
	// silence keepalive pump (waitAudio) is what keeps the station's player
	// fed during that window; do not remove the pump without first
	// narrowing this lock's scope.
	emittedText                     string     // Answer prefix already spoken via streamed deferred parts (guarded by emitMu).
}
type synthClaim struct {
	p       *nativeRequest
	text    string    // re-issued verbatim after an upstream re-dial
	discard bool
	final   bool      // this source is the request's last synthesis: its end finalizes the joined stream
	issued  time.Time // for claim-latency metrics: TtsGenerate send → stream bind
}

// synthWaitLimit bounds how long the next chunk's synthesis waits for the
// previous stream before proceeding anyway.
const synthWaitLimit = 15 * time.Second

type Native struct {
	connectionScope  string
	ctx              context.Context
	cancel           context.CancelFunc
	up, down         Sender
	resolver         Resolver
	mu               sync.Mutex
	requests         map[string]*nativeRequest
	tts              map[string]*nativeRequest
	streams          map[uint32]*nativeRequest
	streamFinal      map[uint32]bool // raw synth streams that are their request's final source
	blocked          map[uint32]*nativeRequest
	pendingSynth     []synthClaim // FIFO of issued TtsGenerate requests awaiting their raw audio stream.
	wg               sync.WaitGroup
	metrics          connectionMetrics
	OnRoute          func(id, route string)
	StreamAnswers    bool
	GateText         bool
	ShouldListen     bool
	DecisionDelay    time.Duration
	ProgressAfter    time.Duration
	ProgressInterval time.Duration
	caps             resolverCaps // optional resolver capabilities, probed once
}

func NewNative(ctx context.Context, up, down Sender, resolver Resolver) *Native {
	ctx, cancel := context.WithCancel(ctx)
	n := &Native{connectionScope: newID(), ctx: ctx, cancel: cancel, up: up, down: down, resolver: resolver, ShouldListen: true, requests: map[string]*nativeRequest{}, tts: map[string]*nativeRequest{}, streams: map[uint32]*nativeRequest{}, streamFinal: map[uint32]bool{}, blocked: map[uint32]*nativeRequest{}}
	n.caps = probeResolver(resolver)
	return n
}
func (n *Native) Close() {
	n.cancel()
	n.mu.Lock()
	for _, p := range n.requests {
		n.teardownLocked(p, false)
	}
	n.mu.Unlock()
	n.wg.Wait()
}
func (n *Native) Client(ctx context.Context, k websocket.MessageType, data []byte) error {
	if k == websocket.MessageBinary {
		n.collectWhisperAudio(data)
	}
	f := wire.Decode(data, wire.ClientToServer)
	if f.Client == nil || f.Client.GetEvent() == nil {
		return n.up(ctx, k, data)
	}
	e := f.Client.GetEvent()
	if stat := e.GetRequestStat(); stat != nil {
		kind := "none"
		raw := strings.ToLower(stat.GetError())
		if raw != "" {
			kind = "other"
			if strings.Contains(raw, "timeout") || strings.Contains(raw, "timed out") || strings.Contains(raw, "deadline") {
				kind = "timeout"
			} else if strings.Contains(raw, "cancel") {
				kind = "canceled"
			}
		}
		timestamps := stat.GetTimestamps()
		reported := func(value string) bool { return value != "" && value != "0" }
		slog.Info("native_playback_status", "request", requestTag(stat.GetRequestMessageId()), "error_class", kind, "playback_reported", reported(timestamps.GetOnSoundPlayerBeginTime()) || reported(timestamps.GetOnPlayerBegin()), "finished_reported", reported(timestamps.GetOnSoundPlayerEndTime()) || reported(timestamps.GetOnPlayerEnd()))
	}
	var req *request.TAliceRequestProto
	switch f.Kind() {
	case "TextInput":
		req = e.GetTextInput()
	case "VoiceInput":
		req = e.GetVoiceInput()
	case "SpeechInput":
		req = e.GetSpeechInput()
	}
	if req != nil && f.Kind() == "TextInput" && strings.TrimSpace(req.GetRequest().GetEvent().GetText()) == "" {
		return n.up(ctx, k, data)
	}
	if req != nil {
		id := e.GetHeader().GetMessageId()
		if id == "" {
			return n.up(ctx, k, data)
		}
		scope := "connection:" + n.connectionScope
		if id := req.GetApplication().GetDeviceId(); id != "" {
			scope = "device:" + id
		} else if id := req.GetApplication().GetUuid(); id != "" {
			scope = "uuid:" + id
		}
		pctx, cancel := context.WithTimeout(dialogue.WithSpeaker(n.ctx, scope), 90*time.Second)
		if tag := req.GetEventSource().GetId(); e.GetVoiceInput() != nil && tag != "" {
			pctx = context.WithValue(pctx, wakewordTagKey{}, tag)
		}
		pctx = reply.WithPolicy(pctx)
		playerState := nativePlaybackState(req)
		pctx = sound.WithPlaybackState(pctx, playerState)
		slog.Info("native_player_state", "request", requestTag(id), "present", playerState.Present, "player_state", playerState.PlayerState, "offset_known", playerState.OffsetKnown, "offset_ms", playerState.OffsetMS, "has_stream", playerState.StreamID != "", "stream_tag", requestTag(playerState.StreamID))
		p := &nativeRequest{id: id, request: proto.Clone(req).(*request.TAliceRequestProto), text: req.GetRequest().GetEvent().GetText(), ctx: pctx, cancel: cancel, arrived: time.Now()}
		p.ctx = sound.WithPlayer(p.ctx, func(ctx context.Context, request sound.Request) error {
			return n.queueSound(ctx, p, request)
		})
		p.header = proto.Clone(e.GetHeader()).(*header.TEventHeader)
		p.isText = f.Kind() == "TextInput"
		p.input = append([]byte{}, data...)
		p.gated = !p.isText || n.GateText

		slog.Info("native_request_shape", "request", requestTag(id), "text_bytes", len(p.text), "event_type", req.GetRequest().GetEvent().GetType().String(), "stream", e.GetHeader().GetStreamId(), "unknown_request_bytes", len(req.ProtoReflect().GetUnknown()), "payload_bytes", len(req.GetRequest().GetEvent().GetPayloadRaw()))
		n.mu.Lock()
		if n.requests[id] != nil {
			n.mu.Unlock()
			cancel()
			return nil
		}
		if len(n.requests) >= 128 {
			n.mu.Unlock()
			cancel()
			return errors.New("too many pending native requests")
		}
		// Barge-in: a fresh request while another one still streams means the
		// user interrupted the answer. Tear the old request's TTS streams down
		// upstream BEFORE the new request is forwarded: leaving them open gets
		// the whole session RST'd by UniProxy.
		type victim struct {
			id      string
			streams []uint32
		}
		var victims []victim
		for other, q := range n.requests {
			if other == id || !(q.streaming || q.markedTTS) {
				continue
			}
			slog.Info("dbg_barge_in", "victim", requestTag(other), "streaming", q.streaming)
			n.teardownLocked(q, true)
			vs := victim{id: other}
			vs.streams = append(vs.streams, q.rawStreams...)
			victims = append(victims, vs)
		}
		for _, v := range victims {
			_ = n.sendCancel(p.ctx, v.id)
		}
		if n.caps.prepare != nil {
			p.ctx = n.caps.prepare(p.ctx)
		}
		p.expires = time.AfterFunc(90*time.Second, func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			n.teardownLocked(p, true)
		})
		n.requests[id] = p
		n.mu.Unlock()
		if p.gated {
			gated, err := wire.EnableApproval(f)
			if err != nil {
				return err
			}
			if err := n.up(ctx, k, gated); err != nil {
				return err
			}
		}
		// The original input stays local until routing has selected exactly one executor.
		n.maybeResolve(p)
		return nil
	}
	if c := e.GetContinueRequest(); c != nil {
		n.mu.Lock()
		p := n.requests[c.GetRequestMessageId()]
		if p != nil && (p.canceled || p.local) {
			n.mu.Unlock()
			return nil
		}
		if p != nil && !p.cloud {
			if p.continuation != nil {
				n.mu.Unlock()
				return nil
			}
			p.continuation = append([]byte{}, data...)
			n.mu.Unlock()
			n.maybeResolve(p)
			return nil
		}
		n.mu.Unlock()
	}
	if c := e.GetCancelRequest(); c != nil {
		n.mu.Lock()
		p := n.requests[c.GetRequestMessageId()]
		ttsID := ""
		if p != nil {
			slog.Info("dbg_cancel_req", "request", requestTag(p.id), "streaming", p.streaming, "synth_queued", len(n.pendingSynth))
			// Soft teardown: keep the entry registered so late events from the
			// firmware still match it (and are discarded as canceled); the
			// expiry timer removes it for good.
			n.teardownLocked(p, false)
			ttsID = p.ttsID
		}
		n.mu.Unlock()
		if ttsID != "" {
			_ = n.sendCancel(ctx, ttsID)
		}
	}
	return n.up(ctx, k, data)
}
func (n *Native) findLocked(h *header.TEventHeader) (*nativeRequest, bool) {
	if h == nil {
		return nil, false
	}
	for _, id := range []string{h.GetRefMessageId(), h.GetSubRequestRef().GetMessageId(), h.GetSubRequestRef().GetRequestId()} {
		if p := n.tts[id]; p != nil {
			return p, true
		}
		if p := n.requests[id]; p != nil {
			return p, false
		}
		if id != "" {
			for _, p := range n.requests {
				if p.request.GetHeader().GetRequestId() == id {
					return p, false
				}
			}
		}
	}
	return nil, false
}

// claimSynthLocked binds the next unknown raw audio stream to the oldest
// pending TtsGenerate. UniProxy never sends TtsSpeak envelopes for these, so
// FIFO order is the only correlation — and its known weakness: if the
// upstream ever answers concurrent syntheses out of order, the head claim is
// bound to the wrong request's audio. That cannot be corrected blind, but it
// CAN be detected: awaitSynth serializes sources per request, so a claim
// landing while its request's previous source never ended cleanly means a
// reorder, the synthWaitLimit escape, or an abrupt upstream close. Every such
// landing is counted and logged. Called with n.mu held.
func (n *Native) claimSynthLocked(sid uint32) *nativeRequest {
	if len(n.pendingSynth) == 0 {
		n.metrics.claimsUnclaimed.Add(1)
		slog.Warn("native_synth_unclaimed", "stream", sid, "total", n.metrics.claimsUnclaimed.Load())
		return nil
	}
	claim := n.pendingSynth[0]
	n.pendingSynth = n.pendingSynth[1:]
	p := claim.p
	if p == nil || p.canceled || !(p.streaming || p.markedTTS) {
		// Non-joining answers keep the historical pass-through: the firmware
		// plays their single stream untouched.
		return p
	}
	n.noteSourceOverlapLocked(p, sid, "raw")
	if p.flow.started {
		p.audio.next()
		p.flow.sourceStart()
	} else {
		p.audioStream = sid
		p.flow.sourceStart()
		p.audio.next()
	}
	p.flow.progress = claim.discard
	p.audio.source.discardAudio = claim.discard
	p.rawStreams = append(p.rawStreams, sid)
	n.streams[sid] = p
	n.streamFinal[sid] = claim.final
	n.metrics.claimsBoundRaw.Add(1)
	slog.Info("native_synth_bound", "request", requestTag(p.id), "stream", sid, "via", "raw", "final", claim.final, "latency_ms", time.Since(claim.issued).Milliseconds(), "queue", len(n.pendingSynth))
	return p
}

// noteSourceOverlapLocked flags a claim binding that arrived while the
// request's previous source never reached a clean end. With per-request
// serialization this should be impossible; when it fires, the joined audio is
// suspect (truncated, duplicated, or interleaved with another request's
// stream). Called with n.mu held, before the joiner advances to the new
// source.
func (n *Native) noteSourceOverlapLocked(p *nativeRequest, sid uint32, via string) {
	if !p.flow.started || p.audio.sourceEnded() {
		return
	}
	n.metrics.sourceOverlaps.Add(1)
	slog.Warn("native_synth_overlap", "request", requestTag(p.id), "stream", sid, "via", via, "queue", len(n.pendingSynth), "total", n.metrics.sourceOverlaps.Load())
}

// claimEnvelopeSynthLocked consumes the head pendingSynth claim when a
// TtsSpeak envelope opens the synth stream before its first raw frame. The
// claim's finality is bound to the stream so the source's end (not the mere
// passage of time) decides whether the joined Ogg gets EOS. Called with n.mu
// held.
func (n *Native) claimEnvelopeSynthLocked(p *nativeRequest, sid uint32) {
	n.noteSourceOverlapLocked(p, sid, "envelope")
	final := false
	if len(n.pendingSynth) > 0 && n.pendingSynth[0].p == p {
		claim := n.pendingSynth[0]
		n.pendingSynth = n.pendingSynth[1:]
		final = claim.final
		n.metrics.claimsBoundEnv.Add(1)
		slog.Info("native_synth_bound", "request", requestTag(p.id), "stream", sid, "via", "envelope", "final", final, "latency_ms", time.Since(claim.issued).Milliseconds(), "queue", len(n.pendingSynth))
	}
	// Bind finality but leave rawStreams untouched: the envelope-claimed
	// stream keeps its n.streams binding until the explicit close, so a close
	// without a message id still finds (and swallows against) this request.
	n.streamFinal[sid] = final
}

// sourceEndedLocked finalizes a finished raw TTS source: the progress prime
// closes progressAudioDone; the final answer source flushes the joined Ogg
// with EOS and closes the stream for the firmware. Returns frames to send
// after the lock is released; called with n.mu held.
func (n *Native) sourceEndedLocked(p *nativeRequest, sid uint32) ([][]byte, error) {
	sourceFinal := n.streamFinal[sid]
	for i, s := range p.rawStreams {
		if s == sid {
			delete(n.streams, sid)
			delete(n.streamFinal, sid)
			p.rawStreams = append(p.rawStreams[:i], p.rawStreams[i+1:]...)
			break
		}
	}
	n.finishSynthLocked(p)
	if act := p.flow.sourceEnded(sourceFinal); !act.Finalize {
		if act.ArmSilence {
			p.flow.waiting = true
			n.waitAudio(p)
		}
		if act.CloseProgressDone && p.progressAudioDone != nil {
			close(p.progressAudioDone)
		}
		return nil, nil
	}
	if err := p.audio.endSource(); err != nil {
		return nil, err
	}
	var frames [][]byte
	if end := p.audio.finish(); len(end) > 0 {
		frames = append(frames, append(binary.BigEndian.AppendUint32(nil, p.audioStream), end...))
	}
	b, err := streamCloseFrame(p.id, p.audioStream)
	if err != nil {
		return nil, err
	}
	frames = append(frames, b)
	n.metrics.sourcesFinalized.Add(1)
	slog.Info("native_stream_finalize", "request", requestTag(p.id), "stream", sid, "via", "source_end", "final", sourceFinal)
	reply.Delivered(p.ctx)
	return frames, nil
}

func (n *Native) ttsKeyLocked(h *header.TEventHeader) string {
	if h == nil {
		return ""
	}
	for _, id := range []string{h.GetRefMessageId(), h.GetSubRequestRef().GetMessageId(), h.GetSubRequestRef().GetRequestId()} {
		if _, ok := n.tts[id]; ok {
			return id
		}
	}
	return ""
}
func (n *Native) Server(ctx context.Context, k websocket.MessageType, data []byte) error {
	f := wire.Decode(data, wire.ServerToClient)
	if f.Server == nil {
		if len(data) >= 4 {
			n.mu.Lock()
			sid := binary.BigEndian.Uint32(data)
			p := n.streams[sid]
			if p == nil && len(data) >= 8 && string(data[4:8]) == "OggS" {
				p = n.claimSynthLocked(sid)
			}
			drop := n.blocked[sid] != nil || (p != nil && p.canceled)
			if p != nil && (p.streaming || p.markedTTS) && !drop {
				// Frames are built under the lock (state transitions must be
				// atomic) but sent only after it is released: a stalled
				// station write must not freeze all request handling.
				var frames [][]byte
				out, err := p.audio.feed(data[4:])
				if err == nil && len(out) > 0 {
					frames = append(frames, append(binary.BigEndian.AppendUint32(nil, p.audioStream), out...))
				}
				if err == nil && p.audio.sourceEnded() {
					var fs [][]byte
					fs, err = n.sourceEndedLocked(p, sid)
					frames = append(frames, fs...)
				}
				n.mu.Unlock()
				if err != nil {
					p.cancel()
					return err
				}
				for _, fr := range frames {
					if err := n.down(ctx, k, fr); err != nil {
						p.cancel()
						return err
					}
				}
				return nil
			}
			n.mu.Unlock()
			if drop {
				return nil
			}
		}
		return n.down(ctx, k, data)
	}
	if sc := f.Server.GetStreamControl(); sc != nil {
		n.mu.Lock()
		if n.blocked[uint32(sc.GetStreamId())] != nil {
			delete(n.blocked, uint32(sc.GetStreamId()))
			n.mu.Unlock()
			return nil
		}
		p := n.tts[sc.GetMessageId()]
		if p == nil {
			p = n.streams[uint32(sc.GetStreamId())]
		}
		sid := uint32(sc.GetStreamId())
		sourceFinal := n.streamFinal[sid]
		if sc.GetClose() != nil {
			delete(n.streams, sid)
			delete(n.streamFinal, sid)
		}
		original := n.requests[sc.GetMessageId()]
		if p != nil {
			if p.canceled {
				n.mu.Unlock()
				return nil
			}
			var frames [][]byte
			if (p.streaming || p.markedTTS) && sc.GetClose() != nil {
				// Original semantics: the intermediate swallow applies only to
				// streaming answers; marked-TTS chained streams finalize here.
				act, flushPending := p.flow.streamClosed(sc.GetClose().GetReason() == 0, sourceFinal || !p.streaming)
				if err := p.audio.endSource(); err != nil {
					p.cancel()
					n.mu.Unlock()
					return err
				}
				if flushPending {
					if tail := p.audio.flushPending(0, 0); len(tail) > 0 {
						frames = append(frames, append(binary.BigEndian.AppendUint32(nil, p.audioStream), tail...))
					}
				}
				if !act.Finalize {
					// Intermediate close (progress prime or streamed chunk):
					// swallow it so the firmware keeps the joined stream open;
					// silence keepalive bridges the gap to the next source.
					if act.ArmSilence {
						p.flow.waiting = true
						n.waitAudio(p)
					}
					if act.CloseProgressDone {
						close(p.progressAudioDone)
					}
					n.finishSynthLocked(p)
					n.mu.Unlock()
					for _, fr := range frames {
						if err := n.down(ctx, k, fr); err != nil {
							return err
						}
					}
					return nil
				}
				n.finishSynthLocked(p)
				end := p.audio.finish()
				frames = append(frames, append(binary.BigEndian.AppendUint32(nil, p.audioStream), end...))
				sc.StreamId = int64(p.audioStream)
				n.metrics.sourcesFinalized.Add(1)
				slog.Info("native_stream_finalize", "request", requestTag(p.id), "stream", sc.GetStreamId(), "via", "close", "final", sourceFinal)
			}
			sc.MessageId = p.id
			finalSpeech := sc.GetClose() != nil && (!p.streaming || !p.flow.progress)
			pctx := p.ctx
			n.mu.Unlock()
			for _, fr := range frames {
				if err := n.down(ctx, k, fr); err != nil {
					return err
				}
			}
			out, err := f.Marshal()
			if err != nil {
				return err
			}
			err = n.down(ctx, k, out)
			if err == nil && finalSpeech {
				reply.Delivered(pctx)
			}
			return err
		}
		drop := original != nil && original.local && (original.header.StreamId == nil || sc.GetStreamId() != original.header.GetStreamId())
		n.mu.Unlock()
		if drop {
			return nil
		}
		return n.down(ctx, k, data)
	}
	e := f.Server.GetEvent()
	if e == nil {
		return n.down(ctx, k, data)
	}

	n.mu.Lock()
	if f.Kind() == "TtsSpeak" {
		delete(n.streams, uint32(e.GetHeader().GetStreamId()))
		delete(n.blocked, uint32(e.GetHeader().GetStreamId()))
	}
	p, isTTS := n.findLocked(e.GetHeader())
	if r := e.GetAliceResponse(); r != nil {
		var ds []string
		for _, d := range r.GetResponse().GetDirectives() {
			ds = append(ds, d.GetType()+":"+d.GetName())
		}

		slog.Info("native_cloud_shape", "matched", p != nil, "directives", ds, "cards", len(r.GetResponse().GetCards()), "voice_text_bytes", len(r.GetVoiceResponse().GetOutputSpeech().GetText()))
	}
	if p == nil {
		n.mu.Unlock()
		return n.down(ctx, k, data)
	}
	if isTTS {
		if p.canceled {
			n.mu.Unlock()
			return nil
		}
		spokenTTS := n.ttsKeyLocked(e.GetHeader())
		e.Header.RefMessageId = p.id
		if ref := e.Header.SubRequestRef; ref != nil {
			ref.MessageId = p.id
			ref.RequestId = p.request.GetHeader().GetRequestId()
		}
		if e.GetTtsSpeak() != nil {
			n.streams[uint32(e.Header.GetStreamId())] = p
			if p.streaming || p.markedTTS {
				if e.GetTtsSpeak().GetFormat() != "audio/opus" {
					p.cancel()
					n.mu.Unlock()
					return errors.New("joining speech segments requires Opus")
				}
				// Judge progress-ness by the spoken TTS message itself: streamed
				// chunks rotate p.ttsID while the prime keeps its own ID.
				p.flow.progress = spokenTTS != "" && spokenTTS == p.progressTTS
				// An envelope that opens a synth stream claims its FIFO entry
				// the same way the first raw frame would: the claim carries the
				// source's finality (only the request's last synthesis may end
				// the joined stream with EOS).
				n.claimEnvelopeSynthLocked(p, uint32(e.Header.GetStreamId()))
				p.audio.next()
				p.audio.source.discardAudio = p.flow.progress
				if p.flow.started {
					p.flow.sourceStart()
					n.mu.Unlock()
					return nil
				}
				p.audioStream = uint32(e.Header.GetStreamId())
				p.flow.sourceStart()
			}
			slog.Info("native_tts_ready", "request", requestTag(p.id), "format", e.GetTtsSpeak().GetFormat(), "stream", e.Header.GetStreamId())
		}
		n.mu.Unlock()
		out, err := f.Marshal()
		if err != nil {
			return err
		}
		return n.down(ctx, k, out)
	}

	if f.Kind() == "TtsSpeak" {
		p.ttsStarted = true
		if p.local || p.canceled {
			n.blocked[uint32(e.GetHeader().GetStreamId())] = p
		}
	}
	if p.local || p.canceled {
		n.mu.Unlock()
		return nil
	}
	if r := e.GetAliceResponse(); r != nil && p.cloud && !p.ttsStarted && canReplaceResponse(r) {
		text := ""
		for _, card := range r.GetResponse().GetCards() {
			text += card.GetText() + " "
		}
		if strings.TrimSpace(text) == "" {
			text = r.GetVoiceResponse().GetOutputSpeech().GetText()
		}

		if n.caps.fallback != nil && n.isRefusal(text) {
			p.local = true
			if n.OnRoute != nil {
				n.OnRoute(p.id, "llm")
			}
			p.ttsID = newID()
			n.tts[p.ttsID] = p
			query := p.text
			n.mu.Unlock()
			n.wg.Go(func() {
				result, err := n.resolveFallback(p, n.caps.fallback, query)
				if p.ctx.Err() != nil {
					return
				}
				if errors.Is(err, context.Canceled) {
					n.discardStale(p)
					return
				}
				if err != nil {
					n.discardSound(p)
					result.Answer = "Сейчас не получилось получить ответ локальной модели."
				}
				_ = n.sendCancel(p.ctx, p.id)
				if err := n.reply(p, result); err != nil {
					slog.Warn("native_reply_failed", "request", requestTag(p.id))
				}
				slog.Info("native_fallback", "request", requestTag(p.id), "route", "llm")
			})
			return nil
		}
	}
	emptyFinal := false
	switch f.Kind() {
	case "AliceResponse":
		if !p.cloud {
			p.canceled = true
			p.cancel()
			slog.Warn("native_approval_not_honored", "request", requestTag(p.id))
		}
	case "ServerRequestReady":
		slog.Info("native_server_ready", "request", requestTag(p.id))
	case "AsrResult":
		asr := e.GetAsrResult()
		aggregate := asr.GetAggregatedRecognition()
		if aggregate != nil && (!p.aggregateSeen || (aggregate.GetIsFinal() && !p.aggregateFinalSeen)) {
			p.aggregateSeen = true
			p.aggregateFinalSeen = p.aggregateFinalSeen || aggregate.GetIsFinal()
			slog.Info("native_asr_aggregate", "request", requestTag(p.id), "end_of_utt", asr.GetEndOfUtt(), "aggregate_present", true, "aggregate_final", aggregate.GetIsFinal(), "aggregate_index", aggregate.GetIndex(), "aggregate_text_bytes", len(aggregate.GetRecognizedText()))
		}
		if asr.GetEndOfUtt() {
			p.final = true
			slog.Info("native_asr_final", "request", requestTag(p.id), "hypotheses", len(asr.GetRecognition()), "code", asr.GetResponseCode().String(), "aggregate_present", aggregate != nil, "aggregate_final", aggregate.GetIsFinal(), "aggregate_index", aggregate.GetIndex(), "aggregate_text_bytes", len(aggregate.GetRecognizedText()))
		}
		if asr.GetEndOfUtt() && len(asr.GetRecognition()) > 0 {
			r := asr.GetRecognition()[0]
			p.text = r.GetNormalized()
			if p.text == "" {
				var words []string
				for _, w := range r.GetWords() {
					words = append(words, w.GetValue())
				}
				p.text = strings.Join(words, " ")
			}
		}
		emptyFinal = !p.started && asr.GetEndOfUtt() && asr.GetResponseCode() == asrcommon.EAsrResponseCode_OK && strings.TrimSpace(p.text) == ""

	}
	n.mu.Unlock()
	if emptyFinal {
		if n.caps.emptyInput != nil {
			n.caps.emptyInput(p.ctx)
		}
	}
	n.maybeResolve(p)
	if err := n.down(ctx, k, data); err != nil {
		return err
	}
	n.captureCloud(p, e)
	return nil
}
func (n *Native) maybeResolve(p *nativeRequest) {
	n.mu.Lock()
	if p.started || p.canceled || (strings.TrimSpace(p.text) == "" && !p.final) {
		n.mu.Unlock()
		return
	}
	p.started = true
	audioInput := p.audioInput
	p.audioInput = nil
	p.audioInputDone = true
	text := p.text
	n.mu.Unlock()
	n.wg.Go(func() {
		if os.Getenv("QUASAR_AUTO_WHISPER") == "1" && strings.TrimSpace(text) != "" && len(audioInput) > 0 {
			inputBytes := len(audioInput)
			pcm, err := decodeWhisperOpus(audioInput)
			audioInput = nil
			if err == nil {
				m := acousticWhisper(pcm)
				n.mu.Lock()
				p.inputWhisper = m.Whisper
				n.mu.Unlock()
				slog.Info("acoustic_whisper", "request", requestTag(p.id), "whisper", m.Whisper, "active_frames", m.Active, "zcr", m.ZCR, "periodicity", m.Periodicity, "voiced_fraction", m.Voiced)
			} else {
				slog.Info("acoustic_whisper_unavailable", "request", requestTag(p.id), "audio_bytes", inputBytes, "reason", err.Error())
			}
		}
		if n.DecisionDelay > 0 {
			select {
			case <-time.After(n.DecisionDelay):
			case <-p.ctx.Done():
				return
			}
		}
		result := Resolution{Route: "yandex"}
		var err error
		if n.resolver != nil && strings.TrimSpace(text) != "" {
			if n.caps.routed != nil {
				routed := n.caps.routed
				decision := routed.Decide(p.ctx, text)
				n.mu.Lock()
				p.listenOnStreamStart = decision.Source == "dialogue_retry"
				n.mu.Unlock()
				slog.Info("native_route_decision", "request", requestTag(p.id), "route", decision.Route, "source", decision.Source)
				stop := func() {}
				if decision.Route != "yandex" && n.ProgressAfter > 0 {
					stop = n.progress(p)
				}
				if n.streamable(decision) {
					// Stream the main llm route through the same deferred-part
					// machinery as cloud refusals: first sentences speak while
					// the model is still generating.
					slog.Info("native_stream_route", "request", requestTag(p.id), "source", decision.Source)
					result, err = n.resolveFallback(p, n.caps.fallback, text)
				} else {
					result, err = routed.ResolveDecision(p.ctx, text, decision)
				}
				stop()
			} else {
				result, err = n.resolver.Resolve(p.ctx, text)
			}
		}
		if p.ctx.Err() != nil {
			return
		}
		if errors.Is(err, context.Canceled) {
			n.discardStale(p)
			return
		}
		if err != nil {
			n.discardSound(p)
			if result.Route == "recovery" {
				slog.Warn("native_recovery_failed", "request", requestTag(p.id), "error", err)
				result.Answer = "Не удалось запланировать аварийное восстановление. Проверьте журнал Quasar Bridge и выполните восстановление по SSH."
			} else if result.Route == "diagnostics" {
				slog.Warn("native_diagnostics_failed", "request", requestTag(p.id), "error", err)
				result.Answer = "Не удалось изменить сетевой доступ к веб-интерфейсу. Проверьте журнал Quasar Bridge."
			} else if result.Route == "home" {
				result.Answer = "Не удалось подтвердить выполнение команды в умном доме."
			} else if result.Route == "airplay" {
				result.Answer = "Не удалось изменить состояние AirPlay. Проверьте журнал Quasar Bridge."
			} else if result.Route == "music" {
				slog.Warn("native_music_failed", "request", requestTag(p.id), "error", err)
				result.Answer = "Не удалось включить музыку с этой страницы."
			} else {
				result.Route = "llm"
				result.Answer = "Сейчас не получилось получить ответ локальной модели."
			}
		}
		n.mu.Lock()
		if p.canceled {
			n.mu.Unlock()
			return
		}
		if n.OnRoute != nil {
			n.OnRoute(p.id, result.Route)
		}
		p.route = result.Route
		if result.Route == "yandex" {
			p.cloud = true
			var err error
			if p.gated {
				control := p.continuation
				if control == nil {
					control, err = n.makeContinue(p)
				}
				if err == nil {
					err = n.up(p.ctx, websocket.MessageBinary, control)
				}
			} else {
				err = n.up(p.ctx, websocket.MessageBinary, p.input)
				if err == nil && p.continuation != nil {
					err = n.up(p.ctx, websocket.MessageBinary, p.continuation)
				}
			}
			n.mu.Unlock()
			slog.Info("native_route", "request", requestTag(p.id), "route", "yandex", "sent", err == nil)
			return
		}
		p.local = true
		if !p.streaming {
			p.ttsID = newID()
			n.tts[p.ttsID] = p
		}
		n.mu.Unlock()
		slog.Info("native_route", "request", requestTag(p.id), "route", result.Route)
		if p.gated && !p.streaming {
			if err := n.sendCancel(p.ctx, p.id); err != nil {
				return
			}
		}
		if err := n.reply(p, result); err != nil {
			slog.Warn("native_reply_failed", "request", requestTag(p.id))
		}
	})
}
func (n *Native) sendCancel(ctx context.Context, id string) error {
	b, err := cancelFrame(id)
	if err != nil {
		return err
	}
	return n.up(ctx, websocket.MessageBinary, b)
}
func (n *Native) reply(p *nativeRequest, result Resolution) error {
	// The station's spotter hears its own speaker: a wake word inside the
	// answer triggers a false barge-in mid-answer (upstream issue #53).
	// Strip it from both the displayed text and the speech before any
	// prefix-trim, so the spoken tail stays consistent with emitted chunks
	// (they are stripped at emission too).
	text := stripWakeWords(result.Answer)
	speech := stripWakeWords(cmp.Or(result.speech, result.Answer))
	if p.streaming {
		p.emitMu.Lock()
		emitted := p.emittedText
		p.emitMu.Unlock()
		if emitted != "" {
			// Streamed chunks already spoke the prefix; send and synthesize
			// only the remaining tail so the full answer is spoken exactly
			// once. When the final text diverges from what was spoken
			// (provider rephrased the beginning, normalization differences),
			// trim to the longest common word-aligned prefix instead of
			// silently re-speaking the whole answer.
			var textCut, speechCut int
			var textExact, speechExact bool
			text, textCut, textExact = trimSpokenPrefix(text, emitted)
			speech, speechCut, speechExact = trimSpokenPrefix(speech, emitted)
			if !textExact || !speechExact {
				n.metrics.tailDivergences.Add(1)
				slog.Warn("native_tail_diverged",
					"request", requestTag(p.id),
					"emitted_bytes", len(emitted),
					"text_prefix_bytes", textCut,
					"speech_prefix_bytes", speechCut,
					"total", n.metrics.tailDivergences.Load())
			}
		}
		return n.finishDeferred(p, text, speech)
	}
	requestID := p.request.GetHeader().GetRequestId()
	if requestID == "" {
		requestID = p.id
	}
	silentPlayback := n.hasSilentPlayback(p)
	directives := n.takeSound(p)
	b, err := replyFrame(replyArgs{
		refMessageID: p.id,
		requestID:    requestID,
		dialogID:     p.request.GetHeader().GetDialogId(),
		sequence:     new(p.request.GetHeader().GetSequenceNumber()),
		text:         text,
		speech:       speech,
		shouldListen: n.ShouldListen && !silentPlayback && reply.ListeningAllowed(p.ctx),
		silent:       silentPlayback,
		directives:   directives,
	})
	if err != nil {
		return err
	}
	if !p.gated && !p.cloud {
		if err := n.ack(p); err != nil {
			return err
		}
	}
	if err := n.down(p.ctx, websocket.MessageBinary, b); err != nil {
		return err
	}
	n.captureDelivered(p, text, true, silentPlayback || len(directives) > 0, true)
	if silentPlayback {
		return nil
	}
	// Single-shot answer: this source is the request's only and last one.
	return n.synthesizeFinal(p, speech)
}

// synthesize requests TTS from the upstream. UniProxy answers with raw
// stream frames only (no TtsSpeak envelope), so the request is queued in
// pendingSynth; the first frame of the next unknown stream claims it.
func (n *Native) synthesize(p *nativeRequest, text string) error {
	return n.synthesizeClaim(p, text, false, false)
}
func (n *Native) synthesizeFinal(p *nativeRequest, text string) error {
	return n.synthesizeClaim(p, text, false, true)
}
func (n *Native) synthesizeClaim(p *nativeRequest, text string, discard, final bool) error {
	if p.inputWhisper {
		// Applied in the single synthesis funnel so streamed chunks get
		// the whisper markup too (moved here from synthesize during the
		// v0.8.0 rebase).
		text = `<speaker is_whisper="true">` + text
		slog.Info("tts_whisper_enabled", "request", requestTag(p.id))
	}
	if strings.Contains(text, "<speaker") {
		n.mu.Lock()
		p.markedTTS = true
		n.mu.Unlock()
	}
	// UniProxy synthesizes concurrent TtsGenerate requests in parallel, which
	// interleaves speech mid-sentence once joined. Serialize per request: wait
	// for the previous stream to end before issuing the next one.
	n.awaitSynth(p)
	n.mu.Lock()
	if p.canceled || p.ctx.Err() != nil {
		n.mu.Unlock()
		return p.ctx.Err()
	}
	p.curSynthDone = make(chan struct{})
	p.synths++
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p, text: text, discard: discard, final: final, issued: time.Now()})
	n.metrics.claimsIssued.Add(1)
	n.mu.Unlock()
	b, err := ttsGenerateFrame(p.ttsID, text)
	if err != nil {
		n.finishSynthLocked(p)
		return err
	}
	if err := n.up(p.ctx, websocket.MessageBinary, b); err != nil {
		n.finishSynthLocked(p)
		n.mu.Lock()
		for i, c := range n.pendingSynth {
			if c.p == p && c.discard == discard {
				n.pendingSynth = append(n.pendingSynth[:i], n.pendingSynth[i+1:]...)
				break
			}
		}
		n.mu.Unlock()
		return err
	}
	return nil
}

// awaitSynth blocks until the previously issued TTS stream ended (or timed
// out / the request died), keeping chunk synthesis strictly sequential.
func (n *Native) awaitSynth(p *nativeRequest) {
	n.mu.Lock()
	ch := p.curSynthDone
	n.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case <-ch:
	case <-time.After(synthWaitLimit):
	case <-p.ctx.Done():
	}
}

// finishSynthLocked releases waiters for the current synthesis slot. Called
// with n.mu held; safe to call repeatedly.
func (n *Native) finishSynthLocked(p *nativeRequest) {
	if ch := p.curSynthDone; ch != nil {
		p.curSynthDone = nil
		close(ch)
	}
}
func requestTag(id string) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("%x", sum[:4])
}

// Quasar uses Yandex TGUID spelling (8-8-8-8), not RFC UUID spelling.
func newID() string {
	return sound.NewStreamID()
}

func (n *Native) makeContinue(p *nativeRequest) ([]byte, error) {
	return continueFrame(p.header, p.id)
}

func (n *Native) ack(p *nativeRequest) error {
	b, err := ackFrame(p.id)
	if err != nil {
		return err
	}
	return n.down(p.ctx, websocket.MessageBinary, b)
}

func canReplaceResponse(r *response.TAliceResponseProto) bool {
	for _, d := range r.GetResponse().GetDirectives() {
		if d.GetType() != "client_action" || d.GetName() != "tts_play_placeholder" {
			return false
		}
	}
	return true
}

func (n *Native) isRefusal(text string) bool {
	if n.caps.isRefusal != nil {
		return n.caps.isRefusal(text)
	}
	return IsRefusal(text)
}

func (n *Native) discardStale(p *nativeRequest) {
	// Soft teardown: the reply path keeps using p to finish protocol
	// bookkeeping; only the upstream synthesis is canceled.
	n.mu.Lock()
	n.teardownLocked(p, false)
	n.mu.Unlock()
	_ = n.sendCancel(n.ctx, p.id)
}

// pruneSynthLocked drops queued TTS claims for a finished request. Called with
// n.mu held.
func (n *Native) pruneSynthLocked(p *nativeRequest) {
	kept := n.pendingSynth[:0]
	for _, c := range n.pendingSynth {
		if c.p != p {
			kept = append(kept, c)
		}
	}
	n.pendingSynth = kept
}

type fallbackStreamer interface {
	FallbackStream(context.Context, string, func(string)) (Resolution, error)
}

// streamable reports whether the main llm route may stream its answer:
// streaming is enabled, the resolver supports it, and the decision is a plain
// llm answer (retry/reset variants need their own ResolveDecision handling).
func (n *Native) streamable(decision RouteDecision) bool {
	return n.StreamAnswers && decision.Streamable() && n.caps.streamer != nil
}

// resolveFallback answers a cloud refusal, streaming deltas through the
// deferred-response protocol when the resolver supports it.
func (n *Native) resolveFallback(p *nativeRequest, resolver FallbackResolver, query string) (Resolution, error) {
	streamer, canStream := resolver.(fallbackStreamer)
	if !n.StreamAnswers || !canStream {
		return resolver.Fallback(p.ctx, query)
	}
	chunks := newSentenceBuffer(func(chunk string) {
		if err := n.emitChunk(p, chunk); err != nil {
			slog.Warn("native_stream_chunk_failed", "request", requestTag(p.id), "error", err)
		}
	})
	defer chunks.Close()
	return streamer.FallbackStream(p.ctx, query, chunks.Add)
}

// emitChunk opens the deferred stream on first use (mirroring progress) and
// delivers one answer chunk as a non-final deferred part with its own TTS
// synthesis.
func (n *Native) emitChunk(p *nativeRequest, chunk string) error {
	// The station's spotter hears its own speaker: a wake word inside a
	// chunk would trigger a false barge-in and kill the answer (upstream
	// issue #53). Strip before the chunk is shown, counted as emitted and
	// synthesized so all three stay consistent.
	chunk = stripWakeWords(chunk)
	if strings.TrimSpace(chunk) == "" {
		return nil
	}
	n.mu.Lock()
	if p.canceled || p.ctx.Err() != nil {
		n.mu.Unlock()
		return p.ctx.Err()
	}
	needBegin := !p.streaming
	if needBegin {
		p.streaming = true
		p.local = true
		p.responseID = newID()
		p.ttsID = newID()
		p.progressTTS = p.ttsID
		p.progressAudioDone = make(chan struct{})
		n.tts[p.ttsID] = p
	} else {
		p.ttsID = newID()
		n.tts[p.ttsID] = p
	}
	n.mu.Unlock()
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	if needBegin {
		if p.gated {
			if err := n.sendCancel(p.ctx, p.id); err != nil {
				return err
			}
		} else if err := n.ack(p); err != nil {
			return err
		}
		if err := n.beginDeferred(p); err != nil {
			return err
		}
		// beginDeferred synthesized the priming "а" under the progress TTS ID;
		// the chunk needs its own correlation ID.
		n.mu.Lock()
		p.ttsID = newID()
		n.tts[p.ttsID] = p
		n.mu.Unlock()
	} else if err := p.ctx.Err(); err != nil {
		return err
	}
	if err := n.deferredPart(p, chunk, false); err != nil {
		return err
	}
	p.emittedText += chunk
	return n.synthesize(p, chunk)
}
