package bridge

import (
	"encoding/binary"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/websocket"
	directive "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/directives"
	"quasarbridge/internal/reply"
)

// progress starts only after the executor is known. Stop joins the pump so no
// partial can race with the final answer or continue after cancellation.
func (n *Native) progress(p *nativeRequest) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	n.wg.Go(func() {
		defer close(done)
		timer := time.NewTimer(n.ProgressAfter)
		defer timer.Stop()
		select {
		case <-stop:
			return
		case <-p.ctx.Done():
			return
		case <-timer.C:
		}
		n.mu.Lock()
		if p.canceled || p.ctx.Err() != nil {
			n.mu.Unlock()
			return
		}
		needBegin := !p.streaming
		if needBegin {
			p.local = true
			p.streaming = true
			p.responseID = newID()
			p.ttsID = newID()
			p.progressTTS = p.ttsID
			p.progressAudioDone = make(chan struct{})
			n.tts[p.ttsID] = p
		}
		n.mu.Unlock()
		// Hold emitMu only while sending, never across waits: streamed answer
		// chunks share this mutex, and holding it for the whole keepalive
		// lifetime would block every chunk until the answer completes.
		p.emitMu.Lock()
		if needBegin {
			if p.gated {
				if n.sendCancel(p.ctx, p.id) != nil {
					p.emitMu.Unlock()
					p.cancel()
					return
				}
			} else {
				if n.ack(p) != nil {
					p.emitMu.Unlock()
					p.cancel()
					return
				}
			}
			if n.beginDeferred(p) != nil {
				p.emitMu.Unlock()
				p.cancel()
				return
			}
		}
		p.emitMu.Unlock()
		slog.Info("native_progress_started", "request", requestTag(p.id))
		interval := n.ProgressInterval
		if interval <= 0 {
			interval = 4 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				p.emitMu.Lock()
				err := n.deferredPart(p, "", false)
				p.emitMu.Unlock()
				if err != nil {
					p.cancel()
					return
				}
			}
		}
	})
	return func() { close(stop); <-done }
}
func (p *nativeRequest) applicationID() string {
	if id := p.request.GetHeader().GetRequestId(); id != "" {
		return id
	}
	return p.id
}
func (n *Native) beginDeferred(p *nativeRequest) error {
	// Station schedules post-speech listening from the initial stream envelope.
	// Only retry is guaranteed informational; other routes may start silent media.
	shouldListen := n.ShouldListen && p.listenOnStreamStart && reply.ListeningAllowed(p.ctx)
	b, err := streamStartFrame(streamStartArgs{
		refMessageID: p.id,
		requestID:    p.applicationID(),
		dialogID:     p.request.GetHeader().GetDialogId(),
		sequence:     new(p.request.GetHeader().GetSequenceNumber()),
		responseID:   p.responseID,
		shouldListen: shouldListen,
	})
	if err != nil {
		return err
	}
	if err := n.down(p.ctx, websocket.MessageBinary, b); err != nil {
		return err
	}
	if err := n.deferredPart(p, "", false); err != nil {
		return err
	}
	// Obtain the provider's actual Opus headers; its speech payload is discarded.
	return n.synthesizeClaim(p, "а", true, false)
}
func (n *Native) deferredPart(p *nativeRequest, text string, last bool) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	p.partial++
	voice := text
	var directives []*directive.TDirective
	shouldListen := false
	if last {
		silentPlayback := n.hasSilentPlayback(p)
		shouldListen = n.ShouldListen && !silentPlayback && reply.ListeningAllowed(p.ctx)
		if silentPlayback {
			voice = ""
		}
		directives = n.takeSound(p)
	}
	b, err := deferredPartFrame(deferredPartArgs{
		refMessageID: p.id,
		requestID:    p.applicationID(),
		partial:      p.partial,
		last:         last,
		text:         text,
		voice:        voice,
		shouldListen: shouldListen,
		directives:   directives,
	})
	if err != nil {
		return err
	}
	if err := n.down(p.ctx, websocket.MessageBinary, b); err != nil {
		return err
	}
	n.captureDelivered(p, text, last, len(directives) > 0, true)
	return nil
}
func (n *Native) finishDeferred(p *nativeRequest, text, speech string) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.progressAudioDone:
	}
	if n.hasSilentPlayback(p) {
		if err := n.finishSilentAudio(p); err != nil {
			return err
		}
		return n.deferredPart(p, text, true)
	}
	n.mu.Lock()
	p.ttsID = newID()
	n.tts[p.ttsID] = p
	p.finalSynth = true
	if strings.TrimSpace(speech) == "" {
		// No tail to synthesize: finalize the joined audio right away unless
		// a source is still in flight (its end triggers the same path).
		var inFlight bool
		for _, c := range n.pendingSynth {
			if c.p == p {
				inFlight = true
				break
			}
		}
		if p.flow.finalizeNoTail(p.audio.sourceEnded(), inFlight) {
			n.metrics.sourcesFinalized.Add(1)
			slog.Info("native_stream_finalize", "request", requestTag(p.id), "stream", p.audioStream, "via", "no_tail", "final", true)
			if err := p.audio.endSource(); err == nil {
				if end := p.audio.finish(); len(end) > 0 {
					_ = n.down(p.ctx, websocket.MessageBinary, append(binary.BigEndian.AppendUint32(nil, p.audioStream), end...))
				}
			}
		}
	}
	n.mu.Unlock()
	p.emitMu.Lock()
	defer p.emitMu.Unlock()
	if err := n.deferredPart(p, text, true); err != nil {
		return err
	}
	if strings.TrimSpace(speech) == "" {
		return nil
	}
	// The tail is the request's last synthesis: only its end may emit EOS.
	return n.synthesizeFinal(p, speech)
}

// Content audio must start after the silent Dialog player releases its focus.
func (n *Native) finishSilentAudio(p *nativeRequest) error {
	n.mu.Lock()
	p.flow.waiting = false
	end := p.audio.finishSilence()
	streamID := p.audioStream
	n.mu.Unlock()
	if err := n.down(p.ctx, websocket.MessageBinary, append(binary.BigEndian.AppendUint32(nil, streamID), end...)); err != nil {
		return err
	}
	b, err := streamCloseFrame(p.id, streamID)
	if err != nil {
		return err
	}
	return n.down(p.ctx, websocket.MessageBinary, b)
}

// Called under n.mu after the first synthesized segment closes. Keep the audio
// alive through final TTS synthesis, until its TtsSpeak starts the next segment.
func (n *Native) waitAudio(p *nativeRequest) {
	n.wg.Go(func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				n.mu.Lock()
				if p.canceled || !p.flow.waiting {
					n.mu.Unlock()
					return
				}
				b := append(binary.BigEndian.AppendUint32(nil, p.audioStream), p.audio.silence()...)
				n.metrics.silencesPumped.Add(1)
				err := n.down(p.ctx, websocket.MessageBinary, b)
				n.mu.Unlock()
				if err != nil {
					p.cancel()
					return
				}
			}
		}
	})
}
