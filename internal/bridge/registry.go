package bridge

// Request registry teardown. Every path that kills a request — Close,
// expiry timer, upstream CancelRequest, barge-in, discardStale — goes
// through teardownLocked so no path can forget to release its TTS claims
// (a forgotten claim blocks the next awaitSynth for synthWaitLimit).

import (
	"log/slog"
	"time"
)

// teardownLocked fully deactivates one request: marks it canceled, cancels
// its context, releases queued synth claims and waiters, stops the expiry
// timer, and — when remove is set — drops it from every registry map.
// Called with n.mu held. Safe to call repeatedly.
func (n *Native) teardownLocked(p *nativeRequest, remove bool) {
	p.canceled = true
	p.cancel()
	n.pruneSynthLocked(p)
	n.finishSynthLocked(p)
	if p.expires != nil {
		p.expires.Stop()
	}
	if !remove {
		return
	}
	slog.Info("native_request_done",
		"request", requestTag(p.id),
		"route", p.route,
		"streaming", p.streaming,
		"finalized", p.flow.finalized,
		"synths", p.synths,
		"ms", time.Since(p.arrived).Milliseconds(),
		"redials", n.metrics.redials.Load(),
		"overlaps", n.metrics.sourceOverlaps.Load())
	delete(n.requests, p.id)
	delete(n.tts, p.ttsID)
	delete(n.tts, p.progressTTS)
	for s, q := range n.streams {
		if q == p {
			delete(n.streams, s)
			delete(n.streamFinal, s)
		}
	}
	for s, q := range n.blocked {
		if q == p {
			delete(n.blocked, s)
		}
	}
}
