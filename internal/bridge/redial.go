package bridge

// Transparent upstream re-dial support: after the proxy swaps a dead
// upstream connection, Native re-establishes the session state that lived
// only on the old wire.

import (
	"context"
	"log/slog"

	"github.com/coder/websocket"
)

// UpstreamRedialed replays upstream-only state onto the fresh connection:
//   - cloud-routed requests get their original input (plus continuation when
//     present) or, when gated, the Continue frame re-sent;
//   - TTS synthesis claims still awaiting their raw audio stream are
//     re-issued verbatim (FIFO order preserved).
//
// Voice frames already relayed from an in-flight ASR request cannot be
// replayed; those requests degrade exactly as they would have without the
// re-dial. Local (LLM) answers are unaffected: they never touched the wire.
func (n *Native) UpstreamRedialed(ctx context.Context) {
	n.mu.Lock()
	n.metrics.redials.Add(1)
	var frames []frame
	replayed := 0
	for _, p := range n.requests {
		if p.canceled || p.local || !p.cloud {
			continue
		}
		if p.gated {
			control := p.continuation
			if control == nil {
				b, err := n.makeContinue(p)
				if err != nil {
					continue
				}
				control = b
			}
			frames = append(frames, frame{p.id, control})
			continue
		}
		frames = append(frames, frame{p.id, p.input})
		if p.continuation != nil {
			frames = append(frames, frame{p.id, p.continuation})
		}
	}
	for _, c := range n.pendingSynth {
		if c.p == nil || c.p.canceled {
			continue
		}
		if b, err := ttsGenerateFrame(c.p.ttsID, c.text); err == nil {
			frames = append(frames, frame{c.p.id, b})
			replayed++
		}
	}
	n.metrics.claimsReplayed.Add(int64(replayed))
	n.mu.Unlock()
	sent := 0
	for _, fr := range frames {
		if err := n.up(ctx, websocket.MessageBinary, fr.data); err != nil {
			slog.Warn("native_redial_replay_failed", "request", requestTag(fr.id), "error", err.Error())
			continue
		}
		sent++
	}
	slog.Info("native_redial_replayed", "frames", len(frames), "sent", sent, "claims", replayed)
}

type frame struct {
	id   string
	data []byte
}
