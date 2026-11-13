package bridge

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// rawStreamFrame builds a station-bound raw audio frame: [4-byte BE stream
// id][Ogg pages]. ends controls the final page's EOS flag so tests can model
// cleanly ended and abruptly cut sources.
func rawStreamFrame(sid uint32, ends bool) []byte {
	o := oggSpeech{serial: sid}
	head := append([]byte("OpusHead"), []byte{1, 1, 0, 0, 0x80, 0xbb, 0, 0, 0, 0, 0}...)
	tags := append([]byte("OpusTags"), make([]byte, 8)...)
	flags := byte(4)
	if !ends {
		flags = 0
	}
	b := o.page(head, 2, 0)
	b = append(b, o.page(tags, 0, 0)...)
	b = append(b, o.page([]byte{0xf8, 0xff, 0xfe}, flags, 960)...)
	return append(binary.BigEndian.AppendUint32(nil, sid), b...)
}

func newMetricsNative(t *testing.T) (*Native, chan []byte) {
	t.Helper()
	down := make(chan []byte, 20)
	n := NewNative(t.Context(), func(context.Context, websocket.MessageType, []byte) error { return nil },
		func(_ context.Context, _ websocket.MessageType, b []byte) error {
			select {
			case down <- b:
			default:
			}
			return nil
		}, nil)
	t.Cleanup(n.Close)
	return n, down
}

func newMetricsRequest(n *Native, id string) *nativeRequest {
	ctx, cancel := context.WithCancel(n.ctx)
	p := &nativeRequest{id: id, ctx: ctx, cancel: cancel, streaming: true, arrived: time.Now()}
	n.mu.Lock()
	n.requests[id] = p
	n.mu.Unlock()
	return p
}

// A raw audio stream with an empty pendingSynth FIFO must be counted as
// unclaimed and passed through untouched.
func TestMetricsUnclaimedStream(t *testing.T) {
	n, down := newMetricsNative(t)
	if err := n.Server(t.Context(), websocket.MessageBinary, rawStreamFrame(77, true)); err != nil {
		t.Fatal(err)
	}
	if got := n.metrics.claimsUnclaimed.Load(); got != 1 {
		t.Fatalf("claimsUnclaimed=%d, want 1", got)
	}
	select {
	case b := <-down:
		if binary.BigEndian.Uint32(b) != 77 {
			t.Fatalf("pass-through frame lost its stream id")
		}
	default:
		t.Fatal("unclaimed stream was not passed through")
	}
}

// A claim bound while its request's previous source never ended cleanly is a
// correlation anomaly (FIFO reorder, synthWaitLimit escape, or abrupt close):
// it must be counted so the joined audio is diagnosable from the log.
func TestMetricsSourceOverlap(t *testing.T) {
	n, _ := newMetricsNative(t)
	p := newMetricsRequest(n, "overlap-req")
	n.mu.Lock()
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p, text: "one"})
	n.mu.Unlock()
	// First source is cut without an EOS page: it never ends cleanly.
	if err := n.Server(t.Context(), websocket.MessageBinary, rawStreamFrame(10, false)); err != nil {
		t.Fatal(err)
	}
	if got := n.metrics.sourceOverlaps.Load(); got != 0 {
		t.Fatalf("sourceOverlaps=%d after first bind, want 0", got)
	}
	n.mu.Lock()
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p, text: "two"})
	n.mu.Unlock()
	if err := n.Server(t.Context(), websocket.MessageBinary, rawStreamFrame(11, true)); err != nil {
		t.Fatal(err)
	}
	if got := n.metrics.sourceOverlaps.Load(); got != 1 {
		t.Fatalf("sourceOverlaps=%d after overlapping bind, want 1", got)
	}
	if got := n.metrics.claimsBoundRaw.Load(); got != 2 {
		t.Fatalf("claimsBoundRaw=%d, want 2", got)
	}
}

// A cleanly ended previous source must not trip the overlap detector: this is
// the normal streamed-chunk cadence.
func TestMetricsNoOverlapAfterCleanEnd(t *testing.T) {
	n, _ := newMetricsNative(t)
	p := newMetricsRequest(n, "clean-req")
	n.mu.Lock()
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p, text: "one"})
	n.mu.Unlock()
	if err := n.Server(t.Context(), websocket.MessageBinary, rawStreamFrame(20, true)); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p, text: "two"})
	n.mu.Unlock()
	if err := n.Server(t.Context(), websocket.MessageBinary, rawStreamFrame(21, true)); err != nil {
		t.Fatal(err)
	}
	if got := n.metrics.sourceOverlaps.Load(); got != 0 {
		t.Fatalf("sourceOverlaps=%d after clean cadence, want 0", got)
	}
}

// UpstreamRedialed counts re-dials and replayed synthesis claims.
func TestMetricsRedialReplay(t *testing.T) {
	n, _ := newMetricsNative(t)
	p := newMetricsRequest(n, "redial-req")
	p.ttsID = "tts-1"
	n.mu.Lock()
	n.pendingSynth = append(n.pendingSynth, synthClaim{p: p, text: "replay me"})
	n.mu.Unlock()
	n.UpstreamRedialed(t.Context())
	if got := n.metrics.redials.Load(); got != 1 {
		t.Fatalf("redials=%d, want 1", got)
	}
	if got := n.metrics.claimsReplayed.Load(); got != 1 {
		t.Fatalf("claimsReplayed=%d, want 1", got)
	}
}
