package bridge

// Per-connection counters for the speech pipeline. They turn voice-reported
// glitches into log data: the FIFO-correlation anomaly detectors and the
// finalize paths below bump a counter and emit an event line, so a single
// answer's health can be reconstructed from the bridge log alone.

import "sync/atomic"

type connectionMetrics struct {
	claimsIssued     atomic.Int64 // TtsGenerate requests sent upstream
	claimsBoundRaw   atomic.Int64 // claims bound to a stream by its first raw frame (FIFO)
	claimsBoundEnv   atomic.Int64 // claims bound by a TtsSpeak envelope
	claimsUnclaimed  atomic.Int64 // raw audio streams that arrived with no pending claim
	claimsReplayed   atomic.Int64 // claims re-issued verbatim after an upstream re-dial
	sourceOverlaps   atomic.Int64 // claim bound while its request's previous source never ended
	tailDivergences  atomic.Int64 // final answers whose spoken prefix diverged from streamed chunks
	silencesPumped   atomic.Int64 // keepalive silence batches sent between sources
	sourcesFinalized atomic.Int64 // joined-stream EOS emissions (source end, close, or no-tail)
	redials          atomic.Int64 // upstream re-dials observed by the session
}
