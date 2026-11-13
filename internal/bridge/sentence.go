package bridge

import (
	"strings"
	"sync"
	"time"
)

const (
	defaultFirstRunes  = 80  // cap for the first chunk only: start speech early
	defaultSentenceRunes  = 200 // later chunks: the pipeline is warm, longer is smoother
	defaultSentenceFlush  = 1500 * time.Millisecond
	sentenceTerminators   = ".!?\n…"
	maxSentenceSkippedRun = 64
	// Soft breaks flush a long clause early so speech starts before the
	// first full sentence ends. DISABLED pending voice verification: shorter,
	// more frequent chunks measurably increased audible gaps at join points
	// (2026-11-12). Re-enable by setting softSentenceRunes > 0.
	softBreakRunes = ",;:—"
)

// softSentenceRunes gates soft clause breaks; 0 disables them.
var softSentenceRunes = 0

// sentenceBuffer converts a stream of answer deltas into speech-sized chunks:
// it flushes when a delta ends a sentence, when the buffer grows past maxRunes
// or when maxFlush passes since the last delta. Flushes are serialized; the
// timer goroutine stops on Close. The unterminated tail is not flushed: the
// final deferred part carries the remaining text.
type sentenceBuffer struct {
	flush      func(string)
	firstRunes int // cap for the first chunk (speech starts early)
	maxRunes   int // cap for later chunks (pipeline is warm)
	maxFlush   time.Duration
	chunks     int

	mu     sync.Mutex
	buf    strings.Builder
	runes  int
	last   time.Time
	timer  *time.Timer
	closed bool
}

func newSentenceBuffer(flush func(string)) *sentenceBuffer {
	return &sentenceBuffer{flush: flush, firstRunes: defaultFirstRunes, maxRunes: defaultSentenceRunes, maxFlush: defaultSentenceFlush}
}

// limit is the size cap for the chunk currently being built: the first chunk
// is kept short so speech starts quickly; later ones run long because they
// synthesize while the user is already listening.
func (b *sentenceBuffer) limit() int {
	if b.chunks == 0 {
		return b.firstRunes
	}
	return b.maxRunes
}

// Add consumes one answer delta.
func (b *sentenceBuffer) Add(delta string) {
	if delta == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.stopTimerLocked()
	b.buf.WriteString(delta)
	b.runes += len([]rune(delta))
	b.last = time.Now()
	if endsSentence(delta) {
		b.flushLocked()
		return
	}
	if b.runes >= b.limit() {
		b.cutLocked()
		return
	}
	if softSentenceRunes > 0 && b.runes >= softSentenceRunes && endsSoftBreak(delta) {
		b.flushLocked()
		return
	}
	b.timer = time.AfterFunc(b.maxFlush, b.timeoutFlush)
}

// cutLocked flushes the buffer at its last word boundary, keeping the
// incomplete tail for the next chunk. A forced cut must never split a word:
// TTS speaks half-words as audio glitches at the join.
func (b *sentenceBuffer) cutLocked() {
	if b.buf.Len() == 0 {
		return
	}
	head, tail, found := cutLastWord(b.buf.String())
	if !found || head == "" {
		b.flushLocked()
		return
	}
	b.flush(head)
	b.buf.Reset()
	b.buf.WriteString(tail)
	b.runes = len([]rune(tail))
	b.chunks++
}

// cutLastWord splits s at its final whitespace boundary.
func cutLastWord(s string) (head, tail string, found bool) {
	i := strings.LastIndexAny(s, " \t\n")
	if i <= 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// Close stops the idle timer without flushing the tail.
func (b *sentenceBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopTimerLocked()
	b.closed = true
}

func (b *sentenceBuffer) timeoutFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.buf.Len() == 0 {
		return
	}
	if time.Since(b.last) < b.maxFlush {
		if b.timer != nil {
			b.timer.Stop()
		}
		b.timer = time.AfterFunc(b.maxFlush-time.Since(b.last), b.timeoutFlush)
		return
	}
	b.flushLocked()
}

func (b *sentenceBuffer) flushLocked() {
	b.stopTimerLocked()
	if b.buf.Len() == 0 {
		return
	}
	b.flush(b.buf.String())
	b.buf.Reset()
	b.runes = 0
	b.chunks++
}

func (b *sentenceBuffer) stopTimerLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
}

// endsSoftBreak reports whether a delta ends on a clause boundary (comma
// and friends) — a good enough place to start speaking a long first clause.
func endsSoftBreak(delta string) bool {
	runes := []rune(strings.TrimRight(delta, " \t\r"))
	if len(runes) > maxSentenceSkippedRun {
		runes = runes[len(runes)-maxSentenceSkippedRun:]
	}
	return len(runes) > 0 && strings.ContainsRune(softBreakRunes, runes[len(runes)-1])
}

// endsSentence reports whether a delta terminates a spoken sentence. Long
// deltas are only inspected near their end to bound the scan.
func endsSentence(delta string) bool {
	runes := []rune(strings.TrimRight(delta, " \t\r"))
	if len(runes) > maxSentenceSkippedRun {
		runes = runes[len(runes)-maxSentenceSkippedRun:]
	}
	return len(runes) > 0 && strings.ContainsRune(sentenceTerminators, runes[len(runes)-1])
}
