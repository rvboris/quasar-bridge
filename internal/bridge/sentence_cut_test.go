package bridge

import (
	"strings"
	"testing"
	"time"
)

// TestCutPreservesText guards against the double-speech bug: a forced
// length cut used to drop the whitespace at the split point, so the
// concatenation of emitted chunks differed from the final answer by one
// space and trimSpokenPrefix re-spoke the tail from a mid-sentence word.
func TestCutPreservesText(t *testing.T) {
	deltas := []string{
		"Сейчас в Питере около плюс трёх, идёт дождь, ветер до тридцати девяти",
		" километров в час.",
	}
	for _, limit := range []int{8, 20, 37, 80, 200} {
		var chunks []string
		b := &sentenceBuffer{flush: func(s string) { chunks = append(chunks, s) },
			firstRunes: limit, maxRunes: limit, maxFlush: time.Hour}
		for _, d := range deltas {
			b.Add(d)
		}
		emitted := strings.Join(chunks, "")
		want := strings.Join(deltas, "")
		if emitted != want {
			t.Fatalf("limit=%d: chunks concatenated to %q, want %q (chunks: %q)", limit, emitted, want, chunks)
		}
		if tail, _, exact := trimSpokenPrefix(want, emitted); !exact && tail != "" {
			t.Fatalf("limit=%d: final tail re-spoken: %q", limit, tail)
		}
	}
}

// The exact production scenario: first chunk hits the 80-rune cap between
// "девяти" and "километров".
func TestCutLastWordKeepsSeparator(t *testing.T) {
	s := "Сейчас в Питере около плюс трёх, идёт дождь, ветер до тридцати девяти километров в час."
	head, tail, found := cutLastWord(s)
	if !found {
		t.Fatal("expected a cut")
	}
	if head+tail != s {
		t.Fatalf("head+tail lost text: %q + %q != original", head, tail)
	}
	if !strings.HasPrefix(tail, " ") {
		t.Fatalf("tail must keep the separator, got %q", tail)
	}
}
