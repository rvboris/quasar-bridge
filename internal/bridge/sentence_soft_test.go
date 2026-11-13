package bridge

import (
	"strings"
	"testing"
)

func TestSentenceBufferSoftBreak(t *testing.T) {
	// Covered by the focused tests below; kept as a placeholder removed.
}

func splitRunes(s string, n int) []string {
	var out []string
	for len(s) > 0 {
		k := n
		if k > len(s) {
			k = len(s)
		}
		out = append(out, s[:k])
		s = s[k:]
	}
	return out
}

func TestSentenceBufferFlushesOnCommaAfterThreshold(t *testing.T) {
	old := softSentenceRunes
	softSentenceRunes = 48
	defer func() { softSentenceRunes = old }()
	var got []string
	b := newSentenceBuffer(func(s string) { got = append(got, s) })
	defer b.Close()

	// 50 runes of clause, comma at the end, no sentence terminator anywhere.
	clause := strings.Repeat("а", 49) + ","
	for _, part := range splitRunes(clause, 10) {
		b.Add(part)
	}
	if len(got) == 0 {
		t.Fatal("expected a soft-break flush at the comma")
	}
	if got[0] != clause {
		t.Fatalf("flushed %q, want %q", got[0], clause)
	}
}

func TestSentenceBufferNoSoftBreakBelowThreshold(t *testing.T) {
	old := softSentenceRunes
	softSentenceRunes = 48
	defer func() { softSentenceRunes = old }()
	var got []string
	b := newSentenceBuffer(func(s string) { got = append(got, s) })
	defer b.Close()

	b.Add(strings.Repeat("а", 20) + ",")
	if len(got) != 0 {
		t.Fatalf("short clause must not flush on a comma: %q", got)
	}
	b.Add(strings.Repeat("б", 100) + "!") // maxRunes + terminator
	if len(got) != 1 {
		t.Fatalf("expected exactly one flush, got %d", len(got))
	}
}

func TestSentenceBufferSoftBreaksDisabledByDefault(t *testing.T) {
	if softSentenceRunes != 0 {
		t.Fatalf("soft breaks must ship disabled, got %d", softSentenceRunes)
	}
	var got []string
	b := newSentenceBuffer(func(s string) { got = append(got, s) })
	defer b.Close()
	b.Add(strings.Repeat("а", 49) + ",")
	if len(got) != 0 {
		t.Fatal("disabled soft breaks must not flush on a comma")
	}
}
