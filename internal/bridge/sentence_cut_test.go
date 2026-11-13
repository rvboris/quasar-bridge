package bridge

import (
	"strings"
	"testing"
)

func TestSentenceBufferForcedCutSplitsAtWordBoundary(t *testing.T) {
	var got []string
	b := newSentenceBuffer(func(s string) { got = append(got, s) })
	defer b.Close()

	// One long sentence without a terminator, fed in small deltas.
	sentence := strings.Repeat("слово ", 30)
	for _, part := range splitRunes(sentence, 9) {
		b.Add(part)
	}
	if len(got) == 0 {
		t.Fatal("expected a forced cut")
	}
	rest := sentence
	for i, chunk := range got {
		if chunk == "" || !strings.HasPrefix(strings.TrimLeft(rest, " "), chunk) {
			t.Fatalf("chunk %d %q is not a prefix of the remaining text", i, chunk)
		}
		consumed := strings.Index(rest, chunk) + len(chunk)
		last := i == len(got)-1
		if !last && consumed < len(rest) && rest[consumed] != ' ' {
			t.Fatalf("chunk %d %q does not end at a word boundary", i, chunk)
		}
		rest = rest[consumed:]
	}
}

func TestSentenceBufferFirstChunkShorterThanLater(t *testing.T) {
	var sizes []int
	b := newSentenceBuffer(func(s string) { sizes = append(sizes, len([]rune(s))) })
	defer b.Close()

	text := strings.Repeat("слово ", 200) // one enormous unbroken sentence
	for _, part := range splitRunes(text, 7) {
		b.Add(part)
	}
	if len(sizes) < 3 {
		t.Fatalf("expected several chunks, got %v", sizes)
	}
	if sizes[0] > defaultFirstRunes {
		t.Fatalf("first chunk too long: %d > %d", sizes[0], defaultFirstRunes)
	}
	if sizes[1] <= sizes[0] {
		t.Fatalf("later chunks should be longer than the first: %v", sizes)
	}
	var total int
	for _, n := range sizes {
		total += n
	}
	if total > len([]rune(text)) {
		t.Fatalf("chunks exceed input: %d > %d", total, len([]rune(text)))
	}
}

func TestCutLastWord(t *testing.T) {
	if h, tail, ok := cutLastWord("привет мир"); !ok || h != "привет" || tail != "мир" {
		t.Fatalf("got %q %q %v", h, tail, ok)
	}
	if _, _, ok := cutLastWord("однослово"); ok {
		t.Fatal("no boundary expected")
	}
	if _, _, ok := cutLastWord(" слово"); ok {
		t.Fatal("boundary at position 0 is not a split")
	}
}
