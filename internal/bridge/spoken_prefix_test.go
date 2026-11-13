package bridge

import "testing"

func TestTrimSpokenPrefixExact(t *testing.T) {
	tail, n, exact := trimSpokenPrefix("hello world and more", "hello world")
	if !exact || tail != " and more" || n != len("hello world") {
		t.Fatalf("exact trim: tail=%q n=%d exact=%v", tail, n, exact)
	}
	// Fully spoken answer: nothing left to synthesize.
	tail, n, exact = trimSpokenPrefix("hello world", "hello world")
	if !exact || tail != "" || n != len("hello world") {
		t.Fatalf("full trim: tail=%q n=%d exact=%v", tail, n, exact)
	}
	// Nothing spoken yet.
	tail, _, exact = trimSpokenPrefix("hello", "")
	if !exact || tail != "hello" {
		t.Fatalf("empty spoken: tail=%q exact=%v", tail, exact)
	}
}

func TestTrimSpokenPrefixDiverged(t *testing.T) {
	// Provider rephrased the ending of the spoken prefix: the tail must start
	// at the word boundary after the divergence, not re-speak everything.
	s := "Санкт-Петербург — самый молодой из больших городов Европы"
	spoken := "Санкт-Петербург — самый юный"
	tail, n, exact := trimSpokenPrefix(s, spoken)
	if exact {
		t.Fatal("diverged trim reported exact")
	}
	if want := "молодой из больших городов Европы"; tail != want {
		t.Fatalf("tail=%q, want %q", tail, want)
	}
	if n != len("Санкт-Петербург — самый ") {
		t.Fatalf("trimmed=%d, want %d", n, len("Санкт-Петербург — самый "))
	}
}

func TestTrimSpokenPrefixNormalization(t *testing.T) {
	// Punctuation differs right after the shared words: the cut backs up to
	// the last separator both texts share, so the tail may re-speak one word
	// but never starts mid-word.
	s := "Город стоит на 42 островах"
	spoken := "Город стоит, на сорока двух"
	tail, _, exact := trimSpokenPrefix(s, spoken)
	if exact {
		t.Fatal("normalization trim reported exact")
	}
	if want := "стоит на 42 островах"; tail != want {
		t.Fatalf("tail=%q, want %q", tail, want)
	}
}

func TestTrimSpokenPrefixNoCommon(t *testing.T) {
	tail, n, exact := trimSpokenPrefix("совсем другой ответ", "абсолютно иной")
	if exact || tail != "совсем другой ответ" || n != 0 {
		t.Fatalf("no-common: tail=%q n=%d exact=%v", tail, n, exact)
	}
}

func TestTrimSpokenPrefixRuneBoundary(t *testing.T) {
	// Byte-level common prefix ends inside a multi-byte rune: 'м'(D0 BC) and
	// 'н'(D0 BD) share the first byte; the cut must back off to zero instead
	// of slicing mid-rune.
	tail, n, exact := trimSpokenPrefix("мама мыла раму", "нана")
	if exact || tail != "мама мыла раму" || n != 0 {
		t.Fatalf("rune boundary: tail=%q n=%d exact=%v", tail, n, exact)
	}
}

func TestTrimSpokenPrefixSingleWord(t *testing.T) {
	// Divergence inside the first word with no separator before it: nothing
	// safe to trim, keep the whole string.
	tail, n, exact := trimSpokenPrefix("Европа", "Европе")
	if exact || tail != "Европа" || n != 0 {
		t.Fatalf("single word: tail=%q n=%d exact=%v", tail, n, exact)
	}
}
