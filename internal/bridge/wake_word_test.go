package bridge

import "testing"

func TestStripWakeWordsBasic(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Алиса, включи свет", "включи свет"},
		{"Спроси у Алисы про погоду", "Спроси у про погоду"},
		{"Это Алиса мне рассказала", "Это мне рассказала"},
		{"Позвони Алисе сейчас", "Позвони сейчас"},
		{"Рассказала Алисой подтверждено", "Рассказала подтверждено"},
		{"никаких слов вызова тут нет", "никаких слов вызова тут нет"},
		{"", ""},
		{"Алиса", ""},
		{"Алиса?", "?"},
	}
	for _, c := range cases {
		if got := stripWakeWords(c.in); got != c.want {
			t.Errorf("stripWakeWords(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStripWakeWordsMidAnswer(t *testing.T) {
	// The exact scenario from upstream issue #53: the wake word in the
	// middle of a streamed answer kills the rest of the playback.
	in := "Конечно, я Алиса, и я могу помочь с этим вопросом"
	want := "Конечно, я и я могу помочь с этим вопросом"
	if got := stripWakeWords(in); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStripWakeWordsDoubleSeparator(t *testing.T) {
	// Word dropped between two commas must not leave ", ," behind.
	in := "Привет, Алиса, как дела?"
	want := "Привет, как дела?"
	if got := stripWakeWords(in); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStripWakeWordsPreservesUnrelated(t *testing.T) {
	// Words merely containing the stem letters but not starting with it,
	// and inflected forms, must survive / be dropped correctly.
	cases := []struct{ in, want string }{
		{"калиса и полалисия", "калиса и полалисия"}, // stem must be a prefix
		{"Галисия — регион Испании", "Галисия — регион Испании"},
		{"АЛИСА большими буквами", "большими буквами"},
	}
	for _, c := range cases {
		if got := stripWakeWords(c.in); got != c.want {
			t.Errorf("stripWakeWords(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStripWakeWordsIdempotent(t *testing.T) {
	once := stripWakeWords("Алиса, алиса и ещё раз Алиса!")
	if twice := stripWakeWords(once); twice != once {
		t.Fatalf("not idempotent: %q -> %q", once, twice)
	}
}

func TestStripWakeWordsNumberBoundary(t *testing.T) {
	// Digits are not letters: the "word" boundary logic must not eat them
	// or drop numbers adjacent to the stripped word.
	in := "У Алисы 3 кота и 12 рыбок"
	want := "У 3 кота и 12 рыбок"
	if got := stripWakeWords(in); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
