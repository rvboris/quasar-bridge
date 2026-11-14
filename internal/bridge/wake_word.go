package bridge

// Wake-word guard: the station's spotter listens to its own speaker, so a
// wake word (e.g. «Алиса») spoken inside an answer triggers a false barge-in
// and truncates the answer mid-sentence (upstream issue #53). Every text that
// is about to be synthesized is therefore stripped of wake-word stems before
// it reaches TTS. Applied to chunk emission, the final spoken tail and the
// single-shot answer path; stripping is idempotent, so layered application is
// safe.

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// wakeWordStems lists word prefixes (lowercase) that trigger the station's
// spotter. A word is dropped when its lowercase form starts with a stem, so
// inflections are covered («Алиса», «Алисы», «Алису», «Алисой», …).
var wakeWordStems = []string{"алис"}

// stripWakeWords removes whole words starting with a wake-word stem, eats a
// dangling separator left behind (", ", " — ") and collapses inner
// whitespace. Leading and trailing whitespace is preserved so streamed chunk
// spacing survives. Text without wake words is returned unchanged.
func stripWakeWords(s string) string {
	if s == "" || len(wakeWordStems) == 0 {
		return s
	}
	runes := []rune(s)
	start := 0
	for start < len(runes) && unicode.IsSpace(runes[start]) {
		start++
	}
	end := len(runes)
	for end > start && unicode.IsSpace(runes[end-1]) {
		end--
	}
	if start >= end {
		return s
	}
	inner := stripWakeWordsInner(string(runes[start:end]))
	return string(runes[:start]) + inner + string(runes[end:])
}

func stripWakeWordsInner(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	runes := []rune(s)
	i := 0
	dropped := false
	for i < len(runes) {
		r := runes[i]
		if !unicode.IsLetter(r) {
			// Eat an orphan separator run left by a dropped word:
			// comma/dash plus surrounding spaces, otherwise keep the
			// punctuation as-is.
			if dropped && isOrphanSep(runes, i) {
				i = skipOrphanSep(runes, i)
				continue
			}
			if dropped && unicode.IsSpace(r) {
				i++
				continue
			}
			dropped = false
			b.WriteRune(r)
			i++
			continue
		}
		j := i
		for j < len(runes) && unicode.IsLetter(runes[j]) {
			j++
		}
		if startsWakeWord(string(runes[i:j])) {
			dropped = true
		} else {
			dropped = false
			b.WriteString(string(runes[i:j]))
		}
		i = j
	}
	return collapseSpaces(b.String())
}

func startsWakeWord(w string) bool {
	lw := strings.ToLower(w)
	for _, stem := range wakeWordStems {
		if strings.HasPrefix(lw, stem) {
			return true
		}
	}
	return false
}

// isOrphanSep reports whether runes[at:] begins a separator run that should
// be swallowed after a dropped word: optional spaces, one comma or dash,
// optional trailing spaces — but only when the run is followed by a letter,
// end of text, or another separator (not mid-number etc.).
func isOrphanSep(runes []rune, at int) bool {
	i := at
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i >= len(runes) {
		return true
	}
	switch runes[i] {
	case ',', '—', '–', '-':
	default:
		return false
	}
	i++
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i >= len(runes) {
		return true
	}
	return unicode.IsLetter(runes[i]) || isOrphanSep(runes, i)
}

// skipOrphanSep advances past the separator run detected by isOrphanSep.
func skipOrphanSep(runes []rune, at int) int {
	i := at
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i < len(runes) {
		switch runes[i] {
		case ',', '—', '–', '-':
			i++
		}
	}
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	return i
}

// collapseSpaces reduces runs of spaces/tabs to one space, never inserting a
// space after an opening punctuation mark.
func collapseSpaces(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			prevSpace = true
			continue
		}
		if prevSpace && b.Len() > 0 && !endsWithOpenPunct(b.String()) {
			b.WriteRune(' ')
		}
		prevSpace = false
		b.WriteRune(r)
	}
	return b.String()
}

func endsWithOpenPunct(s string) bool {
	if s == "" {
		return false
	}
	r, _ := utf8.DecodeLastRuneInString(s)
	switch r {
	case '(', '«':
		return true
	}
	return false
}
