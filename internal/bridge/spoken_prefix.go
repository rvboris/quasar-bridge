package bridge

// Insurance for the streamed-answer tail: the final answer text must have the
// already-spoken chunk prefix removed so the answer is spoken exactly once.
// A strict HasPrefix silently skips the cut when the provider's final text
// diverges from the streamed chunks (rephrased beginning, normalization
// differences), re-speaking the whole answer. trimSpokenPrefix falls back to
// the longest common prefix aligned to a word boundary: a divergence then
// costs re-speaking a few words, never the entire answer.

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// trimSpokenPrefix returns s without the already-spoken prefix. An exact
// prefix match trims verbatim (exact=true). Otherwise the longest common
// prefix is trimmed back to just after its last separator rune, so the tail
// never begins mid-word; with no usable common prefix the whole string is
// kept (exact=false) and the caller is expected to log the divergence.
func trimSpokenPrefix(s, spoken string) (tail string, trimmed int, exact bool) {
	// Leading whitespace in the spoken stream never carries speech and is
	// absent from final answer texts; ignore it or the comparison diverges
	// at byte zero.
	spoken = strings.TrimLeft(spoken, " \t\n\r")
	if spoken == "" {
		return s, 0, true
	}
	if strings.HasPrefix(s, spoken) {
		return s[len(spoken):], len(spoken), true
	}
	n := commonPrefixLen(s, spoken)
	for n > 0 && !utf8.RuneStart(s[n]) {
		n-- // never cut inside a multi-byte rune
	}
	for n > 0 {
		r, size := utf8.DecodeLastRuneInString(s[:n])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break // n sits right after a separator: a clean word boundary
		}
		n -= size
	}
	return s[n:], n, false
}

func commonPrefixLen(a, b string) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

// clip truncates s to at most limit runes for diagnostics logging.
func clip(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "…"
}
