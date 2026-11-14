package bridge

import "testing"

// TestEmitChunkStripsLeadingWhitespace guards the double-speech bug: a
// provider whose first streamed delta opens with "\n\n" made emittedText
// start with whitespace the final answer lacks, so trimSpokenPrefix
// diverged at byte zero and re-spoke the entire answer.
func TestEmitChunkStripsLeadingWhitespace(t *testing.T) {
	emitted := "\n\nДа, хозяин. Ответ."
	final := "Да, хозяин. Ответ."
	tail, _, exact := trimSpokenPrefix(final, emitted)
	if !exact || tail != "" {
		t.Fatalf("with leading \\n\\n the tail re-speaks: exact=%v tail=%q", exact, tail)
	}
}
