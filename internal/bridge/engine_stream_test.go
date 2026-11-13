package bridge

import (
	"context"
	"strings"
	"testing"

	"quasarbridge/internal/llm"
)

type streamCompleter struct {
	completionFunc
	deltas []string
	answer string
}

func (s *streamCompleter) CompleteDialogueStream(ctx context.Context, messages []llm.Message, private bool, emit func(string)) (string, bool, error) {
	for _, d := range s.deltas {
		emit(d)
	}
	return s.answer, private, nil
}

func (s *streamCompleter) CompleteDialogue(ctx context.Context, messages []llm.Message, private bool) (string, bool, error) {
	return s.answer, private, nil
}

func TestFallbackStreamEmitsDeltasAndCommitsFullAnswer(t *testing.T) {
	answer := "Первое предложение. Второе предложение."
	e := &Engine{LLM: &streamCompleter{completionFunc: func(context.Context, []llm.Message, bool) (string, error) {
		t.Fatal("plain Complete used for streaming answerer")
		return "", nil
	}, deltas: []string{"Первое предложение. ", "Второе предложение."}, answer: answer}}
	var got []string
	r, err := e.FallbackStream(t.Context(), "вопрос", func(d string) { got = append(got, d) })
	if err != nil || r.Answer != answer {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	if strings.Join(got, "") != answer {
		t.Fatalf("deltas=%v", got)
	}
	// The committed history keeps the full answer, so a follow-up sees it whole.
	r2, err := e.Fallback(t.Context(), "а что второе?")
	if err != nil || r2.Answer != answer || len(got) != 2 {
		t.Fatalf("history result=%+v deltas=%d err=%v", r2, len(got), err)
	}
}

func TestFallbackStreamWithNonStreamingAnswererIgnoresEmit(t *testing.T) {
	e := &Engine{LLM: completionFunc(func(context.Context, []llm.Message, bool) (string, error) {
		return "Обычный ответ", nil
	})}
	var got []string
	r, err := e.FallbackStream(t.Context(), "вопрос", func(d string) { got = append(got, d) })
	if err != nil || r.Answer != "Обычный ответ" || len(got) != 0 {
		t.Fatalf("result=%+v deltas=%v err=%v", r, got, err)
	}
}
