package assistant

import (
	"context"
	"strings"
	"testing"

	"quasarbridge/internal/llm"
)

type streamChatClient struct {
	chatFunc
	deltas []string
}

func (c *streamChatClient) ChatStream(ctx context.Context, m []llm.Message, tools []llm.Tool, emit func(string)) (llm.Message, error) {
	for _, d := range c.deltas {
		emit(d)
	}
	return llm.Message{Role: "assistant", Content: strings.Join(c.deltas, "") + "  "}, nil
}

func TestCompleteDialogueStreamForwardsDeltasAndTrimsAnswer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Web = false
	cfg.History = false
	client := &streamChatClient{chatFunc: func(context.Context, []llm.Message, []llm.Tool) (llm.Message, error) {
		t.Error("non-streaming Chat used for streaming client")
		return llm.Message{}, nil
	}, deltas: []string{"Первое предложение. ", "Второе."}}
	agent := Agent{Client: client, Tools: &Tools{Config: cfg}, MaxRounds: 1, MaxCalls: 1}
	var got []string
	answer, private, err := agent.CompleteDialogueStream(t.Context(), []llm.Message{{Role: "user", Content: "вопрос"}}, false, func(d string) { got = append(got, d) })
	if err != nil || private {
		t.Fatalf("answer=%q private=%v err=%v", answer, private, err)
	}
	if answer != "Первое предложение. Второе." {
		t.Fatalf("answer=%q", answer)
	}
	if strings.Join(got, "") != "Первое предложение. Второе." {
		t.Fatalf("deltas=%v", got)
	}
}

func TestCompleteDialogueStreamWithNonStreamingClientStillWorks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Web = false
	cfg.History = false
	called := 0
	agent := Agent{Client: chatFunc(func(context.Context, []llm.Message, []llm.Tool) (llm.Message, error) {
		called++
		return llm.Message{Role: "assistant", Content: "Обычный ответ"}, nil
	}), Tools: &Tools{Config: cfg}, MaxRounds: 1, MaxCalls: 1}
	var got []string
	answer, _, err := agent.CompleteDialogueStream(t.Context(), []llm.Message{{Role: "user", Content: "вопрос"}}, false, func(d string) { got = append(got, d) })
	if err != nil || answer != "Обычный ответ" || called != 1 || len(got) != 0 {
		t.Fatalf("answer=%q calls=%d deltas=%v err=%v", answer, called, got, err)
	}
}

type fallbackStreamClient struct {
	chatFunc
	streamErr error
	streamed  *bool
}

func (c *fallbackStreamClient) ChatStream(ctx context.Context, m []llm.Message, tools []llm.Tool, emit func(string)) (llm.Message, error) {
	*c.streamed = true
	if c.streamErr != nil {
		return llm.Message{}, c.streamErr
	}
	return llm.Message{}, nil
}

func TestStreamingFailureBeforeContentFallsBackToChat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Web = false
	cfg.History = false
	streamed := false
	client := &fallbackStreamClient{chatFunc: func(context.Context, []llm.Message, []llm.Tool) (llm.Message, error) {
		return llm.Message{Role: "assistant", Content: "Резервный ответ"}, nil
	}, streamErr: llm.ErrStreamNotStarted, streamed: &streamed}
	agent := Agent{Client: client, Tools: &Tools{Config: cfg}, MaxRounds: 1, MaxCalls: 1}
	answer, _, err := agent.CompleteDialogueStream(t.Context(), []llm.Message{{Role: "user", Content: "вопрос"}}, false, func(string) {})
	if err != nil || answer != "Резервный ответ" || !streamed {
		t.Fatalf("answer=%q streamed=%v err=%v", answer, streamed, err)
	}
}
