package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func streamServer(t *testing.T, chunks ...string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "bad path", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range chunks {
			_, _ = w.Write([]byte("data: " + chunk + "\n\n"))
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
}

func TestChatStreamEmitsDeltasAndReturnsFullMessage(t *testing.T) {
	s := streamServer(t,
		`{"choices":[{"delta":{"role":"assistant","content":"Привет."},"finish_reason":""}]}`,
		`{"choices":[{"delta":{"content":" Как дела?"},"finish_reason":"stop"}]}`,
	)
	defer s.Close()
	c := Client{BaseURL: s.URL + "/v1", Model: "fixture-model"}
	var emitted []string
	m, err := c.ChatStream(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, func(delta string) { emitted = append(emitted, delta) })
	if err != nil {
		t.Fatal(err)
	}
	if m.Role != "assistant" || m.Content != "Привет. Как дела?" {
		t.Fatalf("message=%+v", m)
	}
	if strings.Join(emitted, "") != m.Content || len(emitted) != 2 {
		t.Fatalf("emitted=%v", emitted)
	}
}

func TestChatStreamNilEmitBehavesLikeNonStreaming(t *testing.T) {
	s := streamServer(t, `{"choices":[{"delta":{"content":"Ответ"},"finish_reason":"stop"}]}`)
	defer s.Close()
	c := Client{BaseURL: s.URL + "/v1", Model: "fixture-model"}
	m, err := c.ChatStream(t.Context(), nil, nil, nil)
	if err != nil || m.Content != "Ответ" {
		t.Fatalf("message=%+v err=%v", m, err)
	}
}

func TestChatStreamAccumulatesToolCallsByIndex(t *testing.T) {
	s := streamServer(t,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"current","arguments":""}},{"index":1,"id":"call-2","type":"function","function":{"name":"time","arguments":"{"}}]},"finish_reason":""}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"}"}}]},"finish_reason":"tool_calls"}]}`,
	)
	defer s.Close()
	c := Client{BaseURL: s.URL + "/v1", Model: "fixture-model"}
	m, err := c.ChatStream(t.Context(), nil, nil, func(string) { t.Fatal("tool-call round emitted content") })
	if err != nil {
		t.Fatal(err)
	}
	if m.Content != "" || len(m.ToolCalls) != 2 {
		t.Fatalf("message=%+v", m)
	}
	first, second := m.ToolCalls[0], m.ToolCalls[1]
	if first.ID != "call-1" || first.Function.Name != "current" || first.Function.Arguments != "" ||
		second.ID != "call-2" || second.Function.Name != "time" || second.Function.Arguments != "{}" ||
		second.Type != "function" {
		t.Fatalf("tool calls=%+v %+v", first, second)
	}
}

func TestChatStreamHTTPErrorBeforeContentIsFallbackable(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", 500)
	}))
	defer s.Close()
	c := Client{BaseURL: s.URL, Model: "fixture-model"}
	var emitted int
	_, err := c.ChatStream(t.Context(), nil, nil, func(string) { emitted++ })
	if !errors.Is(err, ErrStreamNotStarted) {
		t.Fatalf("error=%v", err)
	}
	if emitted != 0 {
		t.Fatal("error path emitted content")
	}
}

func TestChatStreamEmptyAnswerIsFallbackable(t *testing.T) {
	s := streamServer(t, `{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	defer s.Close()
	c := Client{BaseURL: s.URL, Model: "fixture-model"}
	if _, err := c.ChatStream(t.Context(), nil, nil, nil); !errors.Is(err, ErrStreamNotStarted) {
		t.Fatalf("error=%v", err)
	}
}

func TestChatStreamErrorAfterContentIsNotFallbackable(t *testing.T) {
	// The stream ends mid-answer (no [DONE], connection closes after content).
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"часть\"},\"finish_reason\":\"\"}]}\n\n"))
	}))
	defer s.Close()
	c := Client{BaseURL: s.URL, Model: "fixture-model"}
	var emitted string
	_, err := c.ChatStream(t.Context(), nil, nil, func(d string) { emitted += d })
	if err == nil || errors.Is(err, ErrStreamNotStarted) {
		t.Fatalf("error=%v", err)
	}
	if emitted != "часть" {
		t.Fatalf("emitted=%q", emitted)
	}
}

func TestChatStreamOversizedStreamRejected(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		chunk := strings.Repeat("а", 8192)
		for range maxStreamBytes/len(chunk) + 2 {
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"" + chunk + "\"},\"finish_reason\":\"\"}]}\n\n"))
		}
	}))
	defer s.Close()
	c := Client{BaseURL: s.URL, Model: "fixture-model"}
	if _, err := c.ChatStream(t.Context(), nil, nil, nil); err == nil {
		t.Fatal("oversized stream accepted")
	}
}

func TestChatStreamFinishLengthRejected(t *testing.T) {
	s := streamServer(t, `{"choices":[{"delta":{"content":"обрезано"},"finish_reason":"length"}]}`)
	defer s.Close()
	c := Client{BaseURL: s.URL + "/v1", Model: "fixture-model"}
	if _, err := c.ChatStream(t.Context(), nil, nil, nil); err == nil || errors.Is(err, ErrStreamNotStarted) {
		t.Fatalf("error=%v", err)
	}
}

func TestChatStreamUnsupportedAPIIsFallbackable(t *testing.T) {
	c := Client{API: "responses"}
	if _, err := c.ChatStream(context.Background(), nil, nil, nil); !errors.Is(err, ErrStreamNotStarted) {
		t.Fatalf("error=%v", err)
	}
}
