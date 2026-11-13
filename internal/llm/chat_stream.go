package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ErrStreamNotStarted marks a streaming failure that happened before any
// content or tool call arrived. Callers may transparently retry such requests
// with the non-streaming Chat.
var ErrStreamNotStarted = errors.New("LLM stream failed before any content")

type streamChatter interface {
	ChatStream(context.Context, []Message, []Tool, func(string)) (Message, error)
}

// HasChatStream reports whether the client supports SSE streaming of answers.
func HasChatStream(client any) bool {
	_, ok := client.(streamChatter)
	return ok
}

// ChatStream requests a chat completion with stream:true and parses the SSE
// response, invoking emit (if non-nil) with each content delta as it arrives.
// The returned Message mirrors the non-streaming Chat result: accumulated
// content, tool calls assembled by index and the last finish_reason.
func (c *Client) ChatStream(ctx context.Context, messages []Message, tools []Tool, emit func(string)) (Message, error) {
	if err := ValidateChatParameters(c.ChatParameters); err != nil {
		return Message{}, err
	}
	if c.API != "" && c.API != "chat_completions" {
		return Message{}, fmt.Errorf("%w: streaming requires chat_completions API", ErrStreamNotStarted)
	}
	request := map[string]any{
		"model": c.Model, "messages": messages, "stream": true,
		"temperature": 0, "max_tokens": 1024,
		"chat_template_kwargs": map[string]any{"enable_thinking": false},
	}
	for key, value := range c.ChatParameters {
		if value == nil {
			delete(request, key)
		} else {
			request[key] = value
		}
	}
	if len(tools) > 0 {
		request["tools"] = tools
		request["tool_choice"] = "auto"
		request["parallel_tool_calls"] = false
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body, err := json.Marshal(request)
	if err != nil {
		return Message{}, errors.New("cannot encode LLM request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Message{}, errors.New("invalid LLM endpoint")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return Message{}, fmt.Errorf("%w: %v", ErrStreamNotStarted, ctx.Err())
		}
		return Message{}, fmt.Errorf("%w: LLM transport failed", ErrStreamNotStarted)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Message{}, fmt.Errorf("%w: %v", ErrStreamNotStarted, llmHTTPError(resp))
	}

	limited := &io.LimitedReader{R: resp.Body, N: maxStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), maxStreamBytes+1)

	var content strings.Builder
	toolCalls := map[int]*ToolCall{}
	started := false // any content delta or tool call fragment was seen
	done := false
	finishReason := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		payload, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		payload = strings.TrimSpace(payload)
		if payload == "" {
			continue
		}
		if payload == "[DONE]" {
			done = true
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Type     string `json:"type"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(payload), &chunk) != nil {
			if !started {
				return Message{}, fmt.Errorf("%w: invalid LLM stream chunk", ErrStreamNotStarted)
			}
			return Message{}, errors.New("invalid LLM stream chunk")
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]
		if choice.FinishReason != "" {
			finishReason = choice.FinishReason
		}
		if choice.Delta.Content != "" {
			started = true
			content.WriteString(choice.Delta.Content)
			if emit != nil {
				slog.Debug("chat_stream_delta", "bytes", len(choice.Delta.Content))
				emit(choice.Delta.Content)
			}
		}
		for _, call := range choice.Delta.ToolCalls {
			started = true
			acc, ok := toolCalls[call.Index]
			if !ok {
				acc = &ToolCall{}
				toolCalls[call.Index] = acc
			}
			if call.ID != "" {
				acc.ID = call.ID
			}
			if call.Type != "" {
				acc.Type = call.Type
			}
			acc.Function.Name += call.Function.Name
			acc.Function.Arguments += call.Function.Arguments
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("LLM transport: %w", ctx.Err())
		} else {
			err = errors.New("cannot read LLM stream")
		}
		if !started {
			return Message{}, fmt.Errorf("%w: %v", ErrStreamNotStarted, err)
		}
		return Message{}, err
	}
	if limited.N == 0 {
		if !started {
			return Message{}, fmt.Errorf("%w: LLM stream exceeded size limit", ErrStreamNotStarted)
		}
		return Message{}, errors.New("LLM stream exceeded size limit")
	}
	if !done {
		err := errors.New("LLM stream not completed")
		if !started {
			return Message{}, fmt.Errorf("%w: %v", ErrStreamNotStarted, err)
		}
		return Message{}, err
	}
	if finishReason == "length" {
		return Message{}, errors.New("LLM answer exceeded token budget")
	}
	m := Message{Role: "assistant", Content: content.String()}
	indexes := make([]int, 0, len(toolCalls))
	for index := range toolCalls {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		m.ToolCalls = append(m.ToolCalls, *toolCalls[index])
	}
	if strings.TrimSpace(m.Content) == "" && len(m.ToolCalls) == 0 {
		return Message{}, fmt.Errorf("%w: LLM returned no answer", ErrStreamNotStarted)
	}
	if len(m.ToolCalls) == 0 {
		if err := ValidateAnswer(m.Content); err != nil {
			return Message{}, err
		}
	}
	return m, nil
}
