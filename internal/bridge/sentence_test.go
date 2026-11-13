package bridge

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSentenceBufferFlushesOnTerminator(t *testing.T) {
	var mu sync.Mutex
	var chunks []string
	b := newSentenceBuffer(func(chunk string) { mu.Lock(); chunks = append(chunks, chunk); mu.Unlock() })
	b.Add("Первое предложение")
	b.Add(" окончено.")
	if len(chunks) != 1 || chunks[0] != "Первое предложение окончено." {
		t.Fatalf("chunks=%v", chunks)
	}
	b.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(chunks) != 1 {
		t.Fatalf("Close flushed the tail: %v", chunks)
	}
}

func TestSentenceBufferFlushesOnSize(t *testing.T) {
	var mu sync.Mutex
	var chunks []string
	b := newSentenceBuffer(func(chunk string) { mu.Lock(); chunks = append(chunks, chunk); mu.Unlock() })
	b.firstRunes = 10
	b.maxRunes = 10
	long := strings.Repeat("а", 12)
	b.Add(long)
	mu.Lock()
	defer mu.Unlock()
	if len(chunks) != 1 || chunks[0] != long {
		t.Fatalf("chunks=%v", chunks)
	}
}

func TestSentenceBufferFlushesAfterIdle(t *testing.T) {
	var mu sync.Mutex
	var chunks []string
	b := newSentenceBuffer(func(chunk string) { mu.Lock(); chunks = append(chunks, chunk); mu.Unlock() })
	b.maxFlush = 30 * time.Millisecond
	b.Add("без точки")
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		done := len(chunks) == 1
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle flush never fired")
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Close()
	mu.Lock()
	defer mu.Unlock()
	if chunks[0] != "без точки" || len(chunks) != 1 {
		t.Fatalf("chunks=%v", chunks)
	}
}

func TestSentenceBufferDeltasAfterCloseAreDropped(t *testing.T) {
	var mu sync.Mutex
	var chunks []string
	b := newSentenceBuffer(func(chunk string) { mu.Lock(); chunks = append(chunks, chunk); mu.Unlock() })
	b.Add("часть.")
	b.Close()
	b.Add("после закрытия.")
	mu.Lock()
	defer mu.Unlock()
	if len(chunks) != 1 || chunks[0] != "часть." {
		t.Fatalf("chunks=%v", chunks)
	}
}

func TestEndsSentence(t *testing.T) {
	for _, s := range []string{"Готово.", "Да!", "Нет?", "много\n", "конец…"} {
		if !endsSentence(s) {
			t.Errorf("missed terminator in %q", s)
		}
	}
	for _, s := range []string{"продолж", "точка внутри. нет", "  "} {
		if endsSentence(s) {
			t.Errorf("false terminator in %q", s)
		}
	}
}
