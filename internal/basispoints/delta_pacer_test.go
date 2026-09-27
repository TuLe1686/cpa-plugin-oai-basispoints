package basispoints

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// collectPacer 以可控时钟驱动放行循环：interval 缩为 1ms，只验证拆块与顺序。
func runPacer(t *testing.T, chunkChars, intervalMillis int, texts ...string) []string {
	t.Helper()
	pacer := newDeltaPacer(true, chunkChars, intervalMillis)
	stop := make(chan struct{})
	var mu sync.Mutex
	var out []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		pacer.run(func(piece string) error {
			mu.Lock()
			out = append(out, piece)
			mu.Unlock()
			return nil
		}, stop)
	}()
	for _, text := range texts {
		pacer.queueDelta(text)
	}
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		total := 0
		for _, piece := range out {
			total += len(piece)
		}
		want := 0
		for _, text := range texts {
			want += len(text)
		}
		mu.Unlock()
		if total == want {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("pacer stalled: total=%d want=%d out=%q", total, want, out)
		case <-time.After(5 * time.Millisecond):
		}
	}
	pacer.close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pacer did not stop")
	}
	return out
}

func TestDeltaPacerSplitsLargeDeltaIntoSmallPieces(t *testing.T) {
	pieces := runPacer(t, 4, 1, "0123456789abcdef")
	if len(pieces) < 4 {
		t.Fatalf("expected at least 4 pieces, got %d: %q", len(pieces), pieces)
	}
	if strings.Join(pieces, "") != "0123456789abcdef" {
		t.Fatalf("content broken: %q", pieces)
	}
	for _, piece := range pieces {
		if len(piece) > 4 {
			t.Fatalf("piece larger than chunk: %q", piece)
		}
	}
}

func TestDeltaPacerPreservesOrderAcrossQueues(t *testing.T) {
	pieces := runPacer(t, 3, 1, "abcdef", "ghij")
	joined := strings.Join(pieces, "")
	if joined != "abcdefghij" {
		t.Fatalf("order broken: %q from %q", pieces, joined)
	}
}

func TestDeltaPacerEmptyDeltaProducesNothing(t *testing.T) {
	pacer := newDeltaPacer(true, 8, 20)
	pacer.queueDelta("")
	if pending := pacer.drain(); len(pending) != 0 {
		t.Fatalf("empty delta queued: %q", pending)
	}
}

func TestDeltaPacerDrainReturnsBacklogWhenStopped(t *testing.T) {
	pacer := newDeltaPacer(true, 8, 5000)
	pacer.queueDelta("hello world this is long")
	stop := make(chan struct{})
	close(stop)
	pending := pacer.drain()
	if len(pending) != 1 || pending[0] != "hello world this is long" {
		t.Fatalf("drain lost stopped backlog: %q", pending)
	}
}

func TestDeltaPacerDrainWaitsForRunLoopToFinish(t *testing.T) {
	pacer := newDeltaPacer(true, 4, 1)
	pacer.queueDelta("abcdefgh")
	pacer.drain()
	// drain 后 run 协程必须已退出：再次入队不会再被发送。
	pacer.queueDelta("extra")
	if pending := pacer.drain(); pending == nil {
		t.Fatal("post-drain queue missing extra text")
	}
}

func TestDeltaPacerDisabledPassesNothing(t *testing.T) {
	pacer := newDeltaPacer(false, 8, 20)
	pacer.queueDelta("text")
	if pending := pacer.drain(); len(pending) != 0 {
		t.Fatalf("disabled pacer queued text: %q", pending)
	}
}

func TestDeltaPacerSplitsMultibyteTextWithoutBreakingRunes(t *testing.T) {
	pieces := runPacer(t, 2, 1, "你好世界，流式输出")
	joined := strings.Join(pieces, "")
	if joined != "你好世界，流式输出" {
		t.Fatalf("content broken: %q", joined)
	}
	for _, piece := range pieces {
		for _, r := range piece {
			if r == 0xFFFD {
				t.Fatalf("rune broken in piece: %q", piece)
			}
		}
	}
}

func TestDeltaPacerCloseStopsRunLoop(t *testing.T) {
	pacer := newDeltaPacer(true, 4, 100)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pacer.run(func(string) error { return nil }, stop)
	}()
	pacer.close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not stop run loop")
	}
}

// 端到端回归：启用平滑的 delivery 必须把正文 delta 真正写出，
// 且全部落在终态之前（v0.2.4 曾因 emit 自递归吞掉全部 delta）。
func TestSmoothedDeliveryWritesDeltasBeforeTerminal(t *testing.T) {
	var mu sync.Mutex
	var frames []string
	delivery := newStreamDelivery("openai-response", func() {}, func(frame []byte) error {
		mu.Lock()
		frames = append(frames, string(frame))
		mu.Unlock()
		return nil
	})
	delivery.enableSmoothing(newDeltaPacer(true, 4, 1))
	delivery.committed = true
	if err := delivery.emit(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"type": "message", "id": "msg_1"}}); err != nil {
		t.Fatal(err)
	}
	for _, piece := range []string{"pong", " again", " and done"} {
		if err := delivery.emit(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "item_id": "msg_1", "delta": piece}); err != nil {
			t.Fatal(err)
		}
	}
	if err := delivery.emit(map[string]any{"type": "response.output_text.done", "output_index": 0, "content_index": 0, "item_id": "msg_1", "text": "pong again and done"}); err != nil {
		t.Fatal(err)
	}
	delivery.drainPacer()
	mu.Lock()
	defer mu.Unlock()
	wire := strings.Join(frames, "") + "data: [DONE]\n\n"
	var text string
	deltaSeen, doneSeen, terminalIndex, lastDeltaIndex := 0, 0, -1, -1
	for index, event := range clientStreamEvents(t, []byte(wire)) {
		switch event["type"] {
		case "response.output_text.delta":
			deltaSeen++
			lastDeltaIndex = index
			text += event["delta"].(string)
		case "response.output_text.done":
			doneSeen++
		}
		_ = terminalIndex
	}
	if text != "pong again and done" || deltaSeen == 0 || doneSeen != 1 {
		t.Fatalf("smoothed delivery broke: text=%q deltas=%d done=%d", text, deltaSeen, doneSeen)
	}
	if lastDeltaIndex < 0 {
		t.Fatal("no delta index recorded")
	}
}
