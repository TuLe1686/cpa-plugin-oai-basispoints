package basispoints

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

type frameSink struct {
	mu     sync.Mutex
	frames [][]byte
	starts int
}

func (s *frameSink) start() { s.mu.Lock(); s.starts++; s.mu.Unlock() }

func (s *frameSink) write(frame []byte) error {
	s.mu.Lock()
	s.frames = append(s.frames, bytes.Clone(frame))
	s.mu.Unlock()
	return nil
}

func (s *frameSink) snapshot() ([][]byte, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.frames...), s.starts
}

func countEvents(t *testing.T, format string, frames [][]byte, kind string) int {
	t.Helper()
	n := 0
	for _, event := range decodeIncrementalFrames(t, format, frames) {
		if event["type"] == kind {
			n++
		}
	}
	return n
}

func TestHeartbeatWaitsForUpstreamCreated(t *testing.T) {
	sink := &frameSink{}
	d := newStreamDelivery("openai-response", sink.start, sink.write)
	if err := d.heartbeat(); err != nil {
		t.Fatal(err)
	}
	if frames, starts := sink.snapshot(); len(frames) != 0 || starts != 0 || d.isCommitted() {
		t.Fatal("heartbeat must not commit before upstream response.created")
	}
}

func TestHeartbeatCommitsSilentStreamThenRepeatsInProgress(t *testing.T) {
	for _, format := range []string{"openai-response", "codex"} {
		t.Run(format, func(t *testing.T) {
			sink := &frameSink{}
			d := newStreamDelivery(format, sink.start, sink.write)
			created := `{"type":"response.created","response":{"id":"resp_ka","status":"in_progress","output":[]}}`
			if err := d.consume("response.created", created); err != nil {
				t.Fatal(err)
			}
			if d.isCommitted() {
				t.Fatal("created alone must stay pending")
			}
			if err := d.heartbeat(); err != nil {
				t.Fatal(err)
			}
			if !d.isCommitted() {
				t.Fatal("heartbeat should commit once upstream created is known")
			}
			if err := d.heartbeat(); err != nil {
				t.Fatal(err)
			}
			frames, starts := sink.snapshot()
			if starts != 1 {
				t.Fatalf("start called %d times", starts)
			}
			if got := countEvents(t, format, frames, "response.created"); got != 1 {
				t.Fatalf("created emitted %d times", got)
			}
			if got := countEvents(t, format, frames, "response.in_progress"); got != 2 {
				t.Fatalf("in_progress emitted %d times, want 2", got)
			}
			response := map[string]any{"id": "resp_ka", "status": "completed", "output": []any{
				map[string]any{"type": "message", "id": "msg_ka", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": "hi", "annotations": []any{}}}},
			}}
			if err := d.validateFinal(response); err != nil {
				t.Fatal(err)
			}
			if err := d.finish(response); err != nil {
				t.Fatal(err)
			}
			frames, _ = sink.snapshot()
			if countEvents(t, format, frames, "response.completed") != 1 {
				t.Fatal("terminal event missing after heartbeat commit")
			}
			var text string
			for _, event := range decodeIncrementalFrames(t, format, frames) {
				if event["type"] == "response.output_text.delta" {
					text += event["delta"].(string)
				}
			}
			if text != "hi" {
				t.Fatalf("replayed text = %q", text)
			}
		})
	}
}

func TestKeepAliveFiresOnlyAfterSilence(t *testing.T) {
	sink := &frameSink{}
	d := newStreamDelivery("openai-response", sink.start, sink.write)
	if err := d.consume("response.created", `{"type":"response.created","response":{"id":"resp_idle","status":"in_progress","output":[]}}`); err != nil {
		t.Fatal(err)
	}
	stop := d.startKeepAlive(80 * time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	if d.isCommitted() {
		t.Fatal("keepalive fired before the silence interval")
	}
	time.Sleep(200 * time.Millisecond)
	stop()
	stop()
	frames, _ := sink.snapshot()
	if !d.isCommitted() || countEvents(t, "openai-response", frames, "response.in_progress") < 2 {
		t.Fatal("keepalive did not commit and repeat in_progress after silence")
	}
}

func TestKeepAliveDisabledIsNoop(t *testing.T) {
	sink := &frameSink{}
	d := newStreamDelivery("openai-response", sink.start, sink.write)
	stop := d.startKeepAlive(0)
	stop()
	if frames, _ := sink.snapshot(); len(frames) != 0 {
		t.Fatal("disabled keepalive wrote frames")
	}
}

func TestResetKeepsWriterForRetry(t *testing.T) {
	sink := &frameSink{}
	d := newStreamDelivery("openai-response", sink.start, sink.write)
	_ = d.consume("response.created", `{"type":"response.created","response":{"id":"resp_a","status":"in_progress","output":[]}}`)
	d.reset()
	if err := d.consume("response.created", `{"type":"response.created","response":{"id":"resp_b","status":"in_progress","output":[]}}`); err != nil {
		t.Fatalf("reset left stale meta: %v", err)
	}
	if err := d.heartbeat(); err != nil || !d.isCommitted() {
		t.Fatal("delivery unusable after reset")
	}
}
