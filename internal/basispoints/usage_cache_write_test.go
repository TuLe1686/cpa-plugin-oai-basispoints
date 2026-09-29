package basispoints

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func cacheWriteResponse() map[string]any {
	return map[string]any{
		"id": "resp_cw", "status": "completed",
		"output": []any{map[string]any{"type": "message", "id": "msg_cw", "role": "assistant", "status": "completed",
			"content": []any{map[string]any{"type": "output_text", "text": "pong", "annotations": []any{}}}}},
		"usage": map[string]any{
			"input_tokens": 22374, "output_tokens": 5, "total_tokens": 22379,
			"input_tokens_details":  map[string]any{"cache_write_tokens": 22306, "cached_tokens": 0},
			"output_tokens_details": map[string]any{"reasoning_tokens": 0},
		},
	}
}

func TestStripCacheWriteTokensOnlyTouchesCacheWrite(t *testing.T) {
	response := cacheWriteResponse()
	stripCacheWriteTokens(response)
	usage := objectValue(response["usage"])
	details := objectValue(usage["input_tokens_details"])
	if details["cache_write_tokens"] != 0 {
		t.Fatalf("cache_write_tokens = %v", details["cache_write_tokens"])
	}
	if usage["input_tokens"] != 22374 || details["cached_tokens"] != 0 || usage["total_tokens"] != 22379 || usage["output_tokens"] != 5 {
		t.Fatalf("other usage fields changed: %v", usage)
	}
	stripCacheWriteTokens(map[string]any{})
	stripCacheWriteTokens(map[string]any{"usage": map[string]any{"input_tokens": 1}})
}

func TestCacheWriteAsInputDefaultOff(t *testing.T) {
	if defaultConfig().CacheWriteAsInput {
		t.Fatal("cache_write_as_input must default to false")
	}
}

func runCacheWriteNonStream(t *testing.T, enabled bool) map[string]any {
	t.Helper()
	service := newHTTPTestService()
	service.cfg.CacheWriteAsInput = enabled
	service.SetHost(func(method string, payload any, out any) error {
		*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Body: jsonBytes(cacheWriteResponse())}
		return nil
	})
	result, err := service.execute(jsonBytes(imageRequest(map[string]any{"type": "input_text", "text": "hi"})), false)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(result.(map[string]any)["Payload"].([]byte), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestCacheWriteAsInputNonStream(t *testing.T) {
	off := objectValue(objectValue(runCacheWriteNonStream(t, false)["usage"])["input_tokens_details"])
	if off["cache_write_tokens"] != float64(22306) {
		t.Fatalf("disabled switch changed cache_write_tokens: %v", off["cache_write_tokens"])
	}
	on := objectValue(runCacheWriteNonStream(t, true)["usage"])
	if objectValue(on["input_tokens_details"])["cache_write_tokens"] != float64(0) || on["input_tokens"] != float64(22374) {
		t.Fatalf("enabled switch result: %v", on)
	}
}

func TestCacheWriteAsInputStreamTerminal(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		sink := &frameSink{}
		d := newStreamDelivery("openai-response", sink.start, sink.write)
		response := cacheWriteResponse()
		if enabled {
			stripCacheWriteTokens(response)
		}
		if err := d.finish(response); err != nil {
			t.Fatal(err)
		}
		frames, _ := sink.snapshot()
		var terminal map[string]any
		for _, event := range decodeIncrementalFrames(t, "openai-response", frames) {
			if event["type"] == "response.completed" {
				terminal = objectValue(event["response"])
			}
		}
		got := objectValue(objectValue(terminal["usage"])["input_tokens_details"])["cache_write_tokens"]
		want := float64(22306)
		if enabled {
			want = 0
		}
		if got != want {
			t.Fatalf("enabled=%v terminal cache_write_tokens=%v want %v", enabled, got, want)
		}
	}
}

func TestCacheWriteAsInputConfigField(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(`{"cache_write_as_input":true}`), &cfg); err != nil || !cfg.CacheWriteAsInput {
		t.Fatalf("json field not wired: %v %v", err, cfg.CacheWriteAsInput)
	}
}

// 走完整的 executor.execute_stream：上游 SSE 带 cache_write_tokens，开关打开后客户端终态里为 0。
func TestCacheWriteAsInputStreamEndToEnd(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		service := newHTTPTestService()
		service.cfg.CacheWriteAsInput = enabled
		service.cfg.StreamKeepAliveSeconds = 0
		var mu sync.Mutex
		var emitted strings.Builder
		closed := make(chan struct{})
		service.SetHost(func(method string, payload any, out any) error {
			mu.Lock()
			defer mu.Unlock()
			var result any
			switch method {
			case "host.http.do_stream":
				result = map[string]any{"status_code": 200, "stream_id": "up", "headers": map[string][]string{"Content-Type": {"text/event-stream"}}}
			case "host.http.stream_read":
				raw := append([]byte("data: "), jsonBytes(map[string]any{"type": "response.completed", "response": cacheWriteResponse()})...)
				result = map[string]any{"payload": append(raw, 10, 10), "done": true}
			case "host.stream.emit":
				var frame []byte
				_ = json.Unmarshal(jsonBytes(payload.(map[string]any)["payload"]), &frame)
				emitted.Write(frame)
			case "host.stream.close":
				close(closed)
			}
			if out != nil && result != nil {
				return json.Unmarshal(jsonBytes(result), out)
			}
			return nil
		})
		request := imageRequest(map[string]any{"type": "input_text", "text": "hi"})
		request.Stream, request.StreamID = true, "down"
		if _, err := service.Handle("executor.execute_stream", jsonBytes(request)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("stream did not terminate")
		}
		mu.Lock()
		out := emitted.String()
		mu.Unlock()
		want := `"cache_write_tokens":22306`
		if enabled {
			want = `"cache_write_tokens":0`
		}
		if !strings.Contains(out, want) || !strings.Contains(out, `"input_tokens":22374`) {
			t.Fatalf("enabled=%v terminal usage missing %s: %s", enabled, want, out)
		}
	}
}
