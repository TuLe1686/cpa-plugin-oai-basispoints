package basispoints

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func sseEvents(events ...map[string]any) []byte {
	var b strings.Builder
	for _, event := range events {
		writeSSE(&b, stringValue(event["type"]), event)
	}
	return []byte(b.String())
}

func cutoffMessage(id, phase, text string) map[string]any {
	item := map[string]any{"type": "message", "id": id, "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}
	if phase != "" {
		item["phase"] = phase
	}
	return item
}

func cutoffStream(items ...map[string]any) []byte {
	events := []map[string]any{{"type": "response.created", "response": map[string]any{"id": "resp_cut", "status": "in_progress", "output": []any{}}}}
	for index, item := range items {
		events = append(events,
			map[string]any{"type": "response.output_item.added", "output_index": index, "item": item},
			map[string]any{"type": "response.output_item.done", "output_index": index, "item": item})
	}
	return sseEvents(events...)
}

func TestRecoverCutoffResponse(t *testing.T) {
	tool := map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": transportName, "arguments": "{}", "status": "completed"}
	for name, tc := range map[string]struct {
		raw  []byte
		want bool
	}{
		"final_answer":      {cutoffStream(cutoffMessage("msg_1", "final_answer", "done")), true},
		"no_phase_answer":   {cutoffStream(cutoffMessage("msg_1", "", "done")), true},
		"tool_call":         {cutoffStream(cutoffMessage("msg_0", "commentary", "working"), tool), true},
		"commentary_only":   {cutoffStream(cutoffMessage("msg_1", "commentary", "let me check")), false},
		"empty_message":     {cutoffStream(cutoffMessage("msg_1", "", "")), false},
		"no_items":          {cutoffStream(), false},
		"item_not_finished": {append(cutoffStream(cutoffMessage("msg_1", "", "done")), sseEvents(map[string]any{"type": "response.output_item.added", "output_index": 1, "item": cutoffMessage("msg_2", "", "")})...), false},
		"upstream_error":    {append(cutoffStream(cutoffMessage("msg_1", "", "done")), sseEvents(map[string]any{"type": "error", "code": "server_error", "message": "x"})...), false},
		"no_created":        {sseEvents(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": cutoffMessage("msg_1", "", "done")}), false},
	} {
		t.Run(name, func(t *testing.T) {
			response, ok := recoverCutoffResponse(tc.raw)
			if ok != tc.want {
				t.Fatalf("recovered=%v want=%v", ok, tc.want)
			}
			if ok && (response["status"] != "completed" || response["id"] != "resp_cut") {
				t.Fatalf("bad recovered response: %v", response)
			}
		})
	}
}

func TestCutoffCompletionIsOptIn(t *testing.T) {
	raw := cutoffStream(cutoffMessage("msg_1", "final_answer", "done"))
	headers := http.Header{"Content-Type": {"text/event-stream"}}
	service := newHTTPTestService()
	service.SetHost(func(string, any, any) error { return nil })
	if _, err := service.parseUpstreamResponse(raw, headers); err == nil || !strings.Contains(err.Error(), "ended without a terminal response") {
		t.Fatalf("cutoff must still fail by default: %v", err)
	}
	service.cfg.CutoffCompletion = true
	var logged string
	service.SetHost(func(method string, payload any, _ any) error {
		if method == "host.log" {
			logged = stringValue(payload.(map[string]any)["message"])
		}
		return nil
	})
	response, err := service.parseUpstreamResponse(raw, headers)
	if err != nil || response["status"] != "completed" {
		t.Fatalf("enabled cutoff completion failed: %v", err)
	}
	if !strings.Contains(logged, "basispoints_cutoff_completed") {
		t.Fatalf("cutoff completion not logged: %q", logged)
	}
	if _, err := service.parseUpstreamResponse([]byte(`{"detail":"bad"}`), http.Header{}); err == nil {
		t.Fatal("unrelated decode errors must not be completed")
	}
}

func TestCutoffCompletionOnLiveStream(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(fmt.Sprintf("interrupted=%t", interrupted), func(t *testing.T) {
			service := newHTTPTestService()
			service.cfg.CutoffCompletion = true
			raw := cutoffStream(cutoffMessage("msg_1", "final_answer", "cut answer"))
			var mu sync.Mutex
			var emitted strings.Builder
			reads := 0
			closed := make(chan struct{})
			service.SetHost(func(method string, payload any, out any) error {
				mu.Lock()
				defer mu.Unlock()
				var result any
				switch method {
				case "host.http.do_stream":
					result = map[string]any{"status_code": 200, "stream_id": "up", "headers": map[string][]string{"Content-Type": {"text/event-stream"}}}
				case "host.http.stream_read":
					reads++
					switch {
					case reads == 1:
						result = map[string]any{"payload": raw}
					case interrupted:
						result = map[string]any{"error": "connection reset"}
					default:
						result = map[string]any{"done": true}
					}
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
			defer mu.Unlock()
			out := emitted.String()
			if !strings.Contains(out, "response.completed") || strings.Contains(out, `"type":"error"`) || strings.Count(out, "cut answer") < 1 {
				t.Fatalf("cutoff stream not completed locally: %s", out)
			}
		})
	}
}
