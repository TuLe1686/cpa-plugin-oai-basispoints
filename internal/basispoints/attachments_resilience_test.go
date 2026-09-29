package basispoints

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// attachmentHost 模拟附件上传与 responses：上传返回递增 ID，stale 集合中的 ID 被 422 拒绝。
type attachmentHost struct {
	mu        sync.Mutex
	uploads   int
	responses int
	failNth   map[int]int
	stale     map[string]bool
	lastBody  map[string]any
}

func (h *attachmentHost) handle(t *testing.T) func(string, any, any) error {
	return func(method string, payload any, out any) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		wire := payload.(map[string]any)
		if strings.HasSuffix(wire["url"].(string), "/attachments") {
			h.uploads++
			if status := h.failNth[h.uploads]; status != 0 {
				*out.(*upstreamResponse) = upstreamResponse{StatusCode: status, Body: []byte(`{"detail":"rejected"}`)}
				return nil
			}
			*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Body: jsonBytes(map[string]any{"openai_file_id": fmt.Sprintf("file-%d", h.uploads)})}
			return nil
		}
		h.responses++
		var body map[string]any
		if err := json.Unmarshal(wire["body"].([]byte), &body); err != nil {
			return err
		}
		h.lastBody = body
		for _, value := range body["input"].([]any) {
			parts, _ := objectValue(value)["content"].([]any)
			for _, part := range parts {
				if h.stale[stringValue(objectValue(part)["file_id"])] {
					*out.(*upstreamResponse) = upstreamResponse{StatusCode: 422, Body: []byte(`{"detail":"Invalid request body."}`)}
					return nil
				}
			}
		}
		response := map[string]any{"id": "resp_img", "status": "completed", "output": []any{
			map[string]any{"type": "message", "id": "msg_img", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": "ok", "annotations": []any{}}}},
		}}
		*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Body: jsonBytes(response)}
		return nil
	}
}

func historyImageRequest(dataURL string, historyImage, latestImage bool) ExecutorRequest {
	history := []any{map[string]any{"type": "input_text", "text": "earlier"}}
	if historyImage {
		history = append(history, map[string]any{"type": "input_image", "image_url": dataURL})
	}
	latest := []any{map[string]any{"type": "input_text", "text": "now"}}
	if latestImage {
		latest = append(latest, map[string]any{"type": "input_image", "image_url": dataURL})
	}
	request := imageRequest()
	request.Payload = jsonBytes(map[string]any{"input": []any{
		map[string]any{"role": "user", "content": history},
		map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "seen"}}},
		map[string]any{"role": "user", "content": latest},
	}})
	return request
}

func sentParts(body map[string]any, index int) []any {
	parts, _ := objectValue(body["input"].([]any)[index])["content"].([]any)
	return parts
}

func imageItemIndexes(body map[string]any) (first, last int) {
	first, last = -1, -1
	for i, value := range body["input"].([]any) {
		if isUserMessage(objectValue(value)) {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	return
}

func TestExpiredCachedFileIDIsReuploadedOnce(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	service := newHTTPTestService()
	host := &attachmentHost{stale: map[string]bool{}}
	service.SetHost(host.handle(t))
	request := imageRequest(map[string]any{"type": "input_image", "image_url": dataURL})
	if _, err := service.execute(jsonBytes(request), false); err != nil {
		t.Fatal(err)
	}
	host.stale["file-1"] = true
	if _, err := service.execute(jsonBytes(request), false); err != nil {
		t.Fatalf("stale cached file ID was not refreshed: %v", err)
	}
	if host.uploads != 2 || host.responses != 3 {
		t.Fatalf("uploads=%d responses=%d, want 2 and 3", host.uploads, host.responses)
	}
	if got := stringValue(objectValue(lastUserContent(host.lastBody)[0])["file_id"]); got != "file-2" {
		t.Fatalf("retry sent %q", got)
	}
	// 新上传的 ID 仍被拒时不再重试，保留上游 422。
	host.stale["file-2"] = true
	host.stale["file-3"] = true
	_, err := service.execute(jsonBytes(request), false)
	api, ok := err.(*APIError)
	if !ok || api.Status != 422 || host.uploads != 3 {
		t.Fatalf("second rejection should surface 422 after one refresh: %v uploads=%d", err, host.uploads)
	}
}

func TestFreshUploadRejectionIsNotRetried(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	service := newHTTPTestService()
	host := &attachmentHost{stale: map[string]bool{"file-1": true}}
	service.SetHost(host.handle(t))
	_, err := service.execute(jsonBytes(imageRequest(map[string]any{"type": "input_image", "image_url": dataURL})), false)
	if api, ok := err.(*APIError); !ok || api.Status != 422 || host.uploads != 1 || host.responses != 1 {
		t.Fatalf("fresh upload must not be re-uploaded: %v uploads=%d responses=%d", err, host.uploads, host.responses)
	}
}

func TestHistoryImageUploadFailureDegradesToText(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	service := newHTTPTestService()
	host := &attachmentHost{failNth: map[int]int{1: 400}, stale: map[string]bool{}}
	service.SetHost(host.handle(t))
	if _, err := service.execute(jsonBytes(historyImageRequest(dataURL, true, false)), false); err != nil {
		t.Fatalf("history image failure blocked the conversation: %v", err)
	}
	first, _ := imageItemIndexes(host.lastBody)
	part := objectValue(sentParts(host.lastBody, first)[1])
	if part["type"] != "input_text" || !strings.Contains(stringValue(part["text"]), "earlier turn was omitted") || !strings.Contains(stringValue(part["text"]), "attachment_upload_error 400") {
		t.Fatalf("history image not replaced by a notice: %v", part)
	}
}

func TestHistoryInvalidImageDegradesToText(t *testing.T) {
	service := newHTTPTestService()
	host := &attachmentHost{stale: map[string]bool{}}
	service.SetHost(host.handle(t))
	if _, err := service.execute(jsonBytes(historyImageRequest("data:image/png;base64,bm90LWFuLWltYWdl", true, false)), false); err != nil {
		t.Fatalf("undecodable history image blocked the conversation: %v", err)
	}
	if host.uploads != 0 {
		t.Fatal("undecodable image should not be uploaded")
	}
}

func TestLatestImageFailureStillFails(t *testing.T) {
	for name, tc := range map[string]struct {
		dataURL string
		failNth map[int]int
		status  int
	}{
		"invalid_input": {"data:image/png;base64,bm90LWFuLWltYWdl", nil, 400},
		"upload_error":  {"", map[int]int{1: 400}, 400},
	} {
		t.Run(name, func(t *testing.T) {
			dataURL := tc.dataURL
			if dataURL == "" {
				dataURL, _ = testImageDataURL(t)
			}
			service := newHTTPTestService()
			host := &attachmentHost{failNth: tc.failNth, stale: map[string]bool{}}
			service.SetHost(host.handle(t))
			_, err := service.execute(jsonBytes(historyImageRequest(dataURL, false, true)), false)
			if api, ok := err.(*APIError); !ok || api.Status != tc.status || host.responses != 0 {
				t.Fatalf("latest image failure must fail the request: %v responses=%d", err, host.responses)
			}
		})
	}
}

func TestAuthFailuresDuringHistoryUploadAreNotMasked(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	for _, status := range []int{401, 403, 429} {
		service := newHTTPTestService()
		host := &attachmentHost{failNth: map[int]int{1: status}, stale: map[string]bool{}}
		service.SetHost(host.handle(t))
		_, err := service.execute(jsonBytes(historyImageRequest(dataURL, true, false)), false)
		if api, ok := err.(*APIError); !ok || api.Status != status {
			t.Fatalf("status %d was degraded instead of surfaced: %v", status, err)
		}
	}
}

func TestExpiredHistoryIDRefreshFailureDegrades(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	service := newHTTPTestService()
	host := &attachmentHost{stale: map[string]bool{}}
	service.SetHost(host.handle(t))
	if _, err := service.execute(jsonBytes(historyImageRequest(dataURL, false, true)), false); err != nil {
		t.Fatal(err)
	}
	// 同一张图进入历史轮次；缓存 ID 过期且重传失败时降级而不是整段失败。
	host.stale["file-1"] = true
	host.failNth = map[int]int{2: 400}
	if _, err := service.execute(jsonBytes(historyImageRequest(dataURL, true, false)), false); err != nil {
		t.Fatalf("expired history image blocked the conversation: %v", err)
	}
	first, _ := imageItemIndexes(host.lastBody)
	if part := objectValue(sentParts(host.lastBody, first)[1]); part["type"] != "input_text" {
		t.Fatalf("history image was not degraded after refresh failure: %v", part)
	}
}

func TestExpiredCachedFileIDRefreshedOnStream(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	service := newHTTPTestService()
	var mu sync.Mutex
	uploads, streams := 0, 0
	stale := map[string]bool{}
	var pendingStatus int
	var emitted strings.Builder
	closed := make(chan struct{}, 4)
	service.SetHost(func(method string, payload any, out any) error {
		mu.Lock()
		defer mu.Unlock()
		var request map[string]json.RawMessage
		if err := json.Unmarshal(jsonBytes(payload), &request); err != nil {
			return err
		}
		var result any
		switch method {
		case "host.http.do":
			uploads++
			result = map[string]any{"StatusCode": 200, "Body": jsonBytes(map[string]any{"openai_file_id": fmt.Sprintf("file-%d", uploads)})}
		case "host.http.do_stream":
			streams++
			var raw []byte
			_ = json.Unmarshal(request["body"], &raw)
			body, _ := rawObject(raw)
			pendingStatus = 200
			if stale[stringValue(objectValue(lastUserContent(body)[0])["file_id"])] {
				pendingStatus = 422
			}
			result = map[string]any{"status_code": pendingStatus, "stream_id": "up", "headers": map[string][]string{"Content-Type": {"text/event-stream"}}}
		case "host.http.stream_read":
			if pendingStatus == 422 {
				result = map[string]any{"payload": []byte(`{"detail":"Invalid request body."}`), "done": true}
				break
			}
			response := map[string]any{"id": "resp-s", "status": "completed", "output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "stream ok"}}}}}
			raw := append([]byte("data: "), jsonBytes(map[string]any{"type": "response.completed", "response": response})...)
			result = map[string]any{"payload": append(raw, 10, 10), "done": true}
		case "host.http.stream_close":
		case "host.stream.emit":
			var frame []byte
			_ = json.Unmarshal(request["payload"], &frame)
			emitted.Write(frame)
		case "host.stream.close":
			closed <- struct{}{}
		default:
			return fmt.Errorf("unexpected host callback %s", method)
		}
		if out != nil && result != nil {
			return json.Unmarshal(jsonBytes(result), out)
		}
		return nil
	})
	request := imageRequest(map[string]any{"type": "input_image", "image_url": dataURL})
	request.Stream, request.StreamID = true, "down"
	run := func() {
		if _, err := service.Handle("executor.execute_stream", jsonBytes(request)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("stream did not terminate")
		}
	}
	run()
	mu.Lock()
	stale["file-1"] = true
	emitted.Reset()
	mu.Unlock()
	run()
	mu.Lock()
	defer mu.Unlock()
	if uploads != 2 || streams != 3 || !strings.Contains(emitted.String(), "stream ok") {
		t.Fatalf("stream refresh failed: uploads=%d streams=%d emitted=%q", uploads, streams, emitted.String())
	}
}
