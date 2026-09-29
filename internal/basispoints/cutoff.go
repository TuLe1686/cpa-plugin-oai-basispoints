package basispoints

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// recoverCutoffResponse 在 SSE 缺少终态时按已完成条目重建 completed 响应。
// 只有全部已开始的条目都已完成、且最后一项是最终回答或工具调用时才补全；
// 仅有 commentary 前言、条目未完成或上游报错时一律不补，保持原有失败。
func recoverCutoffResponse(raw []byte) (map[string]any, bool) {
	var meta map[string]any
	added := map[int]bool{}
	done := map[int]map[string]any{}
	failed := false
	emit := func(event, data string) error {
		if strings.TrimSpace(data) == "[DONE]" {
			return nil
		}
		object, reason := parseRelayObject(data)
		if reason != "" {
			failed = true
			return nil
		}
		kind := stringValue(object["type"])
		if kind == "" {
			kind = event
		}
		switch kind {
		case "error", "response.failed", "response.cancelled", "response.completed", "response.incomplete":
			failed = true
		case "response.created", "response.in_progress":
			if response := objectValue(object["response"]); meta == nil && stringValue(response["id"]) != "" {
				meta = cloneObject(response)
			}
		case "response.output_item.added", "response.output_item.done":
			index, err := streamIndex(object["output_index"])
			item := objectValue(object["item"])
			if err != nil || item == nil {
				failed = true
				return nil
			}
			added[index] = true
			if kind == "response.output_item.done" {
				done[index] = cloneObject(item)
			}
		}
		return nil
	}
	decoder := newSSEDecoder()
	if decoder.feed(raw, emit) != nil || decoder.feed([]byte{10, 10}, emit) != nil || failed || meta == nil || len(done) == 0 || len(added) != len(done) {
		return nil, false
	}
	output := make([]any, len(done))
	for index := range output {
		item, ok := done[index]
		if !ok {
			return nil, false
		}
		output[index] = item
	}
	if !finalCutoffItem(objectValue(output[len(output)-1])) {
		return nil, false
	}
	response := meta
	response["status"] = "completed"
	response["output"] = output
	delete(response, "incomplete_details")
	delete(response, "error")
	return response, true
}

func finalCutoffItem(item map[string]any) bool {
	switch stringValue(item["type"]) {
	case "function_call", "custom_tool_call":
		return true
	case "message":
		if stringValue(item["phase"]) == "commentary" {
			return false
		}
		content, _ := item["content"].([]any)
		for _, part := range content {
			if object := objectValue(part); stringValue(object["type"]) == "output_text" && stringValue(object["text"]) != "" {
				return true
			}
		}
	}
	return false
}

// 只处理「流结束但缺少终态」与流读取中断两类截断，其余错误原样返回。
func cutoffCandidate(err error) bool {
	var api *APIError
	if !errors.As(err, &api) {
		return false
	}
	return (api.Kind == "invalid_upstream_response" && strings.Contains(api.Message, "ended without a terminal response")) ||
		(api.Kind == "upstream_transport" && strings.Contains(api.Message, "stream interrupted"))
}

func (s *Service) completeCutoff(raw []byte, err error) (map[string]any, error) {
	if !s.config().CutoffCompletion || !cutoffCandidate(err) {
		return nil, err
	}
	response, ok := recoverCutoffResponse(raw)
	if !ok {
		return nil, err
	}
	_ = s.call("host.log", map[string]any{"level": "warn", "message": fmt.Sprintf("Basis Points stream cut off before its terminal event; completed locally: basispoints_cutoff_completed items=%d", len(response["output"].([]any)))}, nil)
	return response, nil
}

func (s *Service) parseUpstreamResponse(raw []byte, headers http.Header) (map[string]any, error) {
	response, err := parseResponse(raw, headers)
	if err != nil {
		return s.completeCutoff(raw, err)
	}
	return response, nil
}
