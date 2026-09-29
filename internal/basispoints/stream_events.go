package basispoints

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type streamedPart struct {
	kind     string
	text     strings.Builder
	logs     []any
	textDone bool
	done     bool
}

type streamedMessage struct {
	id    string
	parts map[int]*streamedPart
	done  bool
}

type streamedReasoning struct {
	id          string
	summaries   map[int]*streamedPart
	done        bool
	doneSummary []any
}

// 提前交付普通消息和推理摘要；任何工具名称和参数均等待完整终态及整批校验。
type streamDelivery struct {
	format        string
	start         func()
	write         func([]byte) error
	committed     bool
	disconnected  bool
	sequence      int
	meta          map[string]any
	pending       []map[string]any
	knownMessages map[int]string
	knownParts    map[[2]int]bool
	messages      map[int]*streamedMessage
	reasonings    map[int]*streamedReasoning
	terminal      bool
	sentinel      bool
	pacer         *deltaPacer
	pacerStop     chan struct{}
	pacerOnce     sync.Once
	writeMu       sync.Mutex
	// stateMu 串行化读取协程与保活协程对交付状态的访问；重试只能 reset，不能整体替换结构体。
	stateMu   sync.Mutex
	lastWrite atomic.Int64
}

// reset 为一次未提交的重生成清空协议状态，保留平滑器、写出函数与锁。
func (d *streamDelivery) reset() {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	d.committed, d.disconnected, d.terminal, d.sentinel = false, false, false, false
	d.sequence, d.meta, d.pending = 0, nil, nil
	d.knownMessages = map[int]string{}
	d.knownParts = map[[2]int]bool{}
	d.messages = map[int]*streamedMessage{}
	d.reasonings = map[int]*streamedReasoning{}
}

func newStreamDelivery(format string, start func(), write func([]byte) error) *streamDelivery {
	return &streamDelivery{format: format, start: start, write: write, knownMessages: map[int]string{}, knownParts: map[[2]int]bool{}, messages: map[int]*streamedMessage{}, reasonings: map[int]*streamedReasoning{}}
}

// enableSmoothing 接入正文平滑器；在首个文本增量前调用一次。
// 后台协程按配置节奏发送拆小的 delta，非增量帧经 flushAndWait 保序。
func (d *streamDelivery) enableSmoothing(pacer *deltaPacer) {
	d.pacerOnce.Do(func() {
		d.pacer = pacer
		d.pacerStop = make(chan struct{})
		go pacer.run(func(piece string) error {
			index, content, item := pacer.identity()
			return d.emitDirect(map[string]any{"type": "response.output_text.delta", "output_index": index, "content_index": content, "item_id": item, "delta": piece})
		}, d.pacerStop)
	})
}

func streamEventError() error {
	return fail(502, "invalid_upstream_stream", "Basis Points stream contains inconsistent output events")
}

func streamIndex(value any) (int, error) {
	n, ok := value.(json.Number)
	if !ok {
		return 0, streamEventError()
	}
	i, err := n.Int64()
	if err != nil || i < 0 || i > 1<<30 {
		return 0, streamEventError()
	}
	return int(i), nil
}

func (d *streamDelivery) consume(event, data string) error {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	return d.consumeLocked(event, data)
}

func (d *streamDelivery) consumeLocked(event, data string) error {
	if strings.TrimSpace(data) == "[DONE]" {
		d.sentinel = true
		return nil
	}
	value, reason := parseRelayObject(data)
	if reason != "" {
		return fail(502, "invalid_upstream_response", "Basis Points returned invalid SSE JSON")
	}
	if d.terminal || d.sentinel {
		return streamEventError()
	}
	kind := stringValue(value["type"])
	if kind == "" {
		kind = event
		value["type"] = kind
	}
	switch kind {
	case "error", "response.failed", "response.cancelled":
		return upstreamFailure(kind, value)
	case "response.completed", "response.incomplete":
		d.terminal = true
		return nil
	case "response.created", "response.in_progress":
		meta := objectValue(value["response"])
		if d.meta != nil && meta["id"] != d.meta["id"] {
			return streamEventError()
		}
		if d.meta == nil && stringValue(meta["id"]) != "" {
			d.meta = cloneObject(meta)
		}
		return nil
	case "response.output_item.added", "response.output_item.done":
		item := objectValue(value["item"])
		itemType := stringValue(item["type"])
		if itemType != "message" && itemType != "reasoning" {
			return nil
		}
		index, err := streamIndex(value["output_index"])
		if err != nil {
			return err
		}
		if kind == "response.output_item.added" && itemType == "message" {
			d.knownMessages[index] = stringValue(item["id"])
		}
	case "response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done":
		if _, err := streamIndex(value["output_index"]); err != nil {
			return err
		}
		if _, err := streamIndex(value["summary_index"]); err != nil {
			return err
		}
	case "response.content_part.added", "response.content_part.done", "response.output_text.delta", "response.output_text.done":
		index, err := streamIndex(value["output_index"])
		if err != nil {
			return err
		}
		part, err := streamIndex(value["content_index"])
		if err != nil {
			return err
		}
		if kind == "response.content_part.added" {
			d.knownParts[[2]int{index, part}] = stringValue(objectValue(value["part"])["type"]) == "output_text"
		}
	default:
		// 工具增量及其他终态信息由完整响应保留，不作为正文暴露。
		return nil
	}
	if d.committed {
		frame, err := d.applyEvent(value)
		if err != nil {
			return err
		}
		return d.emit(frame)
	}
	d.pending = append(d.pending, value)
	delta, _ := value["delta"].(string)
	if (kind != "response.output_text.delta" && kind != "response.reasoning_summary_text.delta") || delta == "" || d.meta == nil {
		return nil
	}
	if kind == "response.output_text.delta" {
		index, _ := streamIndex(value["output_index"])
		part, _ := streamIndex(value["content_index"])
		if d.knownMessages[index] == "" || d.knownMessages[index] != stringValue(value["item_id"]) || !d.knownParts[[2]int{index, part}] {
			return nil
		}
	}
	frames := make([]map[string]any, 0, len(d.pending))
	for _, item := range d.pending {
		frame, err := d.applyEvent(item)
		if err != nil {
			return err
		}
		frames = append(frames, frame)
	}
	d.pending = nil
	return d.commitWith(frames)
}

func (d *streamDelivery) commitWith(frames []map[string]any) error {
	d.committed = true
	d.start()
	created := cloneObject(d.meta)
	created["status"], created["output"] = "in_progress", []any{}
	for _, kind := range []string{"response.created", "response.in_progress"} {
		if err := d.emit(map[string]any{"type": kind, "response": created}); err != nil {
			return err
		}
	}
	for _, frame := range frames {
		if err := d.emit(frame); err != nil {
			return err
		}
	}
	return nil
}

// heartbeat 在上游静默时发送 response.in_progress。只在上游已返回 response.created
// 后才会提交流：此时 HTTP 状态已确定，只放弃未提交时的一次工具重生成。
func (d *streamDelivery) heartbeat() error {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	if d.meta == nil || d.terminal || d.sentinel || d.disconnected {
		return nil
	}
	if !d.committed {
		frames := make([]map[string]any, 0, len(d.pending))
		for _, item := range d.pending {
			frame, err := d.applyEvent(item)
			if err != nil {
				return err
			}
			frames = append(frames, frame)
		}
		d.pending = nil
		return d.commitWith(frames)
	}
	progress := cloneObject(d.meta)
	progress["status"], progress["output"] = "in_progress", []any{}
	return d.emit(map[string]any{"type": "response.in_progress", "response": progress})
}

// startKeepAlive 启动静默保活；返回的函数停止协程并等待其退出，须在 finish/fail 之前调用。
func (d *streamDelivery) startKeepAlive(interval time.Duration) func() {
	if interval <= 0 {
		return func() {}
	}
	d.lastWrite.Store(time.Now().UnixNano())
	stop, done := make(chan struct{}), make(chan struct{})
	tick := interval / 4
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, d.lastWrite.Load())) < interval {
					continue
				}
				// 写出失败会标记 disconnected，读取侧随后按断连收尾。
				_ = d.heartbeat()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

func (d *streamDelivery) isCommitted() bool {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	return d.committed
}

func (d *streamDelivery) applyEvent(value map[string]any) (map[string]any, error) {
	kind := stringValue(value["type"])
	if strings.HasPrefix(kind, "response.reasoning_summary_") ||
		((kind == "response.output_item.added" || kind == "response.output_item.done") && objectValue(value["item"])["type"] == "reasoning") {
		return d.applyReasoningEvent(value)
	}
	return d.applyMessageEvent(value)
}

// 推理条目保持原样，使用独立状态记录交付进度，不能登记为普通消息。
func (d *streamDelivery) applyReasoningEvent(value map[string]any) (map[string]any, error) {
	index, err := streamIndex(value["output_index"])
	if err != nil {
		return nil, err
	}
	kind := stringValue(value["type"])
	r := d.reasonings[index]
	if kind == "response.output_item.added" {
		id := stringValue(objectValue(value["item"])["id"])
		if r != nil || d.messages[index] != nil || id == "" {
			return nil, streamEventError()
		}
		d.reasonings[index] = &streamedReasoning{id: id, summaries: map[int]*streamedPart{}}
		return value, nil
	}
	if r == nil || r.done {
		return nil, streamEventError()
	}
	if kind == "response.output_item.done" {
		if err := r.validateItem(objectValue(value["item"])); err != nil {
			return nil, err
		}
		for _, part := range r.summaries {
			if !part.done {
				return nil, streamEventError()
			}
		}
		r.done = true
		r.doneSummary, _ = objectValue(value["item"])["summary"].([]any)
		return value, nil
	}
	if value["item_id"] != r.id {
		return nil, streamEventError()
	}
	partIndex, err := streamIndex(value["summary_index"])
	if err != nil {
		return nil, err
	}
	part := r.summaries[partIndex]
	if kind == "response.reasoning_summary_part.added" {
		if part != nil || partIndex != len(r.summaries) || (partIndex > 0 && !r.summaries[partIndex-1].done) || objectValue(value["part"])["type"] != "summary_text" {
			return nil, streamEventError()
		}
		r.summaries[partIndex] = &streamedPart{kind: "summary_text"}
		return value, nil
	}
	if part == nil || part.done {
		return nil, streamEventError()
	}
	switch kind {
	case "response.reasoning_summary_text.delta":
		text, ok := value["delta"].(string)
		if !ok || part.textDone {
			return nil, streamEventError()
		}
		part.text.WriteString(text)
	case "response.reasoning_summary_text.done":
		if part.textDone || value["text"] != part.text.String() {
			return nil, streamEventError()
		}
		part.textDone = true
	case "response.reasoning_summary_part.done":
		completed := objectValue(value["part"])
		if !part.textDone || completed["type"] != "summary_text" || completed["text"] != part.text.String() {
			return nil, streamEventError()
		}
		part.done = true
	}
	return value, nil
}

func (r *streamedReasoning) validateItem(item map[string]any) error {
	if item["type"] != "reasoning" || item["id"] != r.id {
		return streamEventError()
	}
	summaries, _ := item["summary"].([]any)
	if r.done && !bytes.Equal(jsonBytes(summaries), jsonBytes(r.doneSummary)) {
		return streamEventError()
	}
	for i, part := range r.summaries {
		if i >= len(summaries) {
			return streamEventError()
		}
		finalPart := objectValue(summaries[i])
		text, ok := finalPart["text"].(string)
		if finalPart["type"] != "summary_text" || !ok || !strings.HasPrefix(text, part.text.String()) || (part.textDone && text != part.text.String()) {
			return streamEventError()
		}
	}
	return nil
}

func (d *streamDelivery) applyMessageEvent(value map[string]any) (map[string]any, error) {
	index, err := streamIndex(value["output_index"])
	if err != nil {
		return nil, err
	}
	kind := stringValue(value["type"])
	frame := cloneObject(value)
	m := d.messages[index]
	if kind == "response.output_item.added" {
		item := objectValue(value["item"])
		id := stringValue(item["id"])
		if m != nil || d.reasonings[index] != nil || id == "" {
			return nil, streamEventError()
		}
		d.messages[index] = &streamedMessage{id: id, parts: map[int]*streamedPart{}}
		added := cloneObject(item)
		added["status"], added["content"] = "in_progress", []any{}
		frame["item"] = added
		return frame, nil
	}
	if m == nil || m.done {
		return nil, streamEventError()
	}
	if kind == "response.output_item.done" {
		item := objectValue(value["item"])
		content, _ := item["content"].([]any)
		if item["id"] != m.id || len(content) != len(m.parts) {
			return nil, streamEventError()
		}
		for i, part := range m.parts {
			if !part.done || (part.kind == "output_text" && objectValue(content[i])["text"] != part.text.String()) {
				return nil, streamEventError()
			}
		}
		m.done = true
		return frame, nil
	}
	if value["item_id"] != m.id {
		return nil, streamEventError()
	}
	partIndex, err := streamIndex(value["content_index"])
	if err != nil {
		return nil, err
	}
	part := m.parts[partIndex]
	if kind == "response.content_part.added" {
		if part != nil || partIndex != len(m.parts) || (partIndex > 0 && !m.parts[partIndex-1].done) {
			return nil, streamEventError()
		}
		added := cloneObject(objectValue(value["part"]))
		part = &streamedPart{kind: stringValue(added["type"])}
		m.parts[partIndex] = part
		if part.kind == "output_text" {
			added["text"] = ""
			if _, exists := added["logprobs"]; exists {
				added["logprobs"] = []any{}
			}
		}
		frame["part"] = added
		return frame, nil
	}
	if part == nil || part.done {
		return nil, streamEventError()
	}
	switch kind {
	case "response.output_text.delta":
		text, ok := value["delta"].(string)
		if !ok || part.kind != "output_text" || part.textDone {
			return nil, streamEventError()
		}
		part.text.WriteString(text)
		logs, _ := value["logprobs"].([]any)
		part.logs = append(part.logs, logs...)
		if logs == nil {
			frame["logprobs"] = []any{}
		}
	case "response.output_text.done":
		if part.kind != "output_text" || part.textDone || value["text"] != part.text.String() {
			return nil, streamEventError()
		}
		part.textDone = true
	case "response.content_part.done":
		completed := objectValue(value["part"])
		if stringValue(completed["type"]) != part.kind || (part.kind == "output_text" && (!part.textDone || completed["text"] != part.text.String())) {
			return nil, streamEventError()
		}
		part.done = true
	}
	return frame, nil
}

// 终态必须与已交付文本相符；先核验这一点，再允许工具整批校验写入历史身份缓存。
func (d *streamDelivery) validateFinal(response map[string]any) error {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	if !d.committed {
		return nil
	}
	if response["id"] != d.meta["id"] {
		return streamEventError()
	}
	output, _ := response["output"].([]any)
	for index, reasoning := range d.reasonings {
		if index >= len(output) {
			return streamEventError()
		}
		if err := reasoning.validateItem(objectValue(output[index])); err != nil {
			return err
		}
	}
	for index, message := range d.messages {
		if index >= len(output) {
			return streamEventError()
		}
		item := objectValue(output[index])
		if item["type"] != "message" || item["id"] != message.id {
			return streamEventError()
		}
		content, _ := item["content"].([]any)
		if message.done && len(content) != len(message.parts) {
			return streamEventError()
		}
		for i, part := range message.parts {
			if i >= len(content) {
				return streamEventError()
			}
			finalPart := objectValue(content[i])
			if finalPart["type"] != part.kind {
				return streamEventError()
			}
			if part.kind == "output_text" {
				text, ok := finalPart["text"].(string)
				if !ok || !strings.HasPrefix(text, part.text.String()) || (part.textDone && text != part.text.String()) {
					return streamEventError()
				}
				logs, _ := finalPart["logprobs"].([]any)
				if len(part.logs) > len(logs) || !bytes.Equal(jsonBytes(part.logs), jsonBytes(logs[:len(part.logs)])) {
					if len(part.logs) > 0 {
						return streamEventError()
					}
				}
			}
		}
	}
	return nil
}

// drainPacer 结束放行协程并等它把积压发完；仅在流终局调用。
func (d *streamDelivery) drainPacer() {
	if d.pacer == nil {
		return
	}
	select {
	case <-d.pacerStop:
	default:
		close(d.pacerStop)
	}
	d.pacer.drain()
	d.pacer = nil
}

func (d *streamDelivery) emit(value map[string]any) error {
	// 平滑器只接管文本增量帧；其余帧等积压正文全部写出再直通，
	// 放行协程保持存活，后续增量继续走平滑。
	if d.pacer != nil && stringValue(value["type"]) == "response.output_text.delta" {
		if text, ok := value["delta"].(string); ok && text != "" {
			d.pacer.setIdentity(value)
			d.pacer.queueDelta(text)
			return nil
		}
	}
	if d.pacer != nil {
		d.pacer.flushAndWait()
	}
	return d.emitDirect(value)
}

// emitDirect 是唯一的实际写出路径；平滑协程与冲刷后的直发都走这里。
// 放行协程与调用方可能并发到达（flushAndWait 超时兜底直通），写出互斥。
func (d *streamDelivery) emitDirect(value map[string]any) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	frame := cloneObject(value)
	frame["sequence_number"] = d.sequence
	d.sequence++
	var payload []byte
	if d.format == "codex" {
		payload = append([]byte("data: "), jsonBytes(frame)...)
	} else {
		var b strings.Builder
		writeSSE(&b, stringValue(frame["type"]), frame)
		payload = []byte(b.String())
	}
	return d.emitBytes(payload)
}

func (d *streamDelivery) emitBytes(payload []byte) error {
	d.lastWrite.Store(time.Now().UnixNano())
	if err := d.write(payload); err != nil {
		d.disconnected = true
		return fail(499, "client_disconnected", "client disconnected while receiving stream")
	}
	return nil
}

func (d *streamDelivery) finish(response map[string]any) error {
	if !d.committed {
		d.committed = true
		d.start()
		for _, frame := range executorStreamPayloads(d.format, response) {
			if err := d.emitBytes(frame); err != nil {
				return err
			}
		}
		d.drainPacer()
		return nil
	}
	decoder := newSSEDecoder()
	err := decoder.feed(syntheticStream(response), func(kind, data string) error {
		if data == "[DONE]" {
			if d.format != "codex" {
				return d.emitBytes([]byte("data: [DONE]\n\n"))
			}
			return nil
		}
		value, reason := parseRelayObject(data)
		if reason != "" {
			return streamEventError()
		}
		if kind == "response.created" || kind == "response.in_progress" {
			return nil
		}
		index, indexErr := streamIndex(value["output_index"])
		if reasoning := d.reasonings[index]; indexErr == nil && reasoning != nil {
			if reasoning.done || kind == "response.output_item.added" {
				return nil
			}
			partIndex, partErr := streamIndex(value["summary_index"])
			if part := reasoning.summaries[partIndex]; partErr == nil && part != nil {
				switch kind {
				case "response.reasoning_summary_part.added":
					return nil
				case "response.reasoning_summary_text.delta":
					text := value["delta"].(string)[part.text.Len():]
					if text == "" {
						return nil
					}
					value["delta"] = text
				case "response.reasoning_summary_text.done":
					if part.textDone {
						return nil
					}
				case "response.reasoning_summary_part.done":
					if part.done {
						return nil
					}
				}
			}
		}
		message := d.messages[index]
		if indexErr == nil && message != nil {
			if kind == "response.output_item.added" || (kind == "response.output_item.done" && message.done) {
				return nil
			}
			partIndex, partErr := streamIndex(value["content_index"])
			if part := message.parts[partIndex]; partErr == nil && part != nil {
				switch kind {
				case "response.content_part.added":
					return nil
				case "response.output_text.delta":
					text := value["delta"].(string)[part.text.Len():]
					if text == "" {
						return nil
					}
					value["delta"] = text
					if logs, ok := value["logprobs"].([]any); ok {
						value["logprobs"] = logs[len(part.logs):]
					}
				case "response.output_text.done":
					if part.textDone {
						return nil
					}
				case "response.content_part.done":
					if part.done {
						return nil
					}
				}
			}
		}
		return d.emit(value)
	})
	if err != nil {
		return err
	}
	// 回放的增量同样经过平滑；回放结束后统一排空，保证终态最后写出。
	d.drainPacer()
	return nil
}

func (d *streamDelivery) fail(err error) error {
	d.drainPacer()
	kind, message, status := "stream_failed", "Basis Points stream failed after response delivery began", 502
	errorType := upstreamErrorType(status)
	var api *APIError
	if errors.As(err, &api) {
		kind, message, status = api.Kind, api.Message, api.Status
		errorType = upstreamErrorType(status)
		if api.Type != "" {
			errorType = api.Type
		}
	}
	return d.emit(map[string]any{"type": "error", "status": status, "code": kind, "message": message, "param": nil, "error": map[string]any{"type": errorType, "code": kind, "message": message}})
}
