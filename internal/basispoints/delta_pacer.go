package basispoints

import (
	"strings"
	"sync"
	"time"
)

// deltaPacer 把上游成块的 output_text.delta 拆小并按节奏放行，避免正文
// 一次性涌出。只改可视节奏，不改事件顺序与内容；任何非文本增量帧发送
// 前先 drain（等待放行协程把积压发完），保证顺序不变。
type deltaPacer struct {
	enabled  bool
	chunk    int
	interval time.Duration

	mu     sync.Mutex
	queue  []string
	closed bool
	wake   chan struct{}
	done   chan struct{}

	// 当前增量的身份字段，随最近一次 queueDelta 更新，放行帧沿用。
	currentOutputIndex  int
	currentContentIndex int
	currentItemID       string
}

func newDeltaPacer(enabled bool, chunkChars int, intervalMillis int) *deltaPacer {
	if chunkChars < 1 {
		chunkChars = 8
	}
	if intervalMillis < 1 {
		intervalMillis = 20
	}
	return &deltaPacer{
		enabled:  enabled,
		chunk:    chunkChars,
		interval: time.Duration(intervalMillis) * time.Millisecond,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// setIdentity 记录增量帧的 item/content 标识，放行的拆小块沿用同一身份。
func (p *deltaPacer) setIdentity(value map[string]any) {
	p.mu.Lock()
	p.currentOutputIndex, _ = value["output_index"].(int)
	p.currentContentIndex, _ = value["content_index"].(int)
	p.currentItemID, _ = value["item_id"].(string)
	p.mu.Unlock()
}

// queueDelta 把一个文本增量入队。空串直接返回，不产生帧。
func (p *deltaPacer) queueDelta(text string) {
	if p == nil || !p.enabled || text == "" {
		return
	}
	p.mu.Lock()
	p.queue = append(p.queue, text)
	p.mu.Unlock()
	p.signal()
}

// close 停止接受新增量；run 会把已入队内容发完再退出。
func (p *deltaPacer) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.signal()
}

// drain 关闭并等待放行协程退出。返回剩余未发送文本（仅在 stop 提前打断时非空）。
func (p *deltaPacer) drain() []string {
	if p == nil {
		return nil
	}
	p.close()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
	p.mu.Lock()
	pending := p.queue
	p.queue = nil
	p.mu.Unlock()
	return pending
}

func (p *deltaPacer) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// run 是放行循环：有积压时按 chunk 切块发送，间隔 interval；
// 积压超过 chunk*16 时放大块长加速清空，避免长正文被拖住。
// stop 打断时立即退出（drain 负责兜底剩余内容）。
func (p *deltaPacer) run(send func(string) error, stop <-chan struct{}) {
	defer close(p.done)
	if p == nil || !p.enabled {
		return
	}
	var text string
	for {
		p.mu.Lock()
		if len(p.queue) > 0 {
			text += strings.Join(p.queue, "")
			p.queue = nil
		}
		closed := p.closed
		p.mu.Unlock()
		if text == "" {
			if closed {
				return
			}
			select {
			case <-stop:
				return
			case <-p.wake:
			case <-time.After(p.interval):
				// 节拍到点：可能已 close，回到循环头复查。
				p.mu.Lock()
				empty := len(p.queue) == 0 && p.closed
				p.mu.Unlock()
				if empty {
					return
				}
			}
			continue
		}
		piece, rest := splitChunk(text, p.chunk, len(text) > p.chunk*16)
		text = rest
		if err := send(piece); err != nil {
			return
		}
		select {
		case <-stop:
			_ = send(text)
			return
		case <-p.wake:
		case <-time.After(p.interval):
		}
	}
}

// splitChunk 按 rune 切块：块长以字符计，不把多字节字符切半；
// 积压超过阈值时放大块长加速清空。
func splitChunk(text string, chunk int, accelerate bool) (string, string) {
	if chunk < 1 {
		chunk = 1
	}
	if accelerate {
		chunk *= 4
	}
	count := 0
	for index := range text {
		if count == chunk {
			return text[:index], text[index:]
		}
		count++
	}
	return text, ""
}
