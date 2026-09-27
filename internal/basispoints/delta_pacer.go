package basispoints

import (
	"strings"
	"sync"
	"time"
)

// deltaPacer 把上游成块的 output_text.delta 拆小并按节奏放行，避免正文
// 一次性涌出。只改可视节奏，不改事件顺序与内容；任何非文本增量帧到达
// 时先冲刷队列，保证顺序不变。
type deltaPacer struct {
	enabled  bool
	chunk    int
	interval time.Duration

	mu      sync.Mutex
	queue   []string
	timer   *time.Timer
	wake    chan struct{}
	closed  bool
	emitted []string

	// 当前增量的身份字段，随最近一次 queueDelta 更新，放行帧沿用。
	currentOutputIndex  int
	currentContentIndex int
	currentItemID       string
}

// setIdentity 记录增量帧的 item/content 标识，放行的拆小块沿用同一身份。
func (p *deltaPacer) setIdentity(value map[string]any) {
	p.mu.Lock()
	p.currentOutputIndex, _ = value["output_index"].(int)
	p.currentContentIndex, _ = value["content_index"].(int)
	p.currentItemID, _ = value["item_id"].(string)
	p.mu.Unlock()
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

// flush 立即取出全部积压文本，交由调用方逐帧发送。
func (p *deltaPacer) flush() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	pending := p.queue
	p.queue = nil
	p.mu.Unlock()
	return pending
}

// close 停止后台放行；剩余积压由调用方通过 flush 取出。
func (p *deltaPacer) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		if p.timer != nil {
			p.timer.Stop()
		}
	}
	p.mu.Unlock()
	p.signal()
}

func (p *deltaPacer) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// run 是放行循环：有积压时按 nextChunk 切块发送，间隔 interval；
// 积压超过 chunk*16 时放大块长加速清空，避免长正文被拖住。
// stop 关闭后退出。每帧经 send 写出，send 返回错误时退出。
func (p *deltaPacer) run(send func(string) error, stop <-chan struct{}) {
	if p == nil || !p.enabled {
		return
	}
	for {
		p.mu.Lock()
		closed := p.closed
		text := strings.Join(p.queue, "")
		p.queue = nil
		p.mu.Unlock()
		if closed && text == "" {
			return
		}
		for text != "" {
			piece, rest := splitChunk(text, p.chunk, len(text) > p.chunk*16)
			text = rest
			if err := send(piece); err != nil {
				return
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				if text != "" {
					_ = send(text)
				}
				return
			}
			p.mu.Unlock()
			select {
			case <-stop:
				return
			case <-p.wake:
				// 新增量到达时立即并入，继续发送节奏。
			case <-time.After(p.interval):
			}
			p.mu.Lock()
			if len(p.queue) > 0 {
				text += strings.Join(p.queue, "")
				p.queue = nil
			}
			p.mu.Unlock()
		}
		if closed {
			return
		}
		select {
		case <-stop:
			return
		case <-p.wake:
		case <-time.After(p.interval):
			// 节拍到点：可能有 close 置位，回到循环头复查退出。
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
	}
}
