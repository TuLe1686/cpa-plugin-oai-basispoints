package basispoints

// 既有用例验证原 HTTP/SSE 协议；WS 默认优先及回退由独立真实 socket 测试覆盖。
// 平滑器会改变到达节奏，时序类断言不受它影响，因此协议用例统一关闭平滑；
// 平滑行为由 delta_pacer_test.go 单独覆盖。
func newHTTPTestService() *Service {
	svc := NewService()
	svc.cfg.UpstreamTransport = "http"
	svc.cfg.SmoothStream = false
	return svc
}
