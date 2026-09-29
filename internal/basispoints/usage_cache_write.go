package basispoints

// stripCacheWriteTokens 把下发给客户端的缓存写入计数置 0。
// Basis Points 的 input_tokens 已包含 cache_write_tokens；按「缓存创建与输入分开计价」的下游
// （Anthropic 口径的网关、new-api 等）会把这部分再按缓存创建价计一次。input_tokens、cached_tokens 不动。
// 插件执行器的用量不进 CPA 自身统计（宿主记录为 0 token），所以这里的改写不影响 CPA 侧数字。
func stripCacheWriteTokens(response map[string]any) {
	usage := objectValue(response["usage"])
	if usage == nil {
		return
	}
	for _, key := range []string{"input_tokens_details", "prompt_tokens_details"} {
		details := objectValue(usage[key])
		if details == nil {
			continue
		}
		for _, field := range []string{"cache_write_tokens", "cache_creation_tokens"} {
			if _, ok := details[field]; ok {
				details[field] = 0
			}
		}
	}
}
