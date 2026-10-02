package basispoints

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
)

// 只缓存摘要和文件 ID，不保存图片或凭据；容量不限制单次请求的图片数量。
const maxAttachmentCacheEntries = 512

type cachedAttachment struct {
	key    [sha256.Size]byte
	fileID string
}

type pendingAttachment struct {
	done   chan struct{}
	fileID string
	err    error
}

type attachmentCache struct {
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*list.Element
	order   list.List
	pending map[[sha256.Size]byte]*pendingAttachment
}

func (c *attachmentCache) getOrUpload(key [sha256.Size]byte, upload func() (string, error)) (string, error) {
	c.mu.Lock()
	if entry := c.entries[key]; entry != nil {
		c.order.MoveToFront(entry)
		fileID := entry.Value.(cachedAttachment).fileID
		c.mu.Unlock()
		return fileID, nil
	}
	if pending := c.pending[key]; pending != nil {
		c.mu.Unlock()
		<-pending.done
		return pending.fileID, pending.err
	}
	if c.pending == nil {
		c.pending = make(map[[sha256.Size]byte]*pendingAttachment)
	}
	pending := &pendingAttachment{done: make(chan struct{})}
	c.pending[key] = pending
	c.mu.Unlock()

	fileID, err := upload()
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, key)
	if err == nil {
		if c.entries == nil {
			c.entries = make(map[[sha256.Size]byte]*list.Element)
		}
		c.entries[key] = c.order.PushFront(cachedAttachment{key: key, fileID: fileID})
		if c.order.Len() > maxAttachmentCacheEntries {
			oldest := c.order.Back()
			delete(c.entries, oldest.Value.(cachedAttachment).key)
			c.order.Remove(oldest)
		}
	}
	pending.fileID, pending.err = fileID, err
	close(pending.done)
	return fileID, err
}

type inlineImage struct {
	mediaType string
	filename  string
	data      []byte
}

func decodeInlineImage(dataURL string) (inlineImage, error) {
	metadata, encoded, found := strings.Cut(dataURL[5:], ",")
	if !found {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL is missing its data separator")
	}
	isBase64 := strings.HasSuffix(strings.ToLower(metadata), ";base64")
	if isBase64 {
		metadata = metadata[:len(metadata)-len(";base64")]
	}
	mediaType, _, err := mime.ParseMediaType(metadata)
	if err != nil || !strings.HasPrefix(mediaType, "image/") {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL must declare an image media type")
	}
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL has invalid percent encoding")
	}
	var data []byte
	if isBase64 {
		data, err = base64.StdEncoding.DecodeString(decoded)
	} else {
		data = []byte(decoded)
	}
	if err != nil || len(data) == 0 {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL contains empty or invalid image data")
	}
	// 以字节签名确定格式，避免 MIME 别名或系统扩展名数据库产生无后缀、.jfif 等文件名。
	mediaType = http.DetectContentType(data)
	var extension string
	switch mediaType {
	case "image/png":
		extension = ".png"
	case "image/jpeg":
		extension = ".jpeg"
	case "image/gif":
		extension = ".gif"
	case "image/webp":
		extension = ".webp"
	default:
		return inlineImage{}, fail(400, "invalid_image", "input_image bytes must identify a supported format: PNG, JPEG, GIF, or WebP")
	}
	return inlineImage{mediaType: mediaType, filename: "image" + extension, data: data}, nil
}

func attachmentURL(responsesURL string) (string, error) {
	base, err := url.Parse(strings.TrimRight(responsesURL, "/"))
	if err != nil || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", fail(500, "invalid_config", "cannot derive attachments endpoint from responses_url")
	}
	// 官方附件接口与 responses 同目录、同源，不能把自定义上游的凭据发往其他站点。
	return base.ResolveReference(&url.URL{Path: "attachments"}).String(), nil
}

// attachmentHit 记录本次请求复用的缓存文件 ID，上游以 422 拒绝时据此重新上传。
type attachmentHit struct {
	key      [sha256.Size]byte
	item     int
	part     int
	endpoint string
	image    inlineImage
	history  bool
}

type imageHits struct {
	items     []attachmentHit
	refreshed bool
}

func (c *attachmentCache) forget(key [sha256.Size]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil {
		c.order.Remove(entry)
		delete(c.entries, key)
	}
}

func isUserMessage(item map[string]any) bool {
	itemType := stringValue(item["type"])
	return stringValue(item["role"]) == "user" && (itemType == "" || itemType == "message")
}

func latestUserMessage(items []any) int {
	for i := len(items) - 1; i >= 0; i-- {
		if isUserMessage(objectValue(items[i])) {
			return i
		}
	}
	return -1
}

// 只有输入本身不可用的错误才降级；鉴权、限流与传输故障照常失败，避免掩盖可恢复问题。
func degradableImageError(err error) bool {
	var api *APIError
	if !errors.As(err, &api) {
		return false
	}
	switch api.Kind {
	case "invalid_image":
		return true
	case "attachment_upload_error":
		return api.Status >= 400 && api.Status < 500 && api.Status != 401 && api.Status != 403 && api.Status != 407 && api.Status != 429
	}
	return false
}

func omittedImagePart(err error) map[string]any {
	reason := "unavailable"
	var api *APIError
	if errors.As(err, &api) {
		reason = api.Kind
		if api.Status > 0 {
			reason += fmt.Sprintf(" %d", api.Status)
		}
	}
	return map[string]any{"type": "input_text", "text": "[An image from an earlier turn was omitted because it could not be attached (" + reason + ").]"}
}

func (s *Service) uploadInputImages(request ExecutorRequest, body map[string]any, c credential, cfg Config) error {
	_, err := s.uploadInputImagesTracked(request, body, c, cfg)
	return err
}

func (s *Service) uploadInputImagesTracked(request ExecutorRequest, body map[string]any, c credential, cfg Config) (*imageHits, error) {
	hits := &imageHits{}
	items, _ := body["input"].([]any)
	latest := latestUserMessage(items)
	for i, value := range items {
		item := objectValue(value)
		itemType := stringValue(item["type"])
		if stringValue(item["role"]) != "user" || (itemType != "" && itemType != "message") {
			continue
		}
		parts, _ := item["content"].([]any)
		var updated []any
		for j, value := range parts {
			part := objectValue(value)
			if stringValue(part["type"]) != "input_image" {
				continue
			}
			imageURL := stringValue(part["image_url"])
			fileID := stringValue(part["file_id"])
			if fileID != "" && imageURL != "" {
				return nil, fail(400, "invalid_image", fmt.Sprintf("input[%d].content[%d]: input_image cannot contain both image_url and file_id", i, j))
			}
			var replacement map[string]any
			if fileID == "" {
				if len(imageURL) < 5 || !strings.EqualFold(imageURL[:5], "data:") {
					continue
				}
				id, hit, err := s.attachInlineImage(request, imageURL, i, j, c, cfg)
				if err != nil {
					// 更早轮次的坏图不应卡住整段对话；当前用户消息的图片仍严格报错。
					if i == latest || !degradableImageError(err) {
						return nil, err
					}
					replacement = omittedImagePart(err)
				} else {
					fileID = id
					if hit != nil {
						hit.history = i != latest
						hits.items = append(hits.items, *hit)
					}
				}
			}
			if updated == nil {
				updated = append([]any(nil), parts...)
			}
			if replacement == nil {
				// Basis Points 的文件引用不接受公开 Responses API 的 detail 等字段。
				replacement = map[string]any{"type": "input_image", "file_id": fileID}
			}
			updated[j] = replacement
		}
		if updated != nil {
			copy := cloneObject(item)
			copy["content"] = updated
			items[i] = copy
		}
	}
	return hits, nil
}

// attachInlineImage 返回文件 ID；命中缓存时同时返回命中记录，新上传的 ID 不会因过期被拒。
func (s *Service) attachInlineImage(request ExecutorRequest, imageURL string, i, j int, c credential, cfg Config) (string, *attachmentHit, error) {
	image, err := decodeInlineImage(imageURL)
	if err != nil {
		return "", nil, fail(400, "invalid_image", fmt.Sprintf("input[%d].content[%d]: %s", i, j, err))
	}
	endpoint, err := attachmentURL(cfg.ResponsesURL)
	if err != nil {
		return "", nil, err
	}
	key := attachmentKey(endpoint, c, image)
	uploaded := false
	fileID, err := s.attachments.getOrUpload(key, func() (string, error) {
		uploaded = true
		return s.uploadImage(request, endpoint, image, c)
	})
	if err != nil {
		return "", nil, err
	}
	if uploaded {
		return fileID, nil, nil
	}
	return fileID, &attachmentHit{key: key, item: i, part: j, endpoint: endpoint, image: image}, nil
}

func attachmentKey(endpoint string, c credential, image inlineImage) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write(jsonBytes([]string{endpoint, c.AccountID, c.AuthMode, c.AccessToken, image.mediaType}))
	_, _ = hash.Write(image.data)
	var key [sha256.Size]byte
	copy(key[:], hash.Sum(nil))
	return key
}

// refresh 在上游 422 后作废本次复用的缓存 ID 并重新上传；每个请求最多一次。
// 重传失败时当前轮图片直接报错，更早轮次按降级规则替换成文字说明。
func (h *imageHits) refresh(s *Service, request ExecutorRequest, body map[string]any, c credential) (bool, error) {
	if h == nil || h.refreshed || len(h.items) == 0 {
		return false, nil
	}
	h.refreshed = true
	items, _ := body["input"].([]any)
	for _, hit := range h.items {
		s.attachments.forget(hit.key)
		image := hit.image
		fileID, err := s.attachments.getOrUpload(hit.key, func() (string, error) {
			return s.uploadImage(request, hit.endpoint, image, c)
		})
		replacement := map[string]any{"type": "input_image", "file_id": fileID}
		if err != nil {
			if !hit.history || !degradableImageError(err) {
				return true, err
			}
			replacement = omittedImagePart(err)
		}
		item := cloneObject(objectValue(items[hit.item]))
		parts, _ := item["content"].([]any)
		parts[hit.part] = replacement
		items[hit.item] = item
	}
	return true, nil
}

// retryableAttachmentReject 只把「请求体无效」类 422 视为文件 ID 可能过期。
func retryableAttachmentReject(err error) bool {
	var api *APIError
	return errors.As(err, &api) && api.Status == 422 && api.Kind != "invalid_tool_call"
}

func (s *Service) uploadImage(request ExecutorRequest, endpoint string, image inlineImage, c credential) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	partHeaders := make(textproto.MIMEHeader)
	partHeaders.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": image.filename}))
	partHeaders.Set("Content-Type", image.mediaType)
	part, err := writer.CreatePart(partHeaders)
	if err != nil {
		return "", fail(500, "attachment_encoding", "cannot encode image attachment")
	}
	if _, err := part.Write(image.data); err != nil {
		return "", fail(500, "attachment_encoding", "cannot write image attachment")
	}
	if err := writer.Close(); err != nil {
		return "", fail(500, "attachment_encoding", "cannot finish image attachment")
	}
	headers := responseHeaders(c, false)
	headers.Set("Content-Type", writer.FormDataContentType())
	response, err := s.doHTTP(request, endpoint, headers, body.Bytes())
	if err != nil {
		var apiError *APIError
		if errors.As(err, &apiError) {
			return "", apiError
		}
		return "", fail(502, "attachment_transport", "Basis Points attachment upload transport failed: "+attachmentErrorMessage([]byte(err.Error()), c, image))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fail(response.StatusCode, "attachment_upload_error", fmt.Sprintf("Basis Points attachment upload HTTP %d: %s", response.StatusCode, attachmentErrorMessage(response.Body, c, image)))
	}
	var result struct {
		FileID string `json:"openai_file_id"`
	}
	if json.Unmarshal(response.Body, &result) != nil || strings.TrimSpace(result.FileID) == "" {
		return "", fail(502, "invalid_attachment_response", "Basis Points attachment upload returned no openai_file_id")
	}
	return strings.TrimSpace(result.FileID), nil
}

func attachmentErrorMessage(raw []byte, c credential, image inlineImage) string {
	// Redact long credentials and image data before truncating the raw body, then redact
	// the decoded message again for JSON-escaped dynamic headers and image data.
	message := redactAttachmentSecrets(c.redactMessage(string(raw)), image)
	message = c.redactMessage(errorMessage([]byte(message)))
	return redactTokenMessage(redactAttachmentSecrets(message, image))
}

func redactAttachmentSecrets(message string, image inlineImage) string {
	for _, secret := range []string{base64.StdEncoding.EncodeToString(image.data), string(image.data)} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
