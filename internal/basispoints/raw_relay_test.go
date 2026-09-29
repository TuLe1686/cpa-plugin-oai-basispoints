package basispoints

import (
	"strings"
	"testing"
)

func rawShellSource(extra map[string]any) map[string]any {
	properties := map[string]any{"cmd": map[string]any{"type": "string"}}
	for key, value := range extra {
		properties[key] = value
	}
	return map[string]any{"tools": []any{map[string]any{"type": "function", "name": "exec_command", "parameters": map[string]any{
		"type": "object", "required": []any{"cmd"}, "properties": properties,
	}}}}
}

func TestRawRelayWrapsSingleStringArgument(t *testing.T) {
	commands := []string{
		`grep -rn "fail(" --include=*.go . | awk -F: '{print $1}'`,
		"printf '%s\\n' \"a\\\"b\" C:\\new\\test",
		"[ -f x ] && echo ok",
		"{ echo grouped; }",
		"   ",
	}
	for _, cmd := range commands {
		source := rawShellSource(nil)
		native := rawRelayNative(t.Name(), "exec_command", cmd, []any{"exec_command"})
		call, err := extractNativeClientToolCall(native, clientToolSpecs(source))
		if err != nil {
			t.Fatalf("raw command %q rejected: %v", cmd, err)
		}
		arguments, reason := parseRelayObject(call["arguments"])
		if reason != "" || len(arguments) != 1 || arguments["cmd"] != cmd {
			t.Fatalf("raw command changed: %q -> %v", cmd, call["arguments"])
		}
		replay := translateInputItems([]any{call})
		replayed, err := extractNativeClientToolCall(objectValue(replay[0]), clientToolSpecs(source))
		if err != nil || replayed["arguments"] != call["arguments"] {
			t.Fatal("history replay changed the raw-wrapped payload")
		}
	}
}

func TestRawRelayKeepsJSONObjectForm(t *testing.T) {
	source := rawShellSource(map[string]any{"workdir": map[string]any{"type": "string"}})
	code := string(jsonBytes(map[string]any{"cmd": `echo "hi"`, "workdir": "/tmp"}))
	call, err := extractNativeClientToolCall(rawRelayNative(t.Name(), "exec_command", code, []any{"exec_command"}), clientToolSpecs(source))
	if err != nil {
		t.Fatal(err)
	}
	arguments, _ := parseRelayObject(call["arguments"])
	if arguments["cmd"] != `echo "hi"` || arguments["workdir"] != "/tmp" {
		t.Fatalf("object form lost optional arguments: %v", call["arguments"])
	}
}

func TestRawRelayStillRejectsBrokenJSONObject(t *testing.T) {
	source := rawShellSource(nil)
	_, err := extractNativeClientToolCall(rawRelayNative(t.Name(), "exec_command", `{"cmd":"echo "hi""}`, []any{"exec_command"}), clientToolSpecs(source))
	api, ok := err.(*APIError)
	if !ok || api.Kind != "invalid_tool_call" || !strings.Contains(api.Message, "invalid_json") {
		t.Fatalf("broken JSON object must not be executed as raw text: %v", err)
	}
}

func TestRawRelayOnlyForSingleRequiredString(t *testing.T) {
	for name, parameters := range map[string]map[string]any{
		"two_required": {"type": "object", "required": []any{"a", "b"}, "properties": map[string]any{"a": map[string]any{"type": "string"}, "b": map[string]any{"type": "string"}}},
		"non_string":   {"type": "object", "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": "integer"}}},
		"no_required":  {"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}}},
	} {
		t.Run(name, func(t *testing.T) {
			source := map[string]any{"tools": []any{map[string]any{"type": "function", "name": "tool", "parameters": parameters}}}
			if _, ok := singleStringArgument(clientToolSpecs(source)["tool"]); ok {
				t.Fatal("schema must not qualify for raw transport")
			}
			if strings.Contains(clientToolProtocolInstructions(source), "Raw text accepted:") {
				t.Fatal("catalog advertised raw text for an ineligible tool")
			}
			if _, err := extractNativeClientToolCall(rawRelayNative(t.Name(), "tool", "plain text", []any{"tool"}), clientToolSpecs(source)); err == nil {
				t.Fatal("plain text accepted for an ineligible function")
			}
		})
	}
}

func TestRawRelayAdvertisedInCatalog(t *testing.T) {
	instructions := clientToolProtocolInstructions(rawShellSource(nil))
	if !strings.Contains(instructions, "Raw text accepted: code may be the exact cmd text itself") {
		t.Fatal("eligible tool not marked in catalog")
	}
	examples := relayInstructionExamples(t, instructions)
	if len(examples) != 1 || examples[0]["code"] != `printf '%s\n' "hello"` {
		t.Fatalf("shell example should teach raw text: %v", examples)
	}
}
