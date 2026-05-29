use crate::auth::SharedAuthProvider;
use crate::common::ResponseStream;
use crate::common::ResponsesApiRequest;
use crate::endpoint::session::EndpointSession;
use crate::error::ApiError;
use crate::provider::Provider;
use crate::requests::Compression;
use crate::requests::attach_item_ids;
use crate::requests::headers::build_session_headers;
use crate::requests::headers::insert_header;
use crate::requests::headers::subagent_header;
use crate::sse::anthropic::spawn_anthropic_stream;
use crate::sse::chat_completions::reasoning_content_for_tool_call;
use crate::sse::spawn_chat_completions_stream;
use crate::sse::spawn_response_stream;
use crate::telemetry::SseTelemetry;
use codex_client::HttpTransport;
use codex_client::RequestCompression;
use codex_client::RequestTelemetry;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Method;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::sync::OnceLock;
use tracing::instrument;

pub struct ResponsesClient<T: HttpTransport> {
    session: EndpointSession<T>,
    sse_telemetry: Option<Arc<dyn SseTelemetry>>,
}

#[derive(Default)]
pub struct ResponsesOptions {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub session_source: Option<SessionSource>,
    pub extra_headers: HeaderMap,
    pub compression: Compression,
    pub turn_state: Option<Arc<OnceLock<String>>>,
}

impl<T: HttpTransport> ResponsesClient<T> {
    pub fn new(transport: T, provider: Provider, auth: SharedAuthProvider) -> Self {
        Self {
            session: EndpointSession::new(transport, provider, auth),
            sse_telemetry: None,
        }
    }

    pub fn with_telemetry(
        self,
        request: Option<Arc<dyn RequestTelemetry>>,
        sse: Option<Arc<dyn SseTelemetry>>,
    ) -> Self {
        Self {
            session: self.session.with_request_telemetry(request),
            sse_telemetry: sse,
        }
    }

    #[instrument(
        name = "responses.stream_request",
        level = "info",
        skip_all,
        fields(
            transport = "responses_http",
            http.method = "POST",
            api.path = "responses"
        )
    )]
    pub async fn stream_request(
        &self,
        request: ResponsesApiRequest,
        options: ResponsesOptions,
    ) -> Result<ResponseStream, ApiError> {
        let ResponsesOptions {
            session_id,
            thread_id,
            session_source,
            extra_headers,
            compression,
            turn_state,
        } = options;

        let mut body = serde_json::to_value(&request)
            .map_err(|e| ApiError::Stream(format!("failed to encode responses request: {e}")))?;
        if self.session.provider().is_azure_responses_endpoint() {
            normalize_responses_body_roles(&mut body);
            if request.store {
                attach_item_ids(&mut body, &request.input);
            }
        }

        let mut headers = extra_headers;
        if let Some(ref thread_id) = thread_id {
            insert_header(&mut headers, "x-client-request-id", thread_id);
        }
        headers.extend(build_session_headers(session_id, thread_id));
        if let Some(subagent) = subagent_header(&session_source) {
            insert_header(&mut headers, "x-openai-subagent", &subagent);
        }

        self.stream(body, headers, compression, turn_state).await
    }

    fn path() -> &'static str {
        "responses"
    }

    #[instrument(
        name = "responses.stream",
        level = "info",
        skip_all,
        fields(
            transport = "responses_http",
            http.method = "POST",
            api.path = "responses",
            turn.has_state = turn_state.is_some()
        )
    )]
    pub async fn stream(
        &self,
        body: Value,
        extra_headers: HeaderMap,
        compression: Compression,
        turn_state: Option<Arc<OnceLock<String>>>,
    ) -> Result<ResponseStream, ApiError> {
        let request_compression = match compression {
            Compression::None => RequestCompression::None,
            Compression::Zstd => RequestCompression::Zstd,
        };

        use crate::provider::WireApiKind;
        if self.session.provider().wire_api == WireApiKind::ChatCompletions {
            let chat_body = responses_body_to_chat_completions_body(body)?;
            let stream_response = self
                .session
                .stream_with(
                    Method::POST,
                    "chat/completions",
                    extra_headers,
                    Some(chat_body),
                    |req| {
                        req.headers.insert(
                            http::header::ACCEPT,
                            HeaderValue::from_static("text/event-stream"),
                        );
                        req.compression = request_compression;
                    },
                )
                .await?;
            return Ok(spawn_chat_completions_stream(
                stream_response,
                self.session.provider().stream_idle_timeout,
            ));
        } else if self.session.provider().wire_api == WireApiKind::Anthropic {
            let anthropic_body = responses_body_to_anthropic_body(body)?;
            let stream_response = self
                .session
                .stream_with(
                    Method::POST,
                    "v1/messages",
                    extra_headers,
                    Some(anthropic_body),
                    |req| {
                        req.headers.insert(
                            http::header::ACCEPT,
                            HeaderValue::from_static("text/event-stream"),
                        );
                        req.headers.insert(
                            http::header::CONTENT_TYPE,
                            HeaderValue::from_static("application/json"),
                        );
                        req.headers.insert(
                            HeaderName::from_static("anthropic-version"),
                            HeaderValue::from_static("2023-06-01"),
                        );
                        req.compression = request_compression;
                    },
                )
                .await?;
            return Ok(spawn_anthropic_stream(
                stream_response,
                self.session.provider().stream_idle_timeout,
            ));
        }

        let stream_response = self
            .session
            .stream_with(
                Method::POST,
                Self::path(),
                extra_headers,
                Some(body),
                |req| {
                    req.headers.insert(
                        http::header::ACCEPT,
                        HeaderValue::from_static("text/event-stream"),
                    );
                    req.compression = request_compression;
                },
            )
            .await?;

        Ok(spawn_response_stream(
            stream_response,
            self.session.provider().stream_idle_timeout,
            self.sse_telemetry.clone(),
            turn_state,
        ))
    }
}

fn normalize_responses_body_roles(body: &mut Value) {
    let Some(input) = body.get_mut("input").and_then(Value::as_array_mut) else {
        return;
    };
    for item in input {
        let Some(role) = item.get_mut("role") else {
            continue;
        };
        let Some(role_str) = role.as_str() else {
            continue;
        };
        let normalized = responses_role_for_azure(role_str);
        if normalized != role_str {
            *role = Value::String(normalized.to_string());
        }
    }
}

fn responses_role_for_azure(role: &str) -> &str {
    match role {
        "system" | "user" | "assistant" | "tool" | "latest_reminder" => role,
        "developer" => "system",
        _ => "user",
    }
}

fn responses_body_to_chat_completions_body(body: Value) -> Result<Value, ApiError> {
    let request: ResponsesApiRequest = serde_json::from_value(body).map_err(|err| {
        ApiError::Stream(format!(
            "failed to decode responses request for chat completions: {err}"
        ))
    })?;

    let mut messages = Vec::new();
    if !request.instructions.trim().is_empty() {
        messages.push(json!({
            "role": "system",
            "content": request.instructions,
        }));
    }
    messages.extend(response_items_to_chat_messages(&request.input));

    let tools = response_tools_to_chat_tools(&request.tools);
    let mut chat_body = json!({
        "model": request.model,
        "messages": messages,
        "stream": true,
        "stream_options": { "include_usage": true },
    });
    if !tools.is_empty() {
        chat_body["tools"] = Value::Array(tools);
        chat_body["tool_choice"] = Value::String(request.tool_choice);
        chat_body["parallel_tool_calls"] = Value::Bool(request.parallel_tool_calls);
    }
    Ok(chat_body)
}

/// Default `max_tokens` for the Anthropic Messages API.
///
/// The Messages API requires `max_tokens`, while the Responses API expresses no
/// equivalent ceiling. 16384 is high enough for long code generations yet stays
/// within the documented output limits of current Claude and MiMo models.
const ANTHROPIC_DEFAULT_MAX_TOKENS: u32 = 16384;

/// Appends a content block to `messages`, merging into the trailing message
/// when it already has the same `role`.
///
/// The Anthropic Messages API requires strictly alternating `user`/`assistant`
/// turns and rejects consecutive same-role messages. The Responses input,
/// however, models an assistant text reply and its tool call as two separate
/// items, and emits one `FunctionCallOutput` per tool result. Merging here keeps
/// the wire payload valid: assistant text + `tool_use` collapse into one
/// assistant message, and successive tool results collapse into one user
/// message (with `tool_result` blocks kept ahead of any later user text, as
/// Anthropic requires).
fn push_anthropic_block(messages: &mut Vec<Value>, role: &str, block: Value) {
    if let Some(last) = messages.last_mut()
        && last["role"] == role
        && let Some(arr) = last["content"].as_array_mut()
    {
        arr.push(block);
        return;
    }
    messages.push(json!({ "role": role, "content": [block] }));
}

fn responses_body_to_anthropic_body(body: Value) -> Result<Value, ApiError> {
    let request: ResponsesApiRequest = serde_json::from_value(body).map_err(|err| {
        ApiError::Stream(format!(
            "failed to decode responses request for anthropic: {err}"
        ))
    })?;

    let mut system = String::new();
    let mut messages: Vec<Value> = Vec::new();

    // The top-level `instructions` map to Anthropic's `system` field.
    if !request.instructions.trim().is_empty() {
        system.push_str(&request.instructions);
    }

    for item in &request.input {
        match item {
            ResponseItem::Message { role, content, .. } => {
                let text = content
                    .iter()
                    .filter_map(|c| match c {
                        ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                if role == "system" || role == "developer" || role == "latest_reminder" {
                    // Developer/system/reminder roles fold into the system prompt;
                    // Anthropic only allows `user`/`assistant` in `messages`.
                    if !text.trim().is_empty() {
                        if !system.is_empty() {
                            system.push_str("\n\n");
                        }
                        system.push_str(&text);
                    }
                } else if (role == "user" || role == "assistant") && !text.trim().is_empty() {
                    // Skip empty text: Anthropic rejects empty content blocks.
                    push_anthropic_block(
                        &mut messages,
                        role,
                        json!({ "type": "text", "text": text }),
                    );
                }
            }
            ResponseItem::FunctionCall {
                call_id,
                name,
                arguments,
                ..
            } => {
                // Arguments arrive as a JSON string; Anthropic expects a parsed
                // object for `input`. Fall back to an empty object on parse error.
                let input = serde_json::from_str::<Value>(arguments).unwrap_or_else(|_| json!({}));
                push_anthropic_block(
                    &mut messages,
                    "assistant",
                    json!({
                        "type": "tool_use",
                        "id": call_id,
                        "name": name,
                        "input": input,
                    }),
                );
            }
            ResponseItem::FunctionCallOutput { call_id, output } => {
                push_anthropic_block(
                    &mut messages,
                    "user",
                    json!({
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": output.body.to_text().unwrap_or_default(),
                    }),
                );
            }
            _ => {}
        }
    }

    // Anthropic requires the first message to have `role: "user"`. Drop any
    // leading assistant-only messages that would otherwise trigger a 400.
    while messages
        .first()
        .map(|m| m["role"] == "assistant")
        .unwrap_or(false)
    {
        messages.remove(0);
    }

    let tools: Vec<Value> = request
        .tools
        .iter()
        .filter_map(|tool| {
            let obj = tool.as_object()?;
            if obj.get("type").and_then(Value::as_str) != Some("function") {
                return None;
            }
            let name = obj.get("name")?.as_str()?;
            let description = obj.get("description").and_then(Value::as_str).unwrap_or("");
            let parameters = obj
                .get("parameters")
                .cloned()
                .unwrap_or_else(|| json!({ "type": "object", "properties": {} }));
            Some(json!({
                "name": name,
                "description": description,
                "input_schema": parameters,
            }))
        })
        .collect();

    let mut anthropic_body = json!({
        "model": request.model,
        "max_tokens": ANTHROPIC_DEFAULT_MAX_TOKENS,
        "messages": messages,
        "stream": true,
    });

    if !system.is_empty() {
        anthropic_body["system"] = Value::String(system);
    }
    if !tools.is_empty() {
        anthropic_body["tools"] = Value::Array(tools);
        // Map the Responses `tool_choice` onto Anthropic's object form. `auto`
        // is the default when omitted, so only `required` needs translation.
        if request.tool_choice == "required" {
            anthropic_body["tool_choice"] = json!({ "type": "any" });
        }
    }

    Ok(anthropic_body)
}

fn response_items_to_chat_messages(items: &[ResponseItem]) -> Vec<Value> {
    let mut messages = Vec::new();
    let mut pending_tool_calls = Vec::new();
    let mut pending_reasoning_content = None;

    for item in items {
        if let ResponseItem::FunctionCall {
            call_id,
            name,
            arguments,
            ..
        } = item
        {
            if pending_reasoning_content.is_none() {
                pending_reasoning_content = reasoning_content_for_tool_call(call_id);
            }
            pending_tool_calls.push(chat_tool_call(call_id, name, arguments));
            continue;
        }

        flush_pending_tool_calls(
            &mut messages,
            &mut pending_tool_calls,
            &mut pending_reasoning_content,
        );
        if let Some(message) = response_item_to_chat_message(item) {
            messages.push(message);
        }
    }

    flush_pending_tool_calls(
        &mut messages,
        &mut pending_tool_calls,
        &mut pending_reasoning_content,
    );
    messages
}

fn flush_pending_tool_calls(
    messages: &mut Vec<Value>,
    pending_tool_calls: &mut Vec<Value>,
    pending_reasoning_content: &mut Option<String>,
) {
    if pending_tool_calls.is_empty() {
        return;
    }

    let mut message = json!({
        "role": "assistant",
        "content": null,
        "tool_calls": std::mem::take(pending_tool_calls),
    });
    if let Some(reasoning_content) = pending_reasoning_content.take() {
        message["reasoning_content"] = Value::String(reasoning_content);
    }
    messages.push(message);
}

fn chat_tool_call(call_id: &str, name: &str, arguments: &str) -> Value {
    json!({
        "id": call_id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments,
        }
    })
}

fn response_item_to_chat_message(item: &ResponseItem) -> Option<Value> {
    match item {
        ResponseItem::Message { role, content, .. } => Some(json!({
            "role": chat_completions_role(role),
            "content": content_items_to_chat_content(content),
        })),
        ResponseItem::FunctionCall {
            call_id,
            name,
            arguments,
            ..
        } => Some(json!({
            "role": "assistant",
            "content": null,
            "tool_calls": [chat_tool_call(call_id, name, arguments)]
        })),
        ResponseItem::FunctionCallOutput { call_id, output } => Some(json!({
            "role": "tool",
            "tool_call_id": call_id,
            "content": output.body.to_text().unwrap_or_default(),
        })),
        _ => None,
    }
}

fn chat_completions_role(role: &str) -> &str {
    match role {
        "system" | "user" | "assistant" | "tool" => role,
        "developer" | "latest_reminder" => "system",
        _ => "user",
    }
}

fn content_items_to_chat_content(content: &[ContentItem]) -> Value {
    let mut text_parts = Vec::new();
    let mut structured = Vec::new();
    for item in content {
        match item {
            ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                text_parts.push(text.clone());
                structured.push(json!({ "type": "text", "text": text }));
            }
            ContentItem::InputImage { image_url, .. } => {
                structured.push(json!({
                    "type": "image_url",
                    "image_url": { "url": image_url },
                }));
            }
        }
    }
    if structured
        .iter()
        .any(|item| item.get("type").and_then(Value::as_str) == Some("image_url"))
    {
        Value::Array(structured)
    } else {
        Value::String(text_parts.join("\n"))
    }
}

fn response_tools_to_chat_tools(tools: &[Value]) -> Vec<Value> {
    tools
        .iter()
        .filter_map(|tool| {
            let obj = tool.as_object()?;
            if obj.get("type").and_then(Value::as_str) != Some("function") {
                return None;
            }
            let name = obj.get("name")?.clone();
            let description = obj
                .get("description")
                .cloned()
                .unwrap_or(Value::String(String::new()));
            let parameters = obj
                .get("parameters")
                .cloned()
                .unwrap_or_else(|| json!({ "type": "object", "properties": {} }));
            Some(json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters,
                }
            }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn azure_responses_role_normalizer_maps_unsupported_roles() {
        let mut body = json!({
            "input": [
                {"type": "message", "role": "developer", "content": []},
                {"type": "message", "role": "latest_reminder", "content": []},
                {"type": "message", "role": "assistant", "content": []},
                {"type": "message", "role": "surprise", "content": []},
                {"type": "function_call", "call_id": "call_123"}
            ]
        });

        normalize_responses_body_roles(&mut body);

        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["input"][1]["role"], "latest_reminder");
        assert_eq!(body["input"][2]["role"], "assistant");
        assert_eq!(body["input"][3]["role"], "user");
        assert!(body["input"][4].get("role").is_none());
    }

    #[test]
    fn chat_completions_function_call_includes_cached_reasoning_content() {
        crate::sse::chat_completions::remember_reasoning_content_for_tool_call(
            "call_reasoning",
            "Need to inspect the environment first.",
        );
        let item = ResponseItem::FunctionCall {
            id: None,
            name: "exec_command".to_string(),
            namespace: None,
            arguments: "{\"cmd\":\"pwd\"}".to_string(),
            call_id: "call_reasoning".to_string(),
        };

        let messages = response_items_to_chat_messages(&[item]);

        assert_eq!(
            messages[0]["reasoning_content"],
            "Need to inspect the environment first."
        );
        assert_eq!(messages[0]["tool_calls"][0]["id"], "call_reasoning");
    }

    #[test]
    fn chat_completions_groups_consecutive_parallel_tool_calls() {
        crate::sse::chat_completions::remember_reasoning_content_for_tool_call(
            "call_first",
            "I need two lookups.",
        );
        let items = vec![
            ResponseItem::FunctionCall {
                id: None,
                name: "exec_command".to_string(),
                namespace: None,
                arguments: "{\"cmd\":\"pwd\"}".to_string(),
                call_id: "call_first".to_string(),
            },
            ResponseItem::FunctionCall {
                id: None,
                name: "exec_command".to_string(),
                namespace: None,
                arguments: "{\"cmd\":\"ls\"}".to_string(),
                call_id: "call_second".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_first".to_string(),
                output: codex_protocol::models::FunctionCallOutputPayload {
                    body: codex_protocol::models::FunctionCallOutputBody::Text("/tmp".to_string()),
                    success: Some(true),
                },
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_second".to_string(),
                output: codex_protocol::models::FunctionCallOutputPayload {
                    body: codex_protocol::models::FunctionCallOutputBody::Text("file".to_string()),
                    success: Some(true),
                },
            },
        ];

        let messages = response_items_to_chat_messages(&items);

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[0]["reasoning_content"], "I need two lookups.");
        assert_eq!(messages[0]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(messages[0]["tool_calls"][0]["id"], "call_first");
        assert_eq!(messages[0]["tool_calls"][1]["id"], "call_second");
        assert_eq!(messages[1]["role"], "tool");
        assert_eq!(messages[1]["tool_call_id"], "call_first");
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call_second");
    }

    #[test]
    fn chat_completions_role_maps_responses_only_roles() {
        assert_eq!(chat_completions_role("developer"), "system");
        assert_eq!(chat_completions_role("latest_reminder"), "system");
        assert_eq!(chat_completions_role("user"), "user");
        assert_eq!(chat_completions_role("assistant"), "assistant");
        assert_eq!(chat_completions_role("unexpected"), "user");
    }
}
