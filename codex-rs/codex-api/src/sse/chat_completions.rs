use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::error::ApiError;
use codex_client::ByteStream;
use codex_client::StreamResponse;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TokenUsage;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

static REASONING_CONTENT_BY_TOOL_CALL_ID: OnceLock<Mutex<HashMap<String, String>>> =
    OnceLock::new();

const REQUEST_ID_HEADER: &str = "x-request-id";

pub(crate) fn remember_reasoning_content_for_tool_call(call_id: &str, reasoning_content: &str) {
    if reasoning_content.trim().is_empty() {
        return;
    }
    let store = REASONING_CONTENT_BY_TOOL_CALL_ID.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut store) = store.lock() {
        store.insert(call_id.to_string(), reasoning_content.to_string());
    }
}

pub(crate) fn reasoning_content_for_tool_call(call_id: &str) -> Option<String> {
    let store = REASONING_CONTENT_BY_TOOL_CALL_ID.get()?;
    store.lock().ok()?.get(call_id).cloned()
}

pub fn spawn_chat_completions_stream(
    stream_response: StreamResponse,
    idle_timeout: Duration,
) -> ResponseStream {
    let upstream_request_id = stream_response
        .headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let (tx_event, rx_event) = mpsc::channel::<Result<ResponseEvent, ApiError>>(1600);
    tokio::spawn(async move {
        let _ = tx_event.send(Ok(ResponseEvent::Created)).await;
        process_chat_sse(stream_response.bytes, tx_event, idle_timeout).await;
    });

    ResponseStream {
        rx_event,
        upstream_request_id,
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChunk {
    id: Option<String>,
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    delta: ChatDelta,
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ChatDelta {
    content: Option<String>,
    reasoning_content: Option<String>,
    tool_calls: Option<Vec<ChatToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCallDelta {
    index: usize,
    id: Option<String>,
    function: Option<ChatFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct ChatFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatUsage {
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
}

#[derive(Default)]
struct ToolCallAccumulator {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

async fn process_chat_sse(
    stream: ByteStream,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
) {
    let mut events = stream.eventsource();
    let mut message_started = false;
    let mut message_text = String::new();
    let mut reasoning_content = String::new();
    let mut tool_calls = BTreeMap::<usize, ToolCallAccumulator>::new();
    let mut response_id = String::new();
    let mut usage = None;

    loop {
        let next = timeout(idle_timeout, events.next()).await;
        let event = match next {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(err))) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "chat completions SSE error: {err}"
                    ))))
                    .await;
                return;
            }
            Ok(None) => break,
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(
                        "chat completions stream idle timeout".into(),
                    )))
                    .await;
                return;
            }
        };
        let data = event.data.trim();
        if data == "[DONE]" {
            break;
        }
        let chunk = match serde_json::from_str::<ChatCompletionChunk>(data) {
            Ok(chunk) => chunk,
            Err(err) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "failed to parse chat completions chunk: {err}: {data}"
                    ))))
                    .await;
                return;
            }
        };
        if let Some(id) = chunk.id {
            response_id = id;
        }
        if let Some(chunk_usage) = chunk.usage {
            usage = Some(TokenUsage {
                input_tokens: chunk_usage.prompt_tokens.unwrap_or(0),
                cached_input_tokens: 0,
                output_tokens: chunk_usage.completion_tokens.unwrap_or(0),
                reasoning_output_tokens: 0,
                total_tokens: chunk_usage.total_tokens.unwrap_or(0),
            });
        }
        for choice in chunk.choices {
            if let Some(reasoning_delta) = choice.delta.reasoning_content {
                reasoning_content.push_str(&reasoning_delta);
            }
            if let Some(content) = choice.delta.content
                && !content.is_empty()
            {
                if !message_started {
                    message_started = true;
                    let item = ResponseItem::Message {
                        id: None,
                        role: "assistant".to_string(),
                        content: Vec::new(),
                        phase: None,
                    };
                    let _ = tx_event
                        .send(Ok(ResponseEvent::OutputItemAdded(item)))
                        .await;
                }
                message_text.push_str(&content);
                let _ = tx_event
                    .send(Ok(ResponseEvent::OutputTextDelta(content)))
                    .await;
            }
            if let Some(deltas) = choice.delta.tool_calls {
                for delta in deltas {
                    let entry = tool_calls.entry(delta.index).or_default();
                    if let Some(id) = delta.id {
                        entry.id = Some(id);
                    }
                    if let Some(function) = delta.function {
                        if let Some(name) = function.name {
                            entry.name = Some(name);
                        }
                        if let Some(arguments) = function.arguments {
                            entry.arguments.push_str(&arguments);
                        }
                    }
                }
            }
            let _ = choice.finish_reason;
        }
    }

    if message_started {
        let item = ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText { text: message_text }],
            phase: None,
        };
        let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
    }

    for (_index, call) in tool_calls {
        if let (Some(call_id), Some(name)) = (call.id, call.name) {
            remember_reasoning_content_for_tool_call(&call_id, &reasoning_content);
            let item = ResponseItem::FunctionCall {
                id: None,
                name,
                namespace: None,
                arguments: call.arguments,
                call_id,
            };
            let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
        }
    }

    if response_id.is_empty() {
        response_id = "chatcmpl-codex".to_string();
    }
    let _ = tx_event
        .send(Ok(ResponseEvent::Completed {
            response_id,
            token_usage: usage,
            end_turn: Some(true),
        }))
        .await;
}
