//! Anthropic Messages API SSE stream parser.
//!
//! Converts Anthropic streaming events (message_start, content_block_delta, etc.)
//! into the internal `ResponseEvent` protocol used by Codex.

use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::error::ApiError;
use codex_client::ByteStream;
use codex_client::StreamResponse;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TokenUsage;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

const REQUEST_ID_HEADER: &str = "x-request-id";

pub fn spawn_anthropic_stream(
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
        process_anthropic_sse(stream_response.bytes, tx_event, idle_timeout).await;
    });

    ResponseStream {
        rx_event,
        upstream_request_id,
    }
}

#[derive(Debug, Deserialize)]
struct AnthropicEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    message: Option<AnthropicMessage>,
    #[serde(default)]
    index: Option<usize>,
    #[serde(default)]
    delta: Option<AnthropicDelta>,
    #[serde(default)]
    content_block: Option<AnthropicContentBlock>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
// `content` mirrors the Anthropic schema but is unused: streamed blocks arrive
// via `content_block_start`/`content_block_delta`, not the `message_start`
// snapshot.
#[allow(dead_code)]
struct AnthropicMessage {
    id: Option<String>,
    #[serde(default)]
    content: Vec<AnthropicContentBlock>,
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
// `text`/`input`/`thinking` mirror the Anthropic schema; on
// `content_block_start` only `type`/`id`/`name` are populated, the rest stream
// in as deltas, so these initial fields are intentionally not read here.
#[allow(dead_code)]
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    thinking: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnthropicDelta {
    // Optional: `content_block_delta` carries a typed delta (`text_delta`,
    // `thinking_delta`, `input_json_delta`), but `message_delta` reuses the same
    // `delta` field for `{ "stop_reason": ... }` with no `type`. Making this
    // optional avoids a hard parse failure on `message_delta` events.
    #[serde(default, rename = "type")]
    delta_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    partial_json: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: Option<i64>,
    #[serde(default)]
    output_tokens: Option<i64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<i64>,
    #[serde(default)]
    cache_read_input_tokens: Option<i64>,
}

struct ToolCallState {
    id: String,
    name: String,
    json_accumulator: String,
}

/// Merges streamed `usage` updates into `slot`, keeping the largest value seen
/// for each field.
///
/// Anthropic splits usage across events: `message_start` reports
/// `input_tokens`/`cache_*` while `message_delta` reports the (cumulative)
/// `output_tokens`. MiMo, by contrast, sends the full usage in `message_delta`.
/// Taking the field-wise maximum yields the correct final totals in both cases
/// without one event clobbering values from another.
fn merge_usage(slot: &mut Option<AnthropicUsage>, incoming: AnthropicUsage) {
    let target = slot.get_or_insert_with(AnthropicUsage::default);
    fn keep_max(field: &mut Option<i64>, incoming: Option<i64>) {
        if let Some(value) = incoming
            && value > field.unwrap_or(0)
        {
            *field = Some(value);
        }
    }
    keep_max(&mut target.input_tokens, incoming.input_tokens);
    keep_max(&mut target.output_tokens, incoming.output_tokens);
    keep_max(
        &mut target.cache_creation_input_tokens,
        incoming.cache_creation_input_tokens,
    );
    keep_max(
        &mut target.cache_read_input_tokens,
        incoming.cache_read_input_tokens,
    );
}

/// Tracks the kind of Anthropic content block currently being streamed at a
/// given index, along with any accumulated text. Anthropic streams blocks
/// sequentially (`content_block_start` -> deltas -> `content_block_stop`), and
/// each kind maps to a distinct internal `ResponseItem`.
enum BlockState {
    /// Assistant visible text block (`type: "text"`).
    Text(String),
    /// Extended-thinking block (`type: "thinking"` / `"redacted_thinking"`).
    Thinking(String),
    /// Tool-use block (`type: "tool_use"`); arguments accumulate as JSON.
    ToolUse(ToolCallState),
}

/// Emits the `OutputItemDone` event corresponding to a finished content block.
///
/// The downstream consumer in `codex-core` requires every streamed delta to be
/// bracketed by an `OutputItemAdded`/`OutputItemDone` pair so that an "active
/// item" exists; this closes the item opened in `content_block_start`.
async fn finish_block(tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>, state: BlockState) {
    let item = match state {
        BlockState::Text(text) => ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText { text }],
            phase: None,
        },
        BlockState::Thinking(text) => ResponseItem::Reasoning {
            id: String::new(),
            summary: Vec::new(),
            content: Some(vec![ReasoningItemContent::ReasoningText { text }]),
            encrypted_content: None,
        },
        BlockState::ToolUse(call) => ResponseItem::FunctionCall {
            id: None,
            name: call.name,
            namespace: None,
            arguments: call.json_accumulator,
            call_id: call.id,
        },
    };
    let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
}

async fn process_anthropic_sse(
    stream: ByteStream,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
) {
    let mut events = stream.eventsource();
    // Active content blocks keyed by their Anthropic stream index. Kept as a
    // map (rather than a single value) so an unexpected interleaving still
    // closes the right block on `content_block_stop`.
    let mut blocks = BTreeMap::<usize, BlockState>::new();
    let mut response_id = String::new();
    let mut usage: Option<AnthropicUsage> = None;

    loop {
        let next = timeout(idle_timeout, events.next()).await;
        let event = match next {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(err))) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!("anthropic SSE error: {err}"))))
                    .await;
                return;
            }
            Ok(None) => break,
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(
                        "anthropic stream idle timeout".into(),
                    )))
                    .await;
                return;
            }
        };
        let data = event.data.trim();
        let anthropic_event: AnthropicEvent = match serde_json::from_str(data) {
            Ok(e) => e,
            Err(err) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "failed to parse anthropic event: {err}: {data}"
                    ))))
                    .await;
                return;
            }
        };

        match anthropic_event.event_type.as_str() {
            "message_start" => {
                if let Some(msg) = anthropic_event.message {
                    if let Some(id) = msg.id {
                        response_id = id;
                    }
                    if let Some(u) = msg.usage {
                        merge_usage(&mut usage, u);
                    }
                }
            }
            "content_block_start" => {
                // Open an "active item" for the new block so subsequent deltas
                // have a corresponding item downstream.
                if let (Some(idx), Some(block)) =
                    (anthropic_event.index, anthropic_event.content_block)
                {
                    match block.block_type.as_str() {
                        "thinking" | "redacted_thinking" => {
                            let item = ResponseItem::Reasoning {
                                id: String::new(),
                                summary: Vec::new(),
                                content: None,
                                encrypted_content: None,
                            };
                            let _ = tx_event
                                .send(Ok(ResponseEvent::OutputItemAdded(item)))
                                .await;
                            blocks.insert(idx, BlockState::Thinking(String::new()));
                        }
                        "text" => {
                            let item = ResponseItem::Message {
                                id: None,
                                role: "assistant".to_string(),
                                content: Vec::new(),
                                phase: None,
                            };
                            let _ = tx_event
                                .send(Ok(ResponseEvent::OutputItemAdded(item)))
                                .await;
                            blocks.insert(idx, BlockState::Text(String::new()));
                        }
                        "tool_use" => {
                            if let (Some(id), Some(name)) = (block.id, block.name) {
                                blocks.insert(
                                    idx,
                                    BlockState::ToolUse(ToolCallState {
                                        id,
                                        name,
                                        json_accumulator: String::new(),
                                    }),
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
            "content_block_delta" => {
                let index = anthropic_event.index.unwrap_or(0);
                if let Some(delta) = anthropic_event.delta {
                    match delta.delta_type.as_deref().unwrap_or_default() {
                        "thinking_delta" => {
                            if let Some(thinking) = delta.thinking {
                                if let Some(BlockState::Thinking(acc)) = blocks.get_mut(&index) {
                                    acc.push_str(&thinking);
                                }
                                let _ = tx_event
                                    .send(Ok(ResponseEvent::ReasoningContentDelta {
                                        delta: thinking,
                                        content_index: index as i64,
                                    }))
                                    .await;
                            }
                        }
                        "text_delta" => {
                            if let Some(text) = delta.text {
                                if let Some(BlockState::Text(acc)) = blocks.get_mut(&index) {
                                    acc.push_str(&text);
                                }
                                let _ = tx_event
                                    .send(Ok(ResponseEvent::OutputTextDelta(text)))
                                    .await;
                            }
                        }
                        "input_json_delta" => {
                            if let Some(json) = delta.partial_json
                                && let Some(BlockState::ToolUse(tc)) = blocks.get_mut(&index) {
                                    tc.json_accumulator.push_str(&json);
                                }
                        }
                        _ => {}
                    }
                }
            }
            "content_block_stop" => {
                let index = anthropic_event.index.unwrap_or(0);
                if let Some(state) = blocks.remove(&index) {
                    finish_block(&tx_event, state).await;
                }
            }
            "message_delta" => {
                if let Some(u) = anthropic_event.usage {
                    merge_usage(&mut usage, u);
                }
            }
            "message_stop" => {
                break;
            }
            "ping" => {}
            "error" => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "anthropic error event: {data}"
                    ))))
                    .await;
                return;
            }
            _ => {}
        }
    }

    // Defensively close any blocks that never received an explicit stop event.
    for (_index, state) in std::mem::take(&mut blocks) {
        finish_block(&tx_event, state).await;
    }

    if response_id.is_empty() {
        response_id = "msg-anthropic".to_string();
    }

    let token_usage = usage.map(|u| TokenUsage {
        input_tokens: u.input_tokens.unwrap_or(0),
        cached_input_tokens: u.cache_read_input_tokens.unwrap_or(0),
        output_tokens: u.output_tokens.unwrap_or(0),
        reasoning_output_tokens: 0,
        total_tokens: u.input_tokens.unwrap_or(0) + u.output_tokens.unwrap_or(0),
    });

    let _ = tx_event
        .send(Ok(ResponseEvent::Completed {
            response_id,
            token_usage,
            end_turn: Some(true),
        }))
        .await;
}
