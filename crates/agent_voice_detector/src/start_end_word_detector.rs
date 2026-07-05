use std::thread;
use std::time::Duration;

use anyhow::Context as _;
use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
    select,
};
use gpui::{BackgroundExecutor, Task};

use crate::audio_capture::{AudioCapture, AudioCaptureConfig};
use crate::websocket_stream::{WebSocketCommand, run_websocket_thread};

/// Phrase that signals the user wants to start interacting with the agent.
pub const WAKE_WORD: &str = "hello zed";
/// Phrase that signals the user wants to stop the current interaction.
pub const STOP_WORD: &str = "stop zed";

/// Phrases sent to the detection server, in the order they appear in the
/// server's `found` response array.
const PHRASES: [&str; 2] = [WAKE_WORD, STOP_WORD];

/// An event emitted by the detector when it recognizes one of its phrases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectorEvent {
    /// The [`WAKE_WORD`] was heard: the listener should start voice input.
    Start,
    /// The [`STOP_WORD`] was heard: the listener should stop voice input.
    Stop,
}

/// Port the start/end word detection server listens on. It runs independently
/// from the speech-to-text server, so it uses a distinct port.
const SERVER_PORT: u16 = 8764;

/// Delay before reconnecting after a detector session ends or fails, so a
/// persistently unavailable server does not spin the executor.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Starts a long-running background task that continuously listens to the
/// microphone and emits a [`DetectorEvent`] on `events` whenever it hears the
/// [`WAKE_WORD`] or [`STOP_WORD`].
///
/// The returned [`Task`] runs until dropped, so callers should keep it alive for
/// as long as they want to receive events (for example by storing it in a
/// field). The detector connects to the detection server over a websocket on
/// [`SERVER_PORT`], retrying while the server starts up and reconnecting
/// automatically if the connection drops or a session ends. The task ends on its
/// own once `events` is closed.
pub fn start_detector(
    executor: BackgroundExecutor,
    events: mpsc::UnboundedSender<DetectorEvent>,
) -> Task<()> {
    let loop_executor = executor.clone();
    executor.spawn(async move {
        while !events.is_closed() {
            if let Err(error) = run_session(&loop_executor, &events).await {
                log::error!("start/end word detector session failed: {error:#}");
            }
            loop_executor.timer(RECONNECT_DELAY).await;
        }
    })
}

async fn run_session(
    executor: &BackgroundExecutor,
    events: &mpsc::UnboundedSender<DetectorEvent>,
) -> anyhow::Result<()> {
    let (command_tx, command_rx) = mpsc::unbounded::<WebSocketCommand>();
    let (message_tx, mut message_rx) = mpsc::unbounded::<Result<String, anyhow::Error>>();
    let (connected_tx, connected_rx) = oneshot::channel();

    let websocket_url = detector_websocket_url();
    let websocket_thread = thread::spawn(move || {
        if let Err(error) =
            run_websocket_thread(websocket_url, command_rx, message_tx, connected_tx)
        {
            log::error!("start/end word detector websocket thread failed: {error:#}");
        }
    });

    connected_rx.await.map_err(|_| {
        anyhow::anyhow!("start/end word detector websocket thread exited before connecting")
    })?;

    let (chunk_tx, mut chunk_rx) = mpsc::unbounded();
    let mut capture = AudioCapture::new();
    let capture_task = capture.start(AudioCaptureConfig::default(), chunk_tx, executor.clone());

    log::info!("start/end word detector listening for {WAKE_WORD:?} and {STOP_WORD:?}");

    loop {
        select! {
            chunk = chunk_rx.next() => {
                let Some(chunk) = chunk else {
                    break;
                };
                if command_tx
                    .unbounded_send(WebSocketCommand::SendChunk(chunk))
                    .is_err()
                {
                    break;
                }
            }
            message = message_rx.next() => {
                match message {
                    Some(Ok(text)) => {
                        log_server_response(&text);
                        handle_message(&text, events);
                    }
                    Some(Err(error)) => {
                        log::error!("start/end word detector stream error: {error:#}");
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let _ = command_tx.unbounded_send(WebSocketCommand::Shutdown);
    let _ = websocket_thread.join();

    let stop_task = capture.stop();
    stop_task.await.context("failed to stop audio capture")?;
    capture_task.await.context("audio capture task failed")?;

    Ok(())
}

fn detector_websocket_url() -> String {
    let mut url = format!("ws://127.0.0.1:{SERVER_PORT}/ws/stream");
    for (index, phrase) in PHRASES.iter().enumerate() {
        url.push(if index == 0 { '?' } else { '&' });
        url.push_str("phrase=");
        url.push_str(&percent_encode(phrase));
    }
    url
}

/// Minimal percent-encoding for query-string values. Only the phrase constants
/// flow through here, so encoding spaces and a few reserved characters is
/// sufficient to keep the URL well-formed.
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

fn log_server_response(text: &str) {
    let value = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(value) => value,
        Err(_) => {
            log::warn!("start/end word detector server sent non-JSON: {text}");
            return;
        }
    };

    let message_type = value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");

    match message_type {
        "partial" => log::debug!("start/end word detector server partial: {text}"),
        "final" | "status" => log::debug!("start/end word detector server {message_type}: {text}"),
        "error" => log::warn!("start/end word detector server error: {text}"),
        _ => log::debug!("start/end word detector server message: {text}"),
    }
}

fn handle_message(text: &str, events: &mpsc::UnboundedSender<DetectorEvent>) {
    let Some(event) = detect_event(text) else {
        return;
    };

    log::info!("start/end word detector emitting {event:?}");
    if events.unbounded_send(event).is_err() {
        log::debug!("start/end word detector event receiver dropped");
    }
}

/// Maps a server phrase-match message to a [`DetectorEvent`]. Returns `None` when
/// neither phrase was heard, or when *both* were heard in the same message: in
/// that case the intended action is ambiguous, so we do nothing.
fn detect_event(text: &str) -> Option<DetectorEvent> {
    let found = parse_found(text)?;
    let wake_heard = found.first().copied().unwrap_or(false);
    let stop_heard = found.get(1).copied().unwrap_or(false);

    match (wake_heard, stop_heard) {
        (true, false) => Some(DetectorEvent::Start),
        (false, true) => Some(DetectorEvent::Stop),
        _ => None,
    }
}

/// Parses the server's phrase-match message, returning the `found` flags aligned
/// with [`PHRASES`]. Returns `None` for status/error/unrelated messages.
fn parse_found(text: &str) -> Option<Vec<bool>> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;

    match value.get("type").and_then(|value| value.as_str()) {
        Some("error") => return None,
        Some("partial") | Some("final") => {}
        _ => return None,
    }

    let found = value
        .get("found")?
        .as_array()?
        .iter()
        .map(|entry| entry.as_bool().unwrap_or(false))
        .collect();
    Some(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_url_with_encoded_phrases() {
        assert_eq!(
            detector_websocket_url(),
            format!("ws://127.0.0.1:{SERVER_PORT}/ws/stream?phrase=hello%20zed&phrase=stop%20zed")
        );
    }

    #[test]
    fn parses_found_flags() {
        let found = parse_found(r#"{"type":"final","found":[true,false],"segment_id":0}"#).unwrap();
        assert_eq!(found, vec![true, false]);
    }

    #[test]
    fn ignores_status_messages() {
        assert!(parse_found(r#"{"type":"status","text":"ready"}"#).is_none());
    }

    #[test]
    fn ignores_error_messages() {
        assert!(parse_found(r#"{"type":"error","message":"no model"}"#).is_none());
    }

    #[test]
    fn wake_word_only_starts() {
        assert_eq!(
            detect_event(r#"{"type":"final","found":[true,false]}"#),
            Some(DetectorEvent::Start)
        );
    }

    #[test]
    fn stop_word_only_stops() {
        assert_eq!(
            detect_event(r#"{"type":"final","found":[false,true]}"#),
            Some(DetectorEvent::Stop)
        );
    }

    #[test]
    fn both_words_do_nothing() {
        assert_eq!(detect_event(r#"{"type":"final","found":[true,true]}"#), None);
    }

    #[test]
    fn neither_word_does_nothing() {
        assert_eq!(detect_event(r#"{"type":"final","found":[false,false]}"#), None);
    }
}
