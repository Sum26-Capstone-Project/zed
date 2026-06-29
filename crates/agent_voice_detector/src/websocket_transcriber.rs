use std::sync::{Arc, OnceLock};
use std::thread;

use anyhow::Context as _;
use async_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
    select,
};
use gpui::{BackgroundExecutor, SharedString, Task};
use parking_lot::Mutex;

use crate::audio_capture::{AudioCapture, AudioCaptureConfig, AudioChunk};
use crate::transcriber::{
    Transcriber, TranscriberConfig, TranscriberError, TranscriberEvent, TranscriberState,
    TranscriptUpdate,
};

pub const DEFAULT_WEBSOCKET_URL: &str = "ws://127.0.0.1:8765/ws/stream";

const WEBSOCKET_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn websocket_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create websocket tokio runtime")
    })
}

#[derive(Clone)]
struct WebSocketTranscriberInner {
    state: Arc<Mutex<TranscriberState>>,
    stop_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    capture: Arc<Mutex<AudioCapture>>,
}

pub struct WebSocketTranscriber {
    inner: WebSocketTranscriberInner,
}

impl Default for WebSocketTranscriber {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketTranscriber {
    pub fn new() -> Self {
        Self {
            inner: WebSocketTranscriberInner {
                state: Arc::new(Mutex::new(TranscriberState::Idle)),
                stop_tx: Arc::new(Mutex::new(None)),
                capture: Arc::new(Mutex::new(AudioCapture::new())),
            },
        }
    }
}

impl Transcriber for WebSocketTranscriber {
    fn state(&self) -> TranscriberState {
        *self.inner.state.lock()
    }

    fn start(
        &mut self,
        config: TranscriberConfig,
        events: mpsc::UnboundedSender<TranscriberEvent>,
        executor: BackgroundExecutor,
    ) -> Task<Result<(), TranscriberError>> {
        if *self.inner.state.lock() == TranscriberState::Listening {
            return Task::ready(Err(TranscriberError::AlreadyListening));
        }

        let (stop_tx, stop_rx) = oneshot::channel();
        *self.inner.stop_tx.lock() = Some(stop_tx);
        *self.inner.state.lock() = TranscriberState::Listening;

        let inner = self.inner.clone();
        let state = inner.state.clone();
        let session_executor = executor.clone();
        executor.spawn(async move {
            let result =
                run_session(config, events, stop_rx, inner, session_executor).await;
            *state.lock() = TranscriberState::Idle;
            result
        })
    }

    fn stop(&mut self) -> Task<Result<(), TranscriberError>> {
        if *self.inner.state.lock() != TranscriberState::Listening {
            return Task::ready(Err(TranscriberError::NotListening));
        }

        if let Some(stop_tx) = self.inner.stop_tx.lock().take() {
            let _ = stop_tx.send(());
        }

        Task::ready(Ok(()))
    }
}

impl From<crate::audio_capture::AudioCaptureError> for TranscriberError {
    fn from(error: crate::audio_capture::AudioCaptureError) -> Self {
        match error {
            crate::audio_capture::AudioCaptureError::Other(error) => Self::Other(error),
            other => Self::Other(anyhow::anyhow!(other)),
        }
    }
}

enum WebSocketCommand {
    SendChunk(AudioChunk),
    Shutdown,
}

async fn run_session(
    config: TranscriberConfig,
    events: mpsc::UnboundedSender<TranscriberEvent>,
    stop_rx: oneshot::Receiver<()>,
    inner: WebSocketTranscriberInner,
    executor: BackgroundExecutor,
) -> Result<(), TranscriberError> {
    let _ = events.unbounded_send(TranscriberEvent::Started);

    let session_result =
        run_session_inner(config, &events, stop_rx, inner, executor).await;

    if events.unbounded_send(TranscriberEvent::Stopped).is_err() {
        log::debug!("transcriber event receiver dropped before Stopped");
    }

    session_result
}

async fn run_session_inner(
    config: TranscriberConfig,
    events: &mpsc::UnboundedSender<TranscriberEvent>,
    mut stop_rx: oneshot::Receiver<()>,
    inner: WebSocketTranscriberInner,
    executor: BackgroundExecutor,
) -> Result<(), TranscriberError> {
    let (command_tx, command_rx) = mpsc::unbounded::<WebSocketCommand>();
    let (message_tx, mut message_rx) = mpsc::unbounded::<Result<String, anyhow::Error>>();
    let (connected_tx, connected_rx) = oneshot::channel();

    let websocket_url = config.websocket_url.to_string();
    let websocket_thread = thread::spawn(move || {
        if let Err(error) =
            run_websocket_thread(websocket_url, command_rx, message_tx, connected_tx)
        {
            log::error!("speech-to-text websocket thread failed: {error:#}");
        }
    });

    connected_rx
        .await
        .map_err(|_| TranscriberError::Other(anyhow::anyhow!(
            "speech-to-text websocket thread exited before connecting"
        )))?;

    let (chunk_tx, mut chunk_rx) = mpsc::unbounded();
    let capture_config = AudioCaptureConfig {
        input_device: config.input_device,
        ..AudioCaptureConfig::default()
    };

    let capture_task = inner
        .capture
        .lock()
        .start(capture_config, chunk_tx, executor.clone());

    loop {
        select! {
            stop = stop_rx => {
                let _ = stop;
                break;
            }
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
                        if let Err(error) = handle_text_message(events, &text) {
                            emit_error(events, error.into());
                        }
                    }
                    Some(Err(error)) => {
                        emit_error(events, error);
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let _ = command_tx.unbounded_send(WebSocketCommand::Shutdown);
    let _ = websocket_thread.join();

    let stop_task = inner.capture.lock().stop();
    stop_task.await.map_err(TranscriberError::from)?;
    capture_task.await.map_err(TranscriberError::from)?;

    Ok(())
}

fn run_websocket_thread(
    websocket_url: String,
    mut command_rx: mpsc::UnboundedReceiver<WebSocketCommand>,
    message_tx: mpsc::UnboundedSender<Result<String, anyhow::Error>>,
    connected_tx: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    websocket_runtime().block_on(async move {
        let request = websocket_url
            .into_client_request()
            .context("invalid websocket URL")?;

        let connect = async_tungstenite::tokio::connect_async(request);
        let (mut websocket, _response) = tokio::time::timeout(WEBSOCKET_CONNECT_TIMEOUT, connect)
            .await
            .context("timed out connecting to speech-to-text server")?
            .context("failed to connect to speech-to-text server")?;

        if connected_tx.send(()).is_err() {
            return Ok(());
        }

        loop {
            tokio::select! {
                command = command_rx.next() => {
                    match command {
                        Some(WebSocketCommand::SendChunk(chunk)) => {
                            send_audio_chunk(&mut websocket, &chunk).await?;
                        }
                        Some(WebSocketCommand::Shutdown) | None => break,
                    }
                }
                message = websocket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if message_tx
                                .unbounded_send(Ok(text.to_string()))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            let text = String::from_utf8(bytes.to_vec())
                                .context("speech-to-text server sent invalid UTF-8")?;
                            if message_tx.unbounded_send(Ok(text)).is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) => break,
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            if message_tx
                                .unbounded_send(Err(anyhow::anyhow!(error)))
                                .is_err()
                            {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        let _ = websocket.close(None).await;
        Ok(())
    })
}

async fn send_audio_chunk(
    websocket: &mut async_tungstenite::WebSocketStream<
        async_tungstenite::tokio::ConnectStream,
    >,
    chunk: &AudioChunk,
) -> anyhow::Result<()> {
    use futures::SinkExt as _;

    let mut bytes = Vec::with_capacity(chunk.byte_len());
    for sample in &chunk.samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    websocket
        .send(Message::Binary(bytes.into()))
        .await
        .context("failed to send audio chunk")
}

fn emit_error(events: &mpsc::UnboundedSender<TranscriberEvent>, error: anyhow::Error) {
    log::error!("speech-to-text session failed: {error:#}");
    if events
        .unbounded_send(TranscriberEvent::Error {
            message: error.to_string().into(),
        })
        .is_err()
    {
        log::debug!("transcriber event receiver dropped");
    }
}

fn handle_text_message(
    events: &mpsc::UnboundedSender<TranscriberEvent>,
    text: &str,
) -> Result<(), TranscriberError> {
    match parse_transcript_message(text) {
        Ok(TranscriptServerMessage::Transcript(update)) => {
            if events
                .unbounded_send(TranscriberEvent::Transcript(update))
                .is_err()
            {
                log::debug!("transcriber event receiver dropped");
            }
        }
        Ok(TranscriptServerMessage::Error { message }) => {
            if events
                .unbounded_send(TranscriberEvent::Error { message })
                .is_err()
            {
                log::debug!("transcriber event receiver dropped");
            }
        }
        Err(error) => {
            log::warn!("ignoring invalid speech-to-text message: {error:#}");
        }
    }

    Ok(())
}

enum TranscriptServerMessage {
    Transcript(TranscriptUpdate),
    Error { message: SharedString },
}

fn parse_transcript_message(text: &str) -> anyhow::Result<TranscriptServerMessage> {
    let value: serde_json::Value =
        serde_json::from_str(text).context("speech-to-text message was not valid JSON")?;

    if let Some(error) = value
        .get("error")
        .and_then(|error| error.as_str())
        .or_else(|| {
            value
                .get("type")
                .filter(|message_type| message_type.as_str() == Some("error"))
                .and_then(|_| value.get("message"))
                .and_then(|message| message.as_str())
        })
    {
        return Ok(TranscriptServerMessage::Error {
            message: error.into(),
        });
    }

    if value
        .get("type")
        .and_then(|message_type| message_type.as_str())
        == Some("status")
    {
        return Err(anyhow::anyhow!("control message"));
    }

    let transcript = value
        .get("text")
        .and_then(|text| text.as_str())
        .unwrap_or_default();

    if transcript.is_empty() {
        return Err(anyhow::anyhow!("empty transcript"));
    }

    let is_final = value
        .get("is_final")
        .and_then(|is_final| is_final.as_bool())
        .unwrap_or_else(|| {
            value
                .get("type")
                .and_then(|message_type| message_type.as_str())
                == Some("final")
        });

    Ok(TranscriptServerMessage::Transcript(TranscriptUpdate {
        text: transcript.into(),
        is_final,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_realtime_stt_partial_message() {
        let message =
            parse_transcript_message(r#"{"type":"partial","text":"hello","segment_id":0}"#).unwrap();
        match message {
            TranscriptServerMessage::Transcript(update) => {
                assert_eq!(update.text.as_ref(), "hello");
                assert!(!update.is_final);
            }
            TranscriptServerMessage::Error { .. } => panic!("expected transcript"),
        }
    }

    #[test]
    fn parses_realtime_stt_final_message() {
        let message = parse_transcript_message(
            r#"{"type":"final","text":"hello world","segment_id":0,"is_final":true}"#,
        )
        .unwrap();
        match message {
            TranscriptServerMessage::Transcript(update) => {
                assert_eq!(update.text.as_ref(), "hello world");
                assert!(update.is_final);
            }
            TranscriptServerMessage::Error { .. } => panic!("expected transcript"),
        }
    }

    #[test]
    fn ignores_status_messages() {
        assert!(parse_transcript_message(r#"{"type":"status","text":"ready"}"#).is_err());
    }

    #[test]
    fn parses_partial_transcript_message() {
        let message = parse_transcript_message(r#"{"text":"hello","is_final":false}"#).unwrap();
        match message {
            TranscriptServerMessage::Transcript(update) => {
                assert_eq!(update.text.as_ref(), "hello");
                assert!(!update.is_final);
            }
            TranscriptServerMessage::Error { .. } => panic!("expected transcript"),
        }
    }

    #[test]
    fn parses_error_message() {
        let message = parse_transcript_message(r#"{"error":"model unavailable"}"#).unwrap();
        match message {
            TranscriptServerMessage::Error { message } => {
                assert_eq!(message.as_ref(), "model unavailable");
            }
            TranscriptServerMessage::Transcript(_) => panic!("expected error"),
        }
    }
}
