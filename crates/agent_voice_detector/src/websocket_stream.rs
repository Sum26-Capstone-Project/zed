//! Shared websocket plumbing for streaming microphone audio to a local voice
//! server and receiving text responses. Both [`crate::websocket_transcriber`]
//! and [`crate::start_end_word_detector`] drive a connection through this
//! module; they differ only in the URL they connect to and how they interpret
//! the text messages they receive.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Context as _;
use async_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
};

use crate::audio_capture::AudioChunk;

const WEBSOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WEBSOCKET_CONNECT_ATTEMPTS: u32 = 30;
const WEBSOCKET_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(500);

fn websocket_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create websocket tokio runtime")
    })
}

pub(crate) enum WebSocketCommand {
    SendChunk(AudioChunk),
    Shutdown,
}

/// Runs a blocking websocket session on the current thread: connects to
/// `websocket_url` (retrying while the server starts up), signals readiness via
/// `connected_tx`, forwards [`WebSocketCommand`]s to the server, and relays
/// received text frames back through `message_tx`.
pub(crate) fn run_websocket_thread(
    websocket_url: String,
    mut command_rx: mpsc::UnboundedReceiver<WebSocketCommand>,
    message_tx: mpsc::UnboundedSender<Result<String, anyhow::Error>>,
    connected_tx: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    websocket_runtime().block_on(async move {
        let mut last_error = None;
        let mut websocket = None;
        for attempt in 0..WEBSOCKET_CONNECT_ATTEMPTS {
            let request = websocket_url
                .clone()
                .into_client_request()
                .context("invalid websocket URL")?;
            let connect = async_tungstenite::tokio::connect_async(request);
            match tokio::time::timeout(WEBSOCKET_CONNECT_TIMEOUT, connect).await {
                Ok(Ok((stream, _response))) => {
                    websocket = Some(stream);
                    break;
                }
                Ok(Err(error)) => {
                    last_error = Some(anyhow::anyhow!(error));
                }
                Err(_) => {
                    last_error = Some(anyhow::anyhow!("timed out connecting to voice server"));
                }
            }

            if attempt + 1 < WEBSOCKET_CONNECT_ATTEMPTS {
                tokio::time::sleep(WEBSOCKET_CONNECT_RETRY_DELAY).await;
            }
        }

        let mut websocket = websocket.with_context(|| {
            last_error
                .map(|error| format!("failed to connect to voice server: {error:#}"))
                .unwrap_or_else(|| "failed to connect to voice server".to_string())
        })?;

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
                        Some(WebSocketCommand::Shutdown) | None => {
                            let _ = websocket
                                .send(Message::Text(r#"{"command":"stop"}"#.into()))
                                .await;
                            break;
                        }
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
                                .context("voice server sent invalid UTF-8")?;
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
    websocket: &mut async_tungstenite::WebSocketStream<async_tungstenite::tokio::ConnectStream>,
    chunk: &AudioChunk,
) -> anyhow::Result<()> {
    let mut bytes = Vec::with_capacity(chunk.byte_len());
    for sample in &chunk.samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    websocket
        .send(Message::Binary(bytes.into()))
        .await
        .context("failed to send audio chunk")
}
