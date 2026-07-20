use cpal::DeviceId;
use futures::channel::mpsc;
use gpui::{BackgroundExecutor, SharedString, Task};
use http_client::HttpClient;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriberState {
    Idle,
    Listening,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptUpdate {
    pub text: SharedString,
    pub is_final: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriberEvent {
    Started,
    Stopped,
    Transcript(TranscriptUpdate),
    Error { message: SharedString },
}

#[derive(Clone)]
pub struct TranscriberConfig {
    pub websocket_url: SharedString,
    pub input_device: Option<DeviceId>,
    pub http_client: Arc<dyn HttpClient>,
    pub language: SharedString,
}

impl fmt::Debug for TranscriberConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TranscriberConfig")
            .field("websocket_url", &self.websocket_url)
            .field("input_device", &self.input_device)
            .field("language", &self.language)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TranscriberError {
    #[error("transcriber is already listening")]
    AlreadyListening,
    #[error("transcriber is not listening")]
    NotListening,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl TranscriberError {
    #[allow(dead_code)]
    pub fn other(error: anyhow::Error) -> Self {
        Self::Other(error)
    }
}

/// Streams microphone audio to a backend and emits transcript updates on a channel.
///
/// Implementations run capture and network I/O on a [`BackgroundExecutor`]. They must
/// not update UI state directly. Pass an [`mpsc::UnboundedSender`] to [`Self::start`]
/// and handle [`TranscriberEvent::Transcript`] on the GPUI foreground thread.
pub trait Transcriber: Send {
    fn state(&self) -> TranscriberState;

    fn start(
        &mut self,
        config: TranscriberConfig,
        events: mpsc::UnboundedSender<TranscriberEvent>,
        executor: BackgroundExecutor,
    ) -> Task<Result<(), TranscriberError>>;

    fn stop(&mut self) -> Task<Result<(), TranscriberError>>;
}

impl fmt::Debug for dyn Transcriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transcriber")
            .field("state", &self.state())
            .finish()
    }
}
