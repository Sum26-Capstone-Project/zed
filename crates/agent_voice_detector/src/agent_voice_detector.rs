mod audio_capture;
mod cpal_util;
mod transcriber;
mod websocket_transcriber;

pub use audio_capture::{
    AudioCapture, AudioCaptureConfig, AudioCaptureError, AudioCaptureState, AudioChunk,
    DEFAULT_CHANNEL_COUNT, DEFAULT_CHUNK_DURATION_MS, DEFAULT_SAMPLE_RATE,
};
pub use transcriber::{
    Transcriber, TranscriberConfig, TranscriberError, TranscriberEvent, TranscriberState,
    TranscriptUpdate,
};
pub use websocket_transcriber::{WebSocketTranscriber, DEFAULT_WEBSOCKET_URL};
