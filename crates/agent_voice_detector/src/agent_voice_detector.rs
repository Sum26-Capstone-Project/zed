mod audio_capture;
mod cpal_util;
mod start_end_word_detector;
mod transcriber;
mod websocket_stream;
mod websocket_transcriber;

pub use audio_capture::{
    AudioCapture, AudioCaptureConfig, AudioCaptureError, AudioCaptureState, AudioChunk,
    DEFAULT_CHANNEL_COUNT, DEFAULT_CHUNK_DURATION_MS, DEFAULT_SAMPLE_RATE,
};
pub use start_end_word_detector::{DetectorEvent, STOP_WORD, WAKE_WORD, start_detector};
pub use transcriber::{
    Transcriber, TranscriberConfig, TranscriberError, TranscriberEvent, TranscriberState,
    TranscriptUpdate,
};
pub use websocket_transcriber::{WebSocketTranscriber, DEFAULT_WEBSOCKET_URL};
