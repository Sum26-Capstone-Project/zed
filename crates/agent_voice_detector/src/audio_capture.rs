use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use cpal::{DeviceId, traits::DeviceTrait as _, traits::StreamTrait as _};
use futures::channel::{mpsc, oneshot};
use gpui::{BackgroundExecutor, Priority, Task};
use parking_lot::Mutex;

use crate::cpal_util::{
    append_samples_from_input_data, default_input_device, interleaved_samples_for_duration,
    samples_per_channel_for_duration,
};

pub const DEFAULT_SAMPLE_RATE: u32 = 16_000;
pub const DEFAULT_CHANNEL_COUNT: u32 = 1;
pub const DEFAULT_CHUNK_DURATION_MS: u32 = 100;

const DEVICE_FRAME_MS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioChunk {
    pub samples: Vec<i16>,
    pub sample_rate: u32,
    pub channel_count: u32,
    pub samples_per_channel: u32,
}

impl AudioChunk {
    pub fn byte_len(&self) -> usize {
        self.samples.len() * std::mem::size_of::<i16>()
    }
}

#[derive(Debug, Clone)]
pub struct AudioCaptureConfig {
    pub input_device: Option<DeviceId>,
    pub sample_rate: u32,
    pub channel_count: u32,
    pub chunk_duration_ms: u32,
}

impl Default for AudioCaptureConfig {
    fn default() -> Self {
        Self {
            input_device: None,
            sample_rate: DEFAULT_SAMPLE_RATE,
            channel_count: DEFAULT_CHANNEL_COUNT,
            chunk_duration_ms: DEFAULT_CHUNK_DURATION_MS,
        }
    }
}

impl AudioCaptureConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.sample_rate > 0, "sample_rate must be greater than zero");
        anyhow::ensure!(self.channel_count > 0, "channel_count must be greater than zero");
        anyhow::ensure!(
            self.chunk_duration_ms > 0,
            "chunk_duration_ms must be greater than zero"
        );
        Ok(())
    }

    pub fn samples_per_chunk(&self) -> usize {
        interleaved_samples_for_duration(self.sample_rate, self.channel_count, self.chunk_duration_ms)
    }

    pub fn bytes_per_chunk(&self) -> usize {
        self.samples_per_chunk() * std::mem::size_of::<i16>()
    }

    pub fn samples_per_channel_per_chunk(&self) -> u32 {
        samples_per_channel_for_duration(self.sample_rate, self.chunk_duration_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCaptureState {
    Idle,
    Capturing,
}

#[derive(Debug, thiserror::Error)]
pub enum AudioCaptureError {
    #[error("audio capture is already running")]
    AlreadyCapturing,
    #[error("audio capture is not running")]
    NotCapturing,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

/// Captures microphone audio on a background executor and emits fixed-duration PCM chunks.
///
/// By default chunks are mono 16-bit PCM at 16 kHz with 100 ms frames (1600 samples,
/// 3200 bytes). Use [`AudioCaptureConfig`] to override the output format.
///
/// After [`AudioCapture::stop`] returns, capture may still be winding down briefly while
/// the cpal stream is closed. Wait for the [`Task`] returned from [`AudioCapture::start`]
/// to finish, or for [`AudioCapture::state`] to become [`AudioCaptureState::Idle`], before
/// starting again.
pub struct AudioCapture {
    state: Arc<Mutex<AudioCaptureState>>,
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl Default for AudioCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl AudioCapture {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(AudioCaptureState::Idle)),
            stop_tx: Mutex::new(None),
        }
    }

    pub fn state(&self) -> AudioCaptureState {
        *self.state.lock()
    }

    pub fn start(
        &mut self,
        config: AudioCaptureConfig,
        chunks: mpsc::UnboundedSender<AudioChunk>,
        executor: BackgroundExecutor,
    ) -> Task<Result<(), AudioCaptureError>> {
        if *self.state.lock() == AudioCaptureState::Capturing {
            return Task::ready(Err(AudioCaptureError::AlreadyCapturing));
        }

        if let Err(error) = config.validate() {
            return Task::ready(Err(AudioCaptureError::Other(error)));
        }

        let (stop_tx, stop_rx) = oneshot::channel();
        *self.stop_tx.lock() = Some(stop_tx);
        *self.state.lock() = AudioCaptureState::Capturing;

        let state = self.state.clone();
        let capture_executor = executor.clone();
        executor.spawn(async move {
            let result = run_capture(config, chunks, stop_rx, capture_executor).await;
            *state.lock() = AudioCaptureState::Idle;
            result.map_err(AudioCaptureError::Other)
        })
    }

    pub fn stop(&mut self) -> Task<Result<(), AudioCaptureError>> {
        if *self.state.lock() != AudioCaptureState::Capturing {
            return Task::ready(Err(AudioCaptureError::NotCapturing));
        }

        if let Some(stop_tx) = self.stop_tx.lock().take() {
            let _ = stop_tx.send(());
        }
        Task::ready(Ok(()))
    }
}

async fn run_capture(
    config: AudioCaptureConfig,
    chunks: mpsc::UnboundedSender<AudioChunk>,
    stop_rx: oneshot::Receiver<()>,
    executor: BackgroundExecutor,
) -> anyhow::Result<()> {
    let (device, stream_config) =
        default_input_device(config.input_device.as_ref()).context("failed to open input device")?;

    if let Some(description) = device.description().ok() {
        log::info!("agent voice capture using microphone: {}", description.name());
    }

    #[cfg(any(all(target_os = "windows", target_env = "gnu"), target_os = "freebsd"))]
    log::warn!(
        "agent voice capture cannot resample on this platform; emitted chunks use the device-native format"
    );

    let (stream_tx, stream_rx) = std::sync::mpsc::channel::<()>();
    let capture_task = executor.spawn_with_priority(Priority::RealtimeAudio, async move {
        if let Err(error) = capture_input_stream(device, stream_config, config, chunks, stream_rx)
        {
            log::error!("agent voice capture failed: {error:#}");
        }
    });

    let _ = stop_rx.await;
    drop(stream_tx);
    capture_task.await;
    Ok(())
}

fn capture_input_stream(
    device: cpal::Device,
    stream_config: cpal::SupportedStreamConfig,
    output_config: AudioCaptureConfig,
    chunks: mpsc::UnboundedSender<AudioChunk>,
    stream_rx: std::sync::mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    let device_sample_rate = stream_config.sample_rate();
    let device_channel_count = stream_config.channels() as u32;
    let device_frame_samples = interleaved_samples_for_duration(
        device_sample_rate,
        device_channel_count,
        DEVICE_FRAME_MS,
    );
    let device_samples_per_channel_per_frame =
        samples_per_channel_for_duration(device_sample_rate, DEVICE_FRAME_MS);

    let mut device_pending_samples: Vec<i16> = Vec::with_capacity(device_frame_samples);
    let output_chunk_samples = output_config.samples_per_chunk();
    let mut output_pending_samples: Vec<i16> = Vec::with_capacity(output_chunk_samples);
    let samples_per_channel = output_config.samples_per_channel_per_chunk();
    let output_sample_rate = output_config.sample_rate;
    let output_channel_count = output_config.channel_count;

    #[cfg(not(any(all(target_os = "windows", target_env = "gnu"), target_os = "freebsd")))]
    let mut resampler = libwebrtc::native::audio_resampler::AudioResampler::default();

    let emit_chunks = move |output_pending_samples: &mut Vec<i16>| {
        while output_pending_samples.len() >= output_chunk_samples {
            let mut tail = output_pending_samples.split_off(output_chunk_samples);
            std::mem::swap(output_pending_samples, &mut tail);
            let chunk = AudioChunk {
                samples: tail,
                sample_rate: output_sample_rate,
                channel_count: output_channel_count,
                samples_per_channel,
            };
            if chunks.unbounded_send(chunk).is_err() {
                log::debug!("agent voice capture chunk receiver dropped");
                return;
            }
        }
    };

    let stream = device
        .build_input_stream_raw(
            &stream_config.config(),
            stream_config.sample_format(),
            move |data, _: &_| {
                if append_samples_from_input_data(stream_config.sample_format(), data, &mut device_pending_samples).is_err() {
                    return;
                };

                while device_pending_samples.len() >= device_frame_samples {
                    let device_frame: Vec<i16> =
                        device_pending_samples.drain(..device_frame_samples).collect();

                    #[cfg(not(any(
                        all(target_os = "windows", target_env = "gnu"),
                        target_os = "freebsd"
                    )))]
                    {
                        let resampled = resampler
                            .remix_and_resample(
                                device_frame.as_slice(),
                                device_samples_per_channel_per_frame,
                                device_channel_count,
                                device_sample_rate,
                                output_channel_count,
                                output_sample_rate,
                            )
                            .to_owned();
                        output_pending_samples.extend_from_slice(&resampled);
                    }

                    #[cfg(any(
                        all(target_os = "windows", target_env = "gnu"),
                        target_os = "freebsd"
                    ))]
                    {
                        output_pending_samples.extend_from_slice(&device_frame);
                    }

                    emit_chunks(&mut output_pending_samples);
                }
            },
            |error| log::error!("agent voice capture stream error: {error:?}"),
            Some(Duration::from_millis(100)),
        )
        .context("failed to build input stream")?;

    stream.play().context("failed to start input stream")?;
    stream_rx.recv().ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_stt_client_expectations() {
        let config = AudioCaptureConfig::default();
        assert_eq!(config.sample_rate, 16_000);
        assert_eq!(config.channel_count, 1);
        assert_eq!(config.chunk_duration_ms, 100);
        assert_eq!(config.samples_per_chunk(), 1600);
        assert_eq!(config.bytes_per_chunk(), 3200);
        assert_eq!(config.samples_per_channel_per_chunk(), 1600);
    }

    #[test]
    fn rejects_invalid_config() {
        let config = AudioCaptureConfig {
            chunk_duration_ms: 0,
            ..AudioCaptureConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
