use anyhow::Context as _;
use cpal::{DeviceId, SizedSample, traits::DeviceTrait as _};

use audio::resolve_device;

pub(crate) fn default_input_device(
    device_id: Option<&DeviceId>,
) -> anyhow::Result<(cpal::Device, cpal::SupportedStreamConfig)> {
    let device = resolve_device(device_id, true)?;
    let config = device
        .default_input_config()
        .context("failed to get default input config")?;
    Ok((device, config))
}

pub(crate) fn append_samples_from_input_data(
    sample_format: cpal::SampleFormat,
    data: &cpal::Data,
    samples: &mut Vec<i16>,
) -> anyhow::Result<()> {
    match sample_format {
        cpal::SampleFormat::I16 => {
            samples.extend_from_slice(data.as_slice::<i16>().unwrap());
            Ok(())
        }
        cpal::SampleFormat::I8 => extend_converted_samples::<i8>(data, samples),
        cpal::SampleFormat::I24 => extend_converted_samples::<cpal::I24>(data, samples),
        cpal::SampleFormat::I32 => extend_converted_samples::<i32>(data, samples),
        cpal::SampleFormat::I64 => extend_converted_samples::<i64>(data, samples),
        cpal::SampleFormat::U8 => extend_converted_samples::<u8>(data, samples),
        cpal::SampleFormat::U16 => extend_converted_samples::<u16>(data, samples),
        cpal::SampleFormat::U32 => extend_converted_samples::<u32>(data, samples),
        cpal::SampleFormat::U64 => extend_converted_samples::<u64>(data, samples),
        cpal::SampleFormat::F32 => extend_converted_samples::<f32>(data, samples),
        cpal::SampleFormat::F64 => extend_converted_samples::<f64>(data, samples),
        _ => anyhow::bail!("unsupported sample format: {sample_format:?}"),
    }
}

fn extend_converted_samples<TSource>(data: &cpal::Data, samples: &mut Vec<i16>) -> anyhow::Result<()>
where
    TSource: SizedSample,
    i16: cpal::FromSample<TSource>,
{
    let source = data.as_slice::<TSource>().unwrap();
    samples.reserve(source.len());
    samples.extend(
        source
            .iter()
            .map(|sample| sample.to_sample::<i16>()),
    );
    Ok(())
}

pub(crate) fn interleaved_samples_for_duration(
    sample_rate: u32,
    channel_count: u32,
    duration_ms: u32,
) -> usize {
    (sample_rate as u64 * channel_count as u64 * duration_ms as u64 / 1000) as usize
}

pub(crate) fn samples_per_channel_for_duration(sample_rate: u32, duration_ms: u32) -> u32 {
    sample_rate * duration_ms / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleaved_sample_count_for_ten_ms_mono_48khz() {
        assert_eq!(interleaved_samples_for_duration(48_000, 1, 10), 480);
    }

    #[test]
    fn interleaved_sample_count_for_hundred_ms_mono_16khz() {
        assert_eq!(interleaved_samples_for_duration(16_000, 1, 100), 1600);
    }
}
