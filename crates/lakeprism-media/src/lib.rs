use lakeprism_core::MediaConstraints;
#[cfg(feature = "native-media")]
use std::path::Path;

#[cfg(feature = "native-media")]
use thiserror::Error;

#[cfg(feature = "native-media")]
#[derive(Debug, Error)]
pub enum MediaError {
    #[error(transparent)]
    Ffmpeg(#[from] ffmpeg_next::Error),
    #[error("no video stream was found")]
    MissingVideoStream,
    #[error("no audio stream was found")]
    MissingAudioStream,
    #[error("no decodable video frame was found at or after the requested timestamp")]
    MissingDecodedFrame,
    #[error("FFmpeg input container {container} is not enabled by the bounded runtime profile")]
    UnsupportedContainer { container: String },
    #[error("FFmpeg {media_type} codec {codec} is not enabled by the bounded runtime profile")]
    UnsupportedCodec {
        media_type: &'static str,
        codec: String,
    },
}

/// Runtime versions loaded by the current Rust process. Successful validation
/// proves the linked native libraries can initialize for this target; it does
/// not shell out to, or trust, a separately installed `ffmpeg` executable.
#[cfg(feature = "native-media")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeRuntime {
    pub format_version: u32,
    pub codec_version: u32,
    pub utility_version: u32,
}

#[cfg(feature = "native-media")]
pub fn validate_native_runtime() -> Result<NativeRuntime, MediaError> {
    ffmpeg_next::init()?;
    Ok(NativeRuntime {
        format_version: ffmpeg_next::format::version(),
        codec_version: ffmpeg_next::codec::version(),
        utility_version: ffmpeg_next::util::version(),
    })
}

/// Deliberately small decode coverage for local LakePrism media functions.
/// Applications that need other formats must opt in explicitly with their own
/// `NativeMediaProfile`; unknown containers/codecs never get silently decoded.
#[cfg(feature = "native-media")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMediaProfile {
    pub containers: &'static [&'static str],
    pub video_codecs: &'static [&'static str],
    pub audio_codecs: &'static [&'static str],
}

#[cfg(feature = "native-media")]
impl Default for NativeMediaProfile {
    fn default() -> Self {
        Self {
            containers: &[
                "mov", "mp4", "m4a", "3gp", "3g2", "mj2", "matroska", "webm", "wav",
            ],
            video_codecs: &["h264", "hevc", "vp9", "av1", "mpeg4", "mjpeg"],
            audio_codecs: &[
                "aac",
                "mp3",
                "opus",
                "vorbis",
                "flac",
                "pcm_s16le",
                "pcm_f32le",
            ],
        }
    }
}

#[cfg(feature = "native-media")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaMetadata {
    pub duration_millis: Option<u64>,
    pub video_stream_count: usize,
    pub audio_stream_count: usize,
}

#[cfg(feature = "native-media")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedVideoFrame {
    pub timestamp_millis: Option<u64>,
    pub width: u32,
    pub height: u32,
    pub rgb24_bytes: Vec<u8>,
}

/// A bounded, normalized audio chunk. Samples are interleaved little-endian
/// `f32` values at `sample_rate_hz`; LakePrism currently normalizes to mono.
#[cfg(feature = "native-media")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AudioSegment {
    pub start_millis: u64,
    pub end_millis: u64,
    pub sample_rate_hz: u32,
    pub channel_count: u16,
    pub f32le_bytes: Vec<u8>,
}

/// LakePrism's stable local SQL audio representation.
#[cfg(feature = "native-media")]
pub const NORMALIZED_AUDIO_SAMPLE_RATE_HZ: u32 = 16_000;

#[cfg(feature = "native-media")]
pub fn probe_media(path: impl AsRef<Path>) -> Result<MediaMetadata, MediaError> {
    probe_media_with_profile(path, NativeMediaProfile::default())
}

#[cfg(feature = "native-media")]
pub fn probe_media_with_profile(
    path: impl AsRef<Path>,
    profile: NativeMediaProfile,
) -> Result<MediaMetadata, MediaError> {
    validate_native_runtime()?;
    let input = ffmpeg_next::format::input(&path)?;
    validate_input_profile(&input, profile)?;
    let duration_millis = (input.duration() >= 0).then(|| (input.duration() as u64) / 1_000);
    let video_stream_count = input
        .streams()
        .filter(|stream| stream.parameters().medium() == ffmpeg_next::media::Type::Video)
        .count();
    let audio_stream_count = input
        .streams()
        .filter(|stream| stream.parameters().medium() == ffmpeg_next::media::Type::Audio)
        .count();

    Ok(MediaMetadata {
        duration_millis,
        video_stream_count,
        audio_stream_count,
    })
}

#[cfg(feature = "native-media")]
pub fn decode_video_frame_at_or_after(
    path: impl AsRef<Path>,
    requested_timestamp_millis: u64,
) -> Result<DecodedVideoFrame, MediaError> {
    use ffmpeg_next::{codec, format, frame, media, software, util};

    validate_native_runtime()?;
    let mut input = format::input(&path)?;
    validate_input_profile(&input, NativeMediaProfile::default())?;
    let stream = input
        .streams()
        .best(media::Type::Video)
        .ok_or(MediaError::MissingVideoStream)?;
    let stream_index = stream.index();
    let time_base = stream.time_base();
    let context = codec::context::Context::from_parameters(stream.parameters())?;
    let mut decoder = context.decoder().video()?;
    let mut scaler = software::scaling::Context::get(
        decoder.format(),
        decoder.width(),
        decoder.height(),
        util::format::pixel::Pixel::RGB24,
        decoder.width(),
        decoder.height(),
        software::scaling::flag::Flags::BILINEAR,
    )?;
    let mut decoded = frame::Video::empty();

    for (packet_stream, packet) in input.packets() {
        if packet_stream.index() != stream_index {
            continue;
        }
        decoder.send_packet(&packet)?;
        while decoder.receive_frame(&mut decoded).is_ok() {
            if frame_timestamp_millis(decoded.timestamp(), time_base)
                .is_some_and(|timestamp| timestamp >= requested_timestamp_millis)
            {
                return rgb24_frame(&decoded, &mut scaler, time_base);
            }
        }
    }

    decoder.send_eof()?;
    while decoder.receive_frame(&mut decoded).is_ok() {
        if frame_timestamp_millis(decoded.timestamp(), time_base)
            .is_some_and(|timestamp| timestamp >= requested_timestamp_millis)
        {
            return rgb24_frame(&decoded, &mut scaler, time_base);
        }
    }
    Err(MediaError::MissingDecodedFrame)
}

/// Decode no more than `max_frames` RGB24 frames at regular source-timeline
/// intervals. Callers must apply their concurrency governor before invoking it.
#[cfg(feature = "native-media")]
pub fn decode_video_frames(
    path: impl AsRef<Path>,
    start_millis: u64,
    end_millis: u64,
    every_millis: u64,
    max_frames: usize,
) -> Result<Vec<DecodedVideoFrame>, MediaError> {
    if every_millis == 0 || max_frames == 0 || end_millis < start_millis {
        return Ok(Vec::new());
    }
    let mut frames = Vec::with_capacity(max_frames);
    let mut timestamp = start_millis;
    while timestamp <= end_millis && frames.len() < max_frames {
        match decode_video_frame_at_or_after(&path, timestamp) {
            Ok(frame) => {
                let Some(actual_timestamp) = frame.timestamp_millis else {
                    break;
                };
                if actual_timestamp > end_millis {
                    break;
                }
                if frames.last().is_none_or(|previous: &DecodedVideoFrame| {
                    previous.timestamp_millis != Some(actual_timestamp)
                }) {
                    frames.push(frame);
                }
                timestamp = actual_timestamp.saturating_add(every_millis);
            }
            Err(MediaError::MissingDecodedFrame) => break,
            Err(error) => return Err(error),
        }
    }
    Ok(frames)
}

/// Decode, downmix, and resample an audio stream into bounded timeline chunks.
///
/// The output is always mono, packed IEEE-754 `f32` little-endian at 16 kHz.
/// Decoding stops after `max_segments`; no whole-file PCM buffer is created.
#[cfg(feature = "native-media")]
pub fn decode_audio_segments(
    path: impl AsRef<Path>,
    start_millis: u64,
    end_millis: u64,
    segment_millis: u64,
    max_segments: usize,
) -> Result<Vec<AudioSegment>, MediaError> {
    use ffmpeg_next::{codec, format, frame, media, software, util};

    if segment_millis == 0 || max_segments == 0 || end_millis <= start_millis {
        return Ok(Vec::new());
    }

    validate_native_runtime()?;
    let mut input = format::input(&path)?;
    validate_input_profile(&input, NativeMediaProfile::default())?;
    let stream = input
        .streams()
        .find(|stream| stream.parameters().medium() == media::Type::Audio)
        .ok_or(MediaError::MissingAudioStream)?;
    let stream_index = stream.index();
    let time_base = stream.time_base();
    let context = codec::context::Context::from_parameters(stream.parameters())?;
    let mut decoder = context.decoder().audio()?;
    decoder.set_parameters(stream.parameters())?;
    let input_layout = decoder.channel_layout();
    let input_rate = decoder.rate();
    if input_rate == 0 || input_layout.channels() == 0 {
        return Err(MediaError::MissingAudioStream);
    }

    let output_format = util::format::Sample::F32(util::format::sample::Type::Packed);
    let mut resampler = software::resampling::Context::get(
        decoder.format(),
        input_layout,
        input_rate,
        output_format,
        ffmpeg_next::ChannelLayout::MONO,
        NORMALIZED_AUDIO_SAMPLE_RATE_HZ,
    )?;
    let mut decoded = frame::Audio::empty();
    let mut converted = frame::Audio::empty();
    let chunk_samples = usize::try_from(
        u64::from(NORMALIZED_AUDIO_SAMPLE_RATE_HZ)
            .saturating_mul(segment_millis)
            .div_ceil(1_000),
    )
    .unwrap_or(usize::MAX);
    let mut output = AudioChunkAssembler::new(
        start_millis,
        end_millis,
        segment_millis,
        chunk_samples.max(1),
        max_segments,
    );

    for (packet_stream, packet) in input.packets() {
        if packet_stream.index() != stream_index {
            continue;
        }
        decoder.send_packet(&packet)?;
        while decoder.receive_frame(&mut decoded).is_ok() {
            let frame_start = frame_timestamp_millis(decoded.timestamp(), time_base).unwrap_or(0);
            converted = resampled_audio_frame(decoded.samples(), input_rate, output_format);
            resampler.run(&decoded, &mut converted)?;
            output.push(frame_start, converted.plane::<f32>(0));
            converted = frame::Audio::empty();
            if output.is_full() {
                return Ok(output.finish());
            }
        }
    }
    decoder.send_eof()?;
    while decoder.receive_frame(&mut decoded).is_ok() {
        let frame_start = frame_timestamp_millis(decoded.timestamp(), time_base).unwrap_or(0);
        converted = resampled_audio_frame(decoded.samples(), input_rate, output_format);
        resampler.run(&decoded, &mut converted)?;
        output.push(frame_start, converted.plane::<f32>(0));
        converted = frame::Audio::empty();
        if output.is_full() {
            return Ok(output.finish());
        }
    }
    while resampler.flush(&mut converted)?.is_some() {
        output.push(end_millis, converted.plane::<f32>(0));
        converted = frame::Audio::empty();
        if output.is_full() {
            break;
        }
    }
    Ok(output.finish())
}

#[cfg(feature = "native-media")]
fn validate_input_profile(
    input: &ffmpeg_next::format::context::Input,
    profile: NativeMediaProfile,
) -> Result<(), MediaError> {
    let container = input.format().name().to_owned();
    if !container
        .split(',')
        .any(|name| profile.containers.contains(&name))
    {
        return Err(MediaError::UnsupportedContainer { container });
    }
    for stream in input.streams() {
        let (media_type, allowed) = match stream.parameters().medium() {
            ffmpeg_next::media::Type::Video => ("video", profile.video_codecs),
            ffmpeg_next::media::Type::Audio => ("audio", profile.audio_codecs),
            _ => continue,
        };
        let codec = stream.parameters().id().name().to_owned();
        if !allowed.contains(&codec.as_str()) {
            return Err(MediaError::UnsupportedCodec { media_type, codec });
        }
    }
    Ok(())
}

#[cfg(feature = "native-media")]
fn resampled_audio_frame(
    input_samples: usize,
    input_rate: u32,
    output_format: ffmpeg_next::util::format::Sample,
) -> ffmpeg_next::frame::Audio {
    let output_samples = input_samples
        .saturating_mul(NORMALIZED_AUDIO_SAMPLE_RATE_HZ as usize)
        .div_ceil(input_rate as usize)
        .saturating_add(32);
    ffmpeg_next::frame::Audio::new(
        output_format,
        output_samples,
        ffmpeg_next::ChannelLayout::MONO,
    )
}

#[cfg(feature = "native-media")]
struct AudioChunkAssembler {
    start_millis: u64,
    end_millis: u64,
    segment_millis: u64,
    chunk_samples: usize,
    max_segments: usize,
    samples: Vec<f32>,
    segments: Vec<AudioSegment>,
}

#[cfg(feature = "native-media")]
impl AudioChunkAssembler {
    fn new(
        start_millis: u64,
        end_millis: u64,
        segment_millis: u64,
        chunk_samples: usize,
        max_segments: usize,
    ) -> Self {
        Self {
            start_millis,
            end_millis,
            segment_millis,
            chunk_samples,
            max_segments,
            samples: Vec::with_capacity(chunk_samples),
            segments: Vec::with_capacity(max_segments),
        }
    }

    fn push(&mut self, frame_start_millis: u64, samples: &[f32]) {
        if frame_start_millis >= self.end_millis {
            return;
        }
        let rate = u64::from(NORMALIZED_AUDIO_SAMPLE_RATE_HZ);
        let first = self
            .start_millis
            .saturating_sub(frame_start_millis)
            .saturating_mul(rate)
            .div_ceil(1_000);
        let last = self
            .end_millis
            .saturating_sub(frame_start_millis)
            .saturating_mul(rate)
            .div_ceil(1_000);
        let first = usize::try_from(first)
            .unwrap_or(usize::MAX)
            .min(samples.len());
        let last = usize::try_from(last)
            .unwrap_or(usize::MAX)
            .min(samples.len());
        for sample in &samples[first..last] {
            if self.is_full() {
                break;
            }
            self.samples.push(*sample);
            if self.samples.len() == self.chunk_samples {
                self.flush();
            }
        }
    }

    fn flush(&mut self) {
        if self.samples.is_empty() || self.segments.len() == self.max_segments {
            return;
        }
        let segment_start = self.start_millis.saturating_add(
            u64::try_from(self.segments.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(self.segment_millis),
        );
        if segment_start >= self.end_millis {
            self.samples.clear();
            return;
        }
        let sample_duration = u64::try_from(self.samples.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(1_000)
            / u64::from(NORMALIZED_AUDIO_SAMPLE_RATE_HZ);
        let segment_end = segment_start
            .saturating_add(sample_duration)
            .min(self.end_millis);
        let f32le_bytes = self
            .samples
            .iter()
            .flat_map(|sample| sample.to_le_bytes())
            .collect();
        self.segments.push(AudioSegment {
            start_millis: segment_start,
            end_millis: segment_end,
            sample_rate_hz: NORMALIZED_AUDIO_SAMPLE_RATE_HZ,
            channel_count: 1,
            f32le_bytes,
        });
        self.samples.clear();
    }

    fn is_full(&self) -> bool {
        self.segments.len() == self.max_segments
    }

    fn finish(mut self) -> Vec<AudioSegment> {
        self.flush();
        self.segments
    }
}

#[cfg(feature = "native-media")]
fn rgb24_frame(
    decoded: &ffmpeg_next::frame::Video,
    scaler: &mut ffmpeg_next::software::scaling::Context,
    time_base: ffmpeg_next::Rational,
) -> Result<DecodedVideoFrame, MediaError> {
    let mut rgb24 = ffmpeg_next::frame::Video::empty();
    scaler.run(decoded, &mut rgb24)?;
    let row_bytes = rgb24.width() as usize * 3;
    let mut rgb24_bytes = Vec::with_capacity(row_bytes * rgb24.height() as usize);
    for row in rgb24
        .data(0)
        .chunks(rgb24.stride(0))
        .take(rgb24.height() as usize)
    {
        rgb24_bytes.extend_from_slice(&row[..row_bytes]);
    }
    Ok(DecodedVideoFrame {
        timestamp_millis: frame_timestamp_millis(decoded.timestamp(), time_base),
        width: rgb24.width(),
        height: rgb24.height(),
        rgb24_bytes,
    })
}

#[cfg(feature = "native-media")]
fn frame_timestamp_millis(timestamp: Option<i64>, time_base: ffmpeg_next::Rational) -> Option<u64> {
    let timestamp = timestamp?;
    let numerator = time_base.numerator() as i128;
    let denominator = time_base.denominator() as i128;
    let millis = i128::from(timestamp) * numerator * 1_000 / denominator;
    u64::try_from(millis).ok()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FramePlan {
    pub timestamps_millis: Vec<u64>,
    pub materialize_payload_bytes: bool,
}

pub fn plan_frames(
    available_timestamps_millis: impl IntoIterator<Item = u64>,
    constraints: &MediaConstraints,
) -> FramePlan {
    let timestamps_millis = available_timestamps_millis
        .into_iter()
        .filter(|timestamp| {
            constraints
                .time_range_millis
                .as_ref()
                .is_none_or(|range| range.contains(timestamp))
        })
        .take(constraints.limit_hint.unwrap_or(usize::MAX))
        .collect();

    FramePlan {
        timestamps_millis,
        materialize_payload_bytes: constraints.projection.include_payload_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::{MediaConstraints, MediaProjection};

    #[test]
    fn frame_planning_pushes_time_projection_and_limit_constraints() {
        let constraints = MediaConstraints {
            time_range_millis: Some(1_000..=3_000),
            limit_hint: Some(2),
            projection: MediaProjection {
                include_payload_bytes: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let plan = plan_frames([0, 1_000, 2_000, 3_000, 4_000], &constraints);

        assert_eq!(plan.timestamps_millis, vec![1_000, 2_000]);
        assert!(!plan.materialize_payload_bytes);
    }

    #[cfg(feature = "native-media")]
    #[test]
    fn media_probe_reports_video_stream_metadata() {
        let output_path = std::env::current_dir().unwrap().join(format!(
            "lakeprism-media-{}-{}.mp4",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let status = std::process::Command::new("ffmpeg")
            .args([
                "-y",
                "-f",
                "lavfi",
                "-i",
                "color=c=black:s=16x16:d=0.1",
                "-an",
            ])
            .arg(&output_path)
            .status()
            .unwrap();
        assert!(status.success());

        let metadata = probe_media(&output_path).unwrap();
        let runtime = validate_native_runtime().unwrap();

        assert_eq!(metadata.video_stream_count, 1);
        assert_eq!(metadata.audio_stream_count, 0);
        assert!(metadata.duration_millis.is_some());
        assert!(runtime.format_version > 0 && runtime.codec_version > 0);
        let frame = decode_video_frame_at_or_after(&output_path, 0).unwrap();
        assert_eq!(frame.width, 16);
        assert_eq!(frame.height, 16);
        assert_eq!(frame.rgb24_bytes.len(), 16 * 16 * 3);
        std::fs::remove_file(output_path).unwrap();
    }

    #[cfg(feature = "native-media")]
    #[test]
    fn audio_decode_normalizes_to_mono_f32le_chunks() {
        let output_path = std::env::current_dir().unwrap().join(format!(
            "lakeprism-audio-{}-{}.wav",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let status = std::process::Command::new("ffmpeg")
            .args([
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:sample_rate=8000:duration=0.12",
                "-ac",
                "2",
            ])
            .arg(&output_path)
            .status()
            .unwrap();
        assert!(status.success());

        assert_eq!(probe_media(&output_path).unwrap().audio_stream_count, 1);
        let segments = decode_audio_segments(&output_path, 0, 120, 40, 3).unwrap();

        assert_eq!(segments.len(), 3);
        assert!(segments.iter().all(|segment| {
            segment.sample_rate_hz == NORMALIZED_AUDIO_SAMPLE_RATE_HZ
                && segment.channel_count == 1
                && !segment.f32le_bytes.is_empty()
                && segment.f32le_bytes.len() % std::mem::size_of::<f32>() == 0
        }));
        std::fs::remove_file(output_path).unwrap();
    }
}
