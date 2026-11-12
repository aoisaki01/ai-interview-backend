use anyhow::{anyhow, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Sample, SampleFormat, StreamConfig};
use serde::Serialize;
use std::collections::VecDeque;
use std::f32::consts::PI;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub const ELEVENLABS_SAMPLE_RATE: u32 = 16_000;
const CHUNK_SAMPLES_16K: usize = 1_600; // 100ms at 16kHz mono

#[derive(Debug, Clone, Serialize)]
pub struct AudioDeviceEntry {
    pub name: String,
    pub is_default: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioDevicesList {
    pub host: String,
    pub inputs: Vec<AudioDeviceEntry>,
    pub outputs: Vec<AudioDeviceEntry>,
}

#[derive(Clone)]
pub struct PlaybackController {
    queue: Arc<Mutex<VecDeque<i16>>>,
}

impl PlaybackController {
    pub fn push_pcm16_le(&self, bytes: &[u8]) {
        if bytes.len() < 2 {
            return;
        }
        let mut q = match self.queue.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for chunk in bytes.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            q.push_back(sample);
        }
    }

    pub fn clear(&self) {
        if let Ok(mut q) = self.queue.lock() {
            q.clear();
        }
    }

    pub fn buffered_samples(&self) -> usize {
        self.queue.lock().map(|q| q.len()).unwrap_or(0)
    }
}

pub struct AudioEngine {
    pub playback: PlaybackController,
    muted_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
    pub input_info: String,
    pub output_info: String,
}

impl AudioEngine {
    pub fn start(
        mic_name: Option<&str>,
        speaker_name: Option<&str>,
    ) -> Result<(
        Self,
        mpsc::UnboundedReceiver<Vec<u8>>,
        mpsc::UnboundedReceiver<f32>,
    )> {
        let (mic_tx, mic_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (rms_tx, rms_rx) = mpsc::unbounded_channel::<f32>();
        let playback_queue = Arc::new(Mutex::new(VecDeque::<i16>::with_capacity(32_000)));
        let playback = PlaybackController {
            queue: Arc::clone(&playback_queue),
        };
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_thread = Arc::clone(&stop_flag);
        let muted_flag = Arc::new(AtomicBool::new(false));
        let muted_flag_thread = Arc::clone(&muted_flag);

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(String, String)>>();
        let mic_name_owned = mic_name.map(|s| s.to_string());
        let speaker_name_owned = speaker_name.map(|s| s.to_string());

        let worker = std::thread::Builder::new()
            .name("audio-io".to_string())
            .spawn(move || {
                match init_and_run_audio(
                    mic_name_owned.as_deref(),
                    speaker_name_owned.as_deref(),
                    mic_tx,
                    rms_tx,
                    playback_queue,
                    muted_flag_thread,
                    stop_flag_thread,
                    ready_tx.clone(),
                ) {
                    Ok(()) => {}
                    Err(err) => {
                        let _ = ready_tx.send(Err(err));
                    }
                }
            })
            .context("Gagal membuat thread audio-io")?;

        let (input_info, output_info) = ready_rx
            .recv()
            .context("Thread audio berhenti sebelum inisialisasi selesai")??;

        Ok((
            Self {
                playback,
                muted_flag,
                stop_flag,
                worker: Some(worker),
                input_info,
                output_info,
            },
            mic_rx,
            rms_rx,
        ))
    }

    pub fn set_muted(&self, muted: bool) {
        self.muted_flag.store(muted, Ordering::SeqCst);
    }

    pub fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AudioEngine {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct MicMeterHandle {
    stop_flag: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl MicMeterHandle {
    pub fn start(mic_name: Option<&str>) -> Result<(Self, mpsc::UnboundedReceiver<f32>)> {
        let (rms_tx, rms_rx) = mpsc::unbounded_channel::<f32>();
        let (dummy_mic_tx, _dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop_flag);
        let muted = Arc::new(AtomicBool::new(false));
        let mic_owned = mic_name.map(|s| s.to_string());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<()>>();

        let worker = std::thread::Builder::new()
            .name("mic-meter".to_string())
            .spawn(move || {
                let res = (|| -> Result<()> {
                    let host = cpal::default_host();
                    let in_dev = find_input_device(&host, mic_owned.as_deref())?;
                    let in_supported = in_dev
                        .default_input_config()
                        .context("Gagal membaca default_input_config")?;
                    let in_format = in_supported.sample_format();
                    let in_cfg: StreamConfig = in_supported.into();
                    let stream = build_input_stream(
                        &in_dev,
                        &in_cfg,
                        in_format,
                        dummy_mic_tx,
                        rms_tx,
                        muted,
                    )?;
                    stream.play().context("Gagal memulai stream mic meter")?;
                    let _ = ready_tx.send(Ok(()));
                    while !stop_thread.load(Ordering::SeqCst) {
                        std::thread::sleep(std::time::Duration::from_millis(40));
                    }
                    drop(stream);
                    Ok(())
                })();
                if let Err(err) = res {
                    let _ = ready_tx.send(Err(err));
                }
            })
            .context("Gagal spawn thread mic-meter")?;

        ready_rx.recv().context("Thread mic-meter berhenti")??;

        Ok((
            Self {
                stop_flag,
                worker: Some(worker),
            },
            rms_rx,
        ))
    }

    pub fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
    }
}

impl Drop for MicMeterHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn test_speaker_tone(speaker_name: Option<&str>) -> Result<String> {
    let speaker_owned = speaker_name.map(|s| s.to_string());
    let handle = std::thread::Builder::new()
        .name("speaker-test".to_string())
        .spawn(move || -> Result<String> {
            let host = cpal::default_host();
            let out_dev = find_output_device(&host, speaker_owned.as_deref())?;
            let out_supported = out_dev
                .default_output_config()
                .context("Gagal membaca default_output_config")?;
            let out_format = out_supported.sample_format();
            let out_cfg: StreamConfig = out_supported.into();
            let info = format!(
                "{} ({} Hz, {} ch)",
                out_dev.name().unwrap_or_else(|_| "Speaker".to_string()),
                out_cfg.sample_rate.0,
                out_cfg.channels
            );

            let sr = ELEVENLABS_SAMPLE_RATE as usize;
            let total_samples = (sr * 55) / 100;
            let split = (sr * 22) / 100;
            let mut q = VecDeque::with_capacity(total_samples);
            for i in 0..total_samples {
                let t = i as f32 / ELEVENLABS_SAMPLE_RATE as f32;
                let (freq, local_i, seg_len) = if i < split {
                    (523.25_f32, i, split)
                } else {
                    (659.25_f32, i - split, total_samples - split)
                };
                let attack = (local_i as f32 / 240.0).min(1.0);
                let release = ((seg_len - local_i) as f32 / 480.0).min(1.0);
                let env = attack.min(release);
                let val = (2.0 * PI * freq * t).sin() * 0.25 * env;
                q.push_back((val * 32767.0) as i16);
            }

            let queue = Arc::new(Mutex::new(q));
            let stream = build_output_stream(&out_dev, &out_cfg, out_format, Arc::clone(&queue))?;
            stream.play().context("Gagal memulai stream test speaker")?;
            std::thread::sleep(std::time::Duration::from_millis(650));
            drop(stream);
            Ok(info)
        })
        .context("Gagal spawn thread speaker-test")?;

    handle
        .join()
        .map_err(|_| anyhow!("Thread speaker-test panic"))?
}

pub fn list_audio_devices_data() -> Result<AudioDevicesList> {
    let host = cpal::default_host();
    let default_in = host
        .default_input_device()
        .and_then(|d| d.name().ok())
        .unwrap_or_default();
    let default_out = host
        .default_output_device()
        .and_then(|d| d.name().ok())
        .unwrap_or_default();

    let mut inputs = Vec::new();
    for dev in host.input_devices()? {
        if let Ok(name) = dev.name() {
            let is_default = name == default_in;
            inputs.push(AudioDeviceEntry { name, is_default });
        }
    }

    let mut outputs = Vec::new();
    for dev in host.output_devices()? {
        if let Ok(name) = dev.name() {
            let is_default = name == default_out;
            outputs.push(AudioDeviceEntry { name, is_default });
        }
    }

    Ok(AudioDevicesList {
        host: format!("{:?}", host.id()),
        inputs,
        outputs,
    })
}

pub fn list_audio_devices() -> Result<()> {
    let data = list_audio_devices_data()?;
    println!("Audio Host: {}", data.host);

    println!("\n[Input / Mikrofon]");
    for (idx, dev) in data.inputs.iter().enumerate() {
        let mark = if dev.is_default { " (default)" } else { "" };
        println!("  {}. {}{}", idx + 1, dev.name, mark);
    }

    println!("\n[Output / Speaker]");
    for (idx, dev) in data.outputs.iter().enumerate() {
        let mark = if dev.is_default { " (default)" } else { "" };
        println!("  {}. {}{}", idx + 1, dev.name, mark);
    }

    Ok(())
}

fn find_input_device(host: &cpal::Host, name_filter: Option<&str>) -> Result<Device> {
    if let Some(filter) = name_filter.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let needle = filter.to_ascii_lowercase();
        for dev in host.input_devices()? {
            if let Ok(name) = dev.name() {
                if name.to_ascii_lowercase().contains(&needle) {
                    return Ok(dev);
                }
            }
        }
        return Err(anyhow!("Mikrofon tidak ditemukan: '{filter}'"));
    }
    host.default_input_device()
        .context("Tidak ada perangkat mikrofon default yang aktif")
}

fn find_output_device(host: &cpal::Host, name_filter: Option<&str>) -> Result<Device> {
    if let Some(filter) = name_filter.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let needle = filter.to_ascii_lowercase();
        for dev in host.output_devices()? {
            if let Ok(name) = dev.name() {
                if name.to_ascii_lowercase().contains(&needle) {
                    return Ok(dev);
                }
            }
        }
        return Err(anyhow!("Speaker tidak ditemukan: '{filter}'"));
    }
    host.default_output_device()
        .context("Tidak ada perangkat speaker default yang aktif")
}

#[allow(clippy::too_many_arguments)]
fn init_and_run_audio(
    mic_name: Option<&str>,
    speaker_name: Option<&str>,
    mic_tx: mpsc::UnboundedSender<Vec<u8>>,
    rms_tx: mpsc::UnboundedSender<f32>,
    playback_queue: Arc<Mutex<VecDeque<i16>>>,
    muted_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    ready_tx: std::sync::mpsc::Sender<Result<(String, String)>>,
) -> Result<()> {
    let host = cpal::default_host();
    let in_dev = find_input_device(&host, mic_name)?;
    let out_dev = find_output_device(&host, speaker_name)?;

    let in_supported = in_dev
        .default_input_config()
        .context("Gagal membaca default_input_config")?;
    let out_supported = out_dev
        .default_output_config()
        .context("Gagal membaca default_output_config")?;

    let in_format = in_supported.sample_format();
    let in_cfg: StreamConfig = in_supported.into();

    let out_format = out_supported.sample_format();
    let out_cfg: StreamConfig = out_supported.into();

    let input_info = format!(
        "{} ({} Hz, {} ch, {:?})",
        in_dev.name().unwrap_or_else(|_| "Mic".to_string()),
        in_cfg.sample_rate.0,
        in_cfg.channels,
        in_format
    );
    let output_info = format!(
        "{} ({} Hz, {} ch, {:?})",
        out_dev.name().unwrap_or_else(|_| "Speaker".to_string()),
        out_cfg.sample_rate.0,
        out_cfg.channels,
        out_format
    );

    let in_stream = build_input_stream(&in_dev, &in_cfg, in_format, mic_tx, rms_tx, muted_flag)?;
    let out_stream = build_output_stream(&out_dev, &out_cfg, out_format, playback_queue)?;

    in_stream.play().context("Gagal memulai stream mikrofon")?;
    out_stream.play().context("Gagal memulai stream speaker")?;

    let _ = ready_tx.send(Ok((input_info, output_info)));

    while !stop_flag.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(40));
    }

    drop(in_stream);
    drop(out_stream);
    Ok(())
}

struct InputResampler {
    src_rate: f64,
    dst_rate: f64,
    phase: f64,
    prev_sample: f32,
    chunk_buf: Vec<i16>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
    rms_tx: mpsc::UnboundedSender<f32>,
    muted_flag: Arc<AtomicBool>,
}

impl InputResampler {
    fn new(
        src_rate: u32,
        tx: mpsc::UnboundedSender<Vec<u8>>,
        rms_tx: mpsc::UnboundedSender<f32>,
        muted_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            src_rate: src_rate as f64,
            dst_rate: ELEVENLABS_SAMPLE_RATE as f64,
            phase: 0.0,
            prev_sample: 0.0,
            chunk_buf: Vec::with_capacity(CHUNK_SAMPLES_16K * 2),
            tx,
            rms_tx,
            muted_flag,
        }
    }

    fn push_interleaved(&mut self, samples: &[f32], channels: usize) {
        let ch = channels.max(1);
        let step = self.dst_rate / self.src_rate;
        let muted = self.muted_flag.load(Ordering::Relaxed);

        for frame in samples.chunks_exact(ch) {
            let mono: f32 = frame.iter().copied().sum::<f32>() / (ch as f32);
            self.phase += step;

            while self.phase >= 1.0 {
                self.phase -= 1.0;
                let t = (1.0 - (self.phase / step.max(1e-6))).clamp(0.0, 1.0) as f32;
                let interp = self.prev_sample + (mono - self.prev_sample) * t;
                let s16 = if muted {
                    0
                } else {
                    (interp.clamp(-1.0, 1.0) * 32767.0) as i16
                };
                self.chunk_buf.push(s16);

                if self.chunk_buf.len() >= CHUNK_SAMPLES_16K {
                    let sum_sq: f64 = self
                        .chunk_buf
                        .iter()
                        .map(|&s| {
                            let f = s as f64 / 32768.0;
                            f * f
                        })
                        .sum();
                    let rms = (sum_sq / self.chunk_buf.len() as f64).sqrt() as f32;
                    let _ = self.rms_tx.send((rms * 4.5).clamp(0.0, 1.0));

                    if !muted {
                        let mut bytes = Vec::with_capacity(self.chunk_buf.len() * 2);
                        for s in self.chunk_buf.drain(..) {
                            bytes.extend_from_slice(&s.to_le_bytes());
                        }
                        let _ = self.tx.send(bytes);
                    } else {
                        self.chunk_buf.clear();
                    }
                }
            }
            self.prev_sample = mono;
        }
    }
}

fn build_input_stream(
    device: &Device,
    config: &StreamConfig,
    sample_format: SampleFormat,
    mic_tx: mpsc::UnboundedSender<Vec<u8>>,
    rms_tx: mpsc::UnboundedSender<f32>,
    muted_flag: Arc<AtomicBool>,
) -> Result<cpal::Stream> {
    let channels = config.channels as usize;
    let resampler = Arc::new(Mutex::new(InputResampler::new(
        config.sample_rate.0,
        mic_tx,
        rms_tx,
        muted_flag,
    )));
    let err_fn = |err| eprintln!("[Audio Mic Error] {err}");

    let stream = match sample_format {
        SampleFormat::F32 => {
            let r = Arc::clone(&resampler);
            device.build_input_stream(
                config,
                move |data: &[f32], _| {
                    if let Ok(mut guard) = r.lock() {
                        guard.push_interleaved(data, channels);
                    }
                },
                err_fn,
                None,
            )?
        }
        SampleFormat::I16 => {
            let r = Arc::clone(&resampler);
            device.build_input_stream(
                config,
                move |data: &[i16], _| {
                    let f32_buf: Vec<f32> = data.iter().map(|&s| s as f32 / 32768.0).collect();
                    if let Ok(mut guard) = r.lock() {
                        guard.push_interleaved(&f32_buf, channels);
                    }
                },
                err_fn,
                None,
            )?
        }
        SampleFormat::U16 => {
            let r = Arc::clone(&resampler);
            device.build_input_stream(
                config,
                move |data: &[u16], _| {
                    let f32_buf: Vec<f32> =
                        data.iter().map(|&s| (s as f32 - 32768.0) / 32768.0).collect();
                    if let Ok(mut guard) = r.lock() {
                        guard.push_interleaved(&f32_buf, channels);
                    }
                },
                err_fn,
                None,
            )?
        }
        other => return Err(anyhow!("SampleFormat input tidak didukung: {other:?}")),
    };

    Ok(stream)
}

struct OutputResampler {
    step: f64,
    phase: f64,
    prev: f32,
    curr: f32,
    queue: Arc<Mutex<VecDeque<i16>>>,
}

impl OutputResampler {
    fn new(dst_rate: u32, queue: Arc<Mutex<VecDeque<i16>>>) -> Self {
        Self {
            step: (ELEVENLABS_SAMPLE_RATE as f64) / (dst_rate.max(1) as f64),
            phase: 0.0,
            prev: 0.0,
            curr: 0.0,
            queue,
        }
    }

    fn fill_f32(&mut self, output: &mut [f32], channels: usize) {
        let ch = channels.max(1);
        let mut q = match self.queue.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        for frame in output.chunks_mut(ch) {
            while self.phase >= 1.0 {
                self.phase -= 1.0;
                self.prev = self.curr;
                self.curr = q
                    .pop_front()
                    .map(|s| (s as f32) / 32768.0)
                    .unwrap_or(0.0);
            }

            let sample = self.prev + (self.curr - self.prev) * (self.phase as f32);
            self.phase += self.step;

            for out_ch in frame.iter_mut() {
                *out_ch = sample;
            }
        }
    }
}

fn build_output_stream(
    device: &Device,
    config: &StreamConfig,
    sample_format: SampleFormat,
    queue: Arc<Mutex<VecDeque<i16>>>,
) -> Result<cpal::Stream> {
    let channels = config.channels as usize;
    let resampler = Arc::new(Mutex::new(OutputResampler::new(config.sample_rate.0, queue)));
    let err_fn = |err| eprintln!("[Audio Speaker Error] {err}");

    let stream = match sample_format {
        SampleFormat::F32 => {
            let r = Arc::clone(&resampler);
            device.build_output_stream(
                config,
                move |data: &mut [f32], _| {
                    if let Ok(mut guard) = r.lock() {
                        guard.fill_f32(data, channels);
                    }
                },
                err_fn,
                None,
            )?
        }
        SampleFormat::I16 => {
            let r = Arc::clone(&resampler);
            device.build_output_stream(
                config,
                move |data: &mut [i16], _| {
                    let mut tmp = vec![0.0f32; data.len()];
                    if let Ok(mut guard) = r.lock() {
                        guard.fill_f32(&mut tmp, channels);
                    }
                    for (dst, src) in data.iter_mut().zip(tmp.iter()) {
                        *dst = (src.clamp(-1.0, 1.0) * 32767.0) as i16;
                    }
                },
                err_fn,
                None,
            )?
        }
        SampleFormat::U16 => {
            let r = Arc::clone(&resampler);
            device.build_output_stream(
                config,
                move |data: &mut [u16], _| {
                    let mut tmp = vec![0.0f32; data.len()];
                    if let Ok(mut guard) = r.lock() {
                        guard.fill_f32(&mut tmp, channels);
                    }
                    for (dst, src) in data.iter_mut().zip(tmp.iter()) {
                        *dst = u16::from_sample(*src);
                    }
                },
                err_fn,
                None,
            )?
        }
        other => return Err(anyhow!("SampleFormat output tidak didukung: {other:?}")),
    };

    Ok(stream)
}
