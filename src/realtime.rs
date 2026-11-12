use crate::audio::AudioEngine;
use crate::elevenlabs_api::ElevenLabsClient;
use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptTurn {
    pub speaker: String,
    pub text: String,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub struct InterviewSessionParams {
    pub agent_id: String,
    pub candidate_name: String,
    pub job_role: String,
    pub job_description: String,
    pub cv_content: String,
    pub language: String,
    pub mic_name: Option<String>,
    pub speaker_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InterviewSessionResult {
    pub conversation_id: Option<String>,
    pub transcript: Vec<TranscriptTurn>,
}

#[derive(Debug, Clone)]
pub enum RealtimeControl {
    SetMuted(bool),
    SwitchAudio {
        mic: Option<String>,
        speaker: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RealtimeEvent {
    Phase {
        phase: String,
    },
    Connected {
        conversation_id: Option<String>,
        input_format: String,
        output_format: String,
        mic: String,
        speaker: String,
    },
    MicLevel {
        rms: f32,
    },
    AgentSpeaking {
        speaking: bool,
    },
    UserTranscript {
        text: String,
        timestamp: String,
    },
    AgentResponse {
        text: String,
        timestamp: String,
    },
    AgentResponseCorrection {
        text: String,
    },
    Interruption,
    AudioConfigUpdated {
        mic: String,
        speaker: String,
        muted: bool,
    },
    Error {
        message: String,
    },
    ScorecardReady {
        report_md: String,
        report_json: String,
    },
}

pub async fn run_realtime_interview(
    api_key: &str,
    params: InterviewSessionParams,
) -> Result<InterviewSessionResult> {
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let (_ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<RealtimeControl>();

    tokio::spawn(async move {
        let mut stdin = BufReader::new(tokio::io::stdin()).lines();
        if let Ok(Some(_)) = stdin.next_line().await {
            let _ = stop_tx.send(());
        }
    });

    run_realtime_interview_with_controls(api_key, params, stop_rx, ctrl_rx, None).await
}

pub async fn run_realtime_interview_with_controls(
    api_key: &str,
    params: InterviewSessionParams,
    mut stop_rx: oneshot::Receiver<()>,
    mut control_rx: mpsc::UnboundedReceiver<RealtimeControl>,
    event_tx: Option<broadcast::Sender<RealtimeEvent>>,
) -> Result<InterviewSessionResult> {
    let emit = |ev: RealtimeEvent| {
        if let Some(ref tx) = event_tx {
            let _ = tx.send(ev);
        }
    };

    emit(RealtimeEvent::Phase {
        phase: "connecting".to_string(),
    });

    let client = ElevenLabsClient::new(api_key);
    println!("Mengambil signed WebSocket URL dari ElevenLabs...");
    let ws_url = client.get_signed_url(&params.agent_id).await?;

    println!("Menghubungkan ke ElevenLabs Conversational AI...");
    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .context("Gagal membuka koneksi WebSocket ke ElevenLabs")?;
    let (mut ws_write, mut ws_read) = ws_stream.split();

    let interview_lang_label = if params.language.to_ascii_lowercase().starts_with("id") {
        "Indonesian"
    } else {
        "English"
    };

    let init_payload = json!({
        "type": "conversation_initiation_client_data",
        "dynamic_variables": {
            "candidate_name": params.candidate_name,
            "job_role": params.job_role,
            "job_description": params.job_description,
            "cv_content": params.cv_content,
            "interview_language": interview_lang_label,
        }
    });

    ws_write
        .send(Message::Text(init_payload.to_string().into()))
        .await
        .context("Gagal mengirim conversation_initiation_client_data")?;

    let (mut audio_engine, mut mic_rx, mut rms_rx) =
        AudioEngine::start(params.mic_name.as_deref(), params.speaker_name.as_deref())?;
    let mut playback = audio_engine.playback.clone();
    let mut current_mic = params.mic_name.clone();
    let mut current_speaker = params.speaker_name.clone();
    let mut is_muted = false;

    println!("\n========================================================");
    println!("  AI INTERVIEW AKTIF (Pure Rust + ElevenLabs Real-Time)");
    println!("  Mic     : {}", audio_engine.input_info);
    println!("  Speaker : {}", audio_engine.output_info);
    println!("  Tips    : Gunakan headset agar suara AI tidak memantul ke mic.");
    println!("  Kontrol : Tekan [ENTER] atau Ctrl+C untuk akhiri & skor.");
    println!("========================================================\n");

    let mut conversation_id: Option<String> = None;
    let mut last_interrupt_id: i64 = -1;
    let mut transcript: Vec<TranscriptTurn> = Vec::new();
    let mut last_speaking_state = false;
    let mut speak_tick = tokio::time::interval(tokio::time::Duration::from_millis(120));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n[Sesi diakhiri via Ctrl+C]");
                break;
            }
            _ = &mut stop_rx => {
                println!("\n[Sesi diakhiri oleh operator/kandidat]");
                break;
            }
            _ = speak_tick.tick() => {
                let speaking = playback.buffered_samples() > 0;
                if speaking != last_speaking_state {
                    last_speaking_state = speaking;
                    emit(RealtimeEvent::AgentSpeaking { speaking });
                }
            }
            Some(ctrl) = control_rx.recv() => {
                match ctrl {
                    RealtimeControl::SetMuted(muted) => {
                        is_muted = muted;
                        audio_engine.set_muted(muted);
                        emit(RealtimeEvent::AudioConfigUpdated {
                            mic: audio_engine.input_info.clone(),
                            speaker: audio_engine.output_info.clone(),
                            muted,
                        });
                    }
                    RealtimeControl::SwitchAudio { mic, speaker } => {
                        if mic.is_some() {
                            current_mic = mic;
                        }
                        if speaker.is_some() {
                            current_speaker = speaker;
                        }
                        audio_engine.stop();
                        match AudioEngine::start(current_mic.as_deref(), current_speaker.as_deref()) {
                            Ok((new_engine, new_mic_rx, new_rms_rx)) => {
                                new_engine.set_muted(is_muted);
                                playback = new_engine.playback.clone();
                                audio_engine = new_engine;
                                mic_rx = new_mic_rx;
                                rms_rx = new_rms_rx;
                                emit(RealtimeEvent::AudioConfigUpdated {
                                    mic: audio_engine.input_info.clone(),
                                    speaker: audio_engine.output_info.clone(),
                                    muted: is_muted,
                                });
                            }
                            Err(err) => {
                                emit(RealtimeEvent::Error {
                                    message: format!("Gagal mengganti perangkat audio: {err}"),
                                });
                            }
                        }
                    }
                }
            }
            Some(rms) = rms_rx.recv() => {
                emit(RealtimeEvent::MicLevel { rms });
            }
            Some(pcm_chunk) = mic_rx.recv() => {
                let b64 = B64.encode(&pcm_chunk);
                let msg = json!({ "user_audio_chunk": b64 });
                if let Err(err) = ws_write.send(Message::Text(msg.to_string().into())).await {
                    eprintln!("[WebSocket Send Error] {err}");
                    emit(RealtimeEvent::Error {
                        message: format!("WebSocket Send Error: {err}"),
                    });
                    break;
                }
            }
            ws_msg = ws_read.next() => {
                match ws_msg {
                    Some(Ok(Message::Text(txt))) => {
                        let Ok(val) = serde_json::from_str::<Value>(&txt) else {
                            continue;
                        };
                        let kind = val.get("type").and_then(|v| v.as_str()).unwrap_or("");
                        match kind {
                            "conversation_initiation_metadata" => {
                                if let Some(ev) = val.get("conversation_initiation_metadata_event") {
                                    conversation_id = ev.get("conversation_id")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string());
                                    let in_fmt = ev.get("user_input_audio_format").and_then(|v| v.as_str()).unwrap_or("?");
                                    let out_fmt = ev.get("agent_output_audio_format").and_then(|v| v.as_str()).unwrap_or("?");
                                    println!("[Terhubung] conversation_id={} | in={} | out={}\n",
                                        conversation_id.as_deref().unwrap_or("-"), in_fmt, out_fmt);
                                    emit(RealtimeEvent::Connected {
                                        conversation_id: conversation_id.clone(),
                                        input_format: in_fmt.to_string(),
                                        output_format: out_fmt.to_string(),
                                        mic: audio_engine.input_info.clone(),
                                        speaker: audio_engine.output_info.clone(),
                                    });
                                    emit(RealtimeEvent::Phase {
                                        phase: "active".to_string(),
                                    });
                                }
                            }
                            "ping" => {
                                if let Some(event_id) = val.pointer("/ping_event/event_id").and_then(|v| v.as_i64()) {
                                    let pong = json!({ "type": "pong", "event_id": event_id });
                                    let _ = ws_write.send(Message::Text(pong.to_string().into())).await;
                                }
                            }
                            "audio" => {
                                if let Some(ev) = val.get("audio_event") {
                                    let event_id = ev.get("event_id").and_then(|v| v.as_i64()).unwrap_or(0);
                                    if event_id > last_interrupt_id {
                                        if let Some(b64) = ev.get("audio_base_64").and_then(|v| v.as_str()) {
                                            if let Ok(raw_pcm) = B64.decode(b64) {
                                                playback.push_pcm16_le(&raw_pcm);
                                            }
                                        }
                                    }
                                }
                            }
                            "interruption" => {
                                if let Some(ev) = val.get("interruption_event") {
                                    let event_id = ev.get("event_id").and_then(|v| v.as_i64()).unwrap_or(last_interrupt_id);
                                    if event_id > last_interrupt_id {
                                        last_interrupt_id = event_id;
                                    }
                                }
                                playback.clear();
                                emit(RealtimeEvent::Interruption);
                                println!("[Interupsi terdeteksi - audio AI dihentikan]");
                            }
                            "user_transcript" => {
                                if let Some(text) = val.pointer("/user_transcription_event/user_transcript").and_then(|v| v.as_str()) {
                                    let trimmed = text.trim();
                                    if !trimmed.is_empty() {
                                        let ts = Utc::now().to_rfc3339();
                                        println!("Kandidat : {trimmed}");
                                        let _ = io::stdout().flush();
                                        transcript.push(TranscriptTurn {
                                            speaker: "user".to_string(),
                                            text: trimmed.to_string(),
                                            timestamp: ts.clone(),
                                        });
                                        emit(RealtimeEvent::UserTranscript {
                                            text: trimmed.to_string(),
                                            timestamp: ts,
                                        });
                                    }
                                }
                            }
                            "agent_response" => {
                                if let Some(text) = val.pointer("/agent_response_event/agent_response").and_then(|v| v.as_str()) {
                                    let trimmed = text.trim();
                                    if !trimmed.is_empty() {
                                        let ts = Utc::now().to_rfc3339();
                                        println!("AI       : {trimmed}");
                                        let _ = io::stdout().flush();
                                        transcript.push(TranscriptTurn {
                                            speaker: "agent".to_string(),
                                            text: trimmed.to_string(),
                                            timestamp: ts.clone(),
                                        });
                                        emit(RealtimeEvent::AgentResponse {
                                            text: trimmed.to_string(),
                                            timestamp: ts,
                                        });
                                    }
                                }
                            }
                            "agent_response_correction" => {
                                if let Some(corrected) = val.pointer("/agent_response_correction_event/corrected_agent_response").and_then(|v| v.as_str()) {
                                    let trimmed = corrected.trim();
                                    if !trimmed.is_empty() {
                                        if let Some(last) = transcript.iter_mut().rev().find(|t| t.speaker == "agent") {
                                            last.text = trimmed.to_string();
                                        }
                                        println!("AI (rev) : {trimmed}");
                                        emit(RealtimeEvent::AgentResponseCorrection {
                                            text: trimmed.to_string(),
                                        });
                                    }
                                }
                            }
                            "client_error" => {
                                let msg = val.pointer("/error_event/message").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                                eprintln!("[ElevenLabs Client Error] {msg}");
                                emit(RealtimeEvent::Error {
                                    message: msg.to_string(),
                                });
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        println!("\n[Koneksi WebSocket ditutup oleh ElevenLabs: {:?}]", frame);
                        break;
                    }
                    Some(Err(err)) => {
                        eprintln!("\n[WebSocket Error] {err}");
                        emit(RealtimeEvent::Error {
                            message: format!("WebSocket Error: {err}"),
                        });
                        break;
                    }
                    None => {
                        println!("\n[Stream WebSocket selesai]");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    let _ = ws_write.close().await;
    audio_engine.stop();

    Ok(InterviewSessionResult {
        conversation_id,
        transcript,
    })
}
