mod audio;
mod config;
mod cv;
mod elevenlabs_api;
mod evaluator;
mod realtime;
mod server;
mod store;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::{upsert_env_var, AppConfig};
use elevenlabs_api::{AgentSetupOptions, ElevenLabsClient};
use realtime::{run_realtime_interview, InterviewSessionParams, TranscriptTurn};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "ai-interview",
    version,
    about = "AI Interview Real-Time berbasis Pure Rust + ElevenLabs Conversational AI"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Buat atau perbarui ElevenLabs Interviewer Agent beserta skema scoring CV & kejelasan
    Setup {
        #[arg(long, default_value = "AI Interviewer (Pure Rust)")]
        name: String,
        /// Bahasa utama agent: id atau en
        #[arg(long, default_value = "id")]
        lang: String,
        /// Voice ID ElevenLabs (kosongkan untuk memakai ELEVENLABS_VOICE_ID / default George)
        #[arg(long)]
        voice_id: Option<String>,
        /// Model TTS ElevenLabs (eleven_flash_v2_5 direkomendasikan untuk latensi rendah + multibahasa)
        #[arg(long, default_value = "eleven_flash_v2_5")]
        model_id: String,
        /// LLM percakapan di ElevenLabs Agents
        #[arg(long, default_value = "gemini-2.5-flash")]
        llm: String,
        /// Paksa buat agent baru meski ELEVENLABS_AGENT_ID sudah ada
        #[arg(long, default_value_t = false)]
        force_new: bool,
    },

    /// Daftar voice ElevenLabs yang tersedia pada akun kamu
    Voices {
        #[arg(long)]
        search: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },

    /// Daftar perangkat mikrofon dan speaker lokal yang terdeteksi
    Devices,

    /// Mulai sesi AI Interview suara real-time dengan konteks CV kandidat
    Interview {
        /// Path ke file CV (.pdf, .md, atau .txt)
        #[arg(long)]
        cv: PathBuf,
        /// Nama kandidat
        #[arg(long)]
        name: String,
        /// Posisi/role yang dilamar
        #[arg(long)]
        role: String,
        /// Path opsional ke file Job Description (.pdf, .md, .txt)
        #[arg(long)]
        jd: Option<PathBuf>,
        /// Bahasa wawancara: id atau en
        #[arg(long, default_value = "id")]
        lang: String,
        /// Override agent_id (jika tidak diisi, ambil dari ELEVENLABS_AGENT_ID di .env)
        #[arg(long)]
        agent_id: Option<String>,
        /// Filter nama perangkat mikrofon (opsional)
        #[arg(long)]
        mic: Option<String>,
        /// Filter nama perangkat speaker (opsional)
        #[arg(long)]
        speaker: Option<String>,
    },

    /// Jalankan Pure Rust API & WebSocket server untuk UI Next.js
    Serve {
        /// Port HTTP/WS lokal
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },

    /// Evaluasi & score transkrip interview yang sudah ada terhadap CV kandidat
    Evaluate {
        /// Path ke file CV (.pdf, .md, atau .txt)
        #[arg(long)]
        cv: PathBuf,
        /// Path ke file JSON transkrip lokal (opsional jika memakai --conversation-id)
        #[arg(long)]
        transcript: Option<PathBuf>,
        /// ElevenLabs conversation_id untuk ambil transkrip & analisis langsung dari API
        #[arg(long)]
        conversation_id: Option<String>,
        /// Nama kandidat
        #[arg(long)]
        name: String,
        /// Posisi/role yang dilamar
        #[arg(long)]
        role: String,
        /// Path opsional ke file Job Description
        #[arg(long)]
        jd: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = AppConfig::load();

    match cli.command {
        Commands::Devices => {
            audio::list_audio_devices()?;
        }
        Commands::Voices { search, limit } => {
            let api_key = cfg.require_elevenlabs_key()?;
            let client = ElevenLabsClient::new(api_key);
            let voices = client.list_voices().await?;
            let needle = search.map(|s| s.to_ascii_lowercase());

            println!("Daftar Voice ElevenLabs:");
            let mut shown = 0usize;
            for v in voices {
                if let Some(ref q) = needle {
                    let match_name = v.name.to_ascii_lowercase().contains(q);
                    let match_id = v.voice_id.to_ascii_lowercase().contains(q);
                    if !match_name && !match_id {
                        continue;
                    }
                }
                let cat = v.category.unwrap_or_else(|| "-".to_string());
                let lbls = v
                    .labels
                    .as_ref()
                    .map(|m| {
                        m.iter()
                            .map(|(k, val)| format!("{k}:{val}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                println!(
                    "  - {:<22} | id: {} | category: {} | {}",
                    v.name, v.voice_id, cat, lbls
                );
                shown += 1;
                if shown >= limit {
                    break;
                }
            }
            if shown == 0 {
                println!("  (Tidak ada voice yang cocok)");
            }
        }
        Commands::Setup {
            name,
            lang,
            voice_id,
            model_id,
            llm,
            force_new,
        } => {
            let api_key = cfg.require_elevenlabs_key()?;
            let client = ElevenLabsClient::new(api_key);
            let existing = if force_new {
                None
            } else {
                cfg.elevenlabs_agent_id.as_deref()
            };
            let chosen_voice = voice_id.unwrap_or_else(|| cfg.elevenlabs_voice_id.clone());

            let opts = AgentSetupOptions {
                name,
                voice_id: chosen_voice.clone(),
                tts_model_id: model_id.clone(),
                llm_model: llm.clone(),
                language: lang.clone(),
            };

            let action = if existing.is_some() { "Memperbarui" } else { "Membuat" };
            println!("{action} ElevenLabs Interviewer Agent...");
            let agent_id = client.create_or_update_agent(existing, &opts).await?;
            upsert_env_var("ELEVENLABS_AGENT_ID", &agent_id)?;

            println!("Berhasil!");
            println!("  Agent ID : {agent_id} (tersimpan ke .env)");
            println!("  Voice ID : {chosen_voice}");
            println!("  TTS Model: {model_id}");
            println!("  LLM      : {llm}");
            println!("  Bahasa   : {lang}");
        }
        Commands::Interview {
            cv: cv_path,
            name,
            role,
            jd,
            lang,
            agent_id,
            mic,
            speaker,
        } => {
            let api_key = cfg.require_elevenlabs_key()?;
            let resolved_agent_id = agent_id
                .or_else(|| cfg.elevenlabs_agent_id.clone())
                .context(
                    "ELEVENLABS_AGENT_ID belum ada. Jalankan `cargo run -- setup` terlebih dahulu atau kirim --agent-id.",
                )?;

            let cv_content = cv::load_cv_text(&cv_path)?;
            let job_description = cv::load_optional_jd(jd.as_deref())?;

            let session = run_realtime_interview(
                api_key,
                InterviewSessionParams {
                    agent_id: resolved_agent_id,
                    candidate_name: name.clone(),
                    job_role: role.clone(),
                    job_description: job_description.clone(),
                    cv_content: cv_content.clone(),
                    language: lang,
                    mic_name: mic,
                    speaker_name: speaker,
                },
            )
            .await?;

            let (scorecard, md_path, json_path) = evaluator::evaluate_and_save(
                &cfg,
                &name,
                &role,
                &job_description,
                &cv_content,
                session.conversation_id.as_deref(),
                &session.transcript,
            )
            .await?;

            evaluator::print_scorecard_terminal(&scorecard, &md_path, &json_path);
        }
        Commands::Serve { port } => {
            server::run_http_server(cfg, port).await?;
        }
        Commands::Evaluate {
            cv: cv_path,
            transcript: transcript_path,
            conversation_id,
            name,
            role,
            jd,
        } => {
            let cv_content = cv::load_cv_text(&cv_path)?;
            let job_description = cv::load_optional_jd(jd.as_deref())?;

            let local_transcript: Vec<TranscriptTurn> = match transcript_path {
                Some(path) => {
                    let raw = fs::read_to_string(&path)
                        .with_context(|| format!("Gagal membaca transkrip {}", path.display()))?;
                    if let Ok(turns) = serde_json::from_str::<Vec<TranscriptTurn>>(&raw) {
                        turns
                    } else {
                        let val: serde_json::Value = serde_json::from_str(&raw)
                            .context("Format JSON transkrip tidak valid")?;
                        if let Some(arr) = val.get("transcript") {
                            serde_json::from_value(arr.clone())
                                .context("Gagal membaca array transcript dari file JSON")?
                        } else {
                            anyhow::bail!("JSON tidak berisi array TranscriptTurn maupun field 'transcript'");
                        }
                    }
                }
                None => vec![],
            };

            let (scorecard, md_path, json_path) = evaluator::evaluate_and_save(
                &cfg,
                &name,
                &role,
                &job_description,
                &cv_content,
                conversation_id.as_deref(),
                &local_transcript,
            )
            .await?;

            evaluator::print_scorecard_terminal(&scorecard, &md_path, &json_path);
        }
    }

    Ok(())
}
