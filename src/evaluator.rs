use crate::config::AppConfig;
use crate::elevenlabs_api::{ConversationAnalysis, ConversationDetails, ElevenLabsClient};
use crate::realtime::TranscriptTurn;
use anyhow::{bail, Context, Result};
use chrono::Local;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterviewScorecard {
    pub candidate_name: String,
    pub job_role: String,
    pub conversation_id: Option<String>,
    pub evaluated_at: String,
    pub evaluator_source: String,
    pub cv_alignment_score: u8,
    pub explanation_clarity_score: u8,
    pub technical_depth_score: u8,
    pub problem_solving_score: u8,
    pub overall_score: u8,
    pub verdict: String,
    pub cv_based_summary: String,
    pub explanation_clarity_assessment: String,
    pub technical_and_reasoning_notes: String,
    pub strengths: Vec<String>,
    pub gaps_and_red_flags: Vec<String>,
    pub evidence_quotes: Vec<String>,
    pub recommended_followups: Vec<String>,
    pub elevenlabs_transcript_summary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LlmEvalRaw {
    cv_alignment_score: f64,
    explanation_clarity_score: f64,
    technical_depth_score: f64,
    problem_solving_score: f64,
    verdict: Option<String>,
    cv_based_summary: String,
    explanation_clarity_assessment: String,
    technical_and_reasoning_notes: String,
    #[serde(default)]
    strengths: Vec<String>,
    #[serde(default)]
    gaps_and_red_flags: Vec<String>,
    #[serde(default)]
    evidence_quotes: Vec<String>,
    #[serde(default)]
    recommended_followups: Vec<String>,
}

pub async fn evaluate_and_save(
    cfg: &AppConfig,
    candidate_name: &str,
    job_role: &str,
    job_description: &str,
    cv_content: &str,
    conversation_id: Option<&str>,
    local_transcript: &[TranscriptTurn],
) -> Result<(InterviewScorecard, PathBuf, PathBuf)> {
    let mut final_transcript = local_transcript.to_vec();
    let mut el_details: Option<ConversationDetails> = None;

    if let (Some(conv_id), Some(el_key)) = (conversation_id, cfg.elevenlabs_api_key.as_deref()) {
        println!("\nMengambil transkrip & analisis resmi dari ElevenLabs ({conv_id})...");
        let el = ElevenLabsClient::new(el_key);
        el_details = poll_elevenlabs_conversation(&el, conv_id).await;
        if let Some(ref details) = el_details {
            if !details.transcript.is_empty() {
                final_transcript = details
                    .transcript
                    .iter()
                    .filter_map(|t| {
                        let msg = t.message.as_deref()?.trim();
                        if msg.is_empty() {
                            return None;
                        }
                        Some(TranscriptTurn {
                            speaker: t.role.clone(),
                            text: msg.to_string(),
                            timestamp: t
                                .time_in_call_secs
                                .map(|s| format!("{s:.1}s"))
                                .unwrap_or_default(),
                        })
                    })
                    .collect();
            }
        }
    }

    if final_transcript.is_empty() {
        bail!("Transkrip interview kosong, tidak ada percakapan untuk dinilai.");
    }

    let formatted_transcript = format_transcript(&final_transcript);

    let scorecard = if let Some(llm_key) = cfg.eval_llm_api_key.as_deref() {
        println!(
            "Menjalankan Independent AI Scoring ({}) berdasarkan CV & kejelasan penjelasan...",
            cfg.eval_llm_model
        );
        match run_independent_llm_eval(
            &cfg.eval_llm_base_url,
            llm_key,
            &cfg.eval_llm_model,
            candidate_name,
            job_role,
            job_description,
            cv_content,
            &formatted_transcript,
            conversation_id,
            el_details.as_ref().and_then(|d| d.analysis.as_ref()),
        )
        .await
        {
            Ok(sc) => sc,
            Err(err) => {
                eprintln!("[Peringatan] Independent LLM eval gagal ({err}), mencoba fallback ElevenLabs/lokal...");
                build_fallback_scorecard(
                    candidate_name,
                    job_role,
                    conversation_id,
                    &final_transcript,
                    el_details.as_ref().and_then(|d| d.analysis.as_ref()),
                )
            }
        }
    } else {
        build_fallback_scorecard(
            candidate_name,
            job_role,
            conversation_id,
            &final_transcript,
            el_details.as_ref().and_then(|d| d.analysis.as_ref()),
        )
    };

    let (md_path, json_path) = save_reports(&scorecard, &final_transcript, cv_content)?;
    Ok((scorecard, md_path, json_path))
}

async fn poll_elevenlabs_conversation(
    client: &ElevenLabsClient,
    conversation_id: &str,
) -> Option<ConversationDetails> {
    for attempt in 1..=10 {
        match client.get_conversation(conversation_id).await {
            Ok(details) => {
                let analysis_ready = details
                    .analysis
                    .as_ref()
                    .map(|a| {
                        a.transcript_summary.is_some() || !a.data_collection_results.is_empty()
                    })
                    .unwrap_or(false);
                if analysis_ready || attempt == 10 {
                    return Some(details);
                }
            }
            Err(err) => {
                if attempt == 10 {
                    eprintln!("[Peringatan] Gagal mengambil conversation ElevenLabs: {err}");
                    return None;
                }
            }
        }
        sleep(Duration::from_secs(3)).await;
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn run_independent_llm_eval(
    base_url: &str,
    api_key: &str,
    model: &str,
    candidate_name: &str,
    job_role: &str,
    job_description: &str,
    cv_content: &str,
    transcript: &str,
    conversation_id: Option<&str>,
    el_analysis: Option<&ConversationAnalysis>,
) -> Result<InterviewScorecard> {
    let endpoint = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let http = Client::new();

    let system_prompt = r#"Kamu adalah Lead Technical & HR Interview Assessor. Tugasmu menilai hasil interview kandidat secara objektif, tajam, dan berbasis bukti.
Balas HANYA dalam JSON valid (tanpa markdown fence) dengan struktur persis berikut:
{
  "cv_alignment_score": 0-100,
  "explanation_clarity_score": 0-100,
  "technical_depth_score": 0-100,
  "problem_solving_score": 0-100,
  "verdict": "Strong Hire" | "Hire" | "Consider" | "No Hire",
  "cv_based_summary": "Ringkasan performa interview dibandingkan dengan klaim di CV (apa yang terbukti kuat vs lemah/tidak sinkron)",
  "explanation_clarity_assessment": "Penilaian apakah cara kandidat menjelaskan jelas, terstruktur, runtut, konkret, atau justru bertele-tele/kabur",
  "technical_and_reasoning_notes": "Penilaian kedalaman teknis dan logika pemecahan masalah",
  "strengths": ["poin kuat 1", "..."],
  "gaps_and_red_flags": ["celah atau hal meragukan 1", "..."],
  "evidence_quotes": ["kutipan penting dari jawaban kandidat", "..."],
  "recommended_followups": ["pertanyaan lanjutan jika lanjut tahap berikutnya", "..."]
}"#;

    let user_prompt = format!(
        "KANDIDAT: {candidate_name}\nROLE TARGET: {job_role}\n\nJOB DESCRIPTION:\n{job_description}\n\nCV KANDIDAT:\n{cv_content}\n\nTRANSKRIP INTERVIEW:\n{transcript}"
    );

    let body = json!({
        "model": model,
        "temperature": 0.2,
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": user_prompt }
        ]
    });

    let resp = http
        .post(&endpoint)
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("Gagal memanggil evaluator LLM di {endpoint}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let err_txt = resp.text().await.unwrap_or_default();
        bail!("Evaluator LLM error ({status}): {err_txt}");
    }

    let resp_json: Value = resp.json().await.context("Respons LLM bukan JSON")?;
    let content = resp_json
        .pointer("/choices/0/message/content")
        .and_then(|v| v.as_str())
        .context("Field choices[0].message.content tidak ditemukan")?;

    let cleaned_json = strip_json_fence(content);
    let parsed: LlmEvalRaw = serde_json::from_str(cleaned_json)
        .with_context(|| format!("Gagal parse JSON hasil evaluasi LLM: {cleaned_json}"))?;

    let cv_score = clamp_u8(parsed.cv_alignment_score);
    let clarity_score = clamp_u8(parsed.explanation_clarity_score);
    let tech_score = clamp_u8(parsed.technical_depth_score);
    let problem_score = clamp_u8(parsed.problem_solving_score);

    // Weighted independent overall score computed deterministically in Rust:
    // 30% CV alignment + 30% explanation clarity + 25% technical depth + 15% problem solving
    let overall = compute_weighted_score(cv_score, clarity_score, tech_score, problem_score);
    let verdict = parsed
        .verdict
        .unwrap_or_else(|| verdict_from_score(overall).to_string());

    Ok(InterviewScorecard {
        candidate_name: candidate_name.to_string(),
        job_role: job_role.to_string(),
        conversation_id: conversation_id.map(|s| s.to_string()),
        evaluated_at: Local::now().to_rfc3339(),
        evaluator_source: format!("Independent Rust LLM Scorer ({model})"),
        cv_alignment_score: cv_score,
        explanation_clarity_score: clarity_score,
        technical_depth_score: tech_score,
        problem_solving_score: problem_score,
        overall_score: overall,
        verdict,
        cv_based_summary: parsed.cv_based_summary,
        explanation_clarity_assessment: parsed.explanation_clarity_assessment,
        technical_and_reasoning_notes: parsed.technical_and_reasoning_notes,
        strengths: parsed.strengths,
        gaps_and_red_flags: parsed.gaps_and_red_flags,
        evidence_quotes: parsed.evidence_quotes,
        recommended_followups: parsed.recommended_followups,
        elevenlabs_transcript_summary: el_analysis.and_then(|a| a.transcript_summary.clone()),
    })
}

fn build_fallback_scorecard(
    candidate_name: &str,
    job_role: &str,
    conversation_id: Option<&str>,
    transcript: &[TranscriptTurn],
    el_analysis: Option<&ConversationAnalysis>,
) -> InterviewScorecard {
    if let Some(analysis) = el_analysis {
        let get_int = |key: &str, default_val: u8| -> u8 {
            analysis
                .data_collection_results
                .get(key)
                .and_then(|item| item.value.as_ref())
                .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
                .map(clamp_u8)
                .unwrap_or(default_val)
        };
        let get_str = |key: &str| -> Option<String> {
            analysis
                .data_collection_results
                .get(key)
                .and_then(|item| item.value.as_ref())
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };

        if !analysis.data_collection_results.is_empty() {
            let cv_score = get_int("cv_alignment_score", 70);
            let clarity_score = get_int("explanation_clarity_score", 70);
            let tech_score = get_int("technical_depth_score", 70);
            let problem_score = get_int("problem_solving_score", 70);
            let overall = compute_weighted_score(cv_score, clarity_score, tech_score, problem_score);

            let verdict = get_str("verdict")
                .unwrap_or_else(|| verdict_from_score(overall).to_string());

            return InterviewScorecard {
                candidate_name: candidate_name.to_string(),
                job_role: job_role.to_string(),
                conversation_id: conversation_id.map(|s| s.to_string()),
                evaluated_at: Local::now().to_rfc3339(),
                evaluator_source: "ElevenLabs Built-in Post-Call Analysis".to_string(),
                cv_alignment_score: cv_score,
                explanation_clarity_score: clarity_score,
                technical_depth_score: tech_score,
                problem_solving_score: problem_score,
                overall_score: overall,
                verdict,
                cv_based_summary: get_str("cv_performance_summary")
                    .or_else(|| analysis.transcript_summary.clone())
                    .unwrap_or_else(|| "Ringkasan otomatis ElevenLabs belum tersedia.".to_string()),
                explanation_clarity_assessment: get_str("explanation_clarity_assessment")
                    .unwrap_or_else(|| "Evaluasi kejelasan penjelasan tersedia di ElevenLabs data collection.".to_string()),
                technical_and_reasoning_notes: "Dinilai melalui post-call analysis ElevenLabs.".to_string(),
                strengths: get_str("strengths")
                    .map(|s| vec![s])
                    .unwrap_or_default(),
                gaps_and_red_flags: get_str("gaps_and_red_flags")
                    .map(|s| vec![s])
                    .unwrap_or_default(),
                evidence_quotes: vec![],
                recommended_followups: vec![],
                elevenlabs_transcript_summary: analysis.transcript_summary.clone(),
            };
        }
    }

    // Pure offline heuristic fallback when neither EVAL_LLM_API_KEY nor ElevenLabs analysis exists
    let user_turns: Vec<&TranscriptTurn> = transcript
        .iter()
        .filter(|t| t.speaker.eq_ignore_ascii_case("user") || t.speaker.eq_ignore_ascii_case("candidate"))
        .collect();

    let total_words: usize = user_turns
        .iter()
        .map(|t| t.text.split_whitespace().count())
        .sum();
    let avg_words = if user_turns.is_empty() {
        0
    } else {
        total_words / user_turns.len()
    };

    let clarity_est = match avg_words {
        0..=10 => 45,
        11..=25 => 65,
        26..=90 => 78,
        _ => 72,
    };

    InterviewScorecard {
        candidate_name: candidate_name.to_string(),
        job_role: job_role.to_string(),
        conversation_id: conversation_id.map(|s| s.to_string()),
        evaluated_at: Local::now().to_rfc3339(),
        evaluator_source: "Local Heuristic Fallback (set EVAL_LLM_API_KEY untuk penilaian AI penuh)".to_string(),
        cv_alignment_score: 0,
        explanation_clarity_score: clarity_est,
        technical_depth_score: 0,
        problem_solving_score: 0,
        overall_score: 0,
        verdict: "Pending LLM Review".to_string(),
        cv_based_summary: format!(
            "Transkrip tersimpan ({} giliran kandidat, {} kata total). Set EVAL_LLM_API_KEY di .env atau jalankan interview live dengan ElevenLabs untuk analisis semantik CV vs jawaban.",
            user_turns.len(),
            total_words
        ),
        explanation_clarity_assessment: format!(
            "Rata-rata panjang jawaban kandidat: {} kata per giliran. Untuk penilaian kejelasan semantik (STAR, struktur argumen, dan akurasi istilah), aktifkan EVAL_LLM_API_KEY.",
            avg_words
        ),
        technical_and_reasoning_notes: "Menunggu evaluasi LLM.".to_string(),
        strengths: vec![],
        gaps_and_red_flags: vec!["EVAL_LLM_API_KEY belum dikonfigurasi dan ElevenLabs analysis tidak tersedia.".to_string()],
        evidence_quotes: user_turns.iter().take(3).map(|t| t.text.clone()).collect(),
        recommended_followups: vec![],
        elevenlabs_transcript_summary: None,
    }
}

fn save_reports(
    scorecard: &InterviewScorecard,
    transcript: &[TranscriptTurn],
    cv_content: &str,
) -> Result<(PathBuf, PathBuf)> {
    let reports_dir = Path::new("reports");
    fs::create_dir_all(reports_dir).context("Gagal membuat folder reports/")?;

    let stamp = Local::now().format("%Y%m%d_%H%M%S");
    let slug = sanitize_filename(&scorecard.candidate_name);
    let base = format!("{stamp}_{slug}");

    let json_path = reports_dir.join(format!("{base}.json"));
    let md_path = reports_dir.join(format!("{base}.md"));

    let full_json = json!({
        "scorecard": scorecard,
        "transcript": transcript,
        "cv_excerpt": cv_content.chars().take(1500).collect::<String>(),
    });
    fs::write(&json_path, serde_json::to_string_pretty(&full_json)?)
        .with_context(|| format!("Gagal menulis {}", json_path.display()))?;

    let md = render_markdown_report(scorecard, transcript);
    fs::write(&md_path, md)
        .with_context(|| format!("Gagal menulis {}", md_path.display()))?;

    Ok((md_path, json_path))
}

pub fn render_markdown_report(
    sc: &InterviewScorecard,
    transcript: &[TranscriptTurn],
) -> String {
    let mut out = String::new();
    out.push_str(&format!("# Laporan Evaluasi AI Interview: {}\n\n", sc.candidate_name));
    out.push_str(&format!("- **Role Target:** {}\n", sc.job_role));
    out.push_str(&format!("- **Tanggal Evaluasi:** {}\n", sc.evaluated_at));
    out.push_str(&format!("- **Sumber Penilai:** {}\n", sc.evaluator_source));
    if let Some(ref cid) = sc.conversation_id {
        out.push_str(&format!("- **ElevenLabs Conversation ID:** `{cid}`\n"));
    }
    out.push_str(&format!("- **Rekomendasi Akhir:** **{}**\n\n", sc.verdict));

    out.push_str("## Rincian Skor (Self-Scored 0 - 100)\n\n");
    out.push_str("| Dimensi Penilaian | Bobot | Skor |\n");
    out.push_str("|---|---:|---:|\n");
    out.push_str(&format!("| Kesesuaian & Pembuktian CV | 30% | **{}** |\n", sc.cv_alignment_score));
    out.push_str(&format!("| Kejelasan Penjelasan & Komunikasi | 30% | **{}** |\n", sc.explanation_clarity_score));
    out.push_str(&format!("| Kedalaman Teknis / Domain | 25% | **{}** |\n", sc.technical_depth_score));
    out.push_str(&format!("| Problem Solving & Penalaran | 15% | **{}** |\n", sc.problem_solving_score));
    out.push_str(&format!("| **Skor Akhir Berbobot** | **100%** | **{} / 100** |\n\n", sc.overall_score));

    out.push_str("## Ringkasan Performa Berdasarkan CV\n");
    out.push_str(&format!("{}\n\n", sc.cv_based_summary));

    out.push_str("## Penilaian Kejelasan Cara Menjelaskan\n");
    out.push_str(&format!("{}\n\n", sc.explanation_clarity_assessment));

    out.push_str("## Catatan Teknis & Penalaran\n");
    out.push_str(&format!("{}\n\n", sc.technical_and_reasoning_notes));

    if !sc.strengths.is_empty() {
        out.push_str("## Poin Kuat\n");
        for item in &sc.strengths {
            out.push_str(&format!("- {item}\n"));
        }
        out.push('\n');
    }

    if !sc.gaps_and_red_flags.is_empty() {
        out.push_str("## Celah / Hal yang Perlu Diwaspadai\n");
        for item in &sc.gaps_and_red_flags {
            out.push_str(&format!("- {item}\n"));
        }
        out.push('\n');
    }

    if !sc.evidence_quotes.is_empty() {
        out.push_str("## Kutipan Bukti Jawaban Kandidat\n");
        for q in &sc.evidence_quotes {
            out.push_str(&format!("> \"{q}\"\n\n"));
        }
    }

    if !sc.recommended_followups.is_empty() {
        out.push_str("## Rekomendasi Pertanyaan Lanjutan\n");
        for f in &sc.recommended_followups {
            out.push_str(&format!("- {f}\n"));
        }
        out.push('\n');
    }

    if let Some(ref el_sum) = sc.elevenlabs_transcript_summary {
        out.push_str("## Ringkasan ElevenLabs\n");
        out.push_str(&format!("{el_sum}\n\n"));
    }

    out.push_str("## Transkrip Lengkap\n\n");
    for turn in transcript {
        let label = if turn.speaker == "agent" { "AI" } else { "Kandidat" };
        out.push_str(&format!("**[{label}]** {}\n\n", turn.text));
    }

    out
}

pub fn print_scorecard_terminal(sc: &InterviewScorecard, md_path: &Path, json_path: &Path) {
    println!("\n==================== HASIL EVALUASI INTERVIEW ====================");
    println!("Kandidat        : {} ({})", sc.candidate_name, sc.job_role);
    println!("Penilai         : {}", sc.evaluator_source);
    println!("Rekomendasi     : {} (Overall: {}/100)", sc.verdict, sc.overall_score);
    println!("------------------------------------------------------------------");
    println!("  1. Kesesuaian terhadap CV      (30%): {}/100", sc.cv_alignment_score);
    println!("  2. Kejelasan Cara Menjelaskan  (30%): {}/100", sc.explanation_clarity_score);
    println!("  3. Kedalaman Teknis / Domain   (25%): {}/100", sc.technical_depth_score);
    println!("  4. Problem Solving & Penalaran (15%): {}/100", sc.problem_solving_score);
    println!("------------------------------------------------------------------");
    println!("\n[Ringkasan Berbasis CV]\n{}", sc.cv_based_summary);
    println!("\n[Kejelasan Penjelasan]\n{}", sc.explanation_clarity_assessment);
    println!("\nLaporan tersimpan:");
    println!("  Markdown : {}", md_path.display());
    println!("  JSON     : {}", json_path.display());
    println!("==================================================================\n");
}

fn format_transcript(turns: &[TranscriptTurn]) -> String {
    turns
        .iter()
        .map(|t| {
            let role = if t.speaker == "agent" { "Interviewer" } else { "Kandidat" };
            format!("{role}: {}", t.text)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn strip_json_fence(raw: &str) -> &str {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        rest.trim().strip_suffix("```").unwrap_or(rest).trim()
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        rest.trim().strip_suffix("```").unwrap_or(rest).trim()
    } else {
        trimmed
    }
}

fn clamp_u8(val: f64) -> u8 {
    val.round().clamp(0.0, 100.0) as u8
}

fn compute_weighted_score(cv: u8, clarity: u8, tech: u8, problem: u8) -> u8 {
    let total = (cv as f64) * 0.30
        + (clarity as f64) * 0.30
        + (tech as f64) * 0.25
        + (problem as f64) * 0.15;
    clamp_u8(total)
}

fn verdict_from_score(score: u8) -> &'static str {
    match score {
        85..=100 => "Strong Hire",
        72..=84 => "Hire",
        55..=71 => "Consider",
        _ => "No Hire",
    }
}

fn sanitize_filename(input: &str) -> String {
    let cleaned: String = input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let trimmed = cleaned.trim_matches('_');
    if trimmed.is_empty() {
        "kandidat".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weighted_score_math() {
        assert_eq!(compute_weighted_score(100, 100, 100, 100), 100);
        assert_eq!(compute_weighted_score(80, 90, 70, 60), 78);
        assert_eq!(verdict_from_score(88), "Strong Hire");
        assert_eq!(verdict_from_score(78), "Hire");
        assert_eq!(verdict_from_score(60), "Consider");
        assert_eq!(verdict_from_score(40), "No Hire");
    }

    #[test]
    fn test_strip_json_fence() {
        let wrapped = "```json\n{\"a\": 1}\n```";
        assert_eq!(strip_json_fence(wrapped), "{\"a\": 1}");
    }
}
