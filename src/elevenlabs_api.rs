use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

const BASE_URL: &str = "https://api.elevenlabs.io/v1";

#[derive(Debug, Clone)]
pub struct AgentSetupOptions {
    pub name: String,
    pub voice_id: String,
    pub tts_model_id: String,
    pub llm_model: String,
    pub language: String,
}

#[derive(Debug, Deserialize)]
pub struct VoicesResponse {
    pub voices: Vec<VoiceInfo>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct VoiceInfo {
    pub voice_id: String,
    pub name: String,
    pub category: Option<String>,
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct CreateAgentResponse {
    agent_id: String,
}

#[derive(Debug, Deserialize)]
struct SignedUrlResponse {
    signed_url: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConversationDetails {
    pub conversation_id: String,
    pub agent_id: Option<String>,
    pub status: Option<String>,
    #[serde(default)]
    pub transcript: Vec<ConversationTurnRaw>,
    pub analysis: Option<ConversationAnalysis>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConversationTurnRaw {
    pub role: String,
    pub message: Option<String>,
    pub time_in_call_secs: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConversationAnalysis {
    pub transcript_summary: Option<String>,
    pub call_successful: Option<String>,
    #[serde(default)]
    pub data_collection_results: HashMap<String, DataCollectionResultItem>,
    #[serde(default)]
    pub evaluation_criteria_results: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataCollectionResultItem {
    pub data_collection_id: Option<String>,
    pub value: Option<Value>,
    pub rationale: Option<String>,
}

pub struct ElevenLabsClient {
    http: Client,
    api_key: String,
}

impl ElevenLabsClient {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            http: Client::new(),
            api_key: api_key.into(),
        }
    }

    pub async fn list_voices(&self) -> Result<Vec<VoiceInfo>> {
        let resp = self
            .http
            .get(format!("{BASE_URL}/voices"))
            .header("xi-api-key", &self.api_key)
            .send()
            .await
            .context("Gagal menghubungi ElevenLabs /v1/voices")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("ElevenLabs list_voices error ({status}): {body}");
        }

        let parsed: VoicesResponse = resp
            .json()
            .await
            .context("Gagal parsing respons /v1/voices")?;
        Ok(parsed.voices)
    }

    pub async fn create_or_update_agent(
        &self,
        existing_agent_id: Option<&str>,
        opts: &AgentSetupOptions,
    ) -> Result<String> {
        let payload = build_interviewer_agent_payload(opts);

        let (method, url) = match existing_agent_id {
            Some(id) if !id.trim().is_empty() => (
                reqwest::Method::PATCH,
                format!("{BASE_URL}/convai/agents/{id}"),
            ),
            _ => (
                reqwest::Method::POST,
                format!("{BASE_URL}/convai/agents/create"),
            ),
        };

        let resp = self
            .http
            .request(method, &url)
            .header("xi-api-key", &self.api_key)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Gagal request ke {url}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("ElevenLabs create/update agent gagal ({status}): {body}");
        }

        let parsed: CreateAgentResponse = resp
            .json()
            .await
            .context("Gagal parsing agent_id dari respons ElevenLabs")?;
        Ok(parsed.agent_id)
    }

    pub async fn get_signed_url(&self, agent_id: &str) -> Result<String> {
        let url = format!(
            "{BASE_URL}/convai/conversation/get-signed-url?agent_id={}",
            urlencoding::encode(agent_id)
        );

        let resp = self
            .http
            .get(&url)
            .header("xi-api-key", &self.api_key)
            .send()
            .await
            .context("Gagal mengambil signed URL ElevenLabs")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("ElevenLabs get_signed_url gagal ({status}): {body}");
        }

        let parsed: SignedUrlResponse = resp
            .json()
            .await
            .context("Gagal parsing signed_url dari ElevenLabs")?;
        Ok(parsed.signed_url)
    }

    pub async fn get_conversation(&self, conversation_id: &str) -> Result<ConversationDetails> {
        let url = format!(
            "{BASE_URL}/convai/conversations/{}",
            urlencoding::encode(conversation_id)
        );

        let resp = self
            .http
            .get(&url)
            .header("xi-api-key", &self.api_key)
            .send()
            .await
            .context("Gagal mengambil detail conversation dari ElevenLabs")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("ElevenLabs get_conversation gagal ({status}): {body}");
        }

        let parsed: ConversationDetails = resp
            .json()
            .await
            .context("Gagal parsing ConversationDetails dari ElevenLabs")?;
        Ok(parsed)
    }
}

fn build_interviewer_agent_payload(opts: &AgentSetupOptions) -> Value {
    let is_id = opts.language.to_ascii_lowercase().starts_with("id");

    let first_message = if is_id {
        "Halo {{candidate_name}}, terima kasih sudah hadir hari ini. Saya pewawancara AI untuk posisi {{job_role}}. Saya sudah membaca CV kamu. Sebelum kita mulai menggali pengalamanmu, apakah suara saya terdengar jelas dan kamu siap untuk memulai?"
    } else {
        "Hi {{candidate_name}}, thanks for joining today. I am your AI interviewer for the {{job_role}} role, and I have reviewed your CV. Before we dive into your experience, can you hear me clearly and are you ready to begin?"
    };

    let system_prompt = r#"You are a sharp, empathetic, human-like technical and behavioral interviewer conducting a live voice interview.

LANGUAGE INSTRUCTION:
- Conduct the entire interview in {{interview_language}} (unless the candidate explicitly requests switching).
- Speak naturally like a real senior interviewer in conversation: concise turns (2 to 4 sentences per turn), warm, attentive, never reading long bullet lists aloud.

CANDIDATE CONTEXT:
- Candidate Name: {{candidate_name}}
- Target Role: {{job_role}}
- Job Description / Focus: {{job_description}}
- Candidate CV:
{{cv_content}}

INTERVIEW STRATEGY:
1. Warm up briefly, then verify specific claims on the candidate's CV (projects, metrics, technologies, ownership, timelines).
2. Always probe depth and clarity: if the candidate gives a vague or high-level answer, ask a pointed follow-up ("Bagian mana yang kamu kerjakan sendiri?", "Bagaimana cara kerjanya secara teknis?", "Bisa kasih contoh konkret hasilnya?").
3. Assess both technical/role competence AND how clearly and structurally they explain complex topics.
4. Ask one clear question at a time. Wait patiently if the candidate pauses to think.
5. After covering 4 to 6 substantive areas (background alignment, core technical/role depth, problem-solving/trade-offs, collaboration/ownership, and candidate questions), thank the candidate warmly and close the interview cleanly."#;

    json!({
        "name": opts.name,
        "conversation_config": {
            "asr": {
                "quality": "high",
                "provider": "scribe_realtime",
                "user_input_audio_format": "pcm_16000"
            },
            "turn": {
                "turn_timeout": 12.0,
                "turn_eagerness": "patient",
                "silence_end_call_timeout": 45.0,
                "spelling_patience": "auto"
            },
            "tts": {
                "model_id": opts.tts_model_id,
                "voice_id": opts.voice_id,
                "agent_output_audio_format": "pcm_16000",
                "stability": 0.5,
                "similarity_boost": 0.8,
                "speed": 1.0
            },
            "conversation": {
                "max_duration_seconds": 1800
            },
            "agent": {
                "first_message": first_message,
                "language": opts.language,
                "dynamic_variables": {
                    "dynamic_variable_placeholders": {
                        "candidate_name": "Kandidat",
                        "job_role": "Software Engineer",
                        "job_description": "Evaluasi kecocokan role dan kejelasan komunikasi.",
                        "cv_content": "Belum ada CV dilampirkan.",
                        "interview_language": if is_id { "Indonesian" } else { "English" }
                    }
                },
                "prompt": {
                    "prompt": system_prompt,
                    "llm": opts.llm_model
                }
            }
        },
        "platform_settings": {
            "summary_language": opts.language,
            "data_collection": {
                "cv_alignment_score": {
                    "type": "integer",
                    "description": "Score 0-100 measuring how well the candidate's spoken answers verified and matched the experience, projects, and skills claimed in their CV."
                },
                "explanation_clarity_score": {
                    "type": "integer",
                    "description": "Score 0-100 measuring how clearly, structurally, logically, and concisely the candidate explained their ideas and answers."
                },
                "technical_depth_score": {
                    "type": "integer",
                    "description": "Score 0-100 measuring depth of technical or domain mastery demonstrated during the interview."
                },
                "problem_solving_score": {
                    "type": "integer",
                    "description": "Score 0-100 measuring reasoning quality, trade-off awareness, and concrete problem solving."
                },
                "overall_score": {
                    "type": "integer",
                    "description": "Overall weighted interview performance score 0-100."
                },
                "cv_performance_summary": {
                    "type": "string",
                    "description": "Detailed summary comparing the candidate's interview performance against their CV: what claims were strongly proven vs weak or unverified."
                },
                "explanation_clarity_assessment": {
                    "type": "string",
                    "description": "Assessment of whether the candidate explained clearly or vaguely/rambling, including structure (STAR/cause-effect) and communication effectiveness."
                },
                "strengths": {
                    "type": "string",
                    "description": "Key strengths observed during the interview."
                },
                "gaps_and_red_flags": {
                    "type": "string",
                    "description": "Gaps, weak explanations, or inconsistencies between CV and answers."
                },
                "verdict": {
                    "type": "string",
                    "enum": ["Strong Hire", "Hire", "Consider", "No Hire"],
                    "description": "Final hiring verdict based on CV alignment, clarity, and competence."
                }
            }
        }
    })
}
