use crate::evaluator::InterviewScorecard;
use anyhow::{Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::Path;

const STORE_PATH: &str = "data/store.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobOpening {
    pub id: String,
    pub title: String,
    pub department: String,
    pub location: String,
    pub work_mode: String,
    pub employment_type: String,
    pub compensation: String,
    pub summary: String,
    pub description: String,
    #[serde(default)]
    pub required_skills: Vec<String>,
    #[serde(default)]
    pub custom_questions: Vec<String>,
    pub language: String,
    pub voice_id: String,
    pub passing_score: u8,
    pub duration_minutes: u16,
    pub status: String, // "open" | "paused" | "closed"
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CandidateProfile {
    pub id: String,
    pub full_name: String,
    pub email: String,
    pub phone: String,
    pub location: String,
    pub headline: String,
    pub years_experience: String,
    pub rate_expectation: String,
    pub availability: String,
    pub linkedin_url: String,
    pub portfolio_url: String,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub certified_skills: Vec<String>,
    pub cv_filename: String,
    pub cv_content: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationRecord {
    pub id: String,
    pub job_id: String,
    pub job_title: String,
    pub candidate_name: String,
    pub candidate_email: String,
    pub conversation_id: Option<String>,
    /// micro1 talent statuses:
    /// "interview_pending" | "under_review" | "hiring_manager_reviewing" | "selected" | "not_selected" | "did_not_qualify"
    pub status: String,
    pub report_filename: Option<String>,
    pub overall_score: Option<u8>,
    pub cv_alignment_score: Option<u8>,
    pub explanation_clarity_score: Option<u8>,
    pub technical_depth_score: Option<u8>,
    pub problem_solving_score: Option<u8>,
    pub verdict: Option<String>,
    #[serde(default)]
    pub proctoring_tab_switches: u32,
    #[serde(default)]
    pub duration_secs: u32,
    #[serde(default)]
    pub admin_notes: String,
    pub applied_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppStore {
    #[serde(default)]
    pub jobs: Vec<JobOpening>,
    pub profile: CandidateProfile,
    #[serde(default)]
    pub applications: Vec<ApplicationRecord>,
}

impl AppStore {
    pub fn load_or_init() -> Self {
        let path = Path::new(STORE_PATH);
        if path.exists() {
            if let Ok(raw) = fs::read_to_string(path) {
                if let Ok(mut store) = serde_json::from_str::<AppStore>(&raw) {
                    if store.sync_reports_into_applications() {
                        let _ = store.save();
                    }
                    return store;
                }
            }
        }

        let mut store = Self::seeded();
        store.sync_reports_into_applications();
        let _ = store.save();
        store
    }

    pub fn save(&self) -> Result<()> {
        let dir = Path::new("data");
        if !dir.exists() {
            fs::create_dir_all(dir).context("Gagal membuat folder data/")?;
        }
        let json = serde_json::to_string_pretty(self)?;
        fs::write(STORE_PATH, json).context("Gagal menyimpan data/store.json")?;
        Ok(())
    }

    fn seeded() -> Self {
        let now = Local::now().to_rfc3339();
        let jobs = vec![
            JobOpening {
                id: "job_rust_backend".to_string(),
                title: "Backend Engineer".to_string(),
                department: "Engineering".to_string(),
                location: "Worldwide Remote".to_string(),
                work_mode: "Remote".to_string(),
                employment_type: "Full-time / Contract".to_string(),
                compensation: "$65 - $105 / hr".to_string(),
                summary: "Design low-latency APIs, distributed queues, and reliable PostgreSQL persistence layers in Rust or Go.".to_string(),
                description: "We are hiring a backend engineer who can reason clearly about tail latency, connection pooling, idempotency, and production reliability. Expect deep technical probing on past systems listed on your CV.".to_string(),
                required_skills: vec![
                    "Rust / Go".to_string(),
                    "PostgreSQL".to_string(),
                    "Redis Caching".to_string(),
                    "Distributed Systems".to_string(),
                ],
                custom_questions: vec![
                    "Walk through a production bottleneck you diagnosed and fixed end to end.".to_string(),
                    "How do you prevent duplicate writes or double charges under concurrent retries?".to_string(),
                ],
                language: "id".to_string(),
                voice_id: "cjVigY5qzO86Huf0OWal".to_string(),
                passing_score: 72,
                duration_minutes: 15,
                status: "open".to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            JobOpening {
                id: "job_fullstack_next".to_string(),
                title: "Full Stack TypeScript Engineer".to_string(),
                department: "Product Engineering".to_string(),
                location: "Remote (APAC / Global)".to_string(),
                work_mode: "Remote".to_string(),
                employment_type: "Contract".to_string(),
                compensation: "$50 - $85 / hr".to_string(),
                summary: "Build real-time web interfaces in Next.js, TypeScript, and WebSockets backed by clean REST and streaming APIs.".to_string(),
                description: "You will own user-facing workflows from state modeling to API integration, with focus on responsiveness, error recovery, and clear technical communication.".to_string(),
                required_skills: vec![
                    "Next.js".to_string(),
                    "TypeScript".to_string(),
                    "WebSockets".to_string(),
                    "REST APIs".to_string(),
                ],
                custom_questions: vec![
                    "Explain how you structure real-time state updates and reconnects in a React or Next.js app.".to_string(),
                    "Describe a tricky authentication or API contract bug you resolved recently.".to_string(),
                ],
                language: "id".to_string(),
                voice_id: "XrExE9yKIg1WjnnlVkGX".to_string(),
                passing_score: 70,
                duration_minutes: 15,
                status: "open".to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            JobOpening {
                id: "job_clinical_pharmacy".to_string(),
                title: "Clinical Pharmacy AI Specialist (Apoteker)".to_string(),
                department: "Healthcare & Life Sciences".to_string(),
                location: "Remote (Indonesia)".to_string(),
                work_mode: "Remote".to_string(),
                employment_type: "Part-time Contract".to_string(),
                compensation: "$35 - $60 / hr".to_string(),
                summary: "Evaluate clinical pharmacology reasoning, drug interaction checks, and hospital dispensing protocols for frontier AI systems.".to_string(),
                description: "We partner with licensed pharmacists (Apoteker) experienced in clinical pharmacy (PIO, PTO, MESO, DRP identification, CPOB/CDOB) to author and grade high-precision medical evaluations.".to_string(),
                required_skills: vec![
                    "Farmasi Klinis (PIO / PTO / MESO)".to_string(),
                    "Identifikasi DRP".to_string(),
                    "CPOB / GMP & CDOB".to_string(),
                    "Keselamatan Pasien".to_string(),
                ],
                custom_questions: vec![
                    "Ceritakan satu kasus Drug Related Problem (DRP) nyata yang kamu identifikasi saat rotasi rumah sakit dan bagaimana tindak lanjutnya.".to_string(),
                    "Bagaimana kamu menjelaskan risiko interaksi obat kepada pasien atau dokter secara ringkas dan akurat?".to_string(),
                ],
                language: "id".to_string(),
                voice_id: "hpp4J3VqNfWAUOO0d1Us".to_string(),
                passing_score: 75,
                duration_minutes: 15,
                status: "open".to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            JobOpening {
                id: "job_ai_workflow".to_string(),
                title: "AI Workflow & Operations Architect".to_string(),
                department: "AI Operations".to_string(),
                location: "Worldwide Remote".to_string(),
                work_mode: "Remote".to_string(),
                employment_type: "Full-time".to_string(),
                compensation: "$60 - $95 / hr".to_string(),
                summary: "Design automated business operations, voice pipelines, and tool-use integrations with measurable reliability.".to_string(),
                description: "Ideal for builders who connect LLMs, webhooks, and external APIs into production workflows with strict error handling and evaluation rubrics.".to_string(),
                required_skills: vec![
                    "Tool / MCP Integration".to_string(),
                    "System Architecture".to_string(),
                    "Prompt Evaluation".to_string(),
                    "API Automation".to_string(),
                ],
                custom_questions: vec![
                    "Walk through an end-to-end automation workflow you built and how you caught silent failures.".to_string(),
                ],
                language: "en".to_string(),
                voice_id: "JBFqnCBsd6RMkjVDRZzb".to_string(),
                passing_score: 75,
                duration_minutes: 15,
                status: "open".to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        ];

        let profile = CandidateProfile {
            id: "profile_default".to_string(),
            full_name: String::new(),
            email: String::new(),
            phone: String::new(),
            location: String::new(),
            headline: String::new(),
            years_experience: String::new(),
            rate_expectation: String::new(),
            availability: "Immediate / Flexible".to_string(),
            linkedin_url: String::new(),
            portfolio_url: String::new(),
            skills: Vec::new(),
            certified_skills: Vec::new(),
            cv_filename: String::new(),
            cv_content: String::new(),
            updated_at: now,
        };

        Self {
            jobs,
            profile,
            applications: Vec::new(),
        }
    }

    pub fn sync_reports_into_applications(&mut self) -> bool {
        let dir = Path::new("reports");
        if !dir.exists() {
            return false;
        }
        let Ok(rd) = fs::read_dir(dir) else {
            return false;
        };

        let mut changed = false;
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Some(filename) = path.file_name().and_then(|n| n.to_str()).map(|s| s.to_string())
            else {
                continue;
            };
            if self
                .applications
                .iter()
                .any(|a| a.report_filename.as_deref() == Some(&filename))
            {
                continue;
            }
            let Ok(raw) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(val) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            let Some(sc_val) = val.get("scorecard") else {
                continue;
            };
            let Ok(sc) = serde_json::from_value::<InterviewScorecard>(sc_val.clone()) else {
                continue;
            };

            let matched_job_id = self
                .jobs
                .iter()
                .find(|j| j.title.eq_ignore_ascii_case(&sc.job_role))
                .map(|j| j.id.clone())
                .unwrap_or_else(|| "job_custom".to_string());

            let status = if sc.overall_score >= 72 {
                "hiring_manager_reviewing"
            } else if sc.overall_score > 0 {
                "under_review"
            } else {
                "under_review"
            };

            self.applications.push(ApplicationRecord {
                id: format!("app_{}", filename.trim_end_matches(".json")),
                job_id: matched_job_id,
                job_title: sc.job_role.clone(),
                candidate_name: sc.candidate_name.clone(),
                candidate_email: String::new(),
                conversation_id: sc.conversation_id.clone(),
                status: status.to_string(),
                report_filename: Some(filename),
                overall_score: Some(sc.overall_score),
                cv_alignment_score: Some(sc.cv_alignment_score),
                explanation_clarity_score: Some(sc.explanation_clarity_score),
                technical_depth_score: Some(sc.technical_depth_score),
                problem_solving_score: Some(sc.problem_solving_score),
                verdict: Some(sc.verdict.clone()),
                proctoring_tab_switches: 0,
                duration_secs: 0,
                admin_notes: String::new(),
                applied_at: sc.evaluated_at.clone(),
                updated_at: sc.evaluated_at.clone(),
            });
            changed = true;
        }

        if changed {
            self.applications.sort_by(|a, b| b.applied_at.cmp(&a.applied_at));
        }
        changed
    }
}

pub fn make_id(prefix: &str) -> String {
    let stamp = Local::now().format("%Y%m%d%H%M%S%3f");
    format!("{prefix}_{stamp}")
}
