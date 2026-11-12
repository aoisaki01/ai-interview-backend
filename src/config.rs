use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub elevenlabs_api_key: Option<String>,
    pub elevenlabs_agent_id: Option<String>,
    pub elevenlabs_voice_id: String,
    pub eval_llm_base_url: String,
    pub eval_llm_api_key: Option<String>,
    pub eval_llm_model: String,
    pub admin_username: String,
    pub admin_password: String,
}

impl AppConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        let env_opt = |name: &str| -> Option<String> {
            std::env::var(name)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty() && s != "your_elevenlabs_api_key_here")
        };

        Self {
            elevenlabs_api_key: env_opt("ELEVENLABS_API_KEY"),
            elevenlabs_agent_id: env_opt("ELEVENLABS_AGENT_ID"),
            elevenlabs_voice_id: env_opt("ELEVENLABS_VOICE_ID")
                .unwrap_or_else(|| "JBFqnCBsd6RMkjVDRZzb".to_string()),
            eval_llm_base_url: env_opt("EVAL_LLM_BASE_URL")
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string()),
            eval_llm_api_key: env_opt("EVAL_LLM_API_KEY"),
            eval_llm_model: env_opt("EVAL_LLM_MODEL")
                .unwrap_or_else(|| "gpt-4o-mini".to_string()),
            admin_username: env_opt("ADMIN_USERNAME").unwrap_or_else(|| "admin".to_string()),
            admin_password: env_opt("ADMIN_PASSWORD").unwrap_or_else(|| "admin".to_string()),
        }
    }

    pub fn require_elevenlabs_key(&self) -> Result<&str> {
        self.elevenlabs_api_key.as_deref().context(
            "ELEVENLABS_API_KEY belum diisi. Salin .env.example menjadi .env dan isi ELEVENLABS_API_KEY.",
        )
    }
}

pub fn upsert_env_var(key: &str, value: &str) -> Result<()> {
    let env_path = Path::new(".env");
    let content = if env_path.exists() {
        fs::read_to_string(env_path).context("Gagal membaca file .env")?
    } else if Path::new(".env.example").exists() {
        fs::read_to_string(".env.example").context("Gagal membaca .env.example")?
    } else {
        String::new()
    };

    let mut lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
    let prefix = format!("{key}=");
    let mut updated = false;

    for line in &mut lines {
        if line.trim_start().starts_with(&prefix) {
            *line = format!("{key}={value}");
            updated = true;
            break;
        }
    }

    if !updated {
        lines.push(format!("{key}={value}"));
    }

    let mut final_text = lines.join("\n");
    if !final_text.ends_with('\n') {
        final_text.push('\n');
    }
    fs::write(env_path, final_text).context("Gagal menulis file .env")?;
    Ok(())
}
