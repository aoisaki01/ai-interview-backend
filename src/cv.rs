use anyhow::{bail, Context, Result};
use std::fs;
use std::path::Path;

const MAX_CV_CHARS: usize = 20_000;

pub fn load_cv_text(path: &Path) -> Result<String> {
    if !path.exists() {
        bail!("File CV tidak ditemukan: {}", path.display());
    }

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    let raw = if ext == "pdf" {
        pdf_extract::extract_text(path)
            .with_context(|| format!("Gagal mengekstrak teks dari PDF: {}", path.display()))?
    } else {
        fs::read_to_string(path)
            .with_context(|| format!("Gagal membaca file CV: {}", path.display()))?
    };

    let cleaned = normalize_whitespace(&raw);
    if cleaned.is_empty() {
        bail!("Isi CV kosong setelah dibaca dari {}", path.display());
    }

    Ok(truncate_chars(&cleaned, MAX_CV_CHARS))
}

pub fn extract_cv_from_bytes(filename: &str, bytes: &[u8]) -> Result<String> {
    let ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    let raw = if ext == "pdf" {
        pdf_extract::extract_text_from_mem(bytes)
            .with_context(|| format!("Gagal mengekstrak teks dari PDF: {filename}"))?
    } else {
        String::from_utf8(bytes.to_vec())
            .with_context(|| format!("File bukan UTF-8 teks yang valid: {filename}"))?
    };

    let cleaned = normalize_whitespace(&raw);
    if cleaned.is_empty() {
        bail!("Isi CV kosong setelah diekstrak dari {filename}");
    }

    Ok(truncate_chars(&cleaned, MAX_CV_CHARS))
}

pub fn load_optional_jd(path: Option<&Path>) -> Result<String> {
    let Some(p) = path else {
        return Ok(
            "Tidak ada Job Description terpisah. Evaluasi fokus pada kecocokan dengan role target dan klaim di CV."
                .to_string(),
        );
    };

    if !p.exists() {
        bail!("File Job Description tidak ditemukan: {}", p.display());
    }

    let ext = p
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    let raw = if ext == "pdf" {
        pdf_extract::extract_text(p)
            .with_context(|| format!("Gagal mengekstrak PDF JD: {}", p.display()))?
    } else {
        fs::read_to_string(p)
            .with_context(|| format!("Gagal membaca file JD: {}", p.display()))?
    };

    let cleaned = normalize_whitespace(&raw);
    if cleaned.is_empty() {
        Ok("Job Description kosong.".to_string())
    } else {
        Ok(truncate_chars(&cleaned, 10_000))
    }
}

fn normalize_whitespace(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut empty_run = 0usize;

    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            empty_run += 1;
            if empty_run <= 1 && !out.is_empty() {
                out.push('\n');
            }
        } else {
            empty_run = 0;
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(trimmed);
        }
    }
    out
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let truncated: String = text.chars().take(max_chars).collect();
    format!("{truncated}\n\n[... CV dipotong karena melebihi batas karakter ...]")
}
