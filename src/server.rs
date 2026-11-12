use crate::audio::{list_audio_devices_data, test_speaker_tone, MicMeterHandle};
use crate::config::AppConfig;
use crate::cv::extract_cv_from_bytes;
use crate::elevenlabs_api::{AgentSetupOptions, ElevenLabsClient};
use crate::evaluator::{evaluate_and_save, InterviewScorecard};
use crate::realtime::{
    run_realtime_interview_with_controls, InterviewSessionParams, RealtimeControl, RealtimeEvent,
    TranscriptTurn,
};
use crate::store::{make_id, AppStore, ApplicationRecord, CandidateProfile, JobOpening};
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Multipart, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderName, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::Local;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::fs;
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tower_http::cors::{Any, CorsLayer};

static TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

pub struct ServerState {
    pub cfg: AppConfig,
    pub event_tx: broadcast::Sender<RealtimeEvent>,
    pub session: Mutex<ActiveSessionState>,
    pub store: Mutex<AppStore>,
    pub admin_tokens: Mutex<HashSet<String>>,
}

#[derive(Default)]
pub struct ActiveSessionState {
    pub phase: String,
    pub application_id: Option<String>,
    pub job_id: Option<String>,
    pub conversation_id: Option<String>,
    pub candidate_name: Option<String>,
    pub job_role: Option<String>,
    pub mic: Option<String>,
    pub speaker: Option<String>,
    pub muted: bool,
    pub transcript: Vec<TranscriptTurn>,
    pub scorecard: Option<InterviewScorecard>,
    pub report_md: Option<String>,
    pub report_json: Option<String>,
    pub error: Option<String>,
    pub stop_tx: Option<oneshot::Sender<()>>,
    pub control_tx: Option<mpsc::UnboundedSender<RealtimeControl>>,
}

#[derive(Deserialize)]
struct TestSpeakerReq {
    speaker: Option<String>,
}

#[derive(Deserialize)]
struct MicMeterQuery {
    mic: Option<String>,
}

#[derive(Deserialize)]
struct JobsQuery {
    status: Option<String>,
}

#[derive(Deserialize)]
struct CreateJobReq {
    title: String,
    department: Option<String>,
    location: Option<String>,
    work_mode: Option<String>,
    employment_type: Option<String>,
    compensation: Option<String>,
    summary: Option<String>,
    description: Option<String>,
    #[serde(default)]
    required_skills: Vec<String>,
    #[serde(default)]
    custom_questions: Vec<String>,
    language: Option<String>,
    voice_id: Option<String>,
    passing_score: Option<u8>,
    duration_minutes: Option<u16>,
    status: Option<String>,
}

#[derive(Deserialize)]
struct UpdateJobReq {
    title: Option<String>,
    department: Option<String>,
    location: Option<String>,
    work_mode: Option<String>,
    employment_type: Option<String>,
    compensation: Option<String>,
    summary: Option<String>,
    description: Option<String>,
    required_skills: Option<Vec<String>>,
    custom_questions: Option<Vec<String>>,
    language: Option<String>,
    voice_id: Option<String>,
    passing_score: Option<u8>,
    duration_minutes: Option<u16>,
    status: Option<String>,
}

#[derive(Deserialize)]
struct UpdateProfileReq {
    full_name: String,
    email: Option<String>,
    phone: Option<String>,
    location: Option<String>,
    headline: Option<String>,
    years_experience: Option<String>,
    rate_expectation: Option<String>,
    availability: Option<String>,
    linkedin_url: Option<String>,
    portfolio_url: Option<String>,
    #[serde(default)]
    skills: Vec<String>,
    cv_filename: Option<String>,
    cv_content: Option<String>,
}

#[derive(Deserialize)]
struct UpdateApplicationReq {
    status: Option<String>,
    admin_notes: Option<String>,
}

#[derive(Deserialize)]
struct AdminLoginReq {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct StartInterviewReq {
    job_id: Option<String>,
    candidate_name: String,
    candidate_email: Option<String>,
    job_role: String,
    cv_content: String,
    job_description: Option<String>,
    language: Option<String>,
    mic: Option<String>,
    speaker: Option<String>,
    voice_id: Option<String>,
    agent_id: Option<String>,
}

#[derive(Deserialize)]
struct UpdateAudioReq {
    mic: Option<String>,
    speaker: Option<String>,
    muted: Option<bool>,
}

#[derive(Serialize)]
struct ReportListEntry {
    filename: String,
    candidate_name: String,
    job_role: String,
    overall_score: u8,
    verdict: String,
    evaluated_at: String,
}

pub async fn run_http_server(cfg: AppConfig, port: u16) -> Result<()> {
    let (event_tx, _) = broadcast::channel::<RealtimeEvent>(256);
    let store = AppStore::load_or_init();
    let state = Arc::new(ServerState {
        cfg,
        event_tx,
        session: Mutex::new(ActiveSessionState {
            phase: "idle".to_string(),
            ..Default::default()
        }),
        store: Mutex::new(store),
        admin_tokens: Mutex::new(HashSet::new()),
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-admin-token"),
        ]);

    let app = Router::new()
        .route("/api/health", get(handle_health))
        .route("/api/admin/login", post(handle_admin_login))
        .route("/api/admin/session", get(handle_admin_session))
        .route("/api/admin/logout", post(handle_admin_logout))
        .route("/api/devices", get(handle_devices))
        .route("/api/devices/test-speaker", post(handle_test_speaker))
        .route("/api/devices/mic-meter", get(handle_mic_meter_ws))
        .route("/api/voices", get(handle_voices))
        .route("/api/cv/extract", post(handle_cv_extract))
        .route("/api/jobs", get(handle_list_jobs).post(handle_create_job))
        .route("/api/jobs/{id}", patch(handle_update_job).delete(handle_delete_job))
        .route("/api/profile", get(handle_get_profile).put(handle_put_profile))
        .route("/api/applications/public", get(handle_list_public_applications))
        .route("/api/applications", get(handle_list_applications))
        .route("/api/applications/{id}", patch(handle_update_application))
        .route("/api/applications/{id}/proctor", post(handle_proctor_event))
        .route("/api/interview/status", get(handle_interview_status))
        .route("/api/interview/start", post(handle_interview_start))
        .route("/api/interview/audio", post(handle_interview_audio))
        .route("/api/interview/stop", post(handle_interview_stop))
        .route("/api/interview/ws", get(handle_interview_ws))
        .route("/api/reports", get(handle_list_reports))
        .route("/api/reports/{filename}", get(handle_get_report))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Pure Rust AI Interview API Server berjalan di http://{addr} (LAN & localhost)");
    println!("Endpoint utama: GET /api/devices, GET /api/voices, POST /api/interview/start, WS /api/interview/ws");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn api_err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(json!({ "error": msg.into() }))).into_response()
}

fn generate_admin_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = TOKEN_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut h1 = RandomState::new().build_hasher();
    h1.write_u128(nanos);
    h1.write_u64(seq);
    let mut h2 = RandomState::new().build_hasher();
    h2.write_u32(std::process::id());
    h2.write_u64(seq.wrapping_mul(0x9E3779B97F4A7C15));
    format!("adm_{:016x}{:016x}", h1.finish(), h2.finish())
}

fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(val) = headers.get("x-admin-token").and_then(|v| v.to_str().ok()) {
        let t = val.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(rest) = auth.strip_prefix("Bearer ") {
            let t = rest.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

async fn is_admin_authenticated(state: &ServerState, headers: &HeaderMap) -> bool {
    let Some(token) = extract_token(headers) else {
        return false;
    };
    let tokens = state.admin_tokens.lock().await;
    tokens.contains(&token)
}

async fn require_admin(state: &ServerState, headers: &HeaderMap) -> Result<(), Response> {
    if is_admin_authenticated(state, headers).await {
        Ok(())
    } else {
        Err(api_err(
            StatusCode::UNAUTHORIZED,
            "Akses admin membutuhkan login terlebih dahulu.",
        ))
    }
}

async fn handle_admin_login(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<AdminLoginReq>,
) -> Response {
    if req.username.trim() != state.cfg.admin_username
        || req.password != state.cfg.admin_password
    {
        return api_err(StatusCode::UNAUTHORIZED, "Username atau password admin salah.");
    }
    let token = generate_admin_token();
    state.admin_tokens.lock().await.insert(token.clone());
    Json(json!({
        "ok": true,
        "token": token,
        "username": state.cfg.admin_username,
    }))
    .into_response()
}

async fn handle_admin_session(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    if is_admin_authenticated(&state, &headers).await {
        Json(json!({
            "authenticated": true,
            "username": state.cfg.admin_username,
        }))
        .into_response()
    } else {
        api_err(StatusCode::UNAUTHORIZED, "Sesi admin tidak valid.")
    }
}

async fn handle_admin_logout(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(token) = extract_token(&headers) {
        state.admin_tokens.lock().await.remove(&token);
    }
    Json(json!({ "ok": true }))
}

async fn handle_health(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "has_elevenlabs_key": state.cfg.elevenlabs_api_key.is_some(),
        "has_agent_id": state.cfg.elevenlabs_agent_id.is_some(),
        "default_voice_id": state.cfg.elevenlabs_voice_id,
        "has_eval_llm_key": state.cfg.eval_llm_api_key.is_some(),
    }))
}

async fn handle_devices() -> Response {
    match tokio::task::spawn_blocking(list_audio_devices_data).await {
        Ok(Ok(data)) => Json(data).into_response(),
        Ok(Err(err)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        Err(err) => api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn handle_test_speaker(Json(body): Json<TestSpeakerReq>) -> Response {
    match tokio::task::spawn_blocking(move || test_speaker_tone(body.speaker.as_deref())).await {
        Ok(Ok(info)) => Json(json!({ "ok": true, "device": info })).into_response(),
        Ok(Err(err)) => api_err(StatusCode::BAD_REQUEST, err.to_string()),
        Err(err) => api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn handle_mic_meter_ws(
    ws: WebSocketUpgrade,
    Query(query): Query<MicMeterQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run_mic_meter_socket(socket, query.mic))
}

async fn run_mic_meter_socket(mut socket: WebSocket, mic: Option<String>) {
    let started = tokio::task::spawn_blocking(move || MicMeterHandle::start(mic.as_deref())).await;
    let (mut handle, mut rms_rx) = match started {
        Ok(Ok(res)) => res,
        Ok(Err(err)) => {
            let _ = socket
                .send(Message::Text(json!({ "error": err.to_string() }).to_string().into()))
                .await;
            return;
        }
        Err(err) => {
            let _ = socket
                .send(Message::Text(json!({ "error": err.to_string() }).to_string().into()))
                .await;
            return;
        }
    };

    loop {
        tokio::select! {
            Some(rms) = rms_rx.recv() => {
                if socket.send(Message::Text(json!({ "rms": rms }).to_string().into())).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                if matches!(msg, None | Some(Ok(Message::Close(_))) | Some(Err(_))) {
                    break;
                }
            }
        }
    }

    handle.stop();
}

async fn handle_voices(State(state): State<Arc<ServerState>>) -> Response {
    let Some(ref api_key) = state.cfg.elevenlabs_api_key else {
        return api_err(StatusCode::BAD_REQUEST, "ELEVENLABS_API_KEY belum diisi.");
    };
    let client = ElevenLabsClient::new(api_key);
    match client.list_voices().await {
        Ok(voices) => Json(json!({ "voices": voices })).into_response(),
        Err(err) => api_err(StatusCode::BAD_GATEWAY, err.to_string()),
    }
}

async fn handle_cv_extract(mut multipart: Multipart) -> Response {
    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = field
            .file_name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "cv.txt".to_string());
        match field.bytes().await {
            Ok(bytes) => match extract_cv_from_bytes(&filename, &bytes) {
                Ok(text) => {
                    return Json(json!({ "filename": filename, "text": text })).into_response()
                }
                Err(err) => return api_err(StatusCode::BAD_REQUEST, err.to_string()),
            },
            Err(err) => return api_err(StatusCode::BAD_REQUEST, err.to_string()),
        }
    }
    api_err(StatusCode::BAD_REQUEST, "File CV tidak ditemukan pada form upload.")
}

async fn handle_list_jobs(
    State(state): State<Arc<ServerState>>,
    Query(q): Query<JobsQuery>,
) -> impl IntoResponse {
    let store = state.store.lock().await;
    let jobs: Vec<JobOpening> = match q.status.as_deref() {
        Some(filter) if !filter.is_empty() && filter != "all" => store
            .jobs
            .iter()
            .filter(|j| j.status.eq_ignore_ascii_case(filter))
            .cloned()
            .collect(),
        _ => store.jobs.clone(),
    };
    Json(json!({ "jobs": jobs }))
}

async fn handle_create_job(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(req): Json<CreateJobReq>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    if req.title.trim().is_empty() {
        return api_err(StatusCode::BAD_REQUEST, "Judul lowongan wajib diisi.");
    }
    let now = Local::now().to_rfc3339();
    let job = JobOpening {
        id: make_id("job"),
        title: req.title.trim().to_string(),
        department: req
            .department
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "General".to_string()),
        location: req
            .location
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Worldwide Remote".to_string()),
        work_mode: req
            .work_mode
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Remote".to_string()),
        employment_type: req
            .employment_type
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Contract".to_string()),
        compensation: req
            .compensation
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Competitive".to_string()),
        summary: req.summary.unwrap_or_default(),
        description: req.description.unwrap_or_default(),
        required_skills: req
            .required_skills
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        custom_questions: req
            .custom_questions
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        language: req
            .language
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "id".to_string()),
        voice_id: req
            .voice_id
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| state.cfg.elevenlabs_voice_id.clone()),
        passing_score: req.passing_score.unwrap_or(72).min(100),
        duration_minutes: req.duration_minutes.unwrap_or(15).max(5),
        status: req
            .status
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "open".to_string()),
        created_at: now.clone(),
        updated_at: now,
    };

    let mut store = state.store.lock().await;
    store.jobs.insert(0, job.clone());
    if let Err(err) = store.save() {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    Json(json!({ "job": job })).into_response()
}

async fn handle_update_job(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UpdateJobReq>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let mut store = state.store.lock().await;
    let Some(idx) = store.jobs.iter().position(|j| j.id == id) else {
        return api_err(StatusCode::NOT_FOUND, "Lowongan tidak ditemukan.");
    };

    let job = &mut store.jobs[idx];
    if let Some(v) = req.title {
        job.title = v.trim().to_string();
    }
    if let Some(v) = req.department {
        job.department = v;
    }
    if let Some(v) = req.location {
        job.location = v;
    }
    if let Some(v) = req.work_mode {
        job.work_mode = v;
    }
    if let Some(v) = req.employment_type {
        job.employment_type = v;
    }
    if let Some(v) = req.compensation {
        job.compensation = v;
    }
    if let Some(v) = req.summary {
        job.summary = v;
    }
    if let Some(v) = req.description {
        job.description = v;
    }
    if let Some(v) = req.required_skills {
        job.required_skills = v
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(v) = req.custom_questions {
        job.custom_questions = v
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(v) = req.language {
        job.language = v;
    }
    if let Some(v) = req.voice_id {
        job.voice_id = v;
    }
    if let Some(v) = req.passing_score {
        job.passing_score = v.min(100);
    }
    if let Some(v) = req.duration_minutes {
        job.duration_minutes = v.max(5);
    }
    if let Some(v) = req.status {
        job.status = v;
    }
    job.updated_at = Local::now().to_rfc3339();
    let updated = job.clone();

    if let Err(err) = store.save() {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    Json(json!({ "job": updated })).into_response()
}

async fn handle_delete_job(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let mut store = state.store.lock().await;
    let before = store.jobs.len();
    store.jobs.retain(|j| j.id != id);
    if store.jobs.len() == before {
        return api_err(StatusCode::NOT_FOUND, "Lowongan tidak ditemukan.");
    }
    if let Err(err) = store.save() {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    Json(json!({ "ok": true })).into_response()
}

async fn handle_get_profile(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let store = state.store.lock().await;
    Json(json!({ "profile": store.profile }))
}

async fn handle_put_profile(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<UpdateProfileReq>,
) -> Response {
    let mut store = state.store.lock().await;
    let existing_certified = store.profile.certified_skills.clone();
    store.profile = CandidateProfile {
        id: "profile_default".to_string(),
        full_name: req.full_name.trim().to_string(),
        email: req.email.unwrap_or_default().trim().to_string(),
        phone: req.phone.unwrap_or_default().trim().to_string(),
        location: req.location.unwrap_or_default().trim().to_string(),
        headline: req.headline.unwrap_or_default().trim().to_string(),
        years_experience: req.years_experience.unwrap_or_default().trim().to_string(),
        rate_expectation: req.rate_expectation.unwrap_or_default().trim().to_string(),
        availability: req
            .availability
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Immediate / Flexible".to_string()),
        linkedin_url: req.linkedin_url.unwrap_or_default().trim().to_string(),
        portfolio_url: req.portfolio_url.unwrap_or_default().trim().to_string(),
        skills: req
            .skills
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        certified_skills: existing_certified,
        cv_filename: req.cv_filename.unwrap_or_default(),
        cv_content: req.cv_content.unwrap_or_default(),
        updated_at: Local::now().to_rfc3339(),
    };
    let saved = store.profile.clone();
    if let Err(err) = store.save() {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    Json(json!({ "profile": saved })).into_response()
}

async fn handle_list_public_applications(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    if store.sync_reports_into_applications() {
        let _ = store.save();
    }
    let public_rows: Vec<Value> = store
        .applications
        .iter()
        .map(|a| {
            json!({
                "id": a.id,
                "job_id": a.job_id,
                "job_title": a.job_title,
                "candidate_name": a.candidate_name,
                "status": a.status,
                "duration_secs": a.duration_secs,
                "applied_at": a.applied_at,
                "updated_at": a.updated_at,
            })
        })
        .collect();
    Json(json!({ "applications": public_rows }))
}

async fn handle_list_applications(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let mut store = state.store.lock().await;
    if store.sync_reports_into_applications() {
        let _ = store.save();
    }
    Json(json!({ "applications": store.applications })).into_response()
}

async fn handle_update_application(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UpdateApplicationReq>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let mut store = state.store.lock().await;
    let Some(idx) = store.applications.iter().position(|a| a.id == id) else {
        return api_err(StatusCode::NOT_FOUND, "Aplikasi kandidat tidak ditemukan.");
    };
    if let Some(status) = req.status {
        store.applications[idx].status = status;
    }
    if let Some(notes) = req.admin_notes {
        store.applications[idx].admin_notes = notes;
    }
    store.applications[idx].updated_at = Local::now().to_rfc3339();
    let updated = store.applications[idx].clone();
    if let Err(err) = store.save() {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    Json(json!({ "application": updated })).into_response()
}

async fn handle_proctor_event(
    State(state): State<Arc<ServerState>>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let mut store = state.store.lock().await;
    let Some(app) = store.applications.iter_mut().find(|a| a.id == id) else {
        return api_err(StatusCode::NOT_FOUND, "Aplikasi tidak ditemukan.");
    };
    app.proctoring_tab_switches = app.proctoring_tab_switches.saturating_add(1);
    app.updated_at = Local::now().to_rfc3339();
    let count = app.proctoring_tab_switches;
    let _ = store.save();
    Json(json!({ "ok": true, "proctoring_tab_switches": count })).into_response()
}

async fn handle_interview_status(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let is_admin = is_admin_authenticated(&state, &headers).await;
    let guard = state.session.lock().await;
    Json(json!({
        "phase": guard.phase,
        "application_id": guard.application_id,
        "job_id": guard.job_id,
        "conversation_id": guard.conversation_id,
        "candidate_name": guard.candidate_name,
        "job_role": guard.job_role,
        "mic": guard.mic,
        "speaker": guard.speaker,
        "muted": guard.muted,
        "transcript": guard.transcript,
        "scorecard": if is_admin { serde_json::to_value(&guard.scorecard).unwrap_or(Value::Null) } else { Value::Null },
        "report_md": if is_admin { serde_json::to_value(&guard.report_md).unwrap_or(Value::Null) } else { Value::Null },
        "report_json": if is_admin { serde_json::to_value(&guard.report_json).unwrap_or(Value::Null) } else { Value::Null },
        "error": guard.error,
    }))
}

async fn handle_interview_start(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<StartInterviewReq>,
) -> Response {
    let Some(api_key) = state.cfg.elevenlabs_api_key.clone() else {
        return api_err(StatusCode::BAD_REQUEST, "ELEVENLABS_API_KEY belum diisi.");
    };
    let agent_id = req
        .agent_id
        .clone()
        .or_else(|| state.cfg.elevenlabs_agent_id.clone());
    let Some(agent_id) = agent_id else {
        return api_err(
            StatusCode::BAD_REQUEST,
            "ELEVENLABS_AGENT_ID belum ada. Jalankan `cargo run -- setup` terlebih dahulu.",
        );
    };

    {
        let guard = state.session.lock().await;
        if guard.phase == "connecting" || guard.phase == "active" || guard.phase == "evaluating" {
            return api_err(
                StatusCode::CONFLICT,
                "Masih ada sesi interview yang berjalan. Akhiri sesi tersebut terlebih dahulu.",
            );
        }
    }

    let matched_job: Option<JobOpening> = if let Some(ref jid) = req.job_id {
        let store = state.store.lock().await;
        store.jobs.iter().find(|j| j.id == *jid).cloned()
    } else {
        None
    };

    let resolved_role = matched_job
        .as_ref()
        .map(|j| j.title.clone())
        .unwrap_or_else(|| req.job_role.clone());
    let lang = req
        .language
        .clone()
        .or_else(|| matched_job.as_ref().map(|j| j.language.clone()))
        .unwrap_or_else(|| "id".to_string());
    let voice_to_use = req
        .voice_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| matched_job.as_ref().map(|j| j.voice_id.clone()));
    let passing_score = matched_job
        .as_ref()
        .map(|j| j.passing_score)
        .unwrap_or(72);
    let job_skills = matched_job
        .as_ref()
        .map(|j| j.required_skills.clone())
        .unwrap_or_default();

    let mut jd_parts = Vec::new();
    if let Some(ref job) = matched_job {
        if !job.summary.trim().is_empty() {
            jd_parts.push(format!("Role Summary: {}", job.summary));
        }
        if !job.description.trim().is_empty() {
            jd_parts.push(format!("Job Description: {}", job.description));
        }
        if !job.required_skills.is_empty() {
            jd_parts.push(format!(
                "Required Skills to Probe: {}",
                job.required_skills.join(", ")
            ));
        }
        if !job.custom_questions.is_empty() {
            let qs = job
                .custom_questions
                .iter()
                .enumerate()
                .map(|(i, q)| format!("{}. {}", i + 1, q))
                .collect::<Vec<_>>()
                .join("\n");
            jd_parts.push(format!("Mandatory Interview Questions to Cover:\n{qs}"));
        }
    }
    if let Some(ref custom_jd) = req.job_description {
        if !custom_jd.trim().is_empty() {
            jd_parts.push(custom_jd.trim().to_string());
        }
    }
    let jd = if jd_parts.is_empty() {
        "Tidak ada Job Description terpisah. Evaluasi fokus pada kecocokan dengan role target dan klaim di CV.".to_string()
    } else {
        jd_parts.join("\n\n")
    };

    if let Some(ref voice_id) = voice_to_use {
        let client = ElevenLabsClient::new(&api_key);
        let opts = AgentSetupOptions {
            name: "AI Interviewer (Pure Rust)".to_string(),
            voice_id: voice_id.clone(),
            tts_model_id: "eleven_flash_v2_5".to_string(),
            llm_model: "gemini-2.5-flash".to_string(),
            language: lang.clone(),
        };
        if let Err(err) = client.create_or_update_agent(Some(&agent_id), &opts).await {
            return api_err(
                StatusCode::BAD_GATEWAY,
                format!("Gagal update voice/bahasa agent: {err}"),
            );
        }
    }

    let app_id = make_id("app");
    let now_str = Local::now().to_rfc3339();
    {
        let mut store = state.store.lock().await;
        store.applications.insert(
            0,
            ApplicationRecord {
                id: app_id.clone(),
                job_id: req
                    .job_id
                    .clone()
                    .unwrap_or_else(|| "job_custom".to_string()),
                job_title: resolved_role.clone(),
                candidate_name: req.candidate_name.clone(),
                candidate_email: req.candidate_email.clone().unwrap_or_default(),
                conversation_id: None,
                status: "interview_pending".to_string(),
                report_filename: None,
                overall_score: None,
                cv_alignment_score: None,
                explanation_clarity_score: None,
                technical_depth_score: None,
                problem_solving_score: None,
                verdict: None,
                proctoring_tab_switches: 0,
                duration_secs: 0,
                admin_notes: String::new(),
                applied_at: now_str.clone(),
                updated_at: now_str,
            },
        );
        let _ = store.save();
    }

    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<RealtimeControl>();

    {
        let mut guard = state.session.lock().await;
        *guard = ActiveSessionState {
            phase: "connecting".to_string(),
            application_id: Some(app_id.clone()),
            job_id: req.job_id.clone(),
            conversation_id: None,
            candidate_name: Some(req.candidate_name.clone()),
            job_role: Some(resolved_role.clone()),
            mic: req.mic.clone(),
            speaker: req.speaker.clone(),
            muted: false,
            transcript: Vec::new(),
            scorecard: None,
            report_md: None,
            report_json: None,
            error: None,
            stop_tx: Some(stop_tx),
            control_tx: Some(ctrl_tx),
        };
    }

    let state_bg = Arc::clone(&state);
    let event_tx = state.event_tx.clone();
    let cfg_clone = state.cfg.clone();
    let app_id_bg = app_id.clone();
    let started_instant = Instant::now();

    tokio::spawn(async move {
        let mut rx_mirror = event_tx.subscribe();
        let state_mirror = Arc::clone(&state_bg);
        let mirror_task = tokio::spawn(async move {
            while let Ok(ev) = rx_mirror.recv().await {
                let mut g = state_mirror.session.lock().await;
                match ev {
                    RealtimeEvent::Phase { phase } => g.phase = phase,
                    RealtimeEvent::Connected {
                        conversation_id,
                        mic,
                        speaker,
                        ..
                    } => {
                        g.conversation_id = conversation_id;
                        g.mic = Some(mic);
                        g.speaker = Some(speaker);
                    }
                    RealtimeEvent::UserTranscript { text, timestamp } => {
                        g.transcript.push(TranscriptTurn {
                            speaker: "user".to_string(),
                            text,
                            timestamp,
                        });
                    }
                    RealtimeEvent::AgentResponse { text, timestamp } => {
                        g.transcript.push(TranscriptTurn {
                            speaker: "agent".to_string(),
                            text,
                            timestamp,
                        });
                    }
                    RealtimeEvent::AgentResponseCorrection { text } => {
                        if let Some(last) = g.transcript.iter_mut().rev().find(|t| t.speaker == "agent") {
                            last.text = text;
                        }
                    }
                    RealtimeEvent::AudioConfigUpdated { mic, speaker, muted } => {
                        g.mic = Some(mic);
                        g.speaker = Some(speaker);
                        g.muted = muted;
                    }
                    _ => {}
                }
            }
        });

        let session_res = run_realtime_interview_with_controls(
            &api_key,
            InterviewSessionParams {
                agent_id,
                candidate_name: req.candidate_name.clone(),
                job_role: resolved_role.clone(),
                job_description: jd.clone(),
                cv_content: req.cv_content.clone(),
                language: lang,
                mic_name: req.mic.clone(),
                speaker_name: req.speaker.clone(),
            },
            stop_rx,
            ctrl_rx,
            Some(event_tx.clone()),
        )
        .await;

        mirror_task.abort();
        let duration_secs = started_instant.elapsed().as_secs() as u32;

        match session_res {
            Ok(res) => {
                {
                    let mut g = state_bg.session.lock().await;
                    g.phase = "evaluating".to_string();
                    g.stop_tx = None;
                    g.control_tx = None;
                    if res.conversation_id.is_some() {
                        g.conversation_id = res.conversation_id.clone();
                    }
                    if !res.transcript.is_empty() {
                        g.transcript = res.transcript.clone();
                    }
                }
                let _ = event_tx.send(RealtimeEvent::Phase {
                    phase: "evaluating".to_string(),
                });

                match evaluate_and_save(
                    &cfg_clone,
                    &req.candidate_name,
                    &resolved_role,
                    &jd,
                    &req.cv_content,
                    res.conversation_id.as_deref(),
                    &res.transcript,
                )
                .await
                {
                    Ok((scorecard, md_path, json_path)) => {
                        let md_str = md_path.to_string_lossy().to_string();
                        let json_str = json_path.to_string_lossy().to_string();
                        let json_filename = json_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .map(|s| s.to_string());

                        {
                            let mut store = state_bg.store.lock().await;
                            if let Some(app) =
                                store.applications.iter_mut().find(|a| a.id == app_id_bg)
                            {
                                app.conversation_id = res.conversation_id.clone();
                                app.report_filename = json_filename;
                                app.overall_score = Some(scorecard.overall_score);
                                app.cv_alignment_score = Some(scorecard.cv_alignment_score);
                                app.explanation_clarity_score =
                                    Some(scorecard.explanation_clarity_score);
                                app.technical_depth_score = Some(scorecard.technical_depth_score);
                                app.problem_solving_score = Some(scorecard.problem_solving_score);
                                app.verdict = Some(scorecard.verdict.clone());
                                app.duration_secs = duration_secs;
                                app.status = if scorecard.overall_score >= passing_score {
                                    "hiring_manager_reviewing".to_string()
                                } else {
                                    "under_review".to_string()
                                };
                                app.updated_at = Local::now().to_rfc3339();
                            }
                            if scorecard.overall_score >= passing_score {
                                for skill in &job_skills {
                                    if !store
                                        .profile
                                        .certified_skills
                                        .iter()
                                        .any(|s| s.eq_ignore_ascii_case(skill))
                                    {
                                        store.profile.certified_skills.push(skill.clone());
                                    }
                                }
                            }
                            let _ = store.save();
                        }

                        {
                            let mut g = state_bg.session.lock().await;
                            g.phase = "completed".to_string();
                            g.scorecard = Some(scorecard);
                            g.report_md = Some(md_str.clone());
                            g.report_json = Some(json_str.clone());
                        }
                        let _ = event_tx.send(RealtimeEvent::Phase {
                            phase: "completed".to_string(),
                        });
                        let _ = event_tx.send(RealtimeEvent::ScorecardReady {
                            report_md: md_str,
                            report_json: json_str,
                        });
                    }
                    Err(err) => {
                        let msg = format!("Evaluasi gagal: {err}");
                        {
                            let mut g = state_bg.session.lock().await;
                            g.phase = "error".to_string();
                            g.error = Some(msg.clone());
                        }
                        let _ = event_tx.send(RealtimeEvent::Error { message: msg });
                    }
                }
            }
            Err(err) => {
                let msg = format!("Sesi interview error: {err}");
                {
                    let mut g = state_bg.session.lock().await;
                    g.phase = "error".to_string();
                    g.error = Some(msg.clone());
                    g.stop_tx = None;
                    g.control_tx = None;
                }
                let _ = event_tx.send(RealtimeEvent::Error { message: msg });
            }
        }
    });

    Json(json!({ "ok": true, "phase": "connecting", "application_id": app_id })).into_response()
}

async fn handle_interview_audio(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<UpdateAudioReq>,
) -> Response {
    let guard = state.session.lock().await;
    let Some(ref ctrl_tx) = guard.control_tx else {
        return api_err(StatusCode::CONFLICT, "Tidak ada sesi interview aktif.");
    };

    if let Some(muted) = req.muted {
        let _ = ctrl_tx.send(RealtimeControl::SetMuted(muted));
    }
    if req.mic.is_some() || req.speaker.is_some() {
        let _ = ctrl_tx.send(RealtimeControl::SwitchAudio {
            mic: req.mic,
            speaker: req.speaker,
        });
    }

    Json(json!({ "ok": true })).into_response()
}

async fn handle_interview_stop(State(state): State<Arc<ServerState>>) -> Response {
    let mut guard = state.session.lock().await;
    if let Some(stop_tx) = guard.stop_tx.take() {
        let _ = stop_tx.send(());
        return Json(json!({ "ok": true, "phase": "evaluating" })).into_response();
    }
    api_err(StatusCode::CONFLICT, "Tidak ada sesi interview yang sedang berjalan.")
}

async fn handle_interview_ws(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run_interview_ui_ws(socket, state))
}

async fn run_interview_ui_ws(mut socket: WebSocket, state: Arc<ServerState>) {
    let mut rx = state.event_tx.subscribe();

    loop {
        tokio::select! {
            ev = rx.recv() => {
                match ev {
                    Ok(event) => {
                        if let Ok(txt) = serde_json::to_string(&event) {
                            if socket.send(Message::Text(txt.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = socket.recv() => {
                if matches!(msg, None | Some(Ok(Message::Close(_))) | Some(Err(_))) {
                    break;
                }
            }
        }
    }
}

async fn handle_list_reports(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let dir = Path::new("reports");
    if !dir.exists() {
        return Json(json!({ "reports": [] })).into_response();
    }

    let mut entries = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return Json(json!({ "reports": [] })).into_response();
    };

    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(val) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let Some(sc) = val.get("scorecard") else {
            continue;
        };
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        entries.push(ReportListEntry {
            filename,
            candidate_name: sc["candidate_name"].as_str().unwrap_or("Kandidat").to_string(),
            job_role: sc["job_role"].as_str().unwrap_or("-").to_string(),
            overall_score: sc["overall_score"].as_u64().unwrap_or(0) as u8,
            verdict: sc["verdict"].as_str().unwrap_or("-").to_string(),
            evaluated_at: sc["evaluated_at"].as_str().unwrap_or("").to_string(),
        });
    }

    entries.sort_by(|a, b| b.filename.cmp(&a.filename));
    Json(json!({ "reports": entries })).into_response()
}

async fn handle_get_report(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    AxumPath(filename): AxumPath<String>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    if filename.contains("..") || filename.contains('/') || filename.contains('\\') {
        return api_err(StatusCode::BAD_REQUEST, "Nama file tidak valid.");
    }
    let path = Path::new("reports").join(&filename);
    match fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<Value>(&raw) {
            Ok(val) => Json(val).into_response(),
            Err(err) => api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        },
        Err(_) => api_err(StatusCode::NOT_FOUND, "Report tidak ditemukan."),
    }
}
