# AI Voice Interviewer Core (Pure Rust)

A high performance, real-time two-way voice interview backend engine built entirely in **Pure Rust** using **ElevenLabs Conversational AI** and **Axum HTTP / WebSocket** streaming.

This engine manages full duplex audio streaming with Windows WASAPI (low-latency PCM 16-bit 16000 Hz), bidirectional conversation state, job opening pipelines, applicant CV extraction, and automated post-interview scoring.

---

## Key Features

- **Zero Audio Intermediate Layers**: No Python or Node.js runtime needed for the audio or streaming path. Real-time audio duplex runs directly through native Rust threads.
- **ElevenLabs Conversational AI**: Direct WebSocket streaming with `eleven_flash_v2_5`, dynamic persona instructions, and patient turn eagerness.
- **WASAPI Device Routing**: Enumerate, select, and test specific physical input (microphones) and output (speakers / headsets) devices.
- **Dual Execution Modes**:
  - **Interactive CLI**: Conduct direct terminal-based interviews with live mic input and earphone playback.
  - **High Throughput HTTP/WS API**: Axum-based server powering web studios, candidate application portals, and administrative dashboards.
- **Persistence & Admin Vault**: In-memory and JSON storage for job listings, applicant records, and token-protected administrative routes.
- **4-Pillar Evaluation**: Multi-dimensional candidate scorecard evaluating CV relevance, technical depth, communication clarity, and role readiness.

---

## Architecture Overview

```text
                      +---------------------------------------+
                      |        Next.js Frontend Studio        |
                      +-------------------+-------------------+
                                          | (REST / WebSocket)
                                          v
+-----------------------------------------------------------------------------+
|                          Pure Rust Backend (Axum)                           |
|                                                                             |
|  +--------------------+   +---------------------+   +--------------------+  |
|  |    REST Routes     |   |   Live Audio Engine |   |  Storage & Store   |  |
|  | /api/jobs          |   |  WASAPI (cpal)      |   | data/store.json    |  |
|  | /api/applications  |   |  PCM 16kHz Mono     |   | JobOpening / App   |  |
|  | /api/admin/login   |   |  Mic Input / Output |   +--------------------+  |
|  +--------------------+   +----------+----------+                           |
|                                      |                                      |
|                                      v                                      |
|                        +---------------------------+                        |
|                        | ElevenLabs Conversational |                        |
|                        |     WebSocket Engine      |                        |
|                        +---------------------------+                        |
+-----------------------------------------------------------------------------+
```

---

## Project Structure

```text
.
├── Cargo.toml            # Dependencies: axum, tokio, cpal, tokio-tungstenite, reqwest
├── .env.example          # Template environment configuration with placeholders
├── samples/
│   ├── sample_cv.md      # Sample candidate resume
│   └── sample_transcript.json
├── src/
│   ├── main.rs           # Entry point and CLI subcommand dispatcher
│   ├── server.rs         # Axum HTTP and WebSocket API server
│   ├── realtime.rs       # ElevenLabs WebSocket duplex audio engine
│   ├── audio.rs          # WASAPI device enumeration and audio stream handlers
│   ├── cv.rs             # Resume parsing and plain text extraction
│   ├── evaluator.rs      # Post-interview evaluation and scorecard generator
│   ├── elevenlabs_api.rs # REST client for agent provisioning and signed URLs
│   ├── store.rs          # Persistent application and job storage
│   └── config.rs         # Environment loader and runtime configuration
└── reports/              # Output directory for generated interview reports (.md / .json)
```

---

## Getting Started

### Prerequisites

- **Rust**: Rust toolchain 1.80+ (`rustup default stable`)
- **Windows**: WASAPI audio support (Windows 10 / 11)
- **ElevenLabs Account**: API Key from [elevenlabs.io](https://elevenlabs.io)

### 1. Environment Setup

Copy the template file to `.env`:

```bash
cp .env.example .env
```

Update `.env` with your credentials:

```ini
# ElevenLabs API Key
ELEVENLABS_API_KEY=your_elevenlabs_api_key_here

# Conversational AI Agent ID
ELEVENLABS_AGENT_ID=your_conversational_agent_id_here

# Optional default voice
ELEVENLABS_VOICE_ID=JBFqnCBsd6RMkjVDRZzb

# Admin console credentials
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_secure_password_here
ADMIN_ROUTE=/ops-vault-ix7a

# Server bind port
SERVER_PORT=8787
```

### 2. Auto-provision an ElevenLabs Agent

If you do not have an agent configured, run the built-in setup command to create one automatically:

```bash
cargo run --release -- setup-agent
```

This registers a tailored recruiter persona on your ElevenLabs account and writes the generated `agent_id` into your `.env` file.

### 3. Running the Server

Start the Axum HTTP & WebSocket server:

```bash
cargo run --release -- serve --port 8787
```

The server binds to `0.0.0.0:8787` by default, making it accessible from both localhost and local network devices.

### 4. Interactive CLI Mode

You can also test audio hardware and conduct an interview directly from your terminal:

```bash
# 1. Enumerate WASAPI audio devices
cargo run --release -- devices

# 2. Start an interactive terminal interview
cargo run --release -- interview \
  --cv samples/sample_cv.md \
  --candidate "Alex Morgan" \
  --role "Systems Engineer" \
  --mic "Microphone (NYK NEMESIS USB MIC)" \
  --speaker "Headset Earphone (dbE Kumo GS2)"
```

---

## API Endpoints

| Method | Route | Description | Auth |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/health` | Service health status and credential readiness | Public |
| `GET` | `/api/devices` | Enumerate input microphones and output speakers | Public |
| `GET` | `/api/voices` | List available voices from ElevenLabs | Public |
| `GET` | `/api/jobs` | Retrieve active job openings | Public |
| `GET` | `/api/applications/public` | Public candidate application cards | Public |
| `POST` | `/api/interview/start` | Launch real-time interview session | Public |
| `POST` | `/api/interview/stop` | End active session and trigger 4-pillar evaluation | Public |
| `GET` | `/api/interview/status` | Read current interview state and live transcript | Public |
| `GET` | `/api/interview/ws` | Live WebSocket event stream for web studio | Public |
| `POST` | `/api/admin/login` | Authenticate recruiter admin and receive token | Public |
| `GET` | `/api/admin/session` | Validate admin bearer token | `x-admin-token` |
| `GET` | `/api/applications` | Full candidate profiles and scorecards | `x-admin-token` |
| `POST` | `/api/jobs` | Create new job vacancy | `x-admin-token` |
| `PATCH` | `/api/jobs/:id` | Update job vacancy status or requirements | `x-admin-token` |
| `DELETE` | `/api/jobs/:id` | Remove job vacancy | `x-admin-token` |

---

## License

MIT License. Free for commercial and non-commercial development.
