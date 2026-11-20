# AI Voice Interviewer Core

Real-time voice interview backend engine built in pure Rust with ElevenLabs Conversational AI and Axum.

The engine handles full-duplex audio streaming through Windows WASAPI (16-bit PCM at 16,000 Hz), bidirectional session state, applicant CV extraction, and automated post-interview scoring.

## Architecture

* Audio streaming: Native Windows WASAPI input and output via `cpal`
* WebSocket connection: Bidirectional streaming to ElevenLabs Conversational AI with `eleven_flash_v2_5`
* HTTP and WebSocket server: Axum and Tokio handling client connections and administrative APIs
* Evaluation: Multi-factor scoring against job requirements and extracted resume data

## Core Components

* `src/audio.rs`: WASAPI device enumeration and hardware audio stream handlers
* `src/realtime.rs`: ElevenLabs WebSocket audio duplex engine
* `src/evaluator.rs`: Post-session candidate evaluation and scorecard generation
* `src/cv.rs`: Plain text and PDF resume extraction
* `src/server.rs`: Axum HTTP and WebSocket API endpoints
* `src/store.rs`: Persistent job vacancy and candidate records storage

## Prerequisites

* Rust 1.80 or newer
* Windows 10 or 11 with WASAPI support
* ElevenLabs API key and Agent ID

## Configuration

Copy `.env.example` to `.env` and fill in the required variables:

```ini
ELEVENLABS_API_KEY=your_elevenlabs_api_key
ELEVENLABS_AGENT_ID=your_conversational_agent_id
ELEVENLABS_VOICE_ID=JBFqnCBsd6RMkjVDRZzb
SERVER_PORT=8787
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_password
ADMIN_ROUTE=/admin
```

## Usage

Run the API server:

```bash
cargo run --release -- serve --port 8787
```

Run an interactive terminal interview session:

```bash
cargo run --release -- interview --cv samples/sample_cv.md --candidate "Candidate Name" --role "Software Engineer"
```

List audio hardware devices:

```bash
cargo run --release -- devices
```