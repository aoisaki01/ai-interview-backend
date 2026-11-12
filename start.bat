@echo off
title AI Interview Pure Rust Server
echo ====================================================
echo   AI Voice Interviewer Core - Pure Rust Server
echo ====================================================
echo.

if not exist .env (
    echo [WARN] File .env belum ditemukan. Membuat dari .env.example...
    copy .env.example .env
    echo Silakan isi kredensial ELEVENLABS_API_KEY di dalam .env terlebih dahulu.
    pause
    exit /b 1
)

echo Menjalankan build release...
cargo build --release
if errorlevel 1 (
    echo [ERROR] Build gagal!
    pause
    exit /b 1
)

echo.
echo Server listen di http://0.0.0.0:8787
.\target\release\ai-interview.exe serve --port 8787
pause
