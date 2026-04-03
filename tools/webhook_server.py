#!/usr/bin/env python3
"""
CRM Webhook Receiver
Handles inbound webhooks from ro.am and Zoom.
Runs on port 8081.
"""
import json
import logging
import subprocess
import sys
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    get_secret, LOG_DIR, ROOT,
    SUPABASE_URL, SUPABASE_KEY, ROAM_API_KEY,
)

LOG_FILE = os.path.join(LOG_DIR, "crm-webhook.log")
DEAD_LETTER_FILE = os.path.join(LOG_DIR, "bifrost-missed.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Credentials from environment / .env
ROAM_KEY = ROAM_API_KEY
CHANCELLOR_USER_ID = get_secret("CHANCELLOR_USER_ID")
OPENCLAW_GATEWAY = get_secret("OPENCLAW_GATEWAY") or "http://127.0.0.1:18789"
OPENCLAW_API_KEY = get_secret("OPENCLAW_API_KEY")
OPENCLAW_BIN = get_secret("OPENCLAW_BIN") or "openclaw"


def _write_dead_letter(payload, wake_text, reason):
    """Append a missed message to the dead-letter file for later replay."""
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "reason": reason,
        "wake_text": wake_text,
        "payload": payload,
    }
    try:
        with open(DEAD_LETTER_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
        log.warning(f"Dead-lettered missed message -> {DEAD_LETTER_FILE}")
    except Exception as dlq_err:
        log.error(f"Failed to write dead-letter entry: {dlq_err}")


def _try_wake_agent(wake_text):
    """
    Attempt to wake the agent via openclaw system event.
    Returns (success: bool, error_msg: str).
    """
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:" + env.get("PATH", "/usr/bin:/bin")
    try:
        result = subprocess.run(
            [
                OPENCLAW_BIN, "system", "event",
                "--text", wake_text,
                "--mode", "now",
                "--url", f"ws://127.0.0.1:18789",
                "--token", OPENCLAW_API_KEY
            ],
            capture_output=True, text=True, timeout=15,
            env=env
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, result.stderr.strip()
    except Exception as e:
        return False, str(e)


def handle_roam_chat_message(payload):
    """Process chat:message:channel or chat:message:dm webhook from ro.am"""
    msg = payload.get("message", payload)
    # Ro.am sends sender as "B-<userId>" for bots
    sender = payload.get("sender", msg.get("userId", ""))
    user_id = sender.replace("B-", "").replace("U-", "")
    text = payload.get("text", msg.get("text", ""))
    chat_id = payload.get("chatId", msg.get("chatId", ""))

    # Ignore our own messages
    if CHANCELLOR_USER_ID and user_id == CHANCELLOR_USER_ID:
        log.info(f"Ignoring own message in chat {chat_id}")
        return

    if not text:
        log.info("Empty message, ignoring")
        return

    log.info(f"Ro.am chat message from {user_id[:8]}... in {chat_id}: {text[:100]}")

    wake_text = (
        f"[Ro.am BiFrost] New message in BiFrost from userId {user_id[:8]}:\n\n"
        f"{text}\n\n"
        f"Read the full BiFrost channel and reply if appropriate."
    )

    # First attempt
    success, msg_out = _try_wake_agent(wake_text)
    if success:
        log.info(f"Agent woken via system event: {msg_out}")
        return

    # First attempt failed — log and retry once after 3 seconds
    log.warning(f"Wake attempt 1 failed: {msg_out} — retrying in 3 s")
    time.sleep(3)
    success, msg_out = _try_wake_agent(wake_text)
    if success:
        log.info(f"Agent woken via system event (retry): {msg_out}")
        return

    # Both attempts failed — write to dead-letter queue
    log.error(f"Wake attempt 2 also failed: {msg_out}")
    _write_dead_letter(payload, wake_text, msg_out)


def handle_roam_recording_saved(payload):
    """Process recording:saved event from ro.am"""
    log.info(f"ro.am recording:saved received: {json.dumps(payload)[:200]}")
    recording_id = payload.get("recordingId") or payload.get("id")
    transcript_id = payload.get("transcriptId")
    location = payload.get("location", "Unknown room")
    start_time = payload.get("startTime", "")

    log.info(f"Recording: {recording_id} | Room: {location} | Transcript: {transcript_id}")

    # Run seed_roam.py to process this recording
    seed_roam_path = str(ROOT / "seeds" / "seed_roam.py")
    try:
        result = subprocess.run(
            [sys.executable, seed_roam_path,
             "--recording-id", str(recording_id)],
            capture_output=True, text=True, timeout=120,
        )
        log.info(f"seed_roam.py output: {result.stdout[-500:]}")
        if result.returncode != 0:
            log.error(f"seed_roam.py error: {result.stderr[-200:]}")
    except Exception as e:
        log.error(f"Failed to run seed_roam.py: {e}")


def handle_zoom_recording(payload):
    """Process recording.completed event from Zoom"""
    log.info(f"Zoom recording event received: {json.dumps(payload)[:200]}")
    seed_zoom_path = str(ROOT / "seeds" / "seed_zoom.py")
    try:
        result = subprocess.run(
            [sys.executable, seed_zoom_path],
            capture_output=True, text=True, timeout=120,
        )
        log.info(f"seed_zoom.py output: {result.stdout[-500:]}")
    except Exception as e:
        log.error(f"Failed to run seed_zoom.py: {e}")


class WebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        log.info(f"HTTP {args}")

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {}

        log.info(f"POST {self.path} | {json.dumps(payload)[:100]}")

        if self.path in ("/roam", "/webhook/roam"):
            event_type = payload.get("event") or payload.get("type", "")
            inner = payload.get("payload", payload)
            if event_type == "message" and payload.get("contentType") == "text":
                handle_roam_chat_message(payload)
            elif "chat:message" in event_type:
                handle_roam_chat_message(inner)
            elif "transcript" in event_type.lower() or "recording" in event_type.lower():
                handle_roam_recording_saved(inner)
            log.info(f"ro.am event: {event_type}")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok": true}')

        elif self.path in ("/zoom", "/webhook/zoom"):
            handle_zoom_recording(payload)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok": true}')

        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok": true}')

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status": "CRM webhook receiver running"}')


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    server = HTTPServer(("127.0.0.1", port), WebhookHandler)
    log.info(f"CRM webhook receiver listening on port {port}")
    server.serve_forever()
