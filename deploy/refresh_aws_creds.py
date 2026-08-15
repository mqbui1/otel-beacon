#!/usr/bin/env python3
"""
refresh_aws_creds.py — Push current AWS session credentials into the
running otel-beacon Docker container on EC2.

Run this once before demoing whenever AWS tokens have rotated:
  python3 deploy/refresh_aws_creds.py

The script reads AWS credentials from the current shell environment
(Claude Code sets these automatically), verifies they work, then
SSH-restarts the otel-beacon container with the fresh credentials.
"""

import os
import subprocess
import sys

# ---------------------------------------------------------------------------
# Config (override via environment)
# ---------------------------------------------------------------------------
EC2_HOST = os.environ.get("EC2_HOST", "44.200.105.184")
EC2_PORT = os.environ.get("EC2_PORT", "2222")
EC2_USER = os.environ.get("EC2_USER", "splunk")
EC2_PASS = os.environ.get("EC2_PASS", "Sp1unkH00di3")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
IMAGE = os.environ.get("BEACON_IMAGE", "localhost:9999/otel-beacon:latest")
DATA_DIR = os.environ.get("BEACON_DATA_DIR", "/home/splunk/otel-data")

# ---------------------------------------------------------------------------
# Step 1: Resolve credentials (env vars → credential chain fallback)
# ---------------------------------------------------------------------------
key_id     = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
session    = os.environ.get("AWS_SESSION_TOKEN")

if not (key_id and secret_key):
    # Fall back to exporting from the AWS credential chain
    print("AWS_* env vars not set — exporting from credential chain...")
    try:
        result = subprocess.run(
            ["aws", "configure", "export-credentials", "--format", "env"],
            capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" in line:
                k, v = line.split("=", 1)
                os.environ[k] = v
        key_id   = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        session  = os.environ.get("AWS_SESSION_TOKEN")
    except subprocess.CalledProcessError as e:
        print(f"[error] Could not export credentials: {e.stderr}", file=sys.stderr)
        print("        Re-authenticate via Okta / SSO and retry.", file=sys.stderr)
        sys.exit(1)

if not (key_id and secret_key):
    print("[error] No AWS credentials found.", file=sys.stderr)
    print("        Open a fresh terminal (Claude Code sets these automatically)", file=sys.stderr)
    print("        or re-authenticate via Okta, then re-run this script.", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Step 2: Verify credentials work
# ---------------------------------------------------------------------------
try:
    import boto3
    arn = boto3.client(
        "sts",
        region_name=AWS_REGION,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        aws_session_token=session,
    ).get_caller_identity()["Arn"]
    print(f"  Credentials verified: {arn}")
except Exception as e:
    print(f"[error] Credential check failed: {e}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Step 3: Restart otel-beacon on EC2 with fresh credentials
# ---------------------------------------------------------------------------
env_flags = (
    f"-e AWS_ACCESS_KEY_ID={key_id} "
    f"-e AWS_SECRET_ACCESS_KEY={secret_key} "
    f"-e AWS_REGION={AWS_REGION}"
)
if session:
    env_flags += f" -e AWS_SESSION_TOKEN={session}"

docker_cmd = (
    f"docker stop otel-beacon 2>/dev/null || true; "
    f"docker rm otel-beacon 2>/dev/null || true; "
    f"docker run -d --name otel-beacon --network host "
    f"-v {DATA_DIR}:/data "
    f"{env_flags} "
    f"{IMAGE}"
)

ssh_cmd = [
    "sshpass", "-p", EC2_PASS,
    "ssh", "-o", "StrictHostKeyChecking=no", "-p", EC2_PORT,
    f"{EC2_USER}@{EC2_HOST}",
    docker_cmd,
]

print(f"  Restarting otel-beacon on {EC2_HOST}:{EC2_PORT}...")
result = subprocess.run(ssh_cmd, capture_output=True, text=True)
if result.returncode != 0:
    print(f"[error] SSH failed:\n{result.stderr}", file=sys.stderr)
    sys.exit(1)

container_id = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "(unknown)"
print(f"  Container started: {container_id[:12]}")

# ---------------------------------------------------------------------------
# Step 4: Verify beacon is responding
# ---------------------------------------------------------------------------
import time, urllib.request, urllib.error
print("  Waiting for otel-beacon to be ready", end="", flush=True)
for _ in range(15):
    try:
        urllib.request.urlopen(f"http://{EC2_HOST}:8080/v1/entities", timeout=2)
        print(" ready!")
        break
    except Exception:
        print(".", end="", flush=True)
        time.sleep(2)
else:
    print("\n[warn] otel-beacon may not be responding yet — check manually.")

print()
print("==========================================")
print(" otel-beacon credentials refreshed!")
print("==========================================")
print()
print(f"  UI:      http://{EC2_HOST}:8080")
print(f"  Expires: {os.environ.get('AWS_CREDENTIAL_EXPIRATION', 'unknown')}")
print()
