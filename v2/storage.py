"""
CABlytics V2 — Supabase Storage helper.

Thin wrapper over Supabase Storage's HTTP API. Used by Phase 3 to upload
page screenshots and return public URLs.

Reads two env vars set on Render:
  • SUPABASE_URL          e.g. https://xxxxx.supabase.co
  • SUPABASE_SERVICE_KEY  the secret service-role key

We use raw HTTP with `requests` rather than the supabase Python SDK to
avoid adding another dependency. The Storage API surface we need is small.
"""

import os
import time
import mimetypes
import requests

BUCKET_NAME = "page-screenshots"

ALLOWED_MIME_TYPES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
}

# Max bytes accepted at the Flask layer (the bucket itself is also limited).
# 5 MB matches the bucket's file size limit.
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


# ── Config ─────────────────────────────────────────────────────────────────────

def _get_supabase_url() -> str:
    url = os.environ.get("SUPABASE_URL")
    if not url:
        raise RuntimeError("SUPABASE_URL environment variable is not set.")
    return url.rstrip("/")


def _get_service_key() -> str:
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_KEY environment variable is not set.")
    return key


def is_configured() -> bool:
    """Quick check used by routes to surface a clean error if env vars are missing."""
    return bool(os.environ.get("SUPABASE_URL")) and bool(os.environ.get("SUPABASE_SERVICE_KEY"))


# ── Upload ─────────────────────────────────────────────────────────────────────

def _ext_for_mime(mime: str) -> str:
    if mime == "image/png":
        return "png"
    if mime in ("image/jpeg", "image/jpg"):
        return "jpg"
    if mime == "image/webp":
        return "webp"
    return "bin"


def upload_screenshot(client_slug: str, asset_id: int,
                      file_bytes: bytes, content_type: str) -> str:
    """
    Upload a screenshot to Supabase Storage and return the *path* relative to
    the bucket. The public URL can be derived later via public_url_for_path().

    Raises ValueError on validation errors (bad MIME, too large).
    Raises RuntimeError on Supabase API errors.
    """
    if content_type not in ALLOWED_MIME_TYPES:
        raise ValueError(
            f"Unsupported content type: {content_type}. "
            f"Allowed: {', '.join(sorted(ALLOWED_MIME_TYPES))}"
        )

    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise ValueError(
            f"File too large ({len(file_bytes)} bytes). "
            f"Maximum is {MAX_UPLOAD_BYTES // 1024 // 1024}MB."
        )

    if not file_bytes:
        raise ValueError("File is empty.")

    ext = _ext_for_mime(content_type)
    timestamp = int(time.time())
    safe_slug = "".join(c if c.isalnum() or c in "-_" else "-" for c in client_slug).strip("-")
    if not safe_slug:
        safe_slug = "client"

    storage_path = f"{safe_slug}/{asset_id}-{timestamp}.{ext}"

    upload_url = f"{_get_supabase_url()}/storage/v1/object/{BUCKET_NAME}/{storage_path}"
    headers = {
        "Authorization": f"Bearer {_get_service_key()}",
        "Content-Type": content_type,
        # x-upsert lets us overwrite if the path already exists (it shouldn't
        # because of the timestamp, but this avoids weird edge cases on retries)
        "x-upsert": "true",
    }

    print(f"[V2][storage] Uploading {len(file_bytes)} bytes to {storage_path}", flush=True)

    response = requests.post(upload_url, headers=headers, data=file_bytes, timeout=30)

    if response.status_code not in (200, 201):
        msg = f"Supabase upload failed: HTTP {response.status_code} — {response.text[:300]}"
        print(f"[V2][storage] {msg}", flush=True)
        raise RuntimeError(msg)

    print(f"[V2][storage] Upload OK: {storage_path}", flush=True)
    return storage_path


def public_url_for_path(storage_path: str) -> str:
    """
    Build the public URL for a storage path. The bucket is configured as
    public, so this URL is fetchable without authentication and can be passed
    directly to Claude's vision API as an image source.
    """
    if not storage_path:
        return ""
    return f"{_get_supabase_url()}/storage/v1/object/public/{BUCKET_NAME}/{storage_path}"
