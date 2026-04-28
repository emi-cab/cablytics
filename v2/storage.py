"""
CABlytics V2 — Supabase Storage helper.

Thin wrapper over Supabase Storage's HTTP API. Used by:
  • Phase 3: page screenshots (bucket: page-screenshots)
  • Phase 6: ad creative screenshots (bucket: ad-creatives)

Both buckets must be created as PUBLIC in Supabase Storage so URLs can be
fetched directly by Claude vision without signed URLs.

Reads two env vars set on Render:
  • SUPABASE_URL          e.g. https://xxxxx.supabase.co
  • SUPABASE_SERVICE_KEY  the secret service-role JWT key
"""

import os
import time
import requests

# Bucket names — must match exactly what's configured in Supabase Storage
PAGE_SCREENSHOTS_BUCKET = "page-screenshots"
AD_CREATIVES_BUCKET     = "ad-creatives"

ALLOWED_MIME_TYPES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
}

# Max bytes accepted at the Flask layer (the bucket itself is also limited).
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


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
    return bool(os.environ.get("SUPABASE_URL")) and bool(os.environ.get("SUPABASE_SERVICE_KEY"))


def _ext_for_mime(mime: str) -> str:
    if mime == "image/png":
        return "png"
    if mime in ("image/jpeg", "image/jpg"):
        return "jpg"
    if mime == "image/webp":
        return "webp"
    return "bin"


def _safe_slug(slug: str) -> str:
    out = "".join(c if c.isalnum() or c in "-_" else "-" for c in slug).strip("-")
    return out or "client"


def _upload_to_bucket(bucket: str, storage_path: str,
                      file_bytes: bytes, content_type: str) -> str:
    """Internal — single upload helper used by both bucket-specific functions."""
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

    upload_url = f"{_get_supabase_url()}/storage/v1/object/{bucket}/{storage_path}"
    headers = {
        "Authorization": f"Bearer {_get_service_key()}",
        "Content-Type": content_type,
        "x-upsert": "true",
    }

    print(f"[V2][storage] Uploading {len(file_bytes)} bytes to {bucket}/{storage_path}", flush=True)

    response = requests.post(upload_url, headers=headers, data=file_bytes, timeout=30)

    if response.status_code not in (200, 201):
        msg = f"Supabase upload failed: HTTP {response.status_code} — {response.text[:300]}"
        print(f"[V2][storage] {msg}", flush=True)
        raise RuntimeError(msg)

    print(f"[V2][storage] Upload OK: {bucket}/{storage_path}", flush=True)
    return storage_path


def upload_screenshot(client_slug: str, asset_id: int,
                      file_bytes: bytes, content_type: str) -> str:
    """Upload a page screenshot to the page-screenshots bucket."""
    ext       = _ext_for_mime(content_type)
    timestamp = int(time.time())
    storage_path = f"{_safe_slug(client_slug)}/{asset_id}-{timestamp}.{ext}"
    return _upload_to_bucket(PAGE_SCREENSHOTS_BUCKET, storage_path, file_bytes, content_type)


def upload_ad_creative(client_slug: str, ad_id: int,
                       file_bytes: bytes, content_type: str) -> str:
    """Upload an ad creative screenshot to the ad-creatives bucket."""
    ext       = _ext_for_mime(content_type)
    timestamp = int(time.time())
    storage_path = f"{_safe_slug(client_slug)}/{ad_id}-{timestamp}.{ext}"
    return _upload_to_bucket(AD_CREATIVES_BUCKET, storage_path, file_bytes, content_type)


def public_url_for_path(storage_path: str, bucket: str = PAGE_SCREENSHOTS_BUCKET) -> str:
    """
    Build the public URL for a storage path.

    Defaults to page-screenshots for backward compatibility with Phase 3 callers.
    Phase 6 callers should pass bucket=AD_CREATIVES_BUCKET for ad creative paths.
    """
    if not storage_path:
        return ""
    return f"{_get_supabase_url()}/storage/v1/object/public/{bucket}/{storage_path}"
