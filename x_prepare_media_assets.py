import hashlib
import json
import mimetypes
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from yt_dlp import YoutubeDL

from x_sheet_schema import REVIEW_DROPDOWNS, REVIEW_HEADERS
from x_sheet_utils import apply_dropdown_validation, ensure_exact_headers, get_or_create_worksheet, open_spreadsheet, sanitize_cell

CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def service_account_info():
    raw = os.environ.get("GCP_SA_JSON", "").strip()
    if not raw:
        raise RuntimeError("GCP_SA_JSON is required for spreadsheet access.")
    return json.loads(raw)


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def cloudinary_signature(params: Dict[str, str], api_secret: str) -> str:
    filtered = {k: v for k, v in params.items() if v not in (None, "", [])}
    payload = "&".join(f"{key}={filtered[key]}" for key in sorted(filtered))
    return hashlib.sha1(f"{payload}{api_secret}".encode("utf-8")).hexdigest()


def download_media(url: str) -> Tuple[bytes, str]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    mime = response.headers.get("Content-Type", "").split(";")[0].strip() or mimetypes.guess_type(url)[0] or "application/octet-stream"
    return response.content, mime


def download_video_from_post_url(post_url: str) -> Tuple[bytes, str]:
    if not post_url:
        raise RuntimeError("post_url is required to fetch video with yt-dlp.")

    with tempfile.TemporaryDirectory() as tmpdir:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "format": "best[ext=mp4]/best",
            "outtmpl": os.path.join(tmpdir, "%(id)s.%(ext)s"),
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(post_url, download=True)
            requested = info.get("requested_downloads") or []
            filepath = ""
            if requested and requested[0].get("filepath"):
                filepath = requested[0]["filepath"]
            if not filepath:
                filepath = ydl.prepare_filename(info)
            path = Path(filepath)
            if not path.exists():
                files = sorted(Path(tmpdir).glob("*"))
                if not files:
                    raise RuntimeError("yt-dlp did not produce a downloadable video file.")
                path = files[0]
            mime = mimetypes.guess_type(str(path))[0] or "video/mp4"
            return path.read_bytes(), mime


def upload_to_cloudinary(data: bytes, mime_type: str, public_id: str) -> str:
    cloud_name = env_required("CLOUDINARY_CLOUD_NAME")
    api_key = env_required("CLOUDINARY_API_KEY")
    api_secret = env_required("CLOUDINARY_API_SECRET")
    timestamp = str(int(time.time()))
    resource_type = "video" if mime_type.startswith("video/") else "image"
    params = {"timestamp": timestamp, "public_id": public_id}
    signature = cloudinary_signature(params, api_secret)
    files = {"file": ("upload", data, mime_type)}
    payload = {"api_key": api_key, "timestamp": timestamp, "public_id": public_id, "signature": signature}
    url = f"https://api.cloudinary.com/v1_1/{cloud_name}/{resource_type}/upload"
    response = requests.post(url, data=payload, files=files, timeout=120)
    response.raise_for_status()
    return response.json()["secure_url"]


def first_pipe_value(value: str) -> str:
    parts = [part.strip() for part in str(value or "").split("|") if part.strip()]
    return parts[0] if parts else ""


def looks_like_image_url(url: str) -> bool:
    text = str(url or "").lower().strip()
    return text.endswith((".jpg", ".jpeg", ".png", ".webp")) or "/image/upload/" in text


def safe_slug(text: str, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "-" for ch in text.lower()).strip("-")
    return cleaned or fallback


def build_public_id(post_id: str, account_name: str, suffix: str) -> str:
    config = load_config()
    genre_slug = safe_slug(str(config.get("genre", "liver")), "liver")
    account_slug = safe_slug(account_name, "account")
    return f"{genre_slug}/{account_slug}/{post_id}-{suffix}"


def update_review_rows(ws, rows: List[Dict[str, str]]):
    values = [[sanitize_cell(row.get(header, "")) for header in REVIEW_HEADERS] for row in rows]
    ws.update("A2", values, raw=True)


def run():
    config = load_config()
    spreadsheet = open_spreadsheet()
    review_ws = get_or_create_worksheet(spreadsheet, config["sheet_tabs"]["review"], rows=5000, cols=len(REVIEW_HEADERS) + 5)
    ensure_exact_headers(review_ws, REVIEW_HEADERS)
    apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)
    rows = review_ws.get_all_records(default_blank="")

    changed = False
    for row in rows:
        post_id = str(row.get("投稿ID", "")).strip()
        if not post_id:
            continue

        image_url = first_pipe_value(row.get("画像URL一覧", ""))
        video_url = first_pipe_value(row.get("動画URL一覧", ""))

        if image_url and not row.get("保存メディアURL"):
            data, mime = download_media(image_url)
            public_url = upload_to_cloudinary(data, mime, build_public_id(post_id, str(row.get('アカウント名', '')), "image"))
            row["ドライブ画像ファイルID"] = ""
            row["保存メディアURL"] = public_url
            row["保存メディアパス"] = build_public_id(post_id, str(row.get('アカウント名', '')), "image")
            row["Threads公開画像URL"] = public_url
            changed = True

        existing_video_public_url = str(row.get("Threads公開動画URL", "")).strip()
        needs_video_refresh = bool(video_url) and (
            not existing_video_public_url
            or looks_like_image_url(existing_video_public_url)
        )
        if needs_video_refresh:
            try:
                if video_url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                    data, mime = download_video_from_post_url(str(row.get("元投稿URL", "")).strip())
                else:
                    data, mime = download_media(video_url)
                    if not mime.startswith("video/"):
                        data, mime = download_video_from_post_url(str(row.get("元投稿URL", "")).strip())
                public_url = upload_to_cloudinary(data, mime, build_public_id(post_id, str(row.get('アカウント名', '')), "video"))
                row["ドライブ動画ファイルID"] = ""
                if not row.get("保存メディアURL") or looks_like_image_url(str(row.get("保存メディアURL", "")).strip()):
                    row["保存メディアURL"] = public_url
                    row["保存メディアパス"] = build_public_id(post_id, str(row.get('アカウント名', '')), "video")
                row["Threads公開動画URL"] = public_url
                changed = True
            except Exception:
                # X API からは動画URLが十分に取れないケースが多いので画像側優先で継続する
                pass

    if changed and rows:
        update_review_rows(review_ws, rows)
        apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)
    print(f"[OK] Prepared media assets for {sum(1 for row in rows if row.get('保存メディアURL') or row.get('Threads公開画像URL') or row.get('Threads公開動画URL'))} review rows.")


if __name__ == "__main__":
    run()
