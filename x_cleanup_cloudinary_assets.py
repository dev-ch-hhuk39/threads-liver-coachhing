import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import requests

from x_sheet_schema import QUEUE_HEADERS
from x_sheet_utils import get_or_create_worksheet, open_spreadsheet

JST = timezone(timedelta(hours=9))
DELETE_AFTER_DAYS = int(os.environ.get("CLOUDINARY_DELETE_AFTER_DAYS", "7"))
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "x_pipeline_config.json")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def cloudinary_signature(params: Dict[str, str], api_secret: str) -> str:
    filtered = {k: v for k, v in params.items() if v not in (None, "", [])}
    payload = "&".join(f"{key}={filtered[key]}" for key in sorted(filtered))
    return hashlib.sha1(f"{payload}{api_secret}".encode("utf-8")).hexdigest()


def parse_datetime(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=JST)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JST)
    return dt.astimezone(JST)


def parse_public_id(url: str) -> Optional[str]:
    text = str(url or "").strip()
    if not text or "res.cloudinary.com" not in text:
        return None
    path = unquote(urlparse(text).path)
    if "/upload/" not in path:
        return None
    remainder = path.split("/upload/", 1)[1]
    parts = [part for part in remainder.split("/") if part]
    if not parts:
        return None
    if parts[0].startswith("v") and parts[0][1:].isdigit():
        parts = parts[1:]
    if not parts:
        return None
    joined = "/".join(parts)
    if "." in joined:
        joined = joined.rsplit(".", 1)[0]
    return joined or None


def destroy_cloudinary_asset(public_id: str, resource_type: str):
    cloud_name = env_required("CLOUDINARY_CLOUD_NAME")
    api_key = env_required("CLOUDINARY_API_KEY")
    api_secret = env_required("CLOUDINARY_API_SECRET")
    timestamp = str(int(time.time()))
    params = {"public_id": public_id, "timestamp": timestamp, "invalidate": "true"}
    signature = cloudinary_signature(params, api_secret)
    response = requests.post(
        f"https://api.cloudinary.com/v1_1/{cloud_name}/{resource_type}/destroy",
        data={
            "public_id": public_id,
            "timestamp": timestamp,
            "invalidate": "true",
            "api_key": api_key,
            "signature": signature,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def row_is_ready_for_cleanup(row: Dict[str, str]) -> Tuple[bool, Optional[datetime]]:
    x_target = str(row.get("X投稿対象", "")).strip()
    x_state = str(row.get("X投稿状態", "")).strip()
    threads_target = str(row.get("Threads投稿対象", "")).strip()
    threads_state = str(row.get("Threads投稿状態", "")).strip()

    x_done = x_target != "投稿する" or x_state in {"投稿済み", "スキップ"}
    threads_done = threads_target != "投稿する" or threads_state in {"投稿済み", "スキップ"}
    if not (x_done and threads_done):
        return False, None

    posted_times = [
        parse_datetime(row.get("X投稿日時", "")),
        parse_datetime(row.get("Threads投稿日時", "")),
    ]
    posted_times = [item for item in posted_times if item]
    if not posted_times:
        return False, None

    latest = max(posted_times)
    cutoff = datetime.now(JST) - timedelta(days=DELETE_AFTER_DAYS)
    return latest <= cutoff, latest


def run():
    config = load_config()
    spreadsheet = open_spreadsheet()
    queue_ws = get_or_create_worksheet(spreadsheet, config["sheet_tabs"]["post_queue"], rows=5000, cols=len(QUEUE_HEADERS) + 5)
    rows = queue_ws.get_all_records(expected_headers=QUEUE_HEADERS, default_blank="")

    deleted = []
    for row in rows:
        ok, latest = row_is_ready_for_cleanup(row)
        if not ok:
            continue

        image_public_id = parse_public_id(str(row.get("画像URL", "")).strip()) or parse_public_id(str(row.get("Threads画像URL", "")).strip())
        video_public_id = parse_public_id(str(row.get("動画URL", "")).strip()) or parse_public_id(str(row.get("Threads動画URL", "")).strip())

        if image_public_id:
            try:
                destroy_cloudinary_asset(image_public_id, "image")
                deleted.append({"public_id": image_public_id, "resource_type": "image", "posted_at": latest.isoformat() if latest else ""})
            except requests.HTTPError as exc:
                body = exc.response.text[:500] if exc.response is not None else ""
                print(f"[WARN] Cloudinary image delete failed: public_id={image_public_id} body={body}", flush=True)
        if video_public_id:
            try:
                destroy_cloudinary_asset(video_public_id, "video")
                deleted.append({"public_id": video_public_id, "resource_type": "video", "posted_at": latest.isoformat() if latest else ""})
            except requests.HTTPError as exc:
                body = exc.response.text[:500] if exc.response is not None else ""
                print(f"[WARN] Cloudinary video delete failed: public_id={video_public_id} body={body}", flush=True)

    print(json.dumps({"deleted_count": len(deleted), "deleted": deleted[:20]}, ensure_ascii=False))


if __name__ == "__main__":
    run()
