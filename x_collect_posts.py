import argparse
import base64
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

from x_sheet_schema import (
    COLLECTION_HEADERS,
    DASHBOARD_HEADERS,
    QUEUE_DROPDOWNS,
    QUEUE_HEADERS,
    RAW_HEADERS,
    REVIEW_DROPDOWNS,
    REVIEW_HEADERS,
    SYSTEM_HEADERS,
)
from x_sheet_utils import (
    apply_dropdown_validation,
    ensure_exact_headers,
    get_or_create_worksheet,
    open_spreadsheet,
    upsert_rows,
    write_key_value_rows,
)

JST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Prepare and sync X post collection data into Google Sheets."
    )
    parser.add_argument("--input-json", help="Path to a JSON file exported from your collector.")
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Create required tabs and state rows without importing posts.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Exit successfully when no import source is configured.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Print current sheet tab names and row counts without changing data.",
    )
    return parser.parse_args()


def now_jst():
    return datetime.now(JST)


def as_iso(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        return now_jst()

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                dt = dt.replace(tzinfo=JST)
                break
            except ValueError:
                continue
        else:
            return now_jst()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JST)
    return dt.astimezone(JST)


def to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace(",", "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def to_bool_string(value: Any) -> str:
    return "TRUE" if bool(value) else "FALSE"


def normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if "|" in text:
        return [part.strip() for part in text.split("|") if part.strip()]
    return [text]


def uniq(items: Iterable[str]) -> List[str]:
    seen = set()
    ordered = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def join_pipe(items: Iterable[str]) -> str:
    return " | ".join(uniq(items))


def compact_payload(post: Dict[str, Any]) -> str:
    allowed_keys = [
        "post_id",
        "id",
        "post_url",
        "url",
        "account_handle",
        "handle",
        "account_name",
        "author_name",
        "posted_at",
        "created_at",
        "post_type",
        "text",
        "full_text",
        "hashtags",
        "mentions",
        "image_count",
        "has_video",
        "quote_count",
        "reply_count",
        "comment_count",
        "repost_count",
        "retweet_count",
        "like_count",
        "favorite_count",
        "bookmark_count",
        "impression_count",
        "view_count",
        "matched_keywords",
        "matched_accounts",
        "matched_sources",
        "source_types",
    ]
    compact = {key: post.get(key) for key in allowed_keys if key in post}
    return json.dumps(compact, ensure_ascii=False)


def count_emojis(text: str) -> int:
    return sum(1 for ch in text if ord(ch) > 10000)


def extract_hashtags(text: str) -> List[str]:
    return uniq(part[1:] for part in text.split() if part.startswith("#"))


def extract_mentions(text: str) -> List[str]:
    return uniq(part[1:] for part in text.split() if part.startswith("@"))


def time_slot_for_hour(hour: int) -> str:
    if 0 <= hour <= 5:
        return "深夜"
    if 6 <= hour <= 11:
        return "朝"
    if 12 <= hour <= 17:
        return "昼"
    return "夜"


def weekday_ja(dt: datetime) -> str:
    names = ["月", "火", "水", "木", "金", "土", "日"]
    return names[dt.weekday()]


def normalize_post_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "post": "通常投稿",
        "tweet": "通常投稿",
        "reply": "返信",
        "quote": "引用",
        "retweet": "リポスト",
        "repost": "リポスト",
    }
    return mapping.get(text, "通常投稿")


def first_line(text: str, limit: int = 30) -> str:
    compact = " ".join((text or "").split())
    return compact[:limit]


def load_posts_from_json(path: Optional[str]) -> List[Dict[str, Any]]:
    payload = None
    if path:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    else:
        env_json = Path.cwd().joinpath("data", "x_posts.json")
        raw_env = None
        try:
            import os

            raw_env = os.environ.get("X_POSTS_JSON", "").strip()
        except Exception:
            raw_env = ""
        if raw_env:
            payload = json.loads(raw_env)
        elif env_json.exists():
            payload = json.loads(env_json.read_text(encoding="utf-8"))

    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("posts"), list):
        return payload["posts"]
    raise RuntimeError("Unsupported JSON shape. Expected a list or {'posts': [...]} .")


def parse_state(ws) -> Dict[str, str]:
    rows = ws.get_all_records(default_blank="")
    state = {}
    for row in rows:
        key = str(row.get("key", "")).strip()
        if key:
            state[key] = str(row.get("value", "")).strip()
    return state


def bearer_token_from_env() -> str:
    explicit = os.environ.get("X_BEARER_TOKEN", "").strip()
    if explicit:
        return explicit

    api_key = os.environ.get("X_API_KEY", "").strip()
    api_secret = os.environ.get("X_API_SECRET", "").strip()
    if not api_key or not api_secret:
        return ""

    basic = base64.b64encode(f"{api_key}:{api_secret}".encode("utf-8")).decode("utf-8")
    response = requests.post(
        "https://api.x.com/oauth2/token",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    token = str(payload.get("access_token", "")).strip()
    if not token:
        raise RuntimeError("Failed to obtain X bearer token from API key and secret.")
    return token


def x_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def api_get(url: str, token: str, params: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.get(url, headers=x_headers(token), params=params, timeout=60)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text[:800] if response is not None else ""
        raise RuntimeError(f"X API request failed: {url} status={response.status_code} body={body}") from exc
    return response.json()


def parse_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_collection_windows(state: Dict[str, str], config: Dict[str, Any]) -> Dict[str, Any]:
    now = now_jst()
    collection = config["collection"]
    last_success = parse_datetime(state.get("last_successful_collect_at", ""))
    bootstrap = not bool(state.get("last_successful_collect_at"))

    bootstrap_start = now - timedelta(days=collection.get("bootstrap_lookback_days", 30))
    overlap_start = last_success - timedelta(hours=collection.get("incremental_overlap_hours", 24))
    refresh_start = now - timedelta(days=collection.get("refresh_recent_days", 7))

    if bootstrap:
        account_windows = [(bootstrap_start, now)]
        keyword_windows = [(bootstrap_start, now)]
    else:
        account_windows = [(overlap_start, now), (refresh_start, now)]
        keyword_windows = [(overlap_start, now), (refresh_start, now)]

    return {
        "bootstrap": bootstrap,
        "now": now,
        "account_windows": dedupe_windows(account_windows),
        "keyword_windows": dedupe_windows(keyword_windows),
    }


def dedupe_windows(windows: List[Any]) -> List[Any]:
    seen = []
    normalized = []
    for start, end in windows:
        key = (parse_iso_z(start), parse_iso_z(end))
        if key not in seen:
            seen.append(key)
            normalized.append((start, end))
    return normalized


def lookup_users(handles: List[str], token: str) -> Dict[str, Dict[str, Any]]:
    if not handles:
        return {}

    users: Dict[str, Dict[str, Any]] = {}
    batch_size = 100
    for index in range(0, len(handles), batch_size):
        batch = handles[index:index + batch_size]
        payload = api_get(
            "https://api.x.com/2/users/by",
            token,
            {
                "usernames": ",".join(batch),
                "user.fields": "public_metrics,profile_image_url,url,verified,description",
            },
        )
        for user in payload.get("data", []):
            users[str(user.get("username", "")).lower()] = user
    return users


def media_maps(includes: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {item.get("media_key"): item for item in includes.get("media", []) if item.get("media_key")}


def enrich_media_map_with_variants(media_index: Dict[str, Dict[str, Any]], token: str) -> Dict[str, Dict[str, Any]]:
    media_keys = [
        key for key, item in media_index.items()
        if item.get("type") in {"video", "animated_gif"}
    ]
    if not media_keys:
        return media_index

    batch_size = 100
    for index in range(0, len(media_keys), batch_size):
        batch = media_keys[index:index + batch_size]
        payload = api_get(
            "https://api.x.com/2/media",
            token,
            {
                "media_keys": ",".join(batch),
                "media.fields": "type,url,preview_image_url,variants,duration_ms,width,height",
            },
        )
        for item in payload.get("data", []):
            media_key = item.get("media_key")
            if media_key and media_key in media_index:
                merged = dict(media_index[media_key])
                merged.update(item)
                media_index[media_key] = merged
    return media_index


def best_video_url(media_item: Dict[str, Any]) -> str:
    variants = media_item.get("variants") or []
    mp4_variants = [
        variant for variant in variants
        if str(variant.get("content_type", "")).startswith("video/mp4") and variant.get("url")
    ]
    if mp4_variants:
        ranked = sorted(mp4_variants, key=lambda item: to_int(item.get("bit_rate")), reverse=True)
        return str(ranked[0].get("url") or "").strip()

    for variant in variants:
        if variant.get("url"):
            return str(variant.get("url") or "").strip()

    return str(media_item.get("url") or "").strip()


def extract_metric(metrics: Dict[str, Any], key: str) -> int:
    return to_int((metrics or {}).get(key))


def build_post_record(
    tweet: Dict[str, Any],
    user: Dict[str, Any],
    media_index: Dict[str, Dict[str, Any]],
    matched_keywords: Optional[List[str]] = None,
    matched_accounts: Optional[List[str]] = None,
    source_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    attachments = tweet.get("attachments", {}) or {}
    media_keys = attachments.get("media_keys", []) or []
    media_items = [media_index[key] for key in media_keys if key in media_index]
    hashtags = [tag.get("tag", "") for tag in (tweet.get("entities", {}) or {}).get("hashtags", []) if tag.get("tag")]
    mentions = [mention.get("username", "") for mention in (tweet.get("entities", {}) or {}).get("mentions", []) if mention.get("username")]
    urls = [url.get("expanded_url") or url.get("url") for url in (tweet.get("entities", {}) or {}).get("urls", []) if url.get("expanded_url") or url.get("url")]
    referenced = tweet.get("referenced_tweets", []) or []

    public_metrics = tweet.get("public_metrics", {}) or {}
    follower_metrics = (user.get("public_metrics") or {}) if user else {}
    image_count = sum(1 for item in media_items if item.get("type") == "photo")
    has_video = any(item.get("type") in {"video", "animated_gif"} for item in media_items)
    post_type = "post"
    if referenced:
        ref_type = referenced[0].get("type", "")
        post_type = ref_type or "post"

    handle = str((user or {}).get("username") or tweet.get("username") or "").strip()
    return {
        "post_id": str(tweet.get("id", "")).strip(),
        "post_url": f"https://x.com/{handle}/status/{tweet.get('id')}" if handle and tweet.get("id") else "",
        "account_name": str((user or {}).get("name") or "").strip(),
        "account_id": str(tweet.get("author_id") or (user or {}).get("id") or "").strip(),
        "account_handle": handle,
        "account_url": f"https://x.com/{handle}" if handle else "",
        "follower_count": extract_metric(follower_metrics, "followers_count"),
        "posted_at": str(tweet.get("created_at") or "").strip(),
        "post_type": post_type,
        "text": str(tweet.get("text") or (tweet.get("note_tweet") or {}).get("text") or "").strip(),
        "hashtags": hashtags,
        "mentions": mentions,
        "link_urls": urls,
        "image_count": image_count,
        "has_video": has_video,
        "image_urls": [
            item.get("url") or item.get("preview_image_url")
            for item in media_items
            if item.get("type") == "photo" and (item.get("url") or item.get("preview_image_url"))
        ],
        "video_urls": [
            best_video_url(item)
            for item in media_items
            if item.get("type") in {"video", "animated_gif"} and best_video_url(item)
        ],
        "reply_count": extract_metric(public_metrics, "reply_count"),
        "repost_count": extract_metric(public_metrics, "retweet_count") or extract_metric(public_metrics, "repost_count"),
        "like_count": extract_metric(public_metrics, "like_count"),
        "bookmark_count": extract_metric(public_metrics, "bookmark_count"),
        "quote_count": extract_metric(public_metrics, "quote_count"),
        "impression_count": extract_metric(public_metrics, "impression_count"),
        "matched_keywords": matched_keywords or [],
        "matched_accounts": matched_accounts or [],
        "matched_sources": (matched_accounts or []) + (matched_keywords or []),
        "source_types": source_types or [],
    }


def fetch_account_posts(config: Dict[str, Any], token: str, windows: List[Any]) -> List[Dict[str, Any]]:
    handles = [item["handle"] for item in config.get("monitor_accounts", [])]
    users = lookup_users(handles, token)
    collection = config["collection"]
    results: List[Dict[str, Any]] = []

    for account in config.get("monitor_accounts", []):
        handle = account["handle"]
        user = users.get(handle.lower())
        if not user:
            print(f"[WARN] User lookup missing for @{handle}")
            continue

        for start, end in windows:
            next_token = None
            fetched = 0
            while True:
                params = {
                    "max_results": min(100, collection.get("max_posts_per_account", 50)),
                    "start_time": parse_iso_z(start),
                    "end_time": parse_iso_z(end),
                    "tweet.fields": "created_at,public_metrics,attachments,entities,referenced_tweets,note_tweet",
                    "user.fields": "public_metrics,username,name",
                    "expansions": "attachments.media_keys,author_id",
                    "media.fields": "type,url,preview_image_url",
                }
                if next_token:
                    params["pagination_token"] = next_token
                payload = api_get(f"https://api.x.com/2/users/{user['id']}/tweets", token, params)
                includes = payload.get("includes", {}) or {}
                media_index = media_maps(includes)
                media_index = enrich_media_map_with_variants(media_index, token)
                for tweet in payload.get("data", []):
                    results.append(
                        build_post_record(
                            tweet,
                            user,
                            media_index,
                            matched_accounts=[handle],
                            source_types=["account_monitor"],
                        )
                    )
                    fetched += 1

                next_token = (payload.get("meta") or {}).get("next_token")
                if not next_token or fetched >= collection.get("max_posts_per_account", 50):
                    break
    return results


def fetch_keyword_posts(config: Dict[str, Any], token: str, windows: Dict[str, Any]) -> (List[Dict[str, Any]], List[str]):
    collection = config["collection"]
    bootstrap = windows["bootstrap"]
    now = windows["now"]
    notes: List[str] = []
    results: List[Dict[str, Any]] = []
    users_cache: Dict[str, Dict[str, Any]] = {}

    for keyword in config.get("monitor_keywords", []):
        query = f"({keyword}) -is:retweet"
        for start, end in windows["keyword_windows"]:
            endpoint = "recent"
            if bootstrap:
                endpoint = "all"
            url = f"https://api.x.com/2/tweets/search/{endpoint}"
            params = {
                "query": query,
                "max_results": min(100 if endpoint == "recent" else 100, collection.get("max_posts_per_keyword", 100)),
                "start_time": parse_iso_z(start),
                "end_time": parse_iso_z(end),
                "tweet.fields": "created_at,public_metrics,attachments,entities,referenced_tweets,note_tweet",
                "user.fields": "public_metrics,username,name",
                "expansions": "attachments.media_keys,author_id",
                "media.fields": "type,url,preview_image_url",
            }
            try:
                payload = api_get(url, token, params)
            except RuntimeError as exc:
                status = None
                message = str(exc)
                if "status=" in message:
                    try:
                        status = int(message.split("status=")[1].split()[0].split("body=")[0])
                    except Exception:
                        status = None
                if endpoint == "all" and status in {400, 403, 404}:
                    notes.append("キーワードの初回30日取得は full-archive 権限がないため直近7日に自動フォールバックしました。")
                    start = now - timedelta(days=7)
                    payload = api_get(
                        "https://api.x.com/2/tweets/search/recent",
                        token,
                        {
                            **params,
                            "start_time": parse_iso_z(start),
                            "end_time": parse_iso_z(end),
                        },
                    )
                else:
                    raise

            includes = payload.get("includes", {}) or {}
            media_index = media_maps(includes)
            media_index = enrich_media_map_with_variants(media_index, token)
            for user in includes.get("users", []):
                users_cache[str(user.get("id"))] = user
            for tweet in payload.get("data", []):
                author = users_cache.get(str(tweet.get("author_id")), {})
                results.append(
                    build_post_record(
                        tweet,
                        author,
                        media_index,
                        matched_keywords=[keyword],
                        source_types=["keyword_search"],
                    )
                )
    return results, notes


def merge_posts(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for post in posts:
        key = raw_key(post)
        if not key:
            continue
        if key not in merged:
            merged[key] = post
            continue

        current = merged[key]
        current["matched_keywords"] = uniq(normalize_list(current.get("matched_keywords")) + normalize_list(post.get("matched_keywords")))
        current["matched_accounts"] = uniq(normalize_list(current.get("matched_accounts")) + normalize_list(post.get("matched_accounts")))
        current["matched_sources"] = uniq(normalize_list(current.get("matched_sources")) + normalize_list(post.get("matched_sources")))
        current["source_types"] = uniq(normalize_list(current.get("source_types")) + normalize_list(post.get("source_types")))
        for metric in ["reply_count", "repost_count", "like_count", "bookmark_count", "quote_count", "impression_count", "image_count"]:
            current[metric] = max(to_int(current.get(metric)), to_int(post.get(metric)))
        if not current.get("follower_count"):
            current["follower_count"] = post.get("follower_count")
        if not current.get("text"):
            current["text"] = post.get("text")
        current["has_video"] = bool(current.get("has_video")) or bool(post.get("has_video"))
        current["image_urls"] = uniq(normalize_list(current.get("image_urls")) + normalize_list(post.get("image_urls")))
    return list(merged.values())


def load_posts_from_source(args, config: Dict[str, Any], state: Dict[str, str]) -> (List[Dict[str, Any]], List[str], str):
    source_preference = os.environ.get("X_FETCH_SOURCE", "auto").strip().lower()
    enable_keyword_search = (
        os.environ.get("X_ENABLE_KEYWORD_SEARCH", str(config["collection"].get("enable_keyword_search", False)))
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )
    notes: List[str] = []

    if args.input_json:
        return load_posts_from_json(args.input_json), notes, "json_file"

    can_use_api = bool(os.environ.get("X_BEARER_TOKEN", "").strip()) or (
        bool(os.environ.get("X_API_KEY", "").strip()) and bool(os.environ.get("X_API_SECRET", "").strip())
    )
    if source_preference in {"api", "auto"} and can_use_api:
        token = bearer_token_from_env()
        windows = get_collection_windows(state, config)
        account_posts = []
        keyword_posts = []
        try:
            account_posts = fetch_account_posts(config, token, windows["account_windows"])
        except Exception as exc:
            notes.append(f"アカウント収集で一部または全部失敗: {exc}")
        if enable_keyword_search:
            try:
                keyword_posts, keyword_notes = fetch_keyword_posts(config, token, windows)
                notes.extend(keyword_notes)
            except Exception as exc:
                notes.append(f"キーワード収集で一部または全部失敗: {exc}")
        else:
            notes.append("キーワード検索は現在オフです。アカウント監視のみ実行しました。")
        return merge_posts(account_posts + keyword_posts), notes, "x_api"

    json_posts = load_posts_from_json(None)
    if json_posts:
        notes.append("X_POSTS_JSON または data/x_posts.json から投稿データを読み込みました。")
        return json_posts, notes, "json_env"

    return [], notes, "empty"


def raw_key(post: Dict[str, Any]) -> str:
    return str(post.get("post_id") or post.get("post_url") or "").strip()


def normalize_post(post: Dict[str, Any], existing: Dict[str, str], config: Dict[str, Any]) -> Dict[str, str]:
    current = now_jst()
    posted_at = parse_datetime(
        str(
            post.get("posted_at")
            or post.get("created_at")
            or post.get("tweet_created_at")
            or ""
        )
    )
    text = str(post.get("text") or post.get("full_text") or post.get("content") or "").strip()
    hashtags = normalize_list(post.get("hashtags")) or extract_hashtags(text)
    mentions = normalize_list(post.get("mentions")) or extract_mentions(text)
    link_urls = normalize_list(post.get("link_urls") or post.get("urls"))
    matched_keywords = normalize_list(post.get("matched_keywords") or post.get("keywords"))
    matched_accounts = normalize_list(post.get("matched_accounts"))
    matched_sources = normalize_list(post.get("matched_sources"))
    source_types = normalize_list(post.get("source_types"))

    if not matched_accounts and post.get("account_handle"):
        matched_accounts = [str(post.get("account_handle")).strip()]
    if not matched_sources:
        matched_sources = matched_accounts + matched_keywords
    if not source_types:
        if matched_accounts:
            source_types.append("account_monitor")
        if matched_keywords:
            source_types.append("keyword_search")

    image_count = to_int(post.get("image_count"))
    if image_count == 0 and normalize_list(post.get("image_urls")):
        image_count = len(normalize_list(post.get("image_urls")))

    post_id = str(post.get("post_id") or post.get("id") or "").strip()
    account_handle = str(post.get("account_handle") or post.get("handle") or "").strip().lstrip("@")
    account_url = str(post.get("account_url") or "").strip()
    if not account_url and account_handle:
        account_url = f"https://x.com/{account_handle}"

    post_url = str(post.get("post_url") or post.get("url") or "").strip()
    if not post_url and account_handle and post_id:
        post_url = f"https://x.com/{account_handle}/status/{post_id}"

    resolved_key = post_id or post_url or existing.get("post_id", "")

    return {
        "post_id": resolved_key,
        "post_url": post_url or existing.get("post_url", ""),
        "platform": "x",
        "genre": config.get("genre", "夜職"),
        "account_name": str(post.get("account_name") or post.get("author_name") or "").strip(),
        "account_id": str(post.get("account_id") or post.get("author_id") or "").strip(),
        "account_handle": account_handle,
        "account_url": account_url,
        "follower_count": str(to_int(post.get("follower_count"))),
        "posted_at": as_iso(posted_at),
        "posted_date": posted_at.strftime("%Y-%m-%d"),
        "weekday": weekday_ja(posted_at),
        "hour": str(posted_at.hour),
        "time_slot": time_slot_for_hour(posted_at.hour),
        "post_type": normalize_post_type(post.get("post_type") or post.get("tweet_type") or "post"),
        "text": text,
        "hook_text": first_line(text),
        "text_length": str(len(text)),
        "emoji_count": str(count_emojis(text)),
        "hashtag_count": str(len(hashtags)),
        "mention_count": str(len(mentions)),
        "hashtags": join_pipe(hashtags),
        "mentions": join_pipe(mentions),
        "link_urls": join_pipe(link_urls),
        "has_external_link": to_bool_string(bool(link_urls)),
        "image_count": str(image_count),
        "has_image": to_bool_string(image_count > 0),
        "has_video": to_bool_string(bool(post.get("has_video") or post.get("video_url") or post.get("video_urls"))),
        "has_media": to_bool_string(image_count > 0 or bool(post.get("has_video") or post.get("video_url") or post.get("video_urls"))),
        "image_urls": join_pipe(normalize_list(post.get("image_urls"))),
        "video_urls": join_pipe(normalize_list(post.get("video_urls"))),
        "quote_count": str(to_int(post.get("quote_count"))),
        "reply_count": str(to_int(post.get("reply_count") or post.get("comment_count"))),
        "repost_count": str(to_int(post.get("repost_count") or post.get("retweet_count"))),
        "like_count": str(to_int(post.get("like_count") or post.get("favorite_count"))),
        "bookmark_count": str(to_int(post.get("bookmark_count"))),
        "impression_count": str(to_int(post.get("impression_count") or post.get("view_count"))),
        "matched_keywords": join_pipe(matched_keywords),
        "matched_accounts": join_pipe(matched_accounts),
        "matched_sources": join_pipe(matched_sources),
        "source_types": join_pipe(source_types),
        "is_from_account_monitor": to_bool_string("account_monitor" in source_types),
        "is_from_keyword_search": to_bool_string("keyword_search" in source_types),
        "first_collected_at": existing.get("first_collected_at") or as_iso(current),
        "last_metrics_update_at": as_iso(current),
        "last_source_sync_at": as_iso(current),
        "raw_payload_json": compact_payload(post),
    }


def bootstrap_sheet(config: Dict[str, Any]):
    spreadsheet = open_spreadsheet()
    tabs = config["sheet_tabs"]
    raw_ws = get_or_create_worksheet(spreadsheet, tabs["raw_posts"], rows=5000, cols=len(RAW_HEADERS) + 5)
    dashboard_ws = get_or_create_worksheet(spreadsheet, tabs["dashboard"], rows=1000, cols=len(DASHBOARD_HEADERS) + 2)
    collection_ws = get_or_create_worksheet(spreadsheet, tabs["collection_view"], rows=5000, cols=len(COLLECTION_HEADERS) + 5)
    review_ws = get_or_create_worksheet(spreadsheet, tabs["review"], rows=5000, cols=len(REVIEW_HEADERS) + 5)
    queue_ws = get_or_create_worksheet(spreadsheet, tabs["post_queue"], rows=5000, cols=len(QUEUE_HEADERS) + 5)
    state_ws = get_or_create_worksheet(spreadsheet, tabs["state"], rows=100, cols=len(SYSTEM_HEADERS))

    ensure_exact_headers(raw_ws, RAW_HEADERS)
    ensure_exact_headers(dashboard_ws, DASHBOARD_HEADERS)
    ensure_exact_headers(collection_ws, COLLECTION_HEADERS)
    ensure_exact_headers(review_ws, REVIEW_HEADERS)
    ensure_exact_headers(queue_ws, QUEUE_HEADERS)
    ensure_exact_headers(state_ws, SYSTEM_HEADERS)
    apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)
    apply_dropdown_validation(queue_ws, QUEUE_HEADERS, QUEUE_DROPDOWNS)
    return spreadsheet, raw_ws, dashboard_ws, collection_ws, review_ws, queue_ws, state_ws


def current_state_rows(config: Dict[str, Any], imported_count: int, status: str) -> List[Dict[str, str]]:
    now_str = as_iso(now_jst())
    collection = config["collection"]
    return [
        {"key": "genre", "value": config.get("genre", ""), "updated_at": now_str},
        {"key": "bootstrap_lookback_days", "value": str(collection.get("bootstrap_lookback_days", 30)), "updated_at": now_str},
        {"key": "refresh_recent_days", "value": str(collection.get("refresh_recent_days", 7)), "updated_at": now_str},
        {"key": "incremental_overlap_hours", "value": str(collection.get("incremental_overlap_hours", 24)), "updated_at": now_str},
        {"key": "last_collect_run_at", "value": now_str, "updated_at": now_str},
        {"key": "last_collect_status", "value": status, "updated_at": now_str},
        {"key": "last_imported_post_count", "value": str(imported_count), "updated_at": now_str},
    ]


def run():
    args = parse_args()
    config = load_config()
    spreadsheet, raw_ws, _, _, _, _, state_ws = bootstrap_sheet(config)
    state = parse_state(state_ws)

    if args.check_only:
        print(
            json.dumps(
                {
                    "spreadsheet_title": spreadsheet.title,
                    "tabs": [ws.title for ws in spreadsheet.worksheets()],
                    "raw_row_count": max(raw_ws.row_count - 1, 0),
                },
                ensure_ascii=False,
            )
        )
        return

    if args.bootstrap_only:
        write_key_value_rows(state_ws, current_state_rows(config, 0, "bootstrapped"))
        print("[OK] Created or validated required worksheets.")
        return

    imported_posts, notes, source_name = load_posts_from_source(args, config, state)
    if not imported_posts:
        if args.allow_empty:
            state_rows = current_state_rows(config, 0, "empty_noop")
            state_rows.append({"key": "last_collect_source", "value": source_name, "updated_at": as_iso(now_jst())})
            write_key_value_rows(state_ws, state_rows)
            print("[INFO] No input posts were provided. Sheet bootstrap completed.")
            return
        raise RuntimeError(
            "No post import source was provided. Supply --input-json, X credentials, X_POSTS_JSON, or data/x_posts.json."
        )

    existing_index = {}
    existing_rows = raw_ws.get_all_records(default_blank="")
    for row in existing_rows:
        key = raw_key(row)
        if key:
            existing_index[key] = row

    normalized_rows = []
    for post in imported_posts:
        key = raw_key(post)
        existing = existing_index.get(key, {})
        normalized = normalize_post(post, existing, config)
        normalized_rows.append(normalized)

    upsert_rows(raw_ws, RAW_HEADERS, "post_id", normalized_rows)
    state_rows = current_state_rows(config, len(normalized_rows), "imported")
    state_rows.append({"key": "last_collect_source", "value": source_name, "updated_at": as_iso(now_jst())})
    state_rows.append({"key": "last_successful_collect_at", "value": as_iso(now_jst()), "updated_at": as_iso(now_jst())})
    if notes:
        state_rows.append({"key": "last_collect_notes", "value": " / ".join(notes), "updated_at": as_iso(now_jst())})
    write_key_value_rows(state_ws, state_rows)
    print(f"[OK] Upserted {len(normalized_rows)} rows into {config['sheet_tabs']['raw_posts']}.")


if __name__ == "__main__":
    run()
