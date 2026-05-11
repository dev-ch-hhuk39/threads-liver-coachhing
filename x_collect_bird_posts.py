import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List


CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")
OUTPUT_PATH = Path(__file__).with_name("data").joinpath("x_posts.json")


def load_config() -> Dict[str, Any]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def nested(data: Dict[str, Any], *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_tweets(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("tweets", "data", "items", "results"):
        items = payload.get(key)
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return [payload]


def best_video_url(media: Dict[str, Any]) -> str:
    variants = as_list(nested(media, "video_info", "variants") or media.get("variants"))
    candidates = []
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        url = first_text(variant.get("url"))
        if not url:
            continue
        bitrate = to_int(variant.get("bitrate"))
        content_type = first_text(variant.get("content_type"), variant.get("type"))
        if "mp4" in content_type or ".mp4" in url:
            candidates.append((bitrate, url))
    if not candidates:
        return ""
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def media_urls(tweet: Dict[str, Any]) -> Dict[str, List[str]]:
    media_items: List[Any] = []
    media_items.extend(as_list(tweet.get("media")))
    media_items.extend(as_list(nested(tweet, "extended_entities", "media")))
    media_items.extend(as_list(nested(tweet, "legacy", "extended_entities", "media")))

    image_urls: List[str] = []
    video_urls: List[str] = []
    for item in media_items:
        if not isinstance(item, dict):
            continue
        media_type = first_text(item.get("type"), item.get("media_type")).lower()
        image_url = first_text(item.get("media_url_https"), item.get("media_url"), item.get("url"))
        video_url = best_video_url(item)
        if media_type in {"video", "animated_gif"} or video_url:
            if video_url:
                video_urls.append(video_url)
            if image_url:
                image_urls.append(image_url)
        elif image_url:
            image_urls.append(image_url)
    return {"image_urls": sorted(set(image_urls)), "video_urls": sorted(set(video_urls))}


def normalize_tweet(tweet: Dict[str, Any], fallback_handle: str) -> Dict[str, Any]:
    user = tweet.get("user") if isinstance(tweet.get("user"), dict) else {}
    author = tweet.get("author") if isinstance(tweet.get("author"), dict) else {}
    legacy = tweet.get("legacy") if isinstance(tweet.get("legacy"), dict) else {}
    media = media_urls(tweet)

    post_id = first_text(
        tweet.get("id"),
        tweet.get("id_str"),
        tweet.get("rest_id"),
        legacy.get("id_str"),
    )
    handle = first_text(
        user.get("screen_name"),
        user.get("username"),
        author.get("screen_name"),
        author.get("username"),
        fallback_handle,
    ).lstrip("@")

    return {
        "post_id": post_id,
        "post_url": f"https://x.com/{handle}/status/{post_id}" if handle and post_id else "",
        "account_name": first_text(user.get("name"), author.get("name"), handle),
        "account_handle": handle,
        "account_id": first_text(user.get("id_str"), user.get("id"), author.get("id"), author.get("rest_id")),
        "account_url": f"https://x.com/{handle}" if handle else "",
        "text": first_text(tweet.get("text"), tweet.get("full_text"), tweet.get("plainText"), legacy.get("full_text")),
        "posted_at": first_text(tweet.get("created_at"), tweet.get("createdAt"), legacy.get("created_at")),
        "like_count": to_int(tweet.get("favorite_count") or tweet.get("like_count") or legacy.get("favorite_count")),
        "retweet_count": to_int(tweet.get("retweet_count") or legacy.get("retweet_count")),
        "reply_count": to_int(tweet.get("reply_count") or legacy.get("reply_count")),
        "quote_count": to_int(tweet.get("quote_count") or legacy.get("quote_count")),
        "impression_count": to_int(tweet.get("view_count") or nested(tweet, "views", "count")),
        "image_urls": media["image_urls"],
        "video_urls": media["video_urls"],
        "has_video": bool(media["video_urls"]),
        "image_count": len(media["image_urls"]),
        "matched_accounts": [handle] if handle else [],
        "source_types": ["account_monitor"],
        "raw": tweet,
    }


def run_bird_for_handle(handle: str, limit: int) -> List[Dict[str, Any]]:
    auth_token = os.environ.get("X_AUTH_TOKEN", "").strip()
    ct0 = os.environ.get("X_CT0", "").strip()
    if not auth_token or not ct0:
        print("[INFO] X_AUTH_TOKEN / X_CT0 が未設定のため bird 収集はスキップします。")
        return []

    command = [
        "bird",
        "--auth-token",
        auth_token,
        "--ct0",
        ct0,
        "user-tweets",
        f"@{handle.lstrip('@')}",
        "-n",
        str(limit),
        "--json",
        "--no-color",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)
    return [normalize_tweet(tweet, handle) for tweet in extract_tweets(payload)]


def unique_posts(posts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for post in posts:
        key = first_text(post.get("post_id"), post.get("post_url"))
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(post)
    return unique


def main() -> None:
    config = load_config()
    limit = int(config.get("collection", {}).get("max_posts_per_account", 50))
    posts: List[Dict[str, Any]] = []
    for account in config.get("monitor_accounts", []):
        handle = str(account.get("handle", "")).strip()
        if not handle:
            continue
        account_posts = run_bird_for_handle(handle, limit)
        print(f"[INFO] bird @{handle}: {len(account_posts)}件")
        posts.extend(account_posts)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(unique_posts(posts), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] bird収集データを書き出しました: {OUTPUT_PATH} ({len(unique_posts(posts))}件)")


if __name__ == "__main__":
    main()
