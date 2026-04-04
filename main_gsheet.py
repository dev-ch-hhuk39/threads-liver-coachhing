import os, json, time, requests
from datetime import datetime, timezone, timedelta
import gspread
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

API_BASE = "https://graph.threads.net/v1.0"
JST = timezone(timedelta(hours=9))
HEADERS = ["text", "image_url", "alt_text", "link_attachment",
           "reply_control", "topic_tag", "location_id",
           "status", "posted_at", "error"]


def auth_headers(token):
    return {"Authorization": f"Bearer {token}"}


def open_ws(sheet_url: str, sheet_tab: str):
    gc = gspread.service_account()
    sh = gc.open_by_url(sheet_url)
    ws = sh.worksheet(sheet_tab) if sheet_tab else sh.sheet1
    first = ws.row_values(1)
    if [c.strip().lower() for c in first] != HEADERS:
        ws.update("A1", [HEADERS])
    return ws


def find_next_row(ws):
    records = ws.get_all_records(default_blank="")
    for idx, row in enumerate(records, start=2):
        text = (row.get("text") or "").strip()
        image_url = (row.get("image_url") or "").strip()
        status = (row.get("status") or "").strip().lower()
        if (text or image_url) and status not in ("posted", "done", "済", "posted✅"):
            return idx, row
    return None, None


@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 4), reraise=True)
def create_container(user_id, token, payload):
    url = f"{API_BASE}/{user_id}/threads"
    resp = requests.post(url, headers=auth_headers(token), json=payload, timeout=30)
    if resp.status_code >= 400:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 4), reraise=True)
def publish_container(user_id, token, creation_id):
    url = f"{API_BASE}/{user_id}/threads_publish"
    resp = requests.post(url, headers=auth_headers(token),
                         params={"creation_id": creation_id}, timeout=30)
    if resp.status_code >= 400:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    return resp.json()


def post_one(user_id, token, row):
    text = (row.get("text") or "").strip()
    image_url = (row.get("image_url") or "").strip()

    if image_url:
        payload = {"media_type": "IMAGE", "image_url": image_url, "text": text}
        if row.get("alt_text"):      payload["alt_text"] = row["alt_text"]
        if row.get("reply_control"): payload["reply_control"] = row["reply_control"]
        if row.get("topic_tag"):     payload["topic_tag"] = row["topic_tag"]
        if row.get("location_id"):   payload["location_id"] = row["location_id"]
        data = create_container(user_id, token, payload)
        cid = data.get("id")
        time.sleep(2)
        pub = publish_container(user_id, token, cid)
        return {"status": "published", "container_id": cid, "media_type": "IMAGE", "publish": pub}
    else:
        payload = {"media_type": "TEXT", "text": text, "auto_publish_text": True}
        if row.get("link_attachment"): payload["link_attachment"] = row["link_attachment"]
        if row.get("reply_control"):   payload["reply_control"] = row["reply_control"]
        if row.get("topic_tag"):       payload["topic_tag"] = row["topic_tag"]
        if row.get("location_id"):     payload["location_id"] = row["location_id"]
        data = create_container(user_id, token, payload)
        return {"status": "published", "container_id": data.get("id"), "media_type": "TEXT"}


def update_result(ws, row_idx, status, err=""):
    col = {h: i + 1 for i, h in enumerate(HEADERS)}
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    ws.update_cell(row_idx, col["status"], status)
    ws.update_cell(row_idx, col["posted_at"], now if status == "posted" else "")
    ws.update_cell(row_idx, col["error"], err[:3000] if err else "")


def main():
    token = os.environ.get("THREADS_ACCESS_TOKEN", "").strip()
    user_id = os.environ.get("THREADS_USER_ID", "").strip()
    sheet_url = os.environ.get("SHEET_URL", "").strip()
    sheet_tab = os.environ.get("SHEET_TAB", "").strip()

    if not token or not user_id:
        raise RuntimeError("THREADS_ACCESS_TOKEN / THREADS_USER_ID が未設定")
    if not sheet_url:
        raise RuntimeError("SHEET_URL が未設定")

    ws = open_ws(sheet_url, sheet_tab)
    row_idx, row = find_next_row(ws)
    if not row_idx:
        print(json.dumps({"ok": True, "skipped": "no-row"}))
        return

    try:
        res = post_one(user_id, token, row)
        update_result(ws, row_idx, "posted")
        print(json.dumps({"ok": True, "row_idx": row_idx, "res": res}, ensure_ascii=False))
    except Exception as e:
        update_result(ws, row_idx, "failed", str(e))
        print(json.dumps({"ok": False, "row_idx": row_idx, "err": str(e)[:500]}, ensure_ascii=False))
        raise


if __name__ == "__main__":
    main()
