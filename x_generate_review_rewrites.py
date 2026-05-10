import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import gspread
import requests

from x_sheet_schema import REVIEW_DROPDOWNS, REVIEW_HEADERS
from x_sheet_utils import apply_dropdown_validation, ensure_exact_headers, get_or_create_worksheet, open_spreadsheet

JST = timezone(timedelta(hours=9))
DEFAULT_MODEL = os.environ.get("GEMINI_REWRITE_MODEL", "gemini-2.5-flash").strip()
MAX_ROWS_PER_RUN = int(os.environ.get("GEMINI_REWRITE_MAX_ROWS", "3"))
SECONDS_BETWEEN_CALLS = float(os.environ.get("GEMINI_REWRITE_INTERVAL_SECONDS", "12"))
CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def now_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def clean_text(text: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", str(text or "").strip())


def remove_fences(text: str) -> str:
    stripped = str(text or "").strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", stripped)
        stripped = re.sub(r"\n?```$", "", stripped)
    return stripped.strip()


def parse_plain_response(text: str) -> str:
    cleaned = remove_fences(text)
    cleaned = re.sub(r"^\[[AB]\]\s*", "", cleaned).strip()
    cleaned = re.sub(r"^案[AB][:：]\s*", "", cleaned).strip()
    if not cleaned:
        raise RuntimeError("Gemini rewrite response was empty.")
    return clean_text(cleaned)


def eligible_rows(ws) -> List[Tuple[int, Dict[str, str]]]:
    records = ws.get_all_records(expected_headers=REVIEW_HEADERS, default_blank="")
    output: List[Tuple[int, Dict[str, str]]] = []
    for row_idx, row in enumerate(records, start=2):
        if not str(row.get("投稿ID", "")).strip():
            continue
        if str(row.get("投稿可否", "")).strip() == "投稿OK":
            continue
        if str(row.get("採用案", "")).strip() != "未選択":
            continue
        if str(row.get("転載可否", "")).strip() == "NG":
            continue
        if str(row.get("リライト方針A", "")).strip().startswith("AI"):
            continue
        output.append((row_idx, row))
    return output


def build_prompt(row: Dict[str, str], mode: str) -> str:
    mode_instruction = (
        "A案: 元文の意味・固有名詞・熱量をできるだけ残し、読みやすさだけを整えてください。"
        if mode == "A"
        else "B案: 元文の主張を活かしつつ、冒頭フックを少し強めて再構成してください。"
    )
    return f"""
あなたは日本語SNS運用の編集者です。以下の元投稿を、転載用の下書きとしてリライトしてください。

要件:
- 出力はプレーンテキストのみ
- 見出し、タグ、説明、引用符は書かない
- リライト本文だけ返す
- 今回は {mode}案 を作る
- {mode_instruction}
- 文体は常体にする
- 敬体（です・ます調）は使わない
- 語尾は次の方向に寄せる: 「〜。」「〜なんよ。」「〜なんよね。」「〜だよ。」
- 必要なら体言止めも使ってよい
- ただし不自然に同じ語尾を連発しない
- 元文の熱量やSNSっぽさは残す
- どちらも source の t.co URL は除去する
- ハッシュタグは付けない
- 文字数は日本語で自然な範囲に収める
- 固有名詞がフックとして重要そうなら残す
- 誹謗中傷を強めない

参考情報:
- メディア種別: {row.get("メディア種別", "")}
- 伸びた理由: {row.get("伸びた理由", "")}
- 元投稿URL: {row.get("元投稿URL", "")}

元投稿:
{row.get("投稿本文", "")}
""".strip()


def call_gemini(api_key: str, prompt_text: str, model_name: str = DEFAULT_MODEL) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "generationConfig": {
            "temperature": 0.8,
            "maxOutputTokens": 800,
        },
    }
    response = requests.post(
        url,
        params={"key": api_key},
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    if response.status_code == 429:
        raise RuntimeError(f"Gemini rate limit reached: {response.text[:500]}")
    response.raise_for_status()
    data = response.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    return parse_plain_response(text)


def update_row(ws, row_idx: int, updates: Dict[str, str]):
    header_map = {header: idx + 1 for idx, header in enumerate(REVIEW_HEADERS)}
    cells = [
        gspread.Cell(row=row_idx, col=header_map[key], value=value)
        for key, value in updates.items()
        if key in header_map
    ]
    if cells:
        ws.update_cells(cells, value_input_option="RAW")


def run():
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required")

    spreadsheet = open_spreadsheet()
    config = load_config()
    review_ws = get_or_create_worksheet(spreadsheet, config["sheet_tabs"]["review"], rows=5000, cols=len(REVIEW_HEADERS) + 5)
    ensure_exact_headers(review_ws, REVIEW_HEADERS)
    apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)

    targets = eligible_rows(review_ws)[:MAX_ROWS_PER_RUN]
    rewritten = 0
    failures: List[str] = []

    for row_idx, row in targets:
        try:
            rewrite_a = call_gemini(api_key, build_prompt(row, "A"))
            time.sleep(SECONDS_BETWEEN_CALLS)
            rewrite_b = call_gemini(api_key, build_prompt(row, "B"))
            if not rewrite_a or not rewrite_b:
                raise RuntimeError("Gemini returned empty rewrites.")
            update_row(
                review_ws,
                row_idx,
                {
                    "リライト方針A": "AI軽整形",
                    "リライト案A": rewrite_a,
                    "リライト方針B": "AI再構成",
                    "リライト案B": rewrite_b,
                    "最終同期日時": now_str(),
                },
            )
            rewritten += 1
            time.sleep(SECONDS_BETWEEN_CALLS)
        except Exception as exc:
            failures.append(f"row={row_idx} post_id={row.get('投稿ID', '')} error={exc}")

    print(json.dumps({"rewritten": rewritten, "attempted": len(targets), "failures": failures[:20]}, ensure_ascii=False))


if __name__ == "__main__":
    run()
