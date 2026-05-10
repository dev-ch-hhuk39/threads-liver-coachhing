import base64
import json
import os
import tempfile
import time
from typing import Dict, Iterable, List, Sequence, Tuple

import gspread
from gspread.exceptions import APIError


def column_letter(index: int) -> str:
    result = []
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def sanitize_cell(value):
    text = str(value)
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def get_gspread_client():
    b64 = os.environ.get("SA_JSON_BASE64", "").strip()
    raw = os.environ.get("GCP_SA_JSON", "").strip()

    if b64:
        decoded = base64.b64decode(b64)
        tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="wb")
        try:
            tmp.write(decoded)
            tmp.close()
            return gspread.service_account(filename=tmp.name)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    if raw:
        from google.oauth2.service_account import Credentials

        info = json.loads(raw)
        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)

    return gspread.service_account()


def open_spreadsheet():
    gc = get_gspread_client()
    sheet_url = os.environ.get("SHEET_URL", "").strip()
    sheet_id = os.environ.get("SHEET_ID", "").strip()

    if sheet_url:
        return gc.open_by_url(sheet_url)
    if sheet_id:
        return gc.open_by_key(sheet_id)
    raise RuntimeError("SHEET_URL or SHEET_ID must be set")


def get_or_create_worksheet(spreadsheet, title: str, rows: int = 2000, cols: int = 40):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_headers(ws, headers: Sequence[str]) -> List[str]:
    values = ws.get_all_values()
    if not values:
        retrying_update(ws, range_name="1:1", values=[list(headers)], raw=True)
        return list(headers)

    current = values[0]
    changed = False
    for header in headers:
        if header not in current:
            current.append(header)
            changed = True

    if changed:
        retrying_update(ws, range_name="1:1", values=[current], raw=True)
    return current


def ensure_exact_headers(ws, headers: Sequence[str]) -> List[str]:
    values = ws.get_all_values()
    current = values[0] if values else []
    expected = list(headers)
    if current != expected:
        retrying_update(ws, range_name="1:1", values=[expected], raw=True)
    return expected


def records_with_row_numbers(ws, headers: Sequence[str]) -> List[Tuple[int, Dict[str, str]]]:
    values = ws.get_all_values()
    if len(values) <= 1:
        return []

    rows = []
    for row_number, raw in enumerate(values[1:], start=2):
        row = {header: (raw[idx] if idx < len(raw) else "") for idx, header in enumerate(headers)}
        rows.append((row_number, row))
    return rows


def replace_sheet(ws, headers: Sequence[str], rows: Iterable[Sequence[str]]):
    payload = [[sanitize_cell(cell) for cell in list(headers)]]
    payload.extend([[sanitize_cell(cell) for cell in list(row)] for row in rows])
    ws.clear()
    retrying_update(ws, range_name="A1", values=payload, raw=True)


def retrying_update(ws, *, range_name: str, values: Sequence[Sequence[str]], raw: bool = True):
    delays = [1, 2, 4, 8]
    last_error = None
    for attempt, delay in enumerate(delays, start=1):
        try:
            return ws.update(range_name=range_name, values=values, raw=raw)
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code != 429 or attempt == len(delays):
                raise
            last_error = exc
            time.sleep(delay)
    if last_error:
        raise last_error


def retrying_append_rows(ws, rows: Sequence[Sequence[str]], value_input_option: str = "RAW"):
    delays = [1, 2, 4, 8]
    last_error = None
    for attempt, delay in enumerate(delays, start=1):
        try:
            return ws.append_rows(list(rows), value_input_option=value_input_option)
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code != 429 or attempt == len(delays):
                raise
            last_error = exc
            time.sleep(delay)
    if last_error:
        raise last_error


def chunked_rows(rows: Sequence[Sequence[str]], chunk_size: int = 200) -> Iterable[Sequence[Sequence[str]]]:
    for idx in range(0, len(rows), chunk_size):
        yield rows[idx : idx + chunk_size]


def upsert_rows(
    ws,
    headers: Sequence[str],
    unique_header: str,
    rows: Iterable[Dict[str, str]],
):
    header_list = ensure_headers(ws, headers)
    existing = records_with_row_numbers(ws, header_list)
    index = {}
    for row_number, row in existing:
        key = str(row.get(unique_header, "")).strip()
        if key:
            index[key] = (row_number, row)

    appended: List[List[str]] = []
    pending_updates: List[Tuple[int, List[str]]] = []
    for row in rows:
        key = str(row.get(unique_header, "")).strip()
        if not key:
            continue

        ordered = [sanitize_cell(row.get(header, "")) for header in header_list]
        if key in index:
            row_number, _ = index[key]
            pending_updates.append((row_number, ordered))
        else:
            appended.append(ordered)
            index[key] = (-1, row)

    if pending_updates:
        end_col = column_letter(len(header_list))
        pending_updates.sort(key=lambda item: item[0])
        range_start = pending_updates[0][0]
        previous_row = pending_updates[0][0] - 1
        batch_rows: List[List[str]] = []

        def flush_batch(start_row: int, batch: List[List[str]]):
            if not batch:
                return
            end_row = start_row + len(batch) - 1
            retrying_update(
                ws,
                range_name=f"A{start_row}:{end_col}{end_row}",
                values=batch,
                raw=True,
            )

        current_start = range_start
        for row_number, ordered in pending_updates:
            if row_number != previous_row + 1 and batch_rows:
                flush_batch(current_start, batch_rows)
                current_start = row_number
                batch_rows = []
            batch_rows.append(ordered)
            previous_row = row_number
        flush_batch(current_start, batch_rows)

    if appended:
        for chunk in chunked_rows(appended):
            retrying_append_rows(ws, list(chunk), value_input_option="RAW")


def write_key_value_rows(ws, rows: Iterable[Dict[str, str]]):
    headers = ["key", "value", "updated_at"]
    replace_sheet(
        ws,
        headers,
        ([row.get("key", ""), row.get("value", ""), row.get("updated_at", "")] for row in rows),
    )


def apply_dropdown_validation(ws, headers: Sequence[str], dropdowns: Dict[str, Sequence[str]]):
    header_index = {header: idx for idx, header in enumerate(headers)}
    requests = []
    requests.append(
        {
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(headers),
                },
                "rule": None,
            }
        }
    )
    for header, values in dropdowns.items():
        if header not in header_index:
            continue
        col = header_index[header]
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": str(value)} for value in values],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            }
        )
    if requests:
        ws.spreadsheet.batch_update({"requests": requests})
