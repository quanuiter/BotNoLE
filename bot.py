"""
NhatBrain Bot v2 — Telegram + Gemini AI + Google Sheets
Kiến trúc Obsidian-inspired:
  • Notes có title / content / tags / backlinks
  • Tasks có priority / recurrence / linked note
  • Daily notes tự động
  • Conversation context (Gemini nhớ ngữ cảnh)
  • In-memory cache giảm Sheets API calls
  • Reminders persist qua restart

Schema Google Sheets (4 sheets):
  Tasks   — id, chat_id, title, description, due_datetime, priority, tags,
             status, note_id, recurrence, created_at, updated_at
  Notes   — id, chat_id, title, content, tags, linked_ids,
             daily_date, created_at, updated_at
  Context — chat_id, history_json, updated_at
  (Links tính dynamically từ linked_ids, không cần sheet riêng)
"""

print("[startup] NhatBrain v2 starting...")

import asyncio
import json
import os
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import gspread
from google.oauth2.service_account import Credentials
from google import genai
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
print("[init] Reading env vars...")
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
GEMINI_API_KEY    = os.environ["GEMINI_API_KEY"]
GSHEET_CREDS_JSON = os.environ["GSHEET_CREDS_JSON"]
GSHEET_ID         = os.environ["GSHEET_ID"]
GEMINI_MODEL      = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
CACHE_TTL         = int(os.environ.get("CACHE_TTL", 45))   # seconds
MAX_HISTORY       = int(os.environ.get("MAX_HISTORY", 8))  # messages per user
print("[init] Env OK")

# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI
# ═══════════════════════════════════════════════════════════════════════════════
print("[init] Connecting Gemini...")
gemini_client = genai.Client(api_key=GEMINI_API_KEY)
print(f"[init] Gemini OK — model: {GEMINI_MODEL}")

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════════════════════
print("[init] Connecting Google Sheets...")
_creds = Credentials.from_service_account_info(
    json.loads(GSHEET_CREDS_JSON),
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(_creds)
sh = gc.open_by_key(GSHEET_ID)
print("[init] Google Sheets OK")

SHEET_SCHEMAS = {
    "Tasks": [
        "id", "chat_id", "title", "description", "due_datetime",
        "priority", "tags", "status", "note_id", "recurrence",
        "created_at", "updated_at",
    ],
    "Notes": [
        "id", "chat_id", "title", "content", "tags",
        "linked_ids", "daily_date", "created_at", "updated_at",
    ],
    "Context": ["chat_id", "history_json", "updated_at"],
}

ws: dict[str, gspread.Worksheet] = {}
for _name, _headers in SHEET_SCHEMAS.items():
    try:
        ws[_name] = sh.worksheet(_name)
    except gspread.WorksheetNotFound:
        ws[_name] = sh.add_worksheet(title=_name, rows=2000, cols=len(_headers))
        ws[_name].append_row(_headers)
    print(f"[init] Sheet '{_name}' OK")


# ═══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE  (giảm Sheets API calls ~80%)
# ═══════════════════════════════════════════════════════════════════════════════
_cache: dict[str, list] = {}
_cache_ts: dict[str, float] = {}


def _cache_get(key: str) -> list | None:
    if key in _cache and time.monotonic() - _cache_ts.get(key, 0) < CACHE_TTL:
        return _cache[key]
    return None


def _cache_set(key: str, val: list):
    _cache[key] = val
    _cache_ts[key] = time.monotonic()


def _cache_bust(sheet_name: str):
    """Xóa cache khi có thay đổi dữ liệu"""
    _cache.pop(sheet_name, None)
    _cache_ts.pop(sheet_name, None)


def _all_rows(sheet_name: str) -> list[dict]:
    cached = _cache_get(sheet_name)
    if cached is not None:
        return cached
    records = ws[sheet_name].get_all_records()
    _cache_set(sheet_name, records)
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET LOW-LEVEL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _next_id(sheet_name: str) -> int:
    ids = [int(r["id"]) for r in _all_rows(sheet_name) if str(r.get("id", "")).isdigit()]
    return max(ids) + 1 if ids else 1


def _find_row(sheet_name: str, field: str, value) -> int | None:
    """Trả về row index 1-based (kể header)"""
    for i, r in enumerate(_all_rows(sheet_name)):
        if str(r.get(field, "")) == str(value):
            return i + 2   # header = row 1, data bắt đầu row 2
    return None


def _update_fields(sheet_name: str, record_id, updates: dict):
    """Cập nhật nhiều field của 1 record theo id"""
    headers = ws[sheet_name].row_values(1)
    row = _find_row(sheet_name, "id", record_id)
    if not row:
        return
    for field, value in updates.items():
        if field in headers:
            ws[sheet_name].update_cell(row, headers.index(field) + 1, value)
    _cache_bust(sheet_name)


# ═══════════════════════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════════════════════
DAYS_VI = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
PRIORITY_EMOJI = {1: "🔴", 2: "🟡", 3: "🟢"}
STATUS_EMOJI   = {"pending": "⏳", "done": "✅", "cancelled": "❌"}
HTML = "HTML"   # parse_mode constant

def esc(text) -> str:
    """Escape HTML special chars — chỉ 3 ký tự, không bao giờ crash."""
    return str(text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def b(text) -> str:   return f"<b>{esc(text)}</b>"
def i(text) -> str:   return f"<i>{esc(text)}</i>"
def code(text) -> str: return f"<code>{esc(text)}</code>"


def now() -> datetime:
    return datetime.now().replace(tzinfo=None)


def today_str() -> str:
    return now().strftime("%Y-%m-%d")


def parse_dt(s) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s)).replace(tzinfo=None)
    except ValueError:
        return None


def fmt_dt(s) -> str:
    dt = parse_dt(s)
    return dt.strftime("%H:%M %d/%m/%Y") if dt else "—"


def fmt_tags(tags_str: str) -> str:
    if not tags_str:
        return ""
    return " ".join(f"#{t.strip()}" for t in str(tags_str).split(",") if t.strip())


def parse_tags(tags_str: str) -> list[str]:
    return [t.strip().lower() for t in str(tags_str or "").split(",") if t.strip()]


# ═══════════════════════════════════════════════════════════════════════════════
# CONTEXT — Conversation history per user
# ═══════════════════════════════════════════════════════════════════════════════
_ctx_mem: dict[str, list[dict]] = {}   # in-memory fast layer


def _load_context(chat_id) -> list[dict]:
    if str(chat_id) in _ctx_mem:
        return _ctx_mem[str(chat_id)]
    for r in ws["Context"].get_all_records():
        if str(r["chat_id"]) == str(chat_id):
            try:
                history = json.loads(r["history_json"])
                _ctx_mem[str(chat_id)] = history
                return history
            except Exception:
                pass
    return []


def _save_context(chat_id, history: list[dict]):
    history = history[-MAX_HISTORY:]
    _ctx_mem[str(chat_id)] = history
    payload = json.dumps(history, ensure_ascii=False)
    rows = ws["Context"].get_all_records()
    for i, r in enumerate(rows):
        if str(r["chat_id"]) == str(chat_id):
            ws["Context"].update_cell(i + 2, 2, payload)
            ws["Context"].update_cell(i + 2, 3, now().isoformat())
            return
    ws["Context"].append_row([chat_id, payload, now().isoformat()])


def push_ctx(chat_id, role: str, content: str):
    history = _load_context(chat_id)
    history.append({"role": role, "content": content, "ts": now().isoformat()})
    _save_context(chat_id, history)


# ═══════════════════════════════════════════════════════════════════════════════
# NOTES API
# ═══════════════════════════════════════════════════════════════════════════════
def _daily_id(date_str: str) -> str:
    """ID dạng D20260307 — nhìn biết ngay ngày nào."""
    return "D" + date_str.replace("-", "")


def note_save(chat_id, title: str, content: str,
              tags: str = "", linked_ids: str = "",
              daily_date: str = "") -> str:
    """
    Daily note  → ID = D20260307 (date-based, duy nhất theo ngày).
                   Nếu hôm nay đã có → append thêm entry mới vào.
    Note thường → ID số tự nhiên như cũ.
    """
    if daily_date:
        nid      = _daily_id(daily_date)
        existing = note_get_by_id(nid)
        if existing:
            ts          = now().strftime("%H:%M")
            new_content = existing["content"] + f"\n\n— {ts} —\n{content}"
            _update_fields("Notes", nid, {
                "content":    new_content,
                "updated_at": now().isoformat(),
            })
            return nid
        ws["Notes"].append_row([
            nid, chat_id, title, content, tags,
            linked_ids, daily_date,
            now().isoformat(), now().isoformat(),
        ])
        _cache_bust("Notes")
        return nid

    # Note thường: ID số tự nhiên
    nid = str(_next_id("Notes"))
    ws["Notes"].append_row([
        nid, chat_id, title, content, tags,
        linked_ids, "",
        now().isoformat(), now().isoformat(),
    ])
    _cache_bust("Notes")
    return nid


def note_update(note_id, updates: dict):
    updates["updated_at"] = now().isoformat()
    _update_fields("Notes", note_id, updates)


def note_delete(note_id):
    row = _find_row("Notes", "id", note_id)
    if row:
        ws["Notes"].delete_rows(row)
        _cache_bust("Notes")


def note_get(chat_id, tag: str = None, keyword: str = None) -> list[dict]:
    result = []
    for r in _all_rows("Notes"):
        if str(r["chat_id"]) != str(chat_id):
            continue
        if tag and tag.lower() not in parse_tags(r.get("tags", "")):
            continue
        if keyword and keyword.lower() not in (
            str(r.get("title", "")) + str(r.get("content", ""))
        ).lower():
            continue
        result.append(r)
    return result


def note_get_by_id(note_id) -> dict | None:
    return next(
        (r for r in _all_rows("Notes") if str(r["id"]) == str(note_id)),
        None,
    )


def note_get_daily(chat_id, date_str: str) -> dict | None:
    """Lookup daily note theo date-based ID D20260307."""
    nid = _daily_id(date_str)
    note = note_get_by_id(nid)
    # Chỉ trả về nếu thuộc đúng chat_id
    if note and str(note.get("chat_id", "")) == str(chat_id):
        return note
    return None


def note_get_all_daily(chat_id) -> list[dict]:
    """Trả về tất cả daily notes theo thứ tự ngày, mới nhất trước."""
    result = [
        r for r in _all_rows("Notes")
        if str(r.get("chat_id", "")) == str(chat_id)
        and str(r.get("id", "")).startswith("D")
    ]
    return sorted(result, key=lambda r: r["id"], reverse=True)


def note_backlinks(note_id) -> list[dict]:
    """Tìm tất cả notes khác đang link đến note này"""
    return [
        r for r in _all_rows("Notes")
        if str(note_id) in [x.strip() for x in str(r.get("linked_ids", "")).split(",")]
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# TASKS API
# ═══════════════════════════════════════════════════════════════════════════════
def task_save(chat_id, title: str, description: str = "",
              due_dt: datetime = None, priority: int = 2,
              tags: str = "", note_id: str = "",
              recurrence: str = "") -> int:
    tid = _next_id("Tasks")
    ws["Tasks"].append_row([
        tid, chat_id, title, description,
        due_dt.isoformat() if due_dt else "",
        priority, tags, "pending", note_id, recurrence,
        now().isoformat(), now().isoformat(),
    ])
    _cache_bust("Tasks")
    return tid


def task_update(task_id, updates: dict):
    updates["updated_at"] = now().isoformat()
    _update_fields("Tasks", task_id, updates)


def task_set_status(task_id, status: str):
    task_update(task_id, {"status": status})


def task_get(chat_id, status: str = "pending",
             tag: str = None,
             date_from: datetime = None,
             date_to: datetime = None) -> list[dict]:
    result = []
    for r in _all_rows("Tasks"):
        if str(r["chat_id"]) != str(chat_id):
            continue
        if status and str(r.get("status", "")) != status:
            continue
        if tag and tag.lower() not in parse_tags(r.get("tags", "")):
            continue
        if date_from or date_to:
            dt = parse_dt(r.get("due_datetime", ""))
            if not dt:
                continue
            if date_from and dt < date_from:
                continue
            if date_to and dt > date_to:
                continue
        result.append(r)
    # Sắp theo due_datetime rồi priority
    return sorted(
        result,
        key=lambda x: (x.get("due_datetime") or "9999", int(x.get("priority", 2))),
    )


def task_get_by_id(task_id) -> dict | None:
    return next(
        (r for r in _all_rows("Tasks") if str(r["id"]) == str(task_id)),
        None,
    )


def task_search(chat_id, keyword: str) -> list[dict]:
    return [
        r for r in _all_rows("Tasks")
        if str(r["chat_id"]) == str(chat_id)
        and str(r.get("status", "")) == "pending"
        and keyword.lower() in (
            str(r.get("title", "")) + str(r.get("description", ""))
        ).lower()
    ]


def task_get_all_future() -> list[dict]:
    n = now()
    return [
        r for r in _all_rows("Tasks")
        if str(r.get("status", "")) == "pending"
        and r.get("due_datetime")
        and parse_dt(r["due_datetime"]) > n
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH SERVER
# ═══════════════════════════════════════════════════════════════════════════════
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"NhatBrain v2 OK")
    def log_message(self, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()


# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI PARSER — context-aware prompt
# ═══════════════════════════════════════════════════════════════════════════════
def _build_prompt(user_text: str, chat_id,
                  recent_notes: list, recent_tasks: list,
                  history: list) -> str:
    n = now()
    now_str = f"{DAYS_VI[n.weekday()]}, {n.strftime('%d/%m/%Y %H:%M')}"

    # Tóm tắt context để Gemini hiểu ngữ cảnh hội thoại
    ctx_lines = []
    for h in history[-4:]:
        role = "Người dùng" if h["role"] == "user" else "Bot"
        ctx_lines.append(f"  {role}: {h['content'][:120]}")
    ctx_block = "\nHội thoại gần đây:\n" + "\n".join(ctx_lines) if ctx_lines else ""

    notes_block = ""
    if recent_notes:
        notes_block = "\nNotes hiện có:\n" + "\n".join(
            f"  [{r.get('id','')}] '{r.get('title','')}' tags={r.get('tags','')} "
            f"links=[{r.get('linked_ids','')}]"
            for r in recent_notes[:6]
            if r.get('id')   # bỏ qua row rỗng/header lọt vào
        )

    tasks_block = ""
    if recent_tasks:
        tasks_block = "\nTasks pending:\n" + "\n".join(
            f"  [{r.get('id','')}] '{r.get('title','')}' due={str(r.get('due_datetime',''))[:16]} "
            f"p={r.get('priority',2)}"
            for r in recent_tasks[:6]
            if r.get('id')
        )

    return f"""Mày là NhatBrain — bộ não thứ 2 kiểu Obsidian. Phân tích tin nhắn tiếng Việt → trả về JSON duy nhất.
Thời gian hiện tại: {now_str}
{ctx_block}{notes_block}{tasks_block}

─── INTENT LIST ────────────────────────────────────────────────────────────────
  add_task      — thêm việc cần làm, nhắc nhở, lịch
  add_note      — ghi chú kiến thức, ý tưởng, danh sách, tài liệu học tập
  daily_note    — ghi nhật ký / daily note hôm nay ("hôm nay tao đã...", "daily")
  query_notes   — hỏi về ghi chú (tìm theo tag hoặc từ khoá)
  query_tasks   — xem lịch/tasks theo ngày hoặc khoảng thời gian
  update_task   — cập nhật task (đổi giờ, priority, deadline)
  delete_task   — xóa / huỷ task
  mark_done     — đánh dấu task xong
  show_note     — xem chi tiết note (kể cả backlinks)
  unknown       — câu hỏi tự do, trả lời thẳng bằng "reply"

─── RESPONSE JSON ──────────────────────────────────────────────────────────────
{{
  "intent": "<intent>",

  // ── add_task ──
  "task_title":       "tiêu đề task ngắn gọn",
  "task_description": "mô tả thêm hoặc null",
  "due_datetime":     "YYYY-MM-DDTHH:MM:SS hoặc null",
  "priority":         1,          // 1=gấp🔴 2=bình thường🟡 3=thấp🟢
  "tags":             "tag1,tag2",
  "recurrence":       "daily|weekly|monthly hoặc null",

  // ── add_note / daily_note ──
  "note_title":       "tiêu đề note",
  "note_content":     "nội dung đầy đủ (markdown ok)",
  "note_tags":        "tag1,tag2",
  "linked_note_ids":  "1,3",      // id note liên quan (từ danh sách trên), hoặc ""

  // ── query ──
  "query_tag":        "tag cần tìm hoặc null",
  "query_keyword":    "từ khoá tìm kiếm hoặc null",
  "query_date_from":  "YYYY-MM-DD hoặc null",
  "query_date_to":    "YYYY-MM-DD hoặc null",
  "query_days":       3,

  // ── update / delete / done ──
  "target_id":        123,
  "delete_keyword":   "từ khoá task cần xóa hoặc null",
  "update_fields":    {{"due_datetime": "...", "priority": 2}},

  // ── unknown ──
  "reply": "câu trả lời"
}}

─── RULES ──────────────────────────────────────────────────────────────────────
Datetime (không timezone):
  "tối nay 8h"          → hôm nay 20:00:00
  "8h30 tối nay"        → hôm nay 20:30:00
  "ngày mai 9h"         → ngày mai 09:00:00
  "thứ 7 8h tối"        → thứ 7 tuần này 20:00:00
  không có giờ          → 09:00:00

Priority:
  gấp / quan trọng / deadline gần → 1
  bình thường                      → 2
  không gấp / someday              → 3

Tags: tự suy luận từ nội dung nếu không được nói rõ.
linked_note_ids: nếu note mới liên quan đến note nào trong danh sách, điền id vào.

CHỈ JSON thuần, không markdown, không giải thích.
Tin nhắn: "{user_text}"
"""


def parse_with_gemini(user_text: str, chat_id) -> dict:
    try:
        history      = _load_context(chat_id)
        recent_notes = note_get(chat_id)[-6:]
        recent_tasks = task_get(chat_id, status="pending")[:6]
        prompt       = _build_prompt(user_text, chat_id, recent_notes, recent_tasks, history)

        res = gemini_client.models.generate_content(
            model=GEMINI_MODEL, contents=prompt
        )
        raw = re.sub(r"```json\s*|\s*```", "", res.text.strip()).strip()
        return json.loads(raw)

    except json.JSONDecodeError as e:
        raw_preview = locals().get("raw", "N/A")[:200]
        print(f"[Gemini JSON Error] {e} | raw: {raw_preview}")
        return {"intent": "unknown", "reply": "Tao bị lỗi parse, thử lại!"}
    except Exception as e:
        print(f"[Gemini Error] {e}")
        return {"intent": "unknown", "reply": "Tao bị lỗi, thử lại đi!"}


# ═══════════════════════════════════════════════════════════════════════════════
# REMINDER
# ═══════════════════════════════════════════════════════════════════════════════
async def _send_reminder(context: ContextTypes.DEFAULT_TYPE):
    d = context.job.data
    task = task_get_by_id(d["task_id"])
    if not task or str(task.get("status", "")) != "pending":
        return

    p     = int(task.get("priority", 2))
    tags  = fmt_tags(task.get("tags", ""))
    lines = [
        f"⏰ <b>Nhắc nhở!</b> {PRIORITY_EMOJI.get(p, '⚪')}",
        f"📌 {esc(task.get('title',''))}",
        tags,
        f"✅ /done {task['id']}  🗑️ /del {task['id']}",
    ]
    await context.bot.send_message(
        chat_id=d["chat_id"],
        text="\n".join(l for l in lines if l),
        parse_mode=HTML,
    )

    # Tự động reschedule nếu task lặp
    rec = task.get("recurrence", "")
    if rec:
        delta_map = {
            "daily": timedelta(days=1),
            "weekly": timedelta(weeks=1),
            "monthly": timedelta(days=30),
        }
        delta = delta_map.get(rec)
        if delta:
            old_due = parse_dt(task.get("due_datetime", ""))
            if old_due:
                new_due = old_due + delta
                task_update(task["id"], {"due_datetime": new_due.isoformat()})
                _schedule_reminder(
                    context.application,
                    d["chat_id"], task["id"], task["title"], new_due,
                )


def _schedule_reminder(app, chat_id, task_id, title, due_dt: datetime) -> bool:
    delay = (due_dt.replace(tzinfo=None) - now()).total_seconds()
    if delay <= 0:
        return False
    # Hủy job cũ cùng tên nếu có
    for j in app.job_queue.get_jobs_by_name(f"t{task_id}"):
        j.schedule_removal()
    app.job_queue.run_once(
        _send_reminder, delay,
        data={"chat_id": chat_id, "task_id": task_id, "task_text": title},
        name=f"t{task_id}",
    )
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ═══════════════════════════════════════════════════════════════════════════════
def _fmt_task(r: dict) -> str:
    p       = int(r.get("priority", 2))
    due_str = f"🕐 {fmt_dt(r.get('due_datetime', ''))}" if r.get("due_datetime") else "🕐 —"
    tags    = fmt_tags(r.get("tags", ""))
    desc    = f"\n   <i>{esc(r['description'])}</i>" if r.get("description") else ""
    rec     = f" 🔁 {esc(r.get('recurrence',''))}" if r.get("recurrence") else ""
    return (
        f"<code>{r['id']}</code> {PRIORITY_EMOJI.get(p,'⚪')} <b>{esc(r.get('title',''))}</b>{rec}"
        f"{desc}\n   {due_str} {tags}"
    ).strip()


def _fmt_daily_id(nid: str) -> str:
    """D20260307 → Thứ Bảy 07/03/2026"""
    if not str(nid).startswith("D") or len(str(nid)) != 9:
        return str(nid)
    s = str(nid)[1:]  # '20260307'
    try:
        dt = datetime.strptime(s, "%Y%m%d")
        return f"{DAYS_VI[dt.weekday()]} {dt.strftime('%d/%m/%Y')}"
    except ValueError:
        return str(nid)


def _fmt_note_line(r: dict) -> str:
    tags   = fmt_tags(r.get("tags", ""))
    linked = f" 🔗{r.get('linked_ids','')}" if r.get("linked_ids") else ""
    nid    = r.get("id", "")
    if str(nid).startswith("D"):
        label = _fmt_daily_id(nid)
        return f"<code>{nid}</code> 📔 <b>{label}</b> {tags}"
    return f"<code>{nid}</code> 📝 <b>{esc(r.get('title',''))}</b> {tags}{linked}"


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧠 <b>NhatBrain v2 — Bộ não thứ 2 kiểu Obsidian</b>\n\n"
        "📝 <b>Ghi chú thông minh:</b>\n"
        "• <i>Nhớ môn Toán: bài chương 3, ôn thi giữa kỳ</i>\n"
        "• <i>Ý tưởng app: quản lý chi tiêu, link với note dự án</i>\n"
        "• <i>Hôm nay mình đã hoàn thành sprint planning</i> → Daily note\n\n"
        "✅ <b>Task &amp; nhắc nhở:</b>\n"
        "• <i>Nhắc thứ 7 8h tối hẹn bạn A</i> [gấp]\n"
        "• <i>Mỗi ngày 7h nhắc tập thể dục</i> → lặp hàng ngày\n\n"
        "🔍 <b>Tìm kiếm:</b>\n"
        "• <i>Toán cần gì?</i> / <i>Tìm note về dự án</i>\n"
        "• <i>Tuần này có gì?</i> / <i>Backlinks của note 3</i>\n\n"
        "⌨️ <b>Commands:</b>\n"
        "/list — tất cả tasks\n"
        "/today — hôm nay\n"
        "/notes [#tag] — danh sách notes\n"
        "/note &lt;id&gt; — xem note chi tiết + backlinks\n"
        "/daily — nhật ký hôm nay\n"
        "/daily list — xem tất cả nhật ký\n"
        "/daily dd/mm — xem ngày cụ thể\n"
        "/done &lt;id&gt; — đánh dấu xong\n"
        "/del &lt;id&gt; — xóa task",
        parse_mode=HTML,
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text    = update.message.text.strip()
    chat_id = update.effective_chat.id

    push_ctx(chat_id, "user", text)

    thinking = await update.message.reply_text("🧠 Đang xử lý...")
    p        = parse_with_gemini(text, chat_id)
    intent   = p.get("intent", "unknown")
    await thinking.delete()

    # ──────────────────────────────────────────────────────────────── add_task ──
    if intent == "add_task":
        title   = p.get("task_title") or text
        desc    = p.get("task_description") or ""
        due_dt  = parse_dt(p.get("due_datetime"))
        prio    = int(p.get("priority") or 2)
        tags    = p.get("tags") or ""
        rec     = p.get("recurrence") or ""

        tid = task_save(chat_id, title, desc, due_dt, prio, tags, recurrence=rec)
        emoji = PRIORITY_EMOJI.get(prio, "⚪")
        rec_s = f"\n🔁 Lặp: {rec}" if rec else ""

        if due_dt:
            ok = _schedule_reminder(context.application, chat_id, tid, title, due_dt)
            if ok:
                await update.message.reply_text(
                    f"✅ Task #{tid} đã lưu!\n"
                    f"{emoji} <b>{esc(title)}</b>\n"
                    f"🕐 {due_dt.strftime('%H:%M — %d/%m/%Y')}{rec_s}\n"
                    f"{fmt_tags(tags)}",
                    parse_mode=HTML,
                )
            else:
                await update.message.reply_text("⚠️ Thời gian đã qua rồi bro!")
        else:
            await update.message.reply_text(
                f"✅ Task #{tid} đã lưu!\n"
                f"{emoji} *{esc(title)}* _(không deadline)_\n{fmt_tags(tags)}",
                parse_mode=HTML,
            )
        push_ctx(chat_id, "assistant", f"add_task #{tid}: {title}")

    # ─────────────────────────────────────────────────── add_note / daily_note ──
    elif intent in ("add_note", "daily_note"):
        is_daily = intent == "daily_note"
        title   = p.get("note_title") or (f"Daily {today_str()}" if is_daily else text[:60])
        content = p.get("note_content") or text
        tags    = p.get("note_tags") or ("daily" if is_daily else "")
        linked  = p.get("linked_note_ids") or ""
        daily_d = today_str() if is_daily else ""

        nid    = note_save(chat_id, title, content, tags, linked, daily_d)
        icon   = "📔" if is_daily else "📝"
        link_s = f"\n🔗 Liên kết: {linked}" if linked else ""
        await update.message.reply_text(
            f"{icon} Note #{nid} đã lưu!\n<b>{esc(title)}</b>\n{fmt_tags(tags)}{link_s}\n\n"
            f"<i>/note {nid} để xem chi tiết</i>",
            parse_mode=HTML,
        )
        push_ctx(chat_id, "assistant", f"add_note #{nid}: {title}")

    # ──────────────────────────────────────────────────────────── query_notes ──
    elif intent == "query_notes":
        tag     = p.get("query_tag") or ""
        keyword = p.get("query_keyword") or ""
        notes   = note_get(chat_id, tag=tag or None, keyword=keyword or None)

        if not notes:
            label = f"#{tag}" if tag else f'"{keyword}"'
            await update.message.reply_text(
                f"🤷 Chưa có note nào về <b>{label}</b>.", parse_mode=HTML
            )
        else:
            label = f"#{tag}" if tag else f'"{keyword}"'
            msg   = f"📚 <b>Notes về {label}</b> ({len(notes)}):\n\n"
            for n in notes[-12:]:
                msg += _fmt_note_line(n) + "\n"
            msg += "\n<i>/note &lt;id&gt; để xem đầy đủ + backlinks</i>"
            await update.message.reply_text(msg, parse_mode=HTML)

    # ──────────────────────────────────────────────────────────── query_tasks ──
    elif intent == "query_tasks":
        df_s  = p.get("query_date_from")
        dt_s  = p.get("query_date_to")
        days  = int(p.get("query_days") or 3)

        if df_s:
            df    = parse_dt(df_s + "T00:00:00")
            dt    = parse_dt((dt_s or df_s) + "T23:59:59")
            label = f"{df_s} → {dt_s or df_s}"
        else:
            df    = now()
            dt    = now() + timedelta(days=days)
            label = f"{days} ngày tới"

        tasks = task_get(chat_id, date_from=df, date_to=dt)
        if not tasks:
            await update.message.reply_text(f"😎 <b>{label}</b> trống, chill thôi!", parse_mode=HTML)
        else:
            msg = f"📅 <b>Lịch {label}:</b>\n\n"
            for t in tasks:
                msg += _fmt_task(t) + "\n\n"
            await update.message.reply_text(msg, parse_mode=HTML)

    # ─────────────────────────────────────────────────────────────── mark_done ──
    elif intent == "mark_done":
        tid = p.get("target_id")
        if tid:
            task = task_get_by_id(tid)
            task_set_status(int(tid), "done")
            name = esc(task.get("title","")) if task else f"#{tid}"
            await update.message.reply_text(f"✅ Xong! <b>{name}</b>", parse_mode=HTML)
        else:
            tasks = task_get(chat_id, status="pending")
            if not tasks:
                await update.message.reply_text("📭 Không có task pending nào!")
            else:
                msg = "📋 <b>Pending tasks</b> — /done &lt;id&gt;:\n\n"
                for t in tasks:
                    msg += _fmt_task(t) + "\n\n"
                await update.message.reply_text(msg, parse_mode=HTML)

    # ─────────────────────────────────────────────────────────────── delete_task ──
    elif intent == "delete_task":
        tid     = p.get("target_id")
        keyword = p.get("delete_keyword") or ""

        if tid:
            task = task_get_by_id(tid)
            if task:
                task_set_status(int(tid), "cancelled")
                await update.message.reply_text(
                    f"🗑️ Đã xóa: <b>{esc(task.get('title',''))}</b>", parse_mode=HTML
                )
            else:
                await update.message.reply_text(f"❌ Không thấy task #{tid}")
        elif keyword:
            found = task_search(chat_id, keyword)
            if not found:
                await update.message.reply_text(
                    f"🔍 Không thấy task nào chứa <b>'{esc(keyword)}'</b>\nDùng /list rồi /del &lt;id&gt;.",
                    parse_mode=HTML,
                )
            elif len(found) == 1:
                task_set_status(int(found[0]["id"]), "cancelled")
                await update.message.reply_text(
                    f"🗑️ Đã xóa: <b>{esc(found[0].get('title',''))}</b>", parse_mode=HTML
                )
            else:
                msg = f"🔍 <b>{len(found)} task khớp</b>, dùng /del &lt;id&gt;:\n\n"
                for t in found:
                    msg += _fmt_task(t) + "\n\n"
                await update.message.reply_text(msg, parse_mode=HTML)

    # ─────────────────────────────────────────────────────────────── update_task ──
    elif intent == "update_task":
        tid    = p.get("target_id")
        fields = p.get("update_fields") or {}
        if tid and fields:
            task_update(int(tid), fields)
            if "due_datetime" in fields:
                task = task_get_by_id(tid)
                new_due = parse_dt(fields["due_datetime"])
                if task and new_due:
                    _schedule_reminder(context.application, chat_id, tid, task["title"], new_due)
            await update.message.reply_text(
                f"✏️ Đã cập nhật task #{tid}!", parse_mode=HTML
            )
        else:
            await update.message.reply_text("⚠️ Cần chỉ định task ID và field cần đổi.")

    # ───────────────────────────────────────────────────────────────── show_note ──
    elif intent == "show_note":
        tid     = p.get("target_id")
        keyword = p.get("query_keyword") or ""
        note    = note_get_by_id(tid) if tid else None
        if not note and keyword:
            matches = note_get(chat_id, keyword=keyword)
            note    = matches[0] if matches else None

        if not note:
            await update.message.reply_text("🤷 Không tìm thấy note.", parse_mode=HTML)
        else:
            await _reply_note_detail(update, note)

    # ──────────────────────────────────────────────────────────────────── unknown ──
    else:
        await update.message.reply_text(
            p.get("reply") or "Tao chưa hiểu ý mày, thử lại xem!"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: note detail message
# ═══════════════════════════════════════════════════════════════════════════════
async def _reply_note_detail(update: Update, note: dict):
    tags   = fmt_tags(note.get("tags", ""))
    ts     = parse_dt(note.get("created_at", ""))
    ts_s   = ts.strftime("%d/%m/%Y %H:%M") if ts else ""
    blinks = note_backlinks(note["id"])

    # Header HTML — daily note hiện ngày đẹp thay vì title
    nid = note.get("id", "")
    if str(nid).startswith("D"):
        display_title = _fmt_daily_id(nid)
        icon = "📔"
    else:
        display_title = esc(note.get("title", ""))
        icon = "📝"
    header = (
        f"{icon} <b>{display_title}</b> <code>{nid}</code>\n"
        f"{tags}\n"
        f"<i>Tạo: {ts_s}</i>"
    )
    await update.message.reply_text(header, parse_mode=HTML)

    # Content: plain text vì free-text có thể chứa bất kỳ ký tự nào
    content_text = note.get("content", "") or ""
    if content_text:
        await update.message.reply_text(content_text)

    # Links block HTML
    links_msg = ""
    if note.get("linked_ids"):
        links_msg += "📎 <b>Liên kết đến:</b>"
        for lid in str(note["linked_ids"]).split(","):
            n2 = note_get_by_id(lid.strip())
            if n2:
                links_msg += f"\n  → <code>{lid}</code> {esc(n2.get('title',''))} {fmt_tags(n2.get('tags',''))}"

    if blinks:
        if links_msg:
            links_msg += "\n\n"
        links_msg += f"🔙 <b>Backlinks ({len(blinks)}):</b>"
        for bk in blinks:
            links_msg += f"\n  ← <code>{bk['id']}</code> {esc(bk.get('title',''))}"

    if links_msg:
        await update.message.reply_text(links_msg, parse_mode=HTML)
    else:
        await update.message.reply_text("<i>Chưa có backlinks.</i>", parse_mode=HTML)


# ═══════════════════════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════
async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Dùng: /done &lt;id&gt;", parse_mode=HTML)
        return
    tid  = int(context.args[0])
    task = task_get_by_id(tid)
    if not task:
        await update.message.reply_text(f"❌ Không thấy task #{tid}")
        return
    task_set_status(tid, "done")
    await update.message.reply_text(f"✅ Xong! <b>{esc(task.get('title',''))}</b>", parse_mode=HTML)


async def cmd_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Dùng: /del &lt;id&gt;", parse_mode=HTML)
        return
    tid  = int(context.args[0])
    task = task_get_by_id(tid)
    if not task:
        await update.message.reply_text(f"❌ Không thấy task #{tid}")
        return
    task_set_status(tid, "cancelled")
    await update.message.reply_text(f"🗑️ Đã xóa: <b>{esc(task.get('title',''))}</b>", parse_mode=HTML)


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tasks   = task_get(chat_id, status="pending")
    if not tasks:
        await update.message.reply_text("📭 Không có task pending nào!")
        return

    grouped: dict[str, list] = defaultdict(list)
    no_date: list             = []
    today   = now().date()

    for t in tasks:
        if t.get("due_datetime"):
            day = parse_dt(t["due_datetime"]).strftime("%Y-%m-%d")
            grouped[day].append(t)
        else:
            no_date.append(t)

    chunks = ["📋 <b>Tất cả tasks pending:</b>\n\n"]
    for day in sorted(grouped.keys()):
        d = datetime.strptime(day, "%Y-%m-%d").date()
        if d < today:
            label = f"⚠️ Quá hạn {d.strftime('%d/%m')}"
        elif d == today:
            label = f"🔴 Hôm nay {d.strftime('%d/%m')}"
        elif d == today + timedelta(days=1):
            label = f"🟡 Ngày mai {d.strftime('%d/%m')}"
        else:
            label = f"📆 {d.strftime('%d/%m/%Y')}"
        block = f"<b>{label}</b>\n"
        for t in grouped[day]:
            block += _fmt_task(t) + "\n"
        block += "\n"
        # Tách chunk nếu sắp vượt 4000 ký tự
        if len(chunks[-1]) + len(block) > 4000:
            chunks.append(block)
        else:
            chunks[-1] += block

    if no_date:
        block = "📌 <b>Không deadline:</b>\n"
        for t in no_date:
            block += _fmt_task(t) + "\n"
        if len(chunks[-1]) + len(block) > 4000:
            chunks.append(block)
        else:
            chunks[-1] += block

    for chunk in chunks:
        await update.message.reply_text(chunk, parse_mode=HTML)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s       = now().replace(hour=0,  minute=0,  second=0,  microsecond=0)
    e       = now().replace(hour=23, minute=59, second=59, microsecond=0)
    tasks   = task_get(chat_id, date_from=s, date_to=e)
    label   = now().strftime("%d/%m/%Y")

    msg = f"📅 <b>Hôm nay — {label}:</b>\n\n"
    if tasks:
        for t in tasks:
            msg += _fmt_task(t) + "\n\n"
    else:
        msg += "😎 Trống! Nghỉ ngơi đi thôi.\n"

    await update.message.reply_text(msg, parse_mode=HTML)

    # Daily note gửi riêng — KHÔNG parse_mode vì content là free-text
    daily = note_get_daily(chat_id, today_str())
    if daily:
        label   = _fmt_daily_id(daily["id"])
        content = str(daily.get("content", "") or "")
        preview = content[:300] + ("…" if len(content) > 300 else "")
        await update.message.reply_text(
            f"📔 Nhật ký {label}:\n{preview}\n\n/note {daily['id']} để xem đầy đủ"
        )


async def cmd_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tag     = context.args[0].lstrip("#") if context.args else None
    notes   = note_get(chat_id, tag=tag)

    if not notes:
        msg = f"📭 Không có note nào với #{tag}!" if tag else "📭 Chưa có note nào!"
        await update.message.reply_text(msg)
        return

    label = f"#{tag}" if tag else "tất cả"
    msg   = f"📚 <b>Notes {label}</b> ({len(notes)}):\n\n"
    for n in notes[-15:]:
        msg += _fmt_note_line(n) + "\n"
    msg += "\n<i>/note &lt;id&gt; để xem chi tiết + backlinks</i>"
    await update.message.reply_text(msg, parse_mode=HTML)


async def cmd_note_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Dùng: /note &lt;id&gt; hoặc /note D20260307 cho daily note", parse_mode=HTML)
        return
    raw  = context.args[0].upper()
    note = note_get_by_id(raw) if raw.startswith("D") else (
           note_get_by_id(raw) if raw.isdigit() else None)
    if not note:
        await update.message.reply_text(f"❌ Không thấy note <code>{esc(context.args[0])}</code>", parse_mode=HTML)
        return
    await _reply_note_detail(update, note)


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # /daily        → xem hôm nay
    # /daily list   → danh sách nhật ký
    # /daily 07/03  → xem ngày cụ thể (dd/mm, năm hiện tại)
    arg = context.args[0].lower() if context.args else ""

    if arg == "list":
        entries = note_get_all_daily(chat_id)
        if not entries:
            await update.message.reply_text("📭 Chưa có nhật ký nào!")
            return
        msg = "📔 <b>Nhật ký của bạn:</b>\n\n"
        for e in entries[:20]:
            nid      = e["id"]
            label    = _fmt_daily_id(nid)
            updated  = parse_dt(e.get("updated_at", ""))
            time_str = updated.strftime("%H:%M") if updated else ""
            msg     += f"<code>{nid}</code> 📔 {label}"
            if time_str:
                msg += f" <i>(cập nhật {time_str})</i>"
            msg += "\n"
        msg += "\n<i>/note D20260307 để xem chi tiết</i>"
        await update.message.reply_text(msg, parse_mode=HTML)
        return

    if arg and "/" in arg:
        # parse dd/mm
        try:
            parts   = arg.split("/")
            day, mo = int(parts[0]), int(parts[1])
            yr      = now().year
            target  = datetime(yr, mo, day).strftime("%Y-%m-%d")
        except Exception:
            await update.message.reply_text("❌ Định dạng ngày: dd/mm (VD: /daily 07/03)")
            return
    else:
        target = today_str()

    daily = note_get_daily(chat_id, target)
    if daily:
        await _reply_note_detail(update, daily)
    else:
        label = _fmt_daily_id(_daily_id(target))
        await update.message.reply_text(
            f"📔 <b>{label}</b> chưa có nhật ký.\n"
            f"<i>Nhắn 'Hôm nay mình đã...' để bắt đầu ghi!</i>",
            parse_mode=HTML,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# RESTORE REMINDERS sau khi restart
# ═══════════════════════════════════════════════════════════════════════════════
def restore_reminders(app):
    tasks = task_get_all_future()
    count = 0
    for t in tasks:
        due = parse_dt(t.get("due_datetime", ""))
        if due:
            _schedule_reminder(app, t["chat_id"], t["id"], t["title"], due)
            count += 1
    print(f"[✓] Restored {count} reminders")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("🚀 NhatBrain v2 launching...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("list",  cmd_list))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("notes", cmd_notes))
    app.add_handler(CommandHandler("note",  cmd_note_detail))
    app.add_handler(CommandHandler("daily", cmd_daily))
    app.add_handler(CommandHandler("done",  cmd_done))
    app.add_handler(CommandHandler("del",   cmd_del))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    restore_reminders(app)
    threading.Thread(target=start_health_server, daemon=True).start()

    print("✅ NhatBrain v2 running!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
