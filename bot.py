"""
NhatBrain Bot - Trợ lý Telegram với Gemini AI
Database: Google Sheets (free, persistent, không cần Render Disk)
Deploy: Render Web Service free tier
"""
print("[startup] Python starting...")

import asyncio

# Load .env khi chạy local
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import json
import os
import re
import threading
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

import gspread
from google.oauth2.service_account import Credentials
from google import genai
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ─── CONFIG ────────────────────────────────────────────────────────────────────
try:
    print("[init] Đọc env vars...")
    TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
    GEMINI_API_KEY    = os.environ["GEMINI_API_KEY"]
    GSHEET_CREDS_JSON = os.environ["GSHEET_CREDS_JSON"]
    GSHEET_ID         = os.environ["GSHEET_ID"]
    print("[init] Env vars OK")
except KeyError as e:
    print(f"[FATAL] Thiếu env var: {e}")
    raise

# ─── GEMINI ────────────────────────────────────────────────────────────────────
try:
    print("[init] Kết nối Gemini...")
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    print("[init] Gemini OK")
except Exception as e:
    print(f"[FATAL] Gemini lỗi: {e}")
    raise

# ─── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
try:
    print("[init] Parse credentials JSON...")
    _creds_dict = json.loads(GSHEET_CREDS_JSON)
    print("[init] Tạo credentials...")
    _creds = Credentials.from_service_account_info(
        _creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    print("[init] Kết nối Google Sheets...")
    gc = gspread.authorize(_creds)
    sh = gc.open_by_key(GSHEET_ID)
    print("[init] Google Sheets OK")
except json.JSONDecodeError as e:
    print(f"[FATAL] GSHEET_CREDS_JSON không phải JSON hợp lệ: {e}")
    raise
except Exception as e:
    print(f"[FATAL] Google Sheets lỗi: {e}")
    raise

def _get_or_create_sheet(name, headers):
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=1000, cols=len(headers))
        ws.append_row(headers)
    return ws

try:
    print("[init] Khởi tạo worksheets...")
    ws_tasks = _get_or_create_sheet("Tasks", ["id","chat_id","task_text","due_datetime","category","done","deleted","created_at"])
    ws_notes = _get_or_create_sheet("Notes", ["id","chat_id","category","content","created_at"])
    print("[init] Worksheets OK")
except Exception as e:
    print(f"[FATAL] Worksheet lỗi: {e}")
    raise


# ─── SHEET HELPERS ─────────────────────────────────────────────────────────────
def _next_id(ws):
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return 1
    ids = [int(r[0]) for r in rows[1:] if r[0].isdigit()]
    return max(ids) + 1 if ids else 1

def _all_rows(ws):
    """Trả về list of dict bỏ qua header"""
    return ws.get_all_records()


# ─── DB API (giống SQLite cũ, chỉ đổi backend) ────────────────────────────────
def save_task(chat_id, task_text, due_dt=None, category=None):
    tid = _next_id(ws_tasks)
    ws_tasks.append_row([
        tid, chat_id, task_text,
        due_dt.replace(tzinfo=None).isoformat() if due_dt else "",
        category or "", 0, 0,
        now().isoformat()
    ])
    return tid

def save_note(chat_id, category, content):
    nid = _next_id(ws_notes)
    ws_notes.append_row([nid, chat_id, category, content, now().isoformat()])

def get_notes_by_category(chat_id, category):
    rows = _all_rows(ws_notes)
    return [r["content"] for r in rows
            if str(r["chat_id"]) == str(chat_id)
            and r["category"].lower() == category.lower()]

def get_upcoming_tasks(chat_id, days=3):
    n = now()
    fut = n + timedelta(days=days)
    result = []
    for r in _all_rows(ws_tasks):
        if str(r["chat_id"]) != str(chat_id): continue
        if r["done"] or r["deleted"]: continue
        if not r["due_datetime"]: continue
        dt = parse_dt(r["due_datetime"])
        if n <= dt <= fut:
            result.append((r["task_text"], r["due_datetime"], r["category"]))
    return sorted(result, key=lambda x: x[1])

def get_tasks_on_date(chat_id, target):
    s = target.replace(hour=0,  minute=0,  second=0,  microsecond=0, tzinfo=None)
    e = target.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=None)
    result = []
    for r in _all_rows(ws_tasks):
        if str(r["chat_id"]) != str(chat_id): continue
        if r["done"] or r["deleted"]: continue
        if not r["due_datetime"]: continue
        dt = parse_dt(r["due_datetime"])
        if s <= dt <= e:
            result.append((r["id"], r["task_text"], r["due_datetime"], r["category"]))
    return sorted(result, key=lambda x: x[2])

def get_all_pending(chat_id):
    result = []
    for r in _all_rows(ws_tasks):
        if str(r["chat_id"]) != str(chat_id): continue
        if r["done"] or r["deleted"]: continue
        result.append((r["id"], r["task_text"], r["due_datetime"], r["category"]))
    return result

def search_tasks(chat_id, keyword):
    result = []
    for r in _all_rows(ws_tasks):
        if str(r["chat_id"]) != str(chat_id): continue
        if r["done"] or r["deleted"]: continue
        if keyword.lower() in r["task_text"].lower():
            result.append((r["id"], r["task_text"], r["due_datetime"], r["category"]))
    return result

def _find_row_index(ws, col_name, value):
    """Tìm row index (1-based, tính cả header) theo giá trị cột"""
    records = ws.get_all_records()
    headers = ws.row_values(1)
    col_idx = headers.index(col_name)
    for i, r in enumerate(records):
        if str(r[col_name]) == str(value):
            return i + 2  # +2 vì header ở row 1, records bắt đầu từ row 2
    return None

def _update_task_field(task_id, field, value):
    headers = ws_tasks.row_values(1)
    col = headers.index(field) + 1
    row = _find_row_index(ws_tasks, "id", task_id)
    if row:
        ws_tasks.update_cell(row, col, value)

def mark_done(task_id):
    _update_task_field(task_id, "done", 1)

def delete_task(task_id):
    _update_task_field(task_id, "deleted", 1)

def delete_note(chat_id, category, keyword=None):
    records = ws_notes.get_all_records()
    to_delete = []
    for i, r in enumerate(records):
        if str(r["chat_id"]) != str(chat_id): continue
        if r["category"].lower() != category.lower(): continue
        if keyword and keyword.lower() not in r["content"].lower(): continue
        to_delete.append(i + 2)
    # xóa từ dưới lên để không bị lệch index
    for row_idx in sorted(to_delete, reverse=True):
        ws_notes.delete_rows(row_idx)
    return len(to_delete)

def get_all_future_tasks():
    """Dùng để restore reminders khi restart"""
    n = now()
    result = []
    for r in _all_rows(ws_tasks):
        if r["done"] or r["deleted"]: continue
        if not r["due_datetime"]: continue
        dt = parse_dt(r["due_datetime"])
        if dt > n:
            result.append((r["id"], r["task_text"], r["due_datetime"], r["chat_id"]))
    return result


# ─── UTILS ─────────────────────────────────────────────────────────────────────
def now():
    return datetime.now().replace(tzinfo=None)

def parse_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(str(s)).replace(tzinfo=None)


# ─── HEALTH SERVER ─────────────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()


# ─── GEMINI PARSER ─────────────────────────────────────────────────────────────
def build_prompt(user_text):
    days_vi = ["Thứ Hai","Thứ Ba","Thứ Tư","Thứ Năm","Thứ Sáu","Thứ Bảy","Chủ Nhật"]
    now_str = f"{days_vi[datetime.now().weekday()]}, {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    return f"""Mày là bộ não thứ 2, phân tích tin nhắn tiếng Việt và trả về JSON.
Ngày giờ hiện tại: {now_str}

Trả về JSON:
{{
  "intent": "add_task"|"add_note"|"query_notes"|"query_tasks"|"query_day"|"delete_task"|"delete_note"|"mark_done"|"unknown",
  "task_text": "nội dung task",
  "due_datetime": "YYYY-MM-DDTHH:MM:SS hoặc null (không timezone)",
  "category": "nhóm hoặc null",
  "note_contents": ["item1","item2"],
  "query_category": "category cần hỏi",
  "query_days": 3,
  "query_date": "YYYY-MM-DD",
  "delete_keyword": "từ khóa task cần xóa",
  "delete_category": "category note cần xóa",
  "delete_note_keyword": null,
  "reply": "trả lời khi unknown"
}}

Datetime rules (không timezone):
- "tối nay 8h" → hôm nay 20:00:00
- "8h30 tối nay" → hôm nay 20:30:00
- "thứ 7 tuần này 8h tối" → thứ 7 tuần này 20:00:00
- "ngày mai 9h" → ngày mai 09:00:00
- không có giờ → 09:00:00

CHỈ JSON thuần túy, không markdown.
Tin nhắn: "{user_text}"
"""

def parse_with_gemini(user_text):
    try:
        res = gemini_client.models.generate_content(model="gemini-2.5-flash", contents=build_prompt(user_text))
        raw = re.sub(r"```json\s*|\s*```", "", res.text.strip()).strip()
        return json.loads(raw)
    except Exception as e:
        print(f"[Gemini Error] {e}")
        return {"intent": "unknown", "reply": "Tao bị lỗi, thử lại đi!"}


# ─── REMINDER ──────────────────────────────────────────────────────────────────
async def send_reminder(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    # Check task còn tồn tại không
    rows = _all_rows(ws_tasks)
    task = next((r for r in rows if str(r["id"]) == str(data["task_id"])), None)
    if not task or task["done"] or task["deleted"]:
        return
    await context.bot.send_message(
        chat_id=data["chat_id"],
        text=f"⏰ *Nhắc nhở!*\n{data['task_text']}",
        parse_mode="Markdown",
    )

def schedule_reminder(app, chat_id, task_id, task_text, due_dt):
    delay = (due_dt.replace(tzinfo=None) - now()).total_seconds()
    if delay > 0:
        app.job_queue.run_once(
            send_reminder, delay,
            data={"chat_id": chat_id, "task_text": task_text, "task_id": task_id},
            name=f"task_{task_id}",
        )
        return True
    return False


# ─── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧠 *Chào chủ nhân! Tao là bộ não thứ 2 của mày.*\n\n"
        "💬 *Ví dụ:*\n"
        "• _Nhắc tao thứ 7 8h tối hẹn bạn A_\n"
        "• _Tối nay 8h30 làm bài tập_\n"
        "• _Nhớ môn Toán: bài tập chương 3, ôn thi_\n"
        "• _Môn Toán cần gì?_\n"
        "• _Hôm nay có gì? / 3 ngày tới làm gì?_\n"
        "• _Xóa nhắc hẹn bạn A_\n\n"
        "⌨️ *Lệnh:* `/list` `/today` `/done <id>` `/del <id>`",
        parse_mode="Markdown",
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text    = update.message.text.strip()
    chat_id = update.effective_chat.id

    thinking = await update.message.reply_text("🤔 Để tao nghĩ...")
    parsed   = parse_with_gemini(text)
    intent   = parsed.get("intent", "unknown")
    await thinking.delete()

    if intent == "add_task":
        task_text = parsed.get("task_text") or text
        category  = parsed.get("category")
        due_dt    = parse_dt(parsed.get("due_datetime"))
        task_id   = save_task(chat_id, task_text, due_dt, category)
        cat_str   = f" [{category}]" if category else ""
        if due_dt:
            ok = schedule_reminder(context.application, chat_id, task_id, task_text, due_dt)
            if ok:
                await update.message.reply_text(f"✅ Đã lưu!\n📌 *{task_text}*{cat_str}\n🕐 {due_dt.strftime('%H:%M — %d/%m/%Y')}", parse_mode="Markdown")
            else:
                await update.message.reply_text("⚠️ Thời gian đã qua rồi bro!")
        else:
            await update.message.reply_text(f"✅ Đã lưu!\n📌 *{task_text}*{cat_str}\n_(Không có deadline)_", parse_mode="Markdown")

    elif intent == "add_note":
        category = parsed.get("category") or "Chung"
        items    = parsed.get("note_contents") or [text]
        for item in items:
            save_note(chat_id, category, item.strip())
        await update.message.reply_text(f"✅ Đã nhớ *{category}*:\n" + "\n".join(f"  • {i}" for i in items), parse_mode="Markdown")

    elif intent == "query_notes":
        category = parsed.get("query_category") or ""
        items    = get_notes_by_category(chat_id, category)
        if items:
            await update.message.reply_text(f"📚 *{category}* cần:\n" + "\n".join(f"  • {i}" for i in items), parse_mode="Markdown")
        else:
            await update.message.reply_text(f"🤷 Chưa có gì về *{category}*.", parse_mode="Markdown")

    elif intent == "query_tasks":
        days  = parsed.get("query_days") or 3
        tasks = get_upcoming_tasks(chat_id, days)
        if not tasks:
            await update.message.reply_text(f"😎 {days} ngày tới trống, chill thôi!")
        else:
            msg = f"📅 *{days} ngày tới:*\n"
            for t, due, cat in tasks:
                cat_str = f" [{cat}]" if cat else ""
                msg    += f"• {parse_dt(due).strftime('%H:%M %d/%m')}:{cat_str} {t}\n"
            await update.message.reply_text(msg, parse_mode="Markdown")

    elif intent == "query_day":
        try:
            target = parse_dt(parsed.get("query_date")) or now()
        except Exception:
            target = now()
        tasks = get_tasks_on_date(chat_id, target)
        label = target.strftime("%d/%m/%Y")
        if target.date() == now().date():
            label = f"Hôm nay ({label})"
        elif target.date() == now().date() + timedelta(days=1):
            label = f"Ngày mai ({label})"
        if not tasks:
            await update.message.reply_text(f"😎 *{label}* trống!", parse_mode="Markdown")
        else:
            msg = f"📅 *Lịch {label}:*\n"
            for t_id, t, due, cat in tasks:
                cat_str = f" [{cat}]" if cat else ""
                msg    += f"• `{t_id}` {parse_dt(due).strftime('%H:%M')}:{cat_str} {t}\n"
            await update.message.reply_text(msg, parse_mode="Markdown")

    elif intent == "delete_task":
        keyword = parsed.get("delete_keyword") or ""
        found   = search_tasks(chat_id, keyword)
        if not found:
            await update.message.reply_text(f"🔍 Không thấy task nào chứa *'{keyword}'*\nDùng `/list` rồi `/del <id>`.", parse_mode="Markdown")
        elif len(found) == 1:
            delete_task(found[0][0])
            await update.message.reply_text(f"🗑️ Đã xóa: *{found[0][1]}*", parse_mode="Markdown")
        else:
            msg = f"🔍 Thấy *{len(found)}* task, `/del <id>` để xóa:\n"
            for t_id, t, due, _ in found:
                due_str = parse_dt(due).strftime("%d/%m %H:%M") if due else "no deadline"
                msg    += f"  `{t_id}`. {t} _({due_str})_\n"
            await update.message.reply_text(msg, parse_mode="Markdown")

    elif intent == "delete_note":
        category = parsed.get("delete_category") or ""
        keyword  = parsed.get("delete_note_keyword")
        count    = delete_note(chat_id, category, keyword)
        target   = f"'{keyword}' trong *{category}*" if keyword else f"toàn bộ *{category}*"
        if count:
            await update.message.reply_text(f"🗑️ Đã xóa {count} mục của {target}.", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"🤷 Không có gì để xóa trong *{category}*.", parse_mode="Markdown")

    elif intent == "mark_done":
        tasks = get_all_pending(chat_id)
        if not tasks:
            await update.message.reply_text("📭 Không có việc gì pending!")
        else:
            msg = "📋 *Việc pending — `/done <id>`:*\n"
            for t_id, t, due, cat in tasks:
                due_str = parse_dt(due).strftime("%d/%m %H:%M") if due else "no deadline"
                cat_str = f" [{cat}]" if cat else ""
                msg    += f"  `{t_id}`.{cat_str} {t} _({due_str})_\n"
            await update.message.reply_text(msg, parse_mode="Markdown")

    else:
        await update.message.reply_text(parsed.get("reply") or "Tao chưa hiểu, thử lại!")


async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Dùng: `/done <id>`", parse_mode="Markdown")
        return
    mark_done(int(args[0]))
    await update.message.reply_text(f"✅ Task #{args[0]} xong!")

async def cmd_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Dùng: `/del <id>`", parse_mode="Markdown")
        return
    task_id = int(args[0])
    rows = _all_rows(ws_tasks)
    task = next((r for r in rows if str(r["id"]) == str(task_id)), None)
    if not task:
        await update.message.reply_text(f"❌ Không thấy task #{task_id}")
        return
    delete_task(task_id)
    await update.message.reply_text(f"🗑️ Đã xóa: *{task['task_text']}*", parse_mode="Markdown")

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tasks   = get_all_pending(chat_id)
    if not tasks:
        await update.message.reply_text("📭 Không có việc gì pending!")
        return
    grouped, no_date = {}, []
    for t_id, t, due, cat in tasks:
        if due:
            grouped.setdefault(parse_dt(due).strftime("%d/%m/%Y"), []).append((t_id, t, due, cat))
        else:
            no_date.append((t_id, t, due, cat))
    msg = "📋 *Tất cả việc cần làm:*\n\n"
    for day in sorted(grouped, key=lambda d: datetime.strptime(d, "%d/%m/%Y")):
        dt    = datetime.strptime(day, "%d/%m/%Y").date()
        today = now().date()
        label = "Hôm nay" if dt == today else ("Ngày mai" if dt == today + timedelta(days=1) else day)
        msg  += f"📆 *{label}*\n"
        for t_id, t, due, cat in grouped[day]:
            cat_str = f" [{cat}]" if cat else ""
            msg    += f"  `{t_id}`. {parse_dt(due).strftime('%H:%M')}{cat_str} — {t}\n"
        msg += "\n"
    if no_date:
        msg += "📌 *Không có deadline:*\n"
        for t_id, t, _, cat in no_date:
            msg += f"  `{t_id}`. {t}\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    tasks   = get_tasks_on_date(chat_id, now())
    if not tasks:
        await update.message.reply_text(f"😎 Hôm nay ({now().strftime('%d/%m/%Y')}) trống!")
        return
    msg = f"📅 *Hôm nay — {now().strftime('%d/%m/%Y')}:*\n"
    for t_id, t, due, cat in tasks:
        cat_str = f" [{cat}]" if cat else ""
        msg    += f"• `{t_id}` {parse_dt(due).strftime('%H:%M')}:{cat_str} {t}\n"
    await update.message.reply_text(msg, parse_mode="Markdown")


# ─── RESTORE REMINDERS ─────────────────────────────────────────────────────────
def restore_reminders(app):
    rows = get_all_future_tasks()
    for t_id, t, due, chat_id in rows:
        schedule_reminder(app, chat_id, t_id, t, parse_dt(due))
    print(f"[✓] Khôi phục {len(rows)} reminders")


# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("🚀 Khởi động...")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list",  cmd_list))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("done",  cmd_done))
    app.add_handler(CommandHandler("del",   cmd_del))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    restore_reminders(app)
    print("✅ Bot đang chạy!")

    threading.Thread(target=start_health_server, daemon=True).start()
    # FIX Python 3.14
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()