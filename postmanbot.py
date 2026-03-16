import os
import csv
import asyncio
import sqlite3
import requests

from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message


print("POSTMAN BOT LOADING")


TOKEN = os.getenv("POSTMAN_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
SHEET_URL = os.getenv("POSTMAN_SHEET_URL")
FILE_CHANNEL_ID = int(os.getenv("FILE_CHANNEL_ID"))
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = Bot(token=TOKEN)
dp = Dispatcher()

conn = sqlite3.connect("postman.db")
cursor = conn.cursor()


# ---------------- DATABASE ----------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS posted(
row_key TEXT PRIMARY KEY
)
""")

conn.commit()


# ---------------- SHEET ----------------

def fetch_sheet():

    r = requests.get(SHEET_URL)
    r.raise_for_status()

    lines = r.content.decode("utf-8").splitlines()
    reader = csv.DictReader(lines)

    rows = []

    for row in reader:

        clean = {}

        for k, v in row.items():
            if k:
                clean[k.strip().lower()] = v.strip() if v else ""

        rows.append(clean)

    print(f"Rows fetched: {len(rows)}")

    return rows


# ---------------- PARSE DATETIME ----------------

def parse_datetime(date_str, time_str):

    formats = [
        ("%d/%m/%Y", "%H:%M"),
        ("%d/%m/%Y", "%H:%M:%S"),
        ("%d-%m-%Y", "%H:%M"),
        ("%d-%m-%Y", "%H:%M:%S"),
        ("%Y-%m-%d", "%H:%M"),
        ("%Y-%m-%d", "%H:%M:%S")
    ]

    for df, tf in formats:
        try:
            return datetime.strptime(
                f"{date_str} {time_str}",
                f"{df} {tf}"
            )
        except:
            pass

    return None


# ---------------- ROW KEY ----------------

def row_key(row):
    # Uses post_in so group and channel rows are tracked separately
    return f"{row['date']}_{row['time']}_{row['post_in']}"


# ---------------- RESOLVE CHAT ID ----------------

def resolve_chat_id(post_in):

    val = post_in.strip().lower()

    if val == "group":
        return GROUP_ID

    if val == "channel":
        return CHANNEL_ID

    return None


# ---------------- SEND POST ----------------

async def send_post(row):

    content = row.get("content", "").strip()
    image_url = row.get("image_url", "").strip()
    post_in = row.get("post_in", "").strip()

    if not content:
        print("Empty content, skipping")
        return

    chat_id = resolve_chat_id(post_in)

    if not chat_id:
        print(f"Unknown post_in value: {post_in}, skipping")
        return

    # Preserve line breaks written as \n in sheet
    content = content.replace("\\n", "\n")

    if image_url:

        await bot.send_photo(
            chat_id=chat_id,
            photo=image_url,
            caption=content,
            parse_mode="HTML"
        )

    else:

        await bot.send_message(
            chat_id=chat_id,
            text=content,
            parse_mode="HTML"
        )

    print(f"Posted to {post_in} ({chat_id}): {content[:60]}...")


# ---------------- /start ----------------

@dp.message(F.text == "/start")
async def start(message: Message):

    if message.chat.type != "private":
        return

    if message.from_user.id != ADMIN_ID:
        return

    await message.answer(
f"""Postman Bot Active ✅

<b>Your ID:</b> <code>{message.from_user.id}</code>
<b>GROUP_ID:</b> <code>{GROUP_ID}</code>
<b>CHANNEL_ID:</b> <code>{CHANNEL_ID}</code>
<b>FILE_CHANNEL_ID:</b> <code>{FILE_CHANNEL_ID}</code>
<b>ADMIN_ID:</b> <code>{ADMIN_ID}</code>""",
        parse_mode="HTML"
    )


# ---------------- FILE ID HANDLER ----------------

@dp.message(F.photo & F.chat.id == FILE_CHANNEL_ID)
async def handle_image(message: Message):

    file_id = message.photo[-1].file_id

    await message.reply(f"file_id:\n<code>{file_id}</code>", parse_mode="HTML")


# ---------------- SCHEDULER ----------------

async def scheduler():

    print("Postman scheduler started")

    while True:

        try:
            rows = fetch_sheet()

        except Exception as e:
            print(f"Sheet error: {e}")
            await asyncio.sleep(300)
            continue

        now = datetime.now()
        print(f"Current time: {now}")

        for row in rows:

            post_in = row.get("post_in", "").strip().lower()

            if post_in not in ("group", "channel"):
                print(f"Skipping unknown post_in: {post_in}")
                continue

            date_val = row.get("date")
            time_val = row.get("time")

            if not date_val or not time_val:
                continue

            scheduled = parse_datetime(date_val, time_val)

            if not scheduled:
                print(f"Invalid datetime: {date_val} {time_val}")
                continue

            if now >= scheduled:

                key = row_key(row)

                cursor.execute(
                    "SELECT 1 FROM posted WHERE row_key=?",
                    (key,)
                )

                if cursor.fetchone():
                    continue

                try:
                    await send_post(row)

                    cursor.execute(
                        "INSERT INTO posted(row_key) VALUES(?)",
                        (key,)
                    )

                    conn.commit()
                    print(f"Marked as posted: {key}")

                except Exception as e:
                    print(f"Post error: {e}")

        await asyncio.sleep(300)


# ---------------- MAIN ----------------

async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    asyncio.create_task(scheduler())

    await dp.start_polling(bot)


if __name__ == "__main__":

    asyncio.run(main())
