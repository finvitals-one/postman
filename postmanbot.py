import os
import csv
import asyncio
import sqlite3
import requests

from datetime import datetime
from aiogram import Bot


print("POSTMAN BOT LOADING")


TOKEN = os.getenv("POSTMAN_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("POSTMAN_SHEET_URL")

bot = Bot(token=TOKEN)

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
    return f"{row['date']}_{row['time']}_{row['type']}"


# ---------------- SEND POST ----------------

async def send_post(row):

    content = row.get("content", "").strip()
    image_url = row.get("image_url", "").strip()

    if not content:
        print("Empty content, skipping")
        return

    if image_url:

        await bot.send_photo(
            chat_id=GROUP_ID,
            photo=image_url,
            caption=content,
            parse_mode="HTML"
        )

    else:

        await bot.send_message(
            chat_id=GROUP_ID,
            text=content,
            parse_mode="HTML"
        )

    print(f"Posted: {content[:60]}...")


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

            ptype = row.get("type", "").lower()

            if ptype != "post":
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

    await scheduler()


if __name__ == "__main__":

    asyncio.run(main())
