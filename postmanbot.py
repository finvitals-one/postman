import os
import csv
import asyncio
import requests
from datetime import datetime
from aiogram import Bot

TOKEN = os.getenv("POSTMAN_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("SHEET_URL")

bot = Bot(token=TOKEN)

posted_rows = set()


# ---------------------------
# Fetch Google Sheet CSV
# ---------------------------
def fetch_sheet():

    r = requests.get(SHEET_URL)
    r.raise_for_status()

    lines = r.text.splitlines()
    reader = csv.DictReader(lines)

    rows = []

    for row in reader:

        clean_row = {}

        for k, v in row.items():

            if not k:
                continue

            key = k.strip().lower()
            val = v.strip() if v else ""

            clean_row[key] = val

        rows.append(clean_row)

    return rows


# ---------------------------
# Parse date + time
# ---------------------------
def parse_datetime(date_str, time_str):

    formats = [
        ("%d/%m/%Y", "%H:%M:%S"),
        ("%d/%m/%Y", "%H:%M"),
        ("%d-%m-%Y", "%H:%M:%S"),
        ("%d-%m-%Y", "%H:%M"),
        ("%Y-%m-%d", "%H:%M:%S"),
        ("%Y-%m-%d", "%H:%M"),
    ]

    for df, tf in formats:
        try:
            return datetime.strptime(f"{date_str} {time_str}", f"{df} {tf}")
        except:
            pass

    return None


# ---------------------------
# Build message text
# ---------------------------
def build_message(row):

    ptype = row.get("type", "").lower()
    text = row.get("content", "")
    options = row.get("options", "")
    correct = row.get("correct", "")

    if ptype == "poll":
        return f"/poll {text} | {options.replace('|',' | ')}"

    if ptype == "quiz":
        try:
            correct = int(float(correct))
        except:
            correct = 1

        return f"/quiz {text} | {options.replace('|',' | ')} | {correct}"

    if ptype == "cta":
        return f"/cta {text} | {options.replace('|',' | ')}"

    return text


# ---------------------------
# Send message
# ---------------------------
async def send_post(row):

    msg = build_message(row)
    image = row.get("image_url", "")

    print("Posting:", msg)

    if image:
        await bot.send_photo(GROUP_ID, photo=image, caption=msg)
    else:
        await bot.send_message(GROUP_ID, msg)


# ---------------------------
# Scheduler loop
# ---------------------------
async def scheduler():

    while True:

        try:
            rows = fetch_sheet()
        except Exception as e:
            print("Sheet fetch error:", e)
            await asyncio.sleep(120)
            continue

        now = datetime.now()

        for i, row in enumerate(rows):

            if i in posted_rows:
                continue

            date_val = row.get("date")
            time_val = row.get("time")

            if not date_val or not time_val:
                continue

            scheduled = parse_datetime(date_val, time_val)

            if not scheduled:
                continue

            print("Scheduled:", scheduled, "Now:", now)

            if now >= scheduled:

                try:
                    await send_post(row)
                    posted_rows.add(i)
                except Exception as e:
                    print("Send error:", e)

        await asyncio.sleep(120)


# ---------------------------
# Main
# ---------------------------
async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    print("Postman scheduler started")

    await scheduler()


if __name__ == "__main__":
    asyncio.run(main())
