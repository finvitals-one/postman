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

def fetch_sheet():
    r = requests.get(SHEET_URL)
    r.raise_for_status()
    lines = r.text.splitlines()
    return list(csv.DictReader(lines))

def parse_datetime(date_str, time_str):

    formats = [
        ("%d/%m/%Y", "%H:%M:%S"),
        ("%d/%m/%Y", "%H:%M"),
        ("%d-%m-%Y", "%H:%M:%S"),
        ("%d-%m-%Y", "%H:%M")
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

def build_message(row):

    ptype = row["type"].strip().lower()
    text = row["content"].strip()
    options = row["options"].strip()
    correct = row["correct"].strip()

    if ptype == "poll":
        return f"/poll {text} | {options.replace('|',' | ')}"

    if ptype == "quiz":
        correct = int(float(correct))
        return f"/quiz {text} | {options.replace('|',' | ')} | {correct}"

    if ptype == "cta":
        return f"/cta {text} | {options.replace('|',' | ')}"

    return text

async def send_post(row):

    msg = build_message(row)
    image = row["image_url"].strip()

    if image:
        await bot.send_photo(GROUP_ID, photo=image, caption=msg)
    else:
        await bot.send_message(GROUP_ID, msg)

async def scheduler():

    while True:

        try:
            rows = fetch_sheet()
        except:
            await asyncio.sleep(120)
            continue

        now = datetime.now()

        for i,row in enumerate(rows):

            if i in posted_rows:
                continue

            scheduled = parse_datetime(row["date"], row["time"])

            if not scheduled:
                continue

            if now >= scheduled:

                try:
                    await send_post(row)
                    posted_rows.add(i)
                except:
                    pass

        await asyncio.sleep(120)

async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    while True:
        await scheduler()

if __name__ == "__main__":
    asyncio.run(main())