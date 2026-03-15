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
    print("Fetching sheet...")

    r = requests.get(SHEET_URL)
    r.raise_for_status()

    lines = r.text.splitlines()

    reader = csv.DictReader(lines)

    rows = []

    for row in reader:
        clean = {}
        for k, v in row.items():
            if k:
                clean[k.strip().lower()] = v.strip() if v else ""
        rows.append(clean)

    print("Rows found:", len(rows))

    return rows


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


async def scheduler():

    while True:

        try:
            rows = fetch_sheet()
        except Exception as e:
            print("Sheet error:", e)
            await asyncio.sleep(120)
            continue

        now = datetime.now()

        print("Current time:", now)

        for i, row in enumerate(rows):

            if i in posted_rows:
                continue

            print("Row:", row)

            date_val = row.get("date")
            time_val = row.get("time")

            if not date_val or not time_val:
                print("Skipping row due to missing date/time")
                continue

            scheduled = parse_datetime(date_val, time_val)

            if not scheduled:
                print("Invalid date format:", date_val, time_val)
                continue

            print("Scheduled:", scheduled)

            if now >= scheduled:

                msg = build_message(row)

                try:
                    await bot.send_message(GROUP_ID, msg)
                    print("Posted:", msg)
                    posted_rows.add(i)
                except Exception as e:
                    print("Send error:", e)

        await asyncio.sleep(120)


async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    print("Postman scheduler started")

    await scheduler()


if __name__ == "__main__":
    asyncio.run(main())
