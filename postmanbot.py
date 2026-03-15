import os
import csv
import asyncio
import requests
from datetime import datetime, timedelta

from aiogram import Bot

TOKEN = os.getenv("POSTMAN_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("SHEET_URL")

bot = Bot(token=TOKEN)

posted_rows = set()


# ---------- Fetch Google Sheet ----------

def fetch_sheet():

    print("Fetching sheet...")

    r = requests.get(SHEET_URL)
    r.raise_for_status()

    lines = r.content.decode("utf-8").splitlines()

    reader = csv.DictReader(lines)

    rows = []

    for row in reader:

        clean = {}

        for k, v in row.items():

            if k:
                key = k.strip().lower()
                clean[key] = v.strip() if v else ""

        rows.append(clean)

    print("Rows found:", len(rows))

    return rows


# ---------- Parse Date Time ----------

def parse_datetime(date_str, time_str):

    formats = [

        ("%d/%m/%Y", "%H:%M:%S"),
        ("%d/%m/%Y", "%H:%M"),

        ("%d-%m-%Y", "%H:%M:%S"),
        ("%d-%m-%Y", "%H:%M"),

        ("%Y-%m-%d", "%H:%M:%S"),
        ("%Y-%m-%d", "%H:%M")

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


# ---------- Build Telegram Message ----------

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


# ---------- Send Post ----------

async def send_post(row):

    message = build_message(row)

    image = row.get("image_url", "")

    if image:

        await bot.send_photo(
            GROUP_ID,
            photo=image,
            caption=message
        )

    else:

        await bot.send_message(
            GROUP_ID,
            message
        )


# ---------- Scheduler Loop ----------

async def scheduler():

    while True:

        try:

            rows = fetch_sheet()

        except Exception as e:

            print("Sheet error:", e)

            await asyncio.sleep(120)

            continue


        # Convert server time → IST

        now = datetime.utcnow() + timedelta(hours=5, minutes=30)

        print("Current IST time:", now)


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

                try:

                    await send_post(row)

                    posted_rows.add(i)

                    print("Posted successfully")

                except Exception as e:

                    print("Send error:", e)


        await asyncio.sleep(120)


# ---------- Main ----------

async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    print("Postman scheduler started")

    await scheduler()


if __name__ == "__main__":

    asyncio.run(main())
