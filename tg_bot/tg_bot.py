import logging
import os
import psycopg2
import asyncio
import nest_asyncio
import pandas as pd
import calendar
from aiogram import Bot, Dispatcher, executor, types
import RACHUN.tg_bot.racun_library as rachun


logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)

# logging.getLogger('pyppeteer').setLevel(logging.DEBUG)

APP_TOKEN = os.environ.get("APP_TOKEN")
# Database connection parameters
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

bot = Bot(token=APP_TOKEN)
dp = Dispatcher(bot)

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
)

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    """
    This handler will be called when user sends `/start` or `/help` command
    """
    await message.reply("Please select command:\n \
   /add       to add rachun\n \
   /count     to count all items in DB\n \
   /by_month  total expenses by month" )


@dp.message_handler(commands="add")
async def add_task(payload: types.Message):
    url = payload.get_args().strip()
    data = []

    # Check if URL is already in the DB
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products WHERE URL = %s", (url,))
    count = cursor.fetchone()[0]
    cursor.close()
    # If URL is found in the DB, notify user and return
    if count > 0:
        await payload.reply(f"URL is already present in the DB.", parse_mode="Markdown")
        return
    # Else scrape rachun data and insert data into the DB table
    else:
        logging.info(f"Starting collecting data")
        await payload.reply(f"Starting collecting data")
        await rachun.scrape_main(url, data)
        logging.info(f"Data_Collected")
        df_add = pd.DataFrame(data)
        rachun.transform(df_add)
        cursor = conn.cursor()
        # Insert data into the table
        for index, row in df_add.iterrows():
            cursor.execute(
                "INSERT INTO products (Name, Quantity, UnitPrice, TotalPrice, Date, Shop_name, Category, URL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (row["Name"], row["Quantity"], row["UnitPrice"], row["TotalPrice"], row["Date"], row["Shop_name"], row["Category"], row["URL"])
            )
        conn.commit()
        cursor.close()
        logging.info(f"Added {len(df_add)} items to DB")
        await payload.reply(f"Added {len(df_add)} items to DB", parse_mode="Markdown")


@dp.message_handler(commands="count")
async def add_task(payload: types.Message):

    cursor = conn.cursor()
    # Execute the count query
    cursor.execute("SELECT COUNT(*) FROM products;")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    logging.info(f"Total items in DB: {total_rows}")
    await payload.reply(f"Total items in DB: *{total_rows}*", parse_mode="Markdown")


@dp.message_handler(commands="by_month")
async def total_expenses_by_month(payload: types.Message):
    cursor = conn.cursor()
    # Execute the aggregation query
    cursor.execute("""
    SELECT EXTRACT(MONTH FROM Date) AS month, SUM(TotalPrice) AS total_expense
    FROM products
    GROUP BY EXTRACT(MONTH FROM Date)
    ORDER BY month;
    """)
    results = cursor.fetchall()
    cursor.close()
    # Format the output in table format
    table_header = "Month | Total Expense\n"
    table_header += "---------------------\n"
    table_content = ""
    for row in results:
        month_num = int(row[0])
        month_name = calendar.month_name[month_num]
        total_expense = round(row[1], 2)
        table_content += f"{month_name} | ${total_expense}\n"
    # Sending the table to Telegram
    table_msg = table_header + table_content
    await payload.reply(table_msg, parse_mode="Markdown")


async def clear_updates(bot_token):
    bot = Bot(token=bot_token)
    offset = None

    while True:
        updates = await bot.get_updates(offset=offset, limit=100)
        if not updates:
            break
        offset = updates[-1].update_id + 1

    await bot.session.close()


if __name__ == "__main__":
    # Clear previous updates
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(clear_updates(APP_TOKEN))

    # Start the bot's polling
    executor.start_polling(dp)


