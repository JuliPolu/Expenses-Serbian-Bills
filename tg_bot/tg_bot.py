import logging
import os
import psycopg2
import asyncio
import nest_asyncio
import pandas as pd
import calendar
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.dispatcher import FSMContext
import racun_library as rachun
from racun_library import UrlProcess
from aiogram.contrib.fsm_storage.memory import MemoryStorage


logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)

APP_TOKEN = os.environ.get("APP_TOKEN")
# Database connection parameters
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

bot = Bot(token=APP_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
)

async def send_command_keyboard(message: types.Message):
    markup = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    btn_add = KeyboardButton("/add")
    btn_count = KeyboardButton("/count")
    btn_by_month = KeyboardButton("/by_month")
    btn_by_category = KeyboardButton("/by_category")
    

    # Add buttons to the markup
    markup.add(btn_add, btn_count, btn_by_month, btn_by_category)
    
    await message.answer("Please select a command:", reply_markup=markup)


@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    """
    This handler will be called when user sends `/start` or `/help` command
    """
    await send_command_keyboard(message)
    

@dp.message_handler(commands="add")
async def add_task_command(message: types.Message):
    await message.answer("Please provide the URL to add Rachun:")
    await UrlProcess.waiting_for_url.set()



@dp.message_handler(state=UrlProcess.waiting_for_url)
async def process_url(message: types.Message, state: FSMContext):
    url = message.text.strip() 
    data = []
    # Check if URL is already in the DB
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products WHERE URL = %s", (url,))
    count = cursor.fetchone()[0]
    cursor.close()
    # If URL is found in the DB, notify user and return
    if count > 0:
        await message.reply(f"URL is already present in the DB", parse_mode="Markdown")
        pass
    # Else scrape rachun data and insert data into the DB table
    else:
        logging.info(f"Starting collecting data")
        await message.reply(f"Starting collecting data")
        
        await rachun.scrape_main(url, data)
        logging.info(f"Collected {len(data)} items")
        logging.info(data)
        
        if len(data) > 0 :
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
            await message.reply(f"Added {len(df_add)} items to DB", parse_mode="Markdown")
        else: 
            await message.reply(f"No items were added to DB, check RACHUN!", parse_mode="Markdown")
        
    await state.finish()
    await send_command_keyboard(message)


@dp.message_handler(commands="count")
async def add_task(payload: types.Message):

    cursor = conn.cursor()
    # Execute the count query
    cursor.execute("SELECT COUNT(*) FROM products;")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    logging.info(f"Total items in DB: {total_rows}")
    await payload.reply(f"Total items in DB: *{total_rows}*", parse_mode="Markdown")
    
    await send_command_keyboard(payload)


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
        month_name = calendar.month_name[month_num]  # Using the calendar module to get the month name
        total_expense = round(row[1], 2)
        table_content += f"{month_name} | ${total_expense}\n"
    # Sending the table to Telegram
    table_msg = table_header + table_content
    await payload.reply(table_msg, parse_mode="Markdown")
    
    await send_command_keyboard(payload)



@dp.message_handler(commands="by_category")
async def summary_by_category_command(message: types.Message):
    await message.answer("Please enter the month number (1-12) for the summary by category:")
    await SummaryProcess.waiting_for_month.set()

@dp.message_handler(state = SummaryProcess.waiting_for_month)
async def process_month(message: types.Message, state: FSMContext):
    month = message.text.strip()
    if not month.isdigit() or not 1 <= int(month) <= 12:
        await message.reply("Please enter a valid month number (1-12).")
        return

    cursor = conn.cursor()
    cursor.execute("""
    SELECT Category, SUM(TotalPrice) AS total_expense
    FROM products
    WHERE EXTRACT(MONTH FROM Date) = %s
    GROUP BY Category;
    """, (month,))
    results = cursor.fetchall()
    cursor.close()

    if not results:
        await message.reply("No data found for the selected month.")
    else:
        # Format the output
        response = "Category | Total Expense\n"
        response += "---------------------------\n"
        for category, total_expense in results:
            response += f"{category} | ${round(total_expense, 2)}\n"

        await message.reply(response, parse_mode="Markdown")

    await state.finish()
    await send_command_keyboard(message)

class SummaryProcess(StatesGroup):
    waiting_for_month = State()  

async def clear_updates(bot_token):
    bot = Bot(token=bot_token)
    offset = None

    while True:
        updates = await bot.get_updates(offset=offset, limit=100)
        if not updates:
            break
        offset = updates[-1].update_id + 1

    # await (await bot.get_session()).close()
    await bot.session.close()


if __name__ == "__main__":
    # Clear previous updates
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(clear_updates(APP_TOKEN))

    # Start the bot's polling
    executor.start_polling(dp)


