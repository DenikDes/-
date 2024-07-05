import logging
import telebot
from telebot import types
import asyncio
import concurrent.futures
from connection_for_db import connect_db
from parsing import parse_habr
import os

bot = telebot.TeleBot(os.environ.get('TELEGRAM_TOKEN'))

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, 'Используйте /search <запрос>, чтобы искать вакансии.\nДля остального функционала можно задействовать /help')

@bot.message_handler(commands=['help'])
def help(message):
    bot.reply_to(message, 'Краткая сводка по командам\n/start - запуск/перезапуск бота\n/search <запрос> - поиск вакансий по запросу\n/recent - вывод 5 случайных вакансий\n/count - вывод общего кол-ва вакансий в бд\n/grafic - вывод на выбор режима раб. дня\n/search_company - поиск вакансий по компании из бд\n/search_vacancy - поиск вакансий по названию вакансии из бд')

@bot.message_handler(commands=['search'])
def search(message):
    query = message.text[len('/search '):]
    logging.info(f"Получен запрос для поиска: {query}")
    if not query:
        bot.reply_to(message, 'Пожалуйста, введите запрос после команды /search.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vacancies;")
        initial_count = cur.fetchone()[0]
    conn.close()

    bot.reply_to(message, f'Ищу вакансии для: {query}')
    asyncio.run(run_parse_habr(query))
    bot.reply_to(message, 'Поиск завершен. Проверьте свою базу данных.')

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE id > %s ORDER BY id LIMIT 5;", (initial_count,))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.reply_to(message, 'Новые вакансии не найдены.')
    else:
        bot.reply_to(message, 'Ниже представлены 5 новых вакансий:')
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

async def run_parse_habr(query: str):
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor()
    await loop.run_in_executor(executor, parse_habr, query)

@bot.message_handler(commands=['recent'])
def recent(message):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies ORDER BY RANDOM() LIMIT 5;")
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.reply_to(message, 'Вакансии не найдены.')
    else:
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

@bot.message_handler(commands=['count'])
def count(message):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vacancies;")
        count = cur.fetchone()[0]
    conn.close()
    bot.reply_to(message, f'Общее количество вакансий в базе данных: {count}')

@bot.message_handler(commands=['grafic'])
def grafic(message):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton(text="Неполный рабочий день", callback_data='part_time'),
        types.InlineKeyboardButton(text="Полный рабочий день", callback_data='full_time')
    ]
    keyboard.add(*buttons)
    bot.reply_to(message, "Выберите график работы:", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data in ['part_time', 'full_time'])
def button(call):
    query_data = call.data

    conn = connect_db()
    with conn.cursor() as cur:
        if query_data == 'part_time':
            cur.execute("SELECT COUNT(*) FROM vacancies WHERE location ILIKE '%Неполный рабочий день%';")
        elif query_data == 'full_time':
            cur.execute("SELECT COUNT(*) FROM vacancies WHERE location ILIKE '%Полный рабочий день%';")
        count = cur.fetchone()[0]
    conn.close()

    bot.answer_callback_query(call.id)
    bot.edit_message_text(text=f'Количество вакансий с графиком "{query_data}": {count}',
                          chat_id=call.message.chat.id,
                          message_id=call.message.message_id)

@bot.message_handler(commands=['search_company'])
def search_company(message):
    bot.send_message(message.chat.id, 'Введите название компании, чтобы найти вакансии:')
    bot.register_next_step_handler(message, handle_company_search)

def handle_company_search(message):
    company_name = message.text
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE company ILIKE %s;", (f"%{company_name}%",))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.send_message(message.chat.id, 'Вакансии не найдены.')
    else:
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

@bot.message_handler(commands=['search_vacancy'])
def search_vacancy(message):
    bot.send_message(message.chat.id, 'Введите название вакансии, чтобы найти вакансии:')
    bot.register_next_step_handler(message, handle_vacancy_search)

def handle_vacancy_search(message):
    vacancy_title = message.text
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE vacancy ILIKE %s;", (f"%{vacancy_title}%",))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.send_message(message.chat.id, 'Вакансии не найдены.')
    else:
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

def run_bot():
    logging.info("Запуск бота...")
    bot.infinity_polling()

if __name__ == "__main__":
    run_bot()
