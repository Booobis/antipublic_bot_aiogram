import aiofiles
import asyncio
import logging
import time

import aiosqlite
from aiogram import types, Dispatcher, Bot, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage

TOKEN = ""
logging.basicConfig(level=logging.INFO)
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


async def create_table():
    async with aiosqlite.connect('my_database.db') as db:
        await db.execute("CREATE TABLE IF NOT EXISTS strings(string VARCHAR)")
        await db.commit()

async def insert_strings(db, strings):
    chunk_size = 1000
    chunks = [strings[i:i+chunk_size] for i in range(0, len(strings), chunk_size)]
    async with db.execute('BEGIN'):
        for chunk in chunks:
            await db.executemany('INSERT INTO strings VALUES (?)', [(s,) for s in chunk])
        await db.commit()

async def select_string(db, string):
    async with db.execute('SELECT string FROM strings WHERE string = ?', (string,)) as cursor:
        return await cursor.fetchone()


async def check_strings(db, strings):
    results = []
    for string in strings:
        result = await select_string(db, string)
        results.append(result)
    return results


async def process_file(file_id, message):
    start_time = time.time()
    new = 0
    g = 0
    strings = []
    async with aiofiles.open(f'{file_id}.txt', mode='r', encoding='utf-8') as file:
        async for line in file:
            g += 1
            strings.append(line.strip())
    async with aiosqlite.connect('my_database.db') as db:
        await create_table()
        results = await check_strings(db, strings)
        unique_strings = [string for string, result in zip(strings, results) if result is None]
        new = len(unique_strings)
        await insert_strings(db, unique_strings)
        async with aiofiles.open(f'uniq_{file_id}.txt', mode='w', encoding='utf-8') as uniq:
            await uniq.write('\n'.join(unique_strings))
        try:
            await bot.send_document(message.chat.id, open(f'uniq_{file_id}.txt', "rb"),
                                    caption=f"Уникальных строк: {new}")
        except Exception:
            await bot.send_message(message.chat.id, "Уникальных строк нет :(")
    await bot.send_message(message.chat.id,
                           f"Время выполнения обработки {g} файлов в {time.time() - start_time} секунд")


@dp.message_handler(content_types=['document'])
async def handle_document(message: types.Document):
    start_time = time.time()
    if message.document.file_name.endswith(".txt"):
        file = await bot.get_file(message.document.file_id)
        file_path = file.file_path
        await bot.download_file(file_path, f"{message.document.file_id}.txt")
        file_id = message.document.file_id
        g = 0
        strings = []
        async with aiofiles.open(f'{file_id}.txt', mode='r', encoding='utf-8') as file:
            async for line in file:
                g += 1
                strings.append(line.strip())
        async with aiosqlite.connect('my_database.db') as db:
            await create_table()
            results = await check_strings(db, strings)
            unique_strings = [string for string, result in zip(strings, results) if result is None]
            new = len(unique_strings)
            await insert_strings(db, unique_strings)
            async with aiofiles.open(f'uniq_{file_id}.txt', mode='w', encoding='utf-8') as uniq:
                await uniq.write('\n'.join(unique_strings))
            try:
                await bot.send_document(message.chat.id, open(f'uniq_{file_id}.txt', "rb"),
                                        caption=f'Уникальных строк: {new}')
            except Exception:
                await bot.send_message(message.chat.id, "Уникальных строк нет :(")
            end_time = time.time()
            await bot.send_message(message.chat.id,
                                   f"Время выполнения обработки {g} файлов в {end_time - start_time} секунд")


async def main():
    await create_table()


#asyncio.run(main())
while True:
    try:
        executor.start_polling(dp, skip_updates=True)
    except Exception as e:
        print(f"УПАЛ. {e}")
