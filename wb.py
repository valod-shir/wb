import asyncio
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Tuple

import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger("wb_bot")


def get_bot_token() -> str:
    token_regex = re.compile(r"\d{8,12}:[A-Za-z0-9_-]{30,40}")
    for env_key, env_val in os.environ.items():
        if not env_val:
            continue
        match = token_regex.search(env_val)
        if match:
            return match.group(0)

    for env_key, env_val in os.environ.items():
        if any(k in env_key.upper() for k in ("TELEG", "BOT_TOKEN", "TG_TOKEN", "TOKEN")):
            clean = env_val.strip().strip('"').strip("'")
            if ":" in clean:
                return clean
    return ""


BOT_TOKEN = get_bot_token()
DB_PATH = BASE_DIR / "wb_tracker.db"


# ==========================================
# 1. БАЗА ДАННЫХ (SQLite для избранных товаров)
# ==========================================
def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                nm_id TEXT NOT NULL,
                title TEXT,
                brand TEXT,
                last_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, nm_id)
            )
            """
        )
        conn.commit()


def add_tracked_item(user_id: int, nm_id: str, title: str, brand: str, price: float) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO tracked_items (user_id, nm_id, title, brand, last_price)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, nm_id, title, brand, price),
            )
            conn.commit()
            return True
        except Exception:
            return False


def get_user_items(user_id: int) -> List[Tuple[str, str, str, float]]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT nm_id, title, brand, last_price FROM tracked_items WHERE user_id = ? ORDER BY id DESC",
            (user_id,),
        )
        return cursor.fetchall()


def delete_tracked_item(user_id: int, nm_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tracked_items WHERE user_id = ? AND nm_id = ?", (user_id, nm_id))
        conn.commit()
        return cursor.rowcount > 0


# ==========================================
# 2. ПАРСИНГ WILDBERRIES API
# ==========================================
async def get_wb_product_info(session: aiohttp.ClientSession, nm_id: str) -> Tuple[bool, Dict[str, Any]]:
    url = f"https://card.wb.ru/cards/v1/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm={nm_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"
    }

    try:
        async with session.get(url, headers=headers, timeout=8) as resp:
            if resp.status == 200:
                data = await resp.json()
                products = data.get("data", {}).get("products", [])
                if not products:
                    return False, {"error": "Товар с таким артикулом не найден на Wildberries."}
                
                prod = products[0]
                title = prod.get("name", "Без названия")
                brand = prod.get("brand", "Не указан")
                
                min_price = float('inf')
                sizes = prod.get("sizes", [])
                for size in sizes:
                    price_info = size.get("price", {})
                    val = price_info.get("product", 0) / 100.0
                    if 0 < val < min_price:
                        min_price = val

                if min_price == float('inf'):
                    min_price = 0.0

                return True, {
                    "id": nm_id,
                    "title": title,
                    "brand": brand,
                    "price": min_price,
                    "url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"
                }
            else:
                return False, {"error": f"Ошибка запроса к WB API (HTTP {resp.status})"}
    except Exception as e:
        logger.error(f"Ошибка парсинга WB: {e}")
        return False, {"error": "Не удалось подключиться к серверам Wildberries."}


# ==========================================
# 3. КЛАВИАТУРЫ И РОУТЕР
# ==========================================
def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⭐ Мое избранное"), KeyboardButton(text="🔍 Как искать товары?")],
            [KeyboardButton(text="⚙️ Статус бота")],
        ],
        resize_keyboard=True,
    )


def get_item_inline_kb(nm_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⭐ Сохранить в избранное", callback_data=f"save_wb:{nm_id}")]
        ]
    )


router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    text = (
        "🛍️ <b>Бот-поисковик цен Wildberries</b>\n\n"
        "• Отправьте в чат <b>артикул товара</b> (например: <code>183421364</code>) или ссылку на него.\n"
        "• Бот мгновенно найдет актуальную минимальную цену и бренд."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "🔍 Как искать товары?")
async def cmd_help(message: Message):
    text = (
        "💡 <b>Инструкция:</b>\n\n"
        "1. Откройте приложение Wildberries и скопируйте ссылку на товар или его артикул (числовой код).\n"
        "2. Вставьте его в этот чат.\n"
        "3. Бот покажет актуальную стоимость и позволит сохранить товар в избранное."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "⚙️ Статус бота")
async def cmd_status(message: Message):
    text = (
        "⚙️ <b>Статус системы:</b>\n\n"
        f"📂 База данных: <b>Активна ({DB_PATH.name})</b>\n"
        "🌐 Источник цен: <b>Официальное API Wildberries</b>\n"
        "⚡ Режим работы: <b>Онлайн (24/7)</b>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard())


@router.message(F.text == "⭐ Мое избранное")
async def show_favorites(message: Message):
    items = get_user_items(message.from_user.id)
    if not items:
        await message.answer("📭 Ваш список избранных товаров пуст.", reply_markup=get_main_keyboard())
        return

    lines = ["⭐ <b>Ваши сохраненные товары:</b>\n"]
    for nm_id, title, brand, price in items:
        lines.append(
            f"• <b>{brand}</b> / {title}\n"
            f"  Артикул: <code>{nm_id}</code> | Цена: <b>{price:,.2f} ₽</b>\n"
            f"  🔗 <a href='https://www.wildberries.ru/catalog/{nm_id}/detail.aspx'>Открыть на WB</a>\n"
        )
    await message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True, reply_markup=get_main_keyboard())


@router.callback_query(F.data.startswith("save_wb:"))
async def cb_save_item(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    user_id = callback.from_user.id

    async with aiohttp.ClientSession() as session:
        success, res = await get_wb_product_info(session, nm_id)

    if success:
        add_tracked_item(user_id, nm_id, res["title"], res["brand"], res["price"])
        await callback.answer("Товар успешно сохранен в избранное!")
    else:
        await callback.answer("Не удалось обновить данные товара.")


@router.message(F.text)
async def handle_wb_query(message: Message):
    text = message.text.strip()
    match_art = re.search(r"(\d{6,10})", text)
    if not match_art:
        await message.answer(
            "❓ Пожалуйста, отправьте корректный артикул товара (например, <code>19472654</code>) или ссылку.",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(),
        )
        return

    nm_id = match_art.group(1)
    wait_msg = await message.answer(f"⏳ Запрашиваю актуальную цену для артикула <code>{nm_id}</code>...", parse_mode="HTML")

    async with aiohttp.ClientSession() as session:
        success, result = await get_wb_product_info(session, nm_id)

    await wait_msg.delete()

    if not success:
        await message.answer(f"❌ Ошибка: {result.get('error', 'Не удалось получить данные.')}")
        return

    response_text = (
        f"📦 <b>{result['brand']}</b>\n"
        f"📝 {result['title']}\n\n"
        f"🏷️ Артикул: <code>{result['id']}</code>\n"
        f"💰 <b>Актуальная цена: {result['price']:,.2f} ₽</b>\n\n"
        f"🔗 <a href='{result['url']}'>Открыть товар на Wildberries</a>"
    )

    await message.answer(
        response_text,
        parse_mode="HTML",
        disable_web_page_preview=False,
        reply_markup=get_item_inline_kb(nm_id),
    )


# ==========================================
# 4. ВЕБ-СЕРВЕР ДЛЯ RENDER (АНТИ-СОН)
# ==========================================
async def keep_alive_ping():
    external_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not external_url:
        return
    ping_url = f"{external_url.rstrip('/')}/health"
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            await asyncio.sleep(600)
            try:
                async with session.get(ping_url, timeout=10) as resp:
                    logger.info(f"Keep-alive ping: HTTP {resp.status}")
            except Exception as e:
                logger.warning(f"Keep-alive ping error: {e}")


async def start_render_web_server():
    app = web.Application()
    app.router.add_get("/", lambda request: web.Response(text="WB Price Bot is running!"))
    app.router.add_get("/health", lambda request: web.Response(text="OK"))

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Веб-сервер запущен на порту {port}")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден токен Telegram-бота в переменных окружения!")

    init_db()
    await start_render_web_server()
    asyncio.create_task(keep_alive_ping())

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    logger.info("WB Бот успешно запущен и готов к работе!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
