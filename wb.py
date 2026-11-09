import asyncio
import html
import logging
import os
import re
import socket
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import quote

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
    for _, env_val in os.environ.items():
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

WB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Кэш последних текстовых запросов пользователей для переключения режимов сортировки
LAST_SEARCH_QUERY: Dict[int, str] = {}


# ==========================================
# 1. БАЗА ДАННЫХ (SQLite)
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
                min_seen_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, nm_id)
            )
            """
        )
        conn.commit()


def save_tracked_item(
    user_id: int, nm_id: str, title: str, brand: str, price: float
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT min_seen_price FROM tracked_items WHERE user_id = ? AND nm_id = ?",
            (user_id, nm_id),
        )
        row = cursor.fetchone()
        min_price = min(price, row[0]) if (row and row[0] and row[0] > 0) else price

        cursor.execute(
            """
            INSERT OR REPLACE INTO tracked_items
            (user_id, nm_id, title, brand, last_price, min_seen_price)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, nm_id, title, brand, price, min_price),
        )
        conn.commit()


def update_item_price_in_db(user_id: int, nm_id: str, new_price: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE tracked_items
            SET last_price = ?,
                min_seen_price = CASE
                    WHEN min_seen_price IS NULL OR ? < min_seen_price THEN ?
                    ELSE min_seen_price
                END
            WHERE user_id = ? AND nm_id = ?
            """,
            (new_price, new_price, new_price, user_id, nm_id),
        )
        conn.commit()


def get_user_items(user_id: int) -> List[Tuple[str, str, str, float, float]]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT nm_id, title, brand, last_price, min_seen_price
            FROM tracked_items
            WHERE user_id = ?
            ORDER BY id DESC
            """,
            (user_id,),
        )
        return cursor.fetchall()


def get_all_tracked_items() -> List[Tuple[int, str, str, str, float]]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, nm_id, title, brand, last_price FROM tracked_items"
        )
        return cursor.fetchall()


def delete_tracked_item(user_id: int, nm_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM tracked_items WHERE user_id = ? AND nm_id = ?",
            (user_id, nm_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def clear_user_items(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tracked_items WHERE user_id = ?", (user_id,))
        conn.commit()
        return cursor.rowcount


# ==========================================
# 2. ПАРСИНГ И ПОИСК WILDBERRIES API
# ==========================================
def extract_product_metrics(prod: Dict[str, Any]) -> Dict[str, Any]:
    """Универсальный парсер цены, скидки, рейтинга и остатков из объекта товара WB."""
    nm_id = str(prod.get("id", ""))
    title = str(prod.get("name", "Без названия")).strip()
    brand = str(prod.get("brand", "Без бренда")).strip() or "Без бренда"
    rating = float(prod.get("reviewRating") or prod.get("rating") or 0.0)
    feedbacks = int(prod.get("feedbacks") or 0)

    min_price = float("inf")
    basic_price = 0.0
    total_qty = 0

    sizes = prod.get("sizes", [])
    for s in sizes:
        stocks = s.get("stocks", [])
        for st in stocks:
            total_qty += int(st.get("qty", 0))

        price_obj = s.get("price", {})
        if price_obj:
            p_curr = (price_obj.get("product") or price_obj.get("total") or 0) / 100.0
            p_base = (price_obj.get("basic") or 0) / 100.0
            if 0 < p_curr < min_price:
                min_price = p_curr
                basic_price = p_base

    if min_price == float("inf"):
        sale_u = (prod.get("salePriceU") or prod.get("priceU") or 0) / 100.0
        min_price = sale_u if sale_u > 0 else 0.0
        basic_price = (prod.get("priceU") or 0) / 100.0

    if total_qty == 0 and prod.get("totalQuantity"):
        total_qty = int(prod.get("totalQuantity", 0))

    discount_pct = 0
    if basic_price > min_price > 0:
        discount_pct = round((1 - min_price / basic_price) * 100)

    return {
        "id": nm_id,
        "title": title,
        "brand": brand,
        "price": min_price,
        "old_price": basic_price,
        "discount": discount_pct,
        "rating": rating,
        "feedbacks": feedbacks,
        "qty": total_qty,
        "url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
    }


async def get_wb_product_by_id(
    session: aiohttp.ClientSession, nm_id: str
) -> Tuple[bool, Dict[str, Any]]:
    """Получает детальную информацию о товаре по артикулу (v2 + резервный v1)."""
    urls = [
        f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm={nm_id}",
        f"https://card.wb.ru/cards/v1/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm={nm_id}",
    ]

    for url in urls:
        try:
            async with session.get(url, headers=WB_HEADERS, timeout=8) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    products = data.get("data", {}).get("products", [])
                    if products:
                        info = extract_product_metrics(products[0])
                        if info["price"] > 0 or info["qty"] >= 0:
                            return True, info
        except Exception as e:
            logger.warning(f"Ошибка запроса {url}: {e}")

    return False, {"error": "Товар с таким артикулом не найден или временно недоступен."}


async def search_wb_lowest_prices(
    session: aiohttp.ClientSession, query: str, mode: str = "smart"
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Ищет товары по текстовому запросу и находит самые низкие цены.
    mode='smart': берет топ релевантных товаров (с рейтингом >= 4.4) и сортирует по возрастанию цены.
    mode='priceup': ищет абсолютный минимум цены по всему каталогу WB.
    """
    encoded_q = quote(query.strip())
    wb_sort = "popular" if mode == "smart" else "priceup"

    search_urls = [
        f"https://search.wb.ru/exactmatch/ru/common/v7/search?ab_testing=false&appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false",
        f"https://search.wb.ru/exactmatch/ru/common/v5/search?ab_testing=false&appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false",
        f"https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30",
    ]

    raw_products: List[Dict[str, Any]] = []
    for url in search_urls:
        try:
            async with session.get(url, headers=WB_HEADERS, timeout=9) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    raw_products = data.get("data", {}).get("products", [])
                    if raw_products:
                        break
        except Exception as e:
            logger.warning(f"Ошибка поиска WB ({url[:45]}...): {e}")

    if not raw_products:
        return False, []

    parsed_list = []
    for prod in raw_products:
        item = extract_product_metrics(prod)
        if item["price"] > 0:
            parsed_list.append(item)

    if not parsed_list:
        return False, []

    if mode == "smart":
        # Фильтруем товары с хорошим рейтингом, чтобы отсечь фейки и копеечные комплектующие
        good_rated = [
            p for p in parsed_list if p["rating"] >= 4.4 and p["feedbacks"] >= 5
        ]
        target_pool = good_rated if len(good_rated) >= 3 else parsed_list
        target_pool.sort(key=lambda x: x["price"])
        return True, target_pool[:7]
    else:
        parsed_list.sort(key=lambda x: x["price"])
        return True, parsed_list[:7]


# ==========================================
# 3. КЛАВИАТУРЫ
# ==========================================
def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="⭐ Мое избранное"),
                KeyboardButton(text="🔄 Проверить скидки сейчас"),
            ],
            [
                KeyboardButton(text="🔍 Как искать товары?"),
                KeyboardButton(text="⚙️ Статус бота"),
            ],
        ],
        resize_keyboard=True,
    )


def get_product_inline_kb(nm_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⭐ Отслеживать снижение цены", callback_data=f"fav_add:{nm_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Обновить карточку", callback_data=f"refresh_id:{nm_id}"
                )
            ],
        ]
    )


def get_search_results_kb(
    items: List[Dict[str, Any]], current_mode: str
) -> InlineKeyboardMarkup:
    buttons = []
    # Кнопки для быстрого добавления любого из найденных товаров в трекер
    row = []
    for idx, item in enumerate(items[:5], start=1):
        row.append(
            InlineKeyboardButton(
                text=f"⭐ #{idx} ({int(item['price'])}₽)",
                callback_data=f"fav_add:{item['id']}",
            )
        )
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    # Переключатель режима поиска
    if current_mode == "smart":
        buttons.append(
            [
                InlineKeyboardButton(
                    text="📉 Показать абсолютный минимум по цене",
                    callback_data="search_mode:priceup",
                )
            ]
        )
    else:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="🔥 Показать выгодные хиты (Рейтинг 4.4+)",
                    callback_data="search_mode:smart",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_favorites_kb(items: List[Tuple[str, str, str, float, float]]) -> InlineKeyboardMarkup:
    buttons = []
    for nm_id, title, _, price, _ in items[:8]:
        short_title = title[:18] + ".." if len(title) > 18 else title
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"❌ Удалить {short_title} ({int(price)}₽)",
                    callback_data=f"fav_del:{nm_id}",
                )
            ]
        )
    if len(items) > 1:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="🗑️ Очистить весь список", callback_data="fav_clear_all"
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ==========================================
# 4. ОБРАБОТЧИКИ КОМАНД И СООБЩЕНИЙ
# ==========================================
router = Router()


def format_product_card(info: Dict[str, Any]) -> str:
    brand = html.escape(info["brand"])
    title = html.escape(info["title"])
    price_str = f"{info['price']:,.0f}".replace(",", " ")
    old_str = f"{info['old_price']:,.0f}".replace(",", " ")

    discount_line = (
        f" (<s>{old_str} ₽</s> • <b>-{info['discount']}%</b>)"
        if info["discount"] > 0
        else ""
    )
    stock_str = (
        f"🟢 В наличии ({info['qty']} шт.)"
        if info["qty"] > 0
        else "🔴 Нет в наличии (или остаток скрыт)"
    )

    return (
        f"📦 <b>{brand}</b> — {title}\n\n"
        f"💰 <b>Цена сейчас: {price_str} ₽</b>{discount_line}\n"
        f"⭐ Рейтинг: <b>{info['rating']}</b> ({info['feedbacks']} отз.)\n"
        f"📊 Склад: {stock_str}\n"
        f"🏷️ Артикул: <code>{info['id']}</code>\n\n"
        f"🔗 <a href='{info['url']}'>Открыть на Wildberries</a>"
    )


def format_search_results(
    query: str, items: List[Dict[str, Any]], mode: str
) -> str:
    safe_q = html.escape(query)
    mode_title = (
        "🔥 Выгодные предложения (высокий рейтинг + низкая цена)"
        if mode == "smart"
        else "📉 Самые дешевые товары по возрастанию цены"
    )
    lines = [
        f"🔎 Запрос: <b>«{safe_q}»</b>\n"
        f"<i>{mode_title}:</i>\n"
    ]

    for idx, item in enumerate(items, start=1):
        brand = html.escape(item["brand"])
        title = html.escape(item["title"])
        price_str = f"{item['price']:,.0f}".replace(",", " ")
        disc = f" (-{item['discount']}%)" if item["discount"] > 0 else ""
        lines.append(
            f"<b>{idx}. {price_str} ₽</b>{disc} | ⭐ {item['rating']} ({item['feedbacks']})\n"
            f"   <b>{brand}</b> / {title}\n"
            f"   Арт: <code>{item['id']}</code> • <a href='{item['url']}'>Открыть на WB</a>\n"
        )

    lines.append("💡 <i>Нажмите кнопку с номером товара ниже, чтобы отслеживать падение его цены!</i>")
    return "\n".join(lines)


@router.message(CommandStart())
async def cmd_start(message: Message):
    text = (
        "🛍️ <b>Умный радар минимальных цен Wildberries (10/10)</b>\n\n"
        "Напишите в чат, что вы ищете, в любом формате:\n"
        "1️⃣ <b>Название товара</b> (например: <code>беспроводные наушники</code> или <code>кофе в зернах 1 кг</code>) — бот найдет самые низкие цены.\n"
        "2️⃣ <b>Артикул или ссылку WB</b> (например: <code>183421364</code>) — бот покажет карточку, остатки на складе и возьмет цену на мониторинг 24/7."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "🔍 Как искать товары?")
async def cmd_help(message: Message):
    text = (
        "💡 <b>Что умеет этот бот:</b>\n\n"
        "• <b>Поиск по названию:</b> просто напишите в чат название любого товара. Бот отфильтрует лучшие предложения и выстроит их от самой низкой цены.\n"
        "• <b>Проверка по артикулу/ссылке:</b> отправьте артикул или ссылку из приложения WB.\n"
        "• <b>Авто-радар скидок 24/7:</b> добавьте товар в «⭐ Мое избранное», и бот сам пришлет вам уведомление, как только продавец снизит цену!"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "⚙️ Статус бота")
async def cmd_status(message: Message):
    items = get_user_items(message.from_user.id)
    text = (
        "⚙️ <b>Статус системы:</b>\n\n"
        f"📂 База данных: <b>Активна ({DB_PATH.name})</b>\n"
        f"⭐ Товаров у вас на мониторинге: <b>{len(items)} шт.</b>\n"
        "🌐 Поисковый движок: <b>Wildberries Search & Card API v2</b>\n"
        "🔔 Авто-проверка скидок: <b>Каждые 30 минут (24/7)</b>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "⭐ Мое избранное")
async def show_favorites(message: Message):
    items = get_user_items(message.from_user.id)
    if not items:
        await message.answer(
            "📭 Ваш список отслеживания пуст.\nНапишите название товара или пришлите артикул, чтобы добавить позиции!",
            reply_markup=get_main_keyboard(),
        )
        return

    lines = [f"⭐ <b>Ваши отслеживаемые товары ({len(items)} шт.):</b>\n"]
    for nm_id, title, brand, last_price, min_price in items:
        b_safe = html.escape(brand)
        t_safe = html.escape(title)
        lp_str = f"{last_price:,.0f}".replace(",", " ")
        mp_str = f"{min_price:,.0f}".replace(",", " ")
        lines.append(
            f"🔹 <b>{b_safe}</b> — {t_safe}\n"
            f"   Арт: <code>{nm_id}</code> | Цена: <b>{lp_str} ₽</b> (Мин: {mp_str} ₽)\n"
            f"   🔗 <a href='https://www.wildberries.ru/catalog/{nm_id}/detail.aspx'>Открыть на WB</a>\n"
        )

    await message.answer(
        "\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=get_favorites_kb(items),
    )


@router.message(F.text == "🔄 Проверить скидки сейчас")
async def manual_check_discounts(message: Message):
    items = get_user_items(message.from_user.id)
    if not items:
        await message.answer(
            "📭 Сначала добавьте товары в «⭐ Мое избранное».",
            reply_markup=get_main_keyboard(),
        )
        return

    wait_msg = await message.answer(
        f"⏳ Сверяю актуальные цены для {len(items)} товаров на Wildberries..."
    )

    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    report = ["📊 <b>Результат проверки цен:</b>\n"]

    async with aiohttp.ClientSession(connector=connector) as session:
        for nm_id, title, brand, old_saved_price, _ in items:
            ok, info = await get_wb_product_by_id(session, nm_id)
            t_safe = html.escape(title[:35])
            if not ok or info["price"] <= 0:
                report.append(f"⚪ <code>{nm_id}</code> ({t_safe}) — нет в наличии")
                continue

            new_price = info["price"]
            update_item_price_in_db(message.from_user.id, nm_id, new_price)
            diff = new_price - old_saved_price

            if diff < -1:
                report.append(
                    f"🔥 <b>ПОДЕШЕВЕЛ на {abs(int(diff))} ₽!</b>\n"
                    f"   {t_safe}: было {int(old_saved_price)} ₽ ➡️ <b>стало {int(new_price)} ₽</b>\n"
                    f"   🔗 <a href='{info['url']}'>Забрать на WB</a>"
                )
            elif diff > 1:
                report.append(
                    f"📈 {t_safe}: +{int(diff)} ₽ (сейчас <b>{int(new_price)} ₽</b>)"
                )
            else:
                report.append(
                    f"🟰 {t_safe}: без изменений (<b>{int(new_price)} ₽</b>)"
                )

    await wait_msg.delete()
    await message.answer(
        "\n\n".join(report),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=get_main_keyboard(),
    )


@router.callback_query(F.data.startswith("fav_add:"))
async def cb_add_favorite(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        ok, info = await get_wb_product_by_id(session, nm_id)

    if ok and info["price"] > 0:
        save_tracked_item(
            callback.from_user.id,
            nm_id,
            info["title"],
            info["brand"],
            info["price"],
        )
        await callback.answer(
            f"✅ Сохранено! Слежу за ценой ({int(info['price'])} ₽)", show_alert=False
        )
    else:
        await callback.answer(
            "⚠️ Не удалось получить актуальную цену товара.", show_alert=True
        )


@router.callback_query(F.data.startswith("refresh_id:"))
async def cb_refresh_card(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        ok, info = await get_wb_product_by_id(session, nm_id)

    if not ok:
        await callback.answer("Товар временно недоступен", show_alert=True)
        return

    await callback.answer("Данные обновлены!")
    try:
        await callback.message.edit_text(
            format_product_card(info),
            parse_mode="HTML",
            disable_web_page_preview=False,
            reply_markup=get_product_inline_kb(nm_id),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("search_mode:"))
async def cb_switch_search_mode(callback: CallbackQuery):
    mode = callback.data.split(":")[1]
    user_id = callback.from_user.id
    query = LAST_SEARCH_QUERY.get(user_id)

    if not query:
        await callback.answer("Напишите поисковый запрос в чат заново.", show_alert=True)
        return

    await callback.answer("Пересчитываю выдачу...")
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        ok, items = await search_wb_lowest_prices(session, query, mode=mode)

    if not ok or not items:
        await callback.answer("По этому фильтру ничего не найдено.", show_alert=True)
        return

    try:
        await callback.message.edit_text(
            format_search_results(query, items, mode),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=get_search_results_kb(items, mode),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("fav_del:"))
async def cb_delete_favorite(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    delete_tracked_item(user_id, nm_id)
    await callback.answer("Товар удален из отслеживания")

    items = get_user_items(user_id)
    if not items:
        await callback.message.edit_text("📭 Ваш список отслеживания пуст.")
    else:
        await callback.message.edit_reply_markup(reply_markup=get_favorites_kb(items))


@router.callback_query(F.data == "fav_clear_all")
async def cb_clear_favorites(callback: CallbackQuery):
    clear_user_items(callback.from_user.id)
    await callback.answer("Список очищен!")
    await callback.message.edit_text("📭 Ваш список отслеживания пуст.")


@router.message(F.text)
async def handle_user_input(message: Message):
    text = message.text.strip()

    # 1. Проверяем, прислал ли пользователь ссылку WB или чистый артикул (6-10 цифр)
    url_match = re.search(r"wildberries\.ru/catalog/(\d{5,11})", text)
    pure_digits_match = re.fullmatch(r"\d{6,11}", text)

    nm_id: Optional[str] = None
    if url_match:
        nm_id = url_match.group(1)
    elif pure_digits_match:
        nm_id = pure_digits_match.group(0)

    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)

    # Сценарий А: Поиск конкретного товара по артикулу или ссылке
    if nm_id:
        wait_msg = await message.answer(
            f"⏳ Загружаю карточку артикула <code>{nm_id}</code>...", parse_mode="HTML"
        )
        async with aiohttp.ClientSession(connector=connector) as session:
            ok, info = await get_wb_product_by_id(session, nm_id)

        await wait_msg.delete()
        if not ok:
            await message.answer(
                f"❌ {info.get('error', 'Товар не найден.')}",
                reply_markup=get_main_keyboard(),
            )
            return

        await message.answer(
            format_product_card(info),
            parse_mode="HTML",
            disable_web_page_preview=False,
            reply_markup=get_product_inline_kb(nm_id),
        )
        return

    # Сценарий Б: Поиск самых низких цен по названию товара
    if len(text) < 2:
        await message.answer(
            "❓ Напишите название товара (от 2 букв) или пришлите артикул WB.",
            reply_markup=get_main_keyboard(),
        )
        return

    LAST_SEARCH_QUERY[message.from_user.id] = text
    wait_msg = await message.answer(
        f"🔎 Ищу самые выгодные цены по запросу <b>«{html.escape(text)}»</b>...",
        parse_mode="HTML",
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        ok, items = await search_wb_lowest_prices(session, text, mode="smart")

    await wait_msg.delete()

    if not ok or not items:
        await message.answer(
            "😕 По вашему запросу ничего не найдено. Попробуйте написать название проще или пришлите артикул товара.",
            reply_markup=get_main_keyboard(),
        )
        return

    await message.answer(
        format_search_results(text, items, mode="smart"),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=get_search_results_kb(items, current_mode="smart"),
    )


# ==========================================
# 5. ФОНОВЫЙ РАДАР СКИДОК + СЕРВЕР RENDER
# ==========================================
async def background_price_watcher(bot: Bot):
    """Каждые 30 минут проверяет избранные товары и шлет уведомление при снижении цены."""
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    while True:
        await asyncio.sleep(1800)  # 30 минут
        all_items = get_all_tracked_items()
        if not all_items:
            continue

        logger.info(f"Фоновая проверка цен для {len(all_items)} позиций...")
        async with aiohttp.ClientSession(connector=connector) as session:
            for user_id, nm_id, title, brand, last_price in all_items:
                ok, info = await get_wb_product_by_id(session, nm_id)
                if not ok or info["price"] <= 0:
                    continue

                new_price = info["price"]
                if last_price > 0 and new_price < last_price - 5:
                    drop = int(last_price - new_price)
                    update_item_price_in_db(user_id, nm_id, new_price)
                    alert_text = (
                        f"🔥 <b>ЦЕНА СНИЗИЛАСЬ НА {drop} ₽!</b>\n\n"
                        f"📦 <b>{html.escape(brand)}</b> — {html.escape(title)}\n"
                        f"📉 Было: <s>{int(last_price)} ₽</s> ➡️ <b>Стало: {int(new_price)} ₽</b>\n"
                        f"🏷️ Артикул: <code>{nm_id}</code>\n\n"
                        f"🔗 <a href='{info['url']}'>Успеть купить на Wildberries</a>"
                    )
                    try:
                        await bot.send_message(
                            user_id, alert_text, parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Не удалось отправить алерт {user_id}: {e}")
                elif new_price != last_price:
                    update_item_price_in_db(user_id, nm_id, new_price)

                await asyncio.sleep(1.5)


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
    app.router.add_get("/", lambda request: web.Response(text="WB Radar Bot 10/10 is live!"))
    app.router.add_get("/health", lambda request: web.Response(text="OK"))

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Веб-сервер для Render запущен на порту {port}")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден токен Telegram-бота в переменных окружения (BOT_TOKEN)!")

    init_db()
    await start_render_web_server()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(keep_alive_ping())
    asyncio.create_task(background_price_watcher(bot))

    logger.info("WB Радар цен (10/10) успешно запущен!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
