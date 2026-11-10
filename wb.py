import asyncio
import html
import json
import logging
import os
import random
import re
import socket
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import quote, unquote

import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

# ==========================================
# 0. КОНФИГУРАЦИЯ И СТРУКТУРИРОВАННОЕ ЛОГИРОВАНИЕ
# ==========================================
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("wb_enterprise")


def resolve_bot_token() -> str:
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


BOT_TOKEN = resolve_bot_token()
DB_PATH = BASE_DIR / "wb_tracker.db"

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.127 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
]

LAST_SEARCH_QUERY: Dict[int, str] = {}
WAITING_TARGET_PRICE: Dict[int, str] = {}
BASKET_HOST_CACHE: Dict[int, str] = {}


def get_spoofed_headers(referer: str = "https://www.wildberries.ru/") -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.wildberries.ru",
        "Referer": referer,
    }


# ==========================================
# 1. СЛОЙ БАЗЫ ДАННЫХ (SQLite WAL + Concurrency Safe)
# ==========================================
class DatabaseManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tracked_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    nm_id TEXT NOT NULL,
                    title TEXT,
                    brand TEXT,
                    last_price REAL DEFAULT 0,
                    min_seen_price REAL DEFAULT 0,
                    target_price REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, nm_id)
                )
                """
            )
            for col_sql in (
                "ALTER TABLE tracked_items ADD COLUMN target_price REAL DEFAULT 0",
                "ALTER TABLE tracked_items ADD COLUMN min_seen_price REAL DEFAULT 0",
            ):
                try:
                    cursor.execute(col_sql)
                except sqlite3.OperationalError:
                    pass

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_cache (
                    nm_id TEXT PRIMARY KEY,
                    title TEXT,
                    brand TEXT,
                    subj_name TEXT,
                    price REAL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            conn.commit()

    def get_setting(self, key: str, default: str = "") -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM bot_settings WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
                (key, value.strip()),
            )
            conn.commit()

    def cache_product(
        self, nm_id: str, title: str, brand: str, subj: str, price: float
    ) -> None:
        if not nm_id or title in ("Без названия", ""):
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO catalog_cache (nm_id, title, brand, subj_name, price, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (str(nm_id), title, brand, subj, price),
            )
            conn.commit()

    def get_cached_product(self, nm_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT nm_id, title, brand, subj_name, price FROM catalog_cache WHERE nm_id = ?",
                (str(nm_id),),
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "title": row[1],
                "brand": row[2],
                "subj_name": row[3],
                "price": row[4],
            }

    def search_cached_nm_ids(self, query: str) -> List[str]:
        words = [w.lower() for w in re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{3,}", query)]
        if not words:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT nm_id, title, brand, subj_name FROM catalog_cache ORDER BY updated_at DESC LIMIT 500"
            ).fetchall()

        exact_matches: List[str] = []
        partial_matches: List[str] = []
        for nm_id, title, brand, subj in rows:
            haystack = f"{title} {brand} {subj}".lower()
            if all(w in haystack for w in words):
                exact_matches.append(nm_id)
            elif any(w[:4] in haystack for w in words if len(w) >= 4):
                partial_matches.append(nm_id)

        combined = exact_matches + [m for m in partial_matches if m not in exact_matches]
        return combined[:15]

    def save_item(
        self,
        user_id: int,
        nm_id: str,
        title: str,
        brand: str,
        price: float,
        hist_min: float = 0.0,
        target_price: Optional[float] = None,
    ) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT min_seen_price, target_price FROM tracked_items WHERE user_id = ? AND nm_id = ?",
                (user_id, nm_id),
            ).fetchone()

            candidates = [
                p for p in (price, hist_min, row[0] if row else 0.0) if p and p > 0
            ]
            min_price = min(candidates) if candidates else price
            t_price = (
                target_price
                if target_price is not None
                else (row[1] if row and row[1] else 0.0)
            )

            conn.execute(
                """
                INSERT OR REPLACE INTO tracked_items
                (user_id, nm_id, title, brand, last_price, min_seen_price, target_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, str(nm_id), title, brand, price, min_price, t_price),
            )
            conn.commit()

    def set_target_price(self, user_id: int, nm_id: str, target_price: float) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE tracked_items SET target_price = ? WHERE user_id = ? AND nm_id = ?",
                (target_price, user_id, str(nm_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def update_price(self, user_id: int, nm_id: str, new_price: float) -> None:
        if new_price <= 0:
            return
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tracked_items
                SET last_price = ?,
                    min_seen_price = CASE
                        WHEN min_seen_price IS NULL OR min_seen_price <= 0 OR ? < min_seen_price THEN ?
                        ELSE min_seen_price
                    END
                WHERE user_id = ? AND nm_id = ?
                """,
                (new_price, new_price, new_price, user_id, str(nm_id)),
            )
            conn.commit()

    def get_user_items(
        self, user_id: int
    ) -> List[Tuple[str, str, str, float, float, float]]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT nm_id, title, brand, last_price, min_seen_price, COALESCE(target_price, 0)
                FROM tracked_items
                WHERE user_id = ?
                ORDER BY id DESC
                """,
                (user_id,),
            ).fetchall()

    def get_all_items(self) -> List[Tuple[int, str, str, str, float, float]]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT user_id, nm_id, title, brand, last_price, COALESCE(target_price, 0) FROM tracked_items"
            ).fetchall()

    def delete_item(self, user_id: int, nm_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM tracked_items WHERE user_id = ? AND nm_id = ?",
                (user_id, str(nm_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def clear_user_items(self, user_id: int) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM tracked_items WHERE user_id = ?", (user_id,)
            )
            conn.commit()
            return cur.rowcount


db = DatabaseManager(DB_PATH)


# ==========================================
# 2. МЕНЕДЖЕР ПРОКСИ И СЕТЕВОЙ УСТОЙЧИВОСТИ
# ==========================================
class ProxyManager:
    """Автоматически распознает форматы ip:port:user:pass, user:pass@ip:port и пулы прокси."""

    @staticmethod
    def normalize_proxy(raw: str) -> Optional[str]:
        clean = raw.strip()
        if not clean or clean.lower() in ("off", "none", "clear", "0"):
            return None

        # Формат ip:port:login:password (популярен у российских провайдеров прокси)
        parts = clean.split(":")
        if len(parts) == 4 and not clean.startswith("http"):
            ip, port, user, pw = parts
            return f"http://{user}:{pw}@{ip}:{port}"

        if len(parts) == 2 and not clean.startswith("http") and "@" not in clean:
            return f"http://{clean}"

        if not clean.startswith("http"):
            return f"http://{clean}"
        return clean

    @classmethod
    def get_active_proxies(cls) -> List[str]:
        raw_db = db.get_setting("proxy_pool", "").strip()
        raw_env = os.getenv("PROXY_URL", "").strip()
        combined = f"{raw_db}\n{raw_env}".strip()
        if not combined:
            return []

        proxies: List[str] = []
        for token in re.split(r"[\s,;]+", combined):
            norm = cls.normalize_proxy(token)
            if norm and norm not in proxies:
                proxies.append(norm)
        return proxies

    @classmethod
    def pick_proxy(cls) -> Optional[str]:
        pool = cls.get_active_proxies()
        return random.choice(pool) if pool else None


# ==========================================
# 3. ENTERPRISE-ЯДРО WILDBERRIES (5-УРОВНЕВЫЙ КАСКАД)
# ==========================================
class WildberriesEnterpriseEngine:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.cdn_semaphore = asyncio.Semaphore(10)

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(
                family=socket.AF_INET,
                ssl=False,
                limit=50,
                ttl_dns_cache=600,
            )
            timeout = aiohttp.ClientTimeout(total=14, connect=5)
            self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self.session

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    @staticmethod
    def get_basket_number(vol: int) -> str:
        ranges = [
            (143, "01"), (287, "02"), (431, "03"), (719, "04"), (1007, "05"),
            (1061, "06"), (1115, "07"), (1169, "08"), (1313, "09"), (1601, "10"),
            (1655, "11"), (1919, "12"), (2045, "13"), (2189, "14"), (2405, "15"),
            (2621, "16"), (2837, "17"), (3053, "18"), (3269, "19"), (3485, "20"),
            (3701, "21"), (3917, "22"), (4133, "23"), (4349, "24"), (4565, "25"),
            (4877, "26"), (5189, "27"), (5501, "28"), (5813, "29"), (6125, "30"),
            (6437, "31"), (6749, "32"), (7061, "33"), (7373, "34"), (7685, "35"),
            (7997, "36"), (8309, "37"), (8621, "38"),
        ]
        for max_vol, b_num in ranges:
            if vol <= max_vol:
                return b_num
        return "39"

    async def autocorrect_typo(self, query: str) -> str:
        """Исправляет опечатки через глобальный сервис Yandex Speller (например: 'Комбенизон' -> 'Комбинезон')."""
        session = await self.get_session()
        url = f"https://speller.yandex.net/services/spellservice.json/checkText?text={quote(query)}&lang=ru,en"
        try:
            async with session.get(url, timeout=3.5) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    corrected = query
                    for item in data:
                        word = item.get("word")
                        suggestions = item.get("s")
                        if word and suggestions:
                            corrected = corrected.replace(word, suggestions[0], 1)
                    return corrected.strip()
        except Exception as e:
            logger.debug(f"Speller check skipped: {e}")
        return query

    @staticmethod
    def _extract_products(data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                return []
        if not isinstance(data, dict):
            return []
        if isinstance(data.get("products"), list) and data["products"]:
            return data["products"]
        nested = data.get("data")
        if isinstance(nested, dict) and isinstance(nested.get("products"), list):
            return nested["products"]
        return []

    @staticmethod
    def _parse_commercial_prod(prod: Dict[str, Any]) -> Dict[str, Any]:
        nm_id = str(prod.get("id") or prod.get("nm_id") or "")
        title = str(prod.get("name") or prod.get("imt_name") or "Без названия").strip()
        brand = str(prod.get("brand") or "Без бренда").strip() or "Без бренда"
        subj_name = str(prod.get("entity") or prod.get("subj_name") or "").strip()
        rating = float(
            prod.get("reviewRating")
            or prod.get("rating")
            or prod.get("nmReviewRating")
            or 0.0
        )
        feedbacks = int(prod.get("feedbacks") or prod.get("nmFeedbacks") or 0)

        min_price = float("inf")
        basic_price = 0.0
        total_qty = 0

        for s in prod.get("sizes", []):
            for st in s.get("stocks", []):
                total_qty += int(st.get("qty", 0))
            price_obj = s.get("price", {})
            if isinstance(price_obj, dict):
                p_curr = (
                    price_obj.get("product")
                    or price_obj.get("total")
                    or price_obj.get("basic")
                    or 0
                ) / 100.0
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
            "subj_name": subj_name,
            "price": min_price,
            "old_price": basic_price,
            "discount": discount_pct,
            "rating": rating,
            "feedbacks": feedbacks,
            "qty": total_qty,
            "history_min": 0.0,
            "history_max": 0.0,
            "history_avg": 0.0,
            "variants": [],
            "url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
        }

    async def fetch_cdn_basket(self, nm_id: str) -> Dict[str, Any]:
        """Загружает статическую карточку card.json и историю цен с глобального CDN wbbasket.ru."""
        try:
            nm_int = int(nm_id)
        except ValueError:
            return {}

        async with self.cdn_semaphore:
            session = await self.get_session()
            vol = nm_int // 100000
            part = nm_int // 1000
            rel_base = f"/vol{vol}/part{part}/{nm_int}"

            primary_b = BASKET_HOST_CACHE.get(vol) or self.get_basket_number(vol)
            primary_int = int(primary_b)

            candidates = [primary_b]
            for offset in (-1, 1, -2, 2, 3, -3, 4, 5):
                c_num = primary_int + offset
                if 1 <= c_num <= 42:
                    c_str = f"{c_num:02d}"
                    if c_str not in candidates:
                        candidates.append(c_str)

            card_data: Dict[str, Any] = {}
            working_host: Optional[str] = None

            for b_num in candidates:
                host = f"https://basket-{b_num}.wbbasket.ru"
                card_url = f"{host}{rel_base}/info/ru/card.json"
                try:
                    async with session.get(
                        card_url, headers=get_spoofed_headers(), timeout=3.5
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if isinstance(data, dict) and (
                                data.get("imt_name")
                                or data.get("subj_name")
                                or data.get("nm_id")
                            ):
                                card_data = data
                                working_host = host
                                BASKET_HOST_CACHE[vol] = b_num
                                break
                except Exception:
                    continue

            if not card_data or not working_host:
                return {}

            title = str(
                card_data.get("imt_name")
                or card_data.get("subj_name")
                or "Товар Wildberries"
            ).strip()
            subj_name = str(card_data.get("subj_name") or "").strip()
            selling = card_data.get("selling", {})
            brand = str(
                selling.get("brand_name")
                or card_data.get("brand_name")
                or "Без бренда"
            ).strip()

            variants: List[str] = []
            for c in card_data.get("colors") or []:
                if isinstance(c, int) or (isinstance(c, str) and c.isdigit()):
                    c_str = str(c)
                    if c_str != str(nm_id) and c_str not in variants:
                        variants.append(c_str)
                elif isinstance(c, dict) and c.get("nm_id"):
                    c_str = str(c["nm_id"])
                    if c_str != str(nm_id) and c_str not in variants:
                        variants.append(c_str)

            for fc in card_data.get("full_colors") or []:
                if isinstance(fc, dict) and fc.get("nm_id"):
                    c_str = str(fc["nm_id"])
                    if c_str != str(nm_id) and c_str not in variants:
                        variants.append(c_str)

            latest_price = 0.0
            min_price = 0.0
            max_price = 0.0
            avg_price = 0.0

            hist_url = f"{working_host}{rel_base}/info/price-history.json"
            try:
                async with session.get(
                    hist_url, headers=get_spoofed_headers(), timeout=3.5
                ) as resp:
                    if resp.status == 200:
                        hist_list = await resp.json(content_type=None)
                        if isinstance(hist_list, list) and hist_list:
                            rub_prices = [
                                (item.get("price", {}).get("RUB") or 0) / 100.0
                                for item in hist_list
                                if isinstance(item, dict)
                                and item.get("price", {}).get("RUB")
                            ]
                            if rub_prices:
                                latest_price = rub_prices[-1]
                                min_price = min(rub_prices)
                                max_price = max(rub_prices)
                                avg_price = sum(rub_prices) / len(rub_prices)
            except Exception:
                pass

            db.cache_product(str(nm_id), title, brand, subj_name, latest_price)

            return {
                "title": title,
                "brand": brand,
                "subj_name": subj_name,
                "latest_hist_price": latest_price,
                "min_hist_price": min_price,
                "max_hist_price": max_price,
                "avg_hist_price": avg_price,
                "variants": variants[:12],
            }

    async def get_product_by_id(self, nm_id: str) -> Tuple[bool, Dict[str, Any]]:
        session = await self.get_session()
        proxy = ProxyManager.pick_proxy()

        detail_urls = [
            f"https://card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm={nm_id}",
            f"https://u-card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm={nm_id}",
            f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm={nm_id}",
        ]

        cdn_task = asyncio.create_task(self.fetch_cdn_basket(nm_id))

        commercial_info: Optional[Dict[str, Any]] = None
        for url in detail_urls:
            try:
                async with session.get(
                    url, headers=get_spoofed_headers(), proxy=proxy, timeout=4.5
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        products = self._extract_products(data)
                        if products:
                            target = products[0]
                            for p in products:
                                if str(p.get("id")) == str(nm_id):
                                    target = p
                                    break
                            commercial_info = self._parse_commercial_prod(target)
                            if (
                                commercial_info["price"] > 0
                                or commercial_info["title"] != "Без названия"
                            ):
                                break
            except Exception:
                continue

        cdn_data = await cdn_task

        if commercial_info:
            if cdn_data:
                if commercial_info["title"] in ("Без названия", ""):
                    commercial_info["title"] = cdn_data["title"]
                if commercial_info["brand"] in ("Без бренда", ""):
                    commercial_info["brand"] = cdn_data["brand"]
                if not commercial_info.get("subj_name"):
                    commercial_info["subj_name"] = cdn_data.get("subj_name", "")
                commercial_info["history_min"] = cdn_data.get("min_hist_price", 0.0)
                commercial_info["history_max"] = cdn_data.get("max_hist_price", 0.0)
                commercial_info["history_avg"] = cdn_data.get("avg_hist_price", 0.0)
                commercial_info["variants"] = cdn_data.get("variants", [])
                if (
                    commercial_info["price"] <= 0
                    and cdn_data.get("latest_hist_price", 0) > 0
                ):
                    commercial_info["price"] = cdn_data["latest_hist_price"]

            db.cache_product(
                str(nm_id),
                commercial_info["title"],
                commercial_info["brand"],
                commercial_info.get("subj_name", ""),
                commercial_info["price"],
            )
            return True, commercial_info

        if cdn_data:
            return True, {
                "id": str(nm_id),
                "title": cdn_data["title"],
                "brand": cdn_data["brand"],
                "subj_name": cdn_data.get("subj_name", ""),
                "price": cdn_data.get("latest_hist_price", 0.0),
                "old_price": cdn_data.get("max_hist_price", 0.0),
                "discount": 0,
                "rating": 0.0,
                "feedbacks": 0,
                "qty": 0,
                "history_min": cdn_data.get("min_hist_price", 0.0),
                "history_max": cdn_data.get("max_hist_price", 0.0),
                "history_avg": cdn_data.get("avg_hist_price", 0.0),
                "variants": cdn_data.get("variants", []),
                "url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
            }

        return False, {"error": "Товар с таким артикулом не найден в базе Wildberries."}

    async def _fetch_search_via_relays(
        self, encoded_q: str, wb_sort: str
    ) -> List[Dict[str, Any]]:
        """Туннелирует запрос к WB Search API через публичные прокси-ретрансляторы в обход блокировки IP Render."""
        session = await self.get_session()
        target_wb_url = (
            f"https://search.wb.ru/exactmatch/ru/common/v18/search"
            f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
            f"&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false"
        )
        relay_urls = [
            f"https://api.codetabs.com/v1/proxy?quest={quote(target_wb_url, safe='')}",
            f"https://api.allorigins.win/raw?url={quote(target_wb_url, safe='')}",
            f"https://corsproxy.io/?{quote(target_wb_url, safe='')}",
        ]

        for r_url in relay_urls:
            try:
                async with session.get(
                    r_url, headers=get_spoofed_headers(), timeout=6.5
                ) as resp:
                    if resp.status == 200:
                        text_body = await resp.text(errors="ignore")
                        products = self._extract_products(text_body)
                        if products:
                            return products
            except Exception as e:
                logger.debug(f"Relay {r_url[:35]} failed: {e}")
        return []

    async def _discover_ids_via_web_index(self, query: str) -> List[str]:
        """
        Многопоточный парсер поисковых систем (POST + GET) с полным декодированием URL (%2F -> /),
        извлекающий артикулы Wildberries из редирект-ссылок DuckDuckGo, Yahoo и Bing.
        """
        session = await self.get_session()
        clean_q = query.strip()
        q_site = f"site:wildberries.ru/catalog/ {clean_q}"
        q_broad = f"wildberries catalog detail {clean_q}"

        found_ids: List[str] = []
        id_regexes = [
            re.compile(r"wildberries\.[a-z]+/catalog/(\d{6,12})", re.IGNORECASE),
            re.compile(r"/catalog/(\d{7,11})/detail", re.IGNORECASE),
        ]

        def extract_from_html(raw_html: str) -> List[str]:
            # Декодируем %2F и %3A в обычные слэши, чтобы видеть ссылки внутри редиректов uddg= / RU=
            decoded = unquote(unquote(raw_html))
            extracted: List[str] = []
            for rgx in id_regexes:
                for m in rgx.findall(decoded):
                    if m not in extracted:
                        extracted.append(m)
            return extracted

        async def post_ddg_html(q_str: str) -> List[str]:
            headers = get_spoofed_headers("https://html.duckduckgo.com/")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            try:
                async with session.post(
                    "https://html.duckduckgo.com/html/",
                    data={"q": q_str, "kl": "ru-ru"},
                    headers=headers,
                    timeout=6.0,
                ) as resp:
                    if resp.status == 200:
                        return extract_from_html(await resp.text(errors="ignore"))
            except Exception:
                pass
            return []

        async def post_ddg_lite(q_str: str) -> List[str]:
            headers = get_spoofed_headers("https://lite.duckduckgo.com/")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            try:
                async with session.post(
                    "https://lite.duckduckgo.com/lite/",
                    data={"q": q_str, "kl": "ru-ru"},
                    headers=headers,
                    timeout=6.0,
                ) as resp:
                    if resp.status == 200:
                        return extract_from_html(await resp.text(errors="ignore"))
            except Exception:
                pass
            return []

        async def get_search_page(url: str) -> List[str]:
            try:
                async with session.get(
                    url, headers=get_spoofed_headers("https://www.google.com/"), timeout=6.0
                ) as resp:
                    if resp.status == 200:
                        return extract_from_html(await resp.text(errors="ignore"))
            except Exception:
                pass
            return []

        tasks = [
            post_ddg_html(q_site),
            post_ddg_lite(q_broad),
            get_search_page(f"https://search.yahoo.com/search?p={quote(q_site)}&n=20"),
            get_search_page(f"https://search.yahoo.com/search?p={quote(q_broad)}&n=20"),
            get_search_page(f"https://www.bing.com/search?q={quote(q_site)}&count=20"),
            get_search_page(f"https://www.bing.com/search?q={quote(q_broad)}&count=20"),
        ]

        results = await asyncio.gather(*tasks)
        for id_list in results:
            for nm in id_list:
                if nm not in found_ids:
                    found_ids.append(nm)

        return found_ids[:16]

    async def enrich_items_with_cdn(
        self, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Догружает историю цен и недостающие цены через CDN wbbasket для списка товаров."""
        async def _enrich_one(item: Dict[str, Any]) -> Dict[str, Any]:
            cdn = await self.fetch_cdn_basket(item["id"])
            if cdn:
                if item["price"] <= 0 and cdn.get("latest_hist_price", 0) > 0:
                    item["price"] = cdn["latest_hist_price"]
                if item["title"] in ("Без названия", ""):
                    item["title"] = cdn["title"]
                if item["brand"] in ("Без бренда", ""):
                    item["brand"] = cdn["brand"]
                item["history_min"] = cdn.get("min_hist_price", 0.0)
                item["history_max"] = cdn.get("max_hist_price", 0.0)
                item["history_avg"] = cdn.get("avg_hist_price", 0.0)
                item["subj_name"] = cdn.get("subj_name", "")
            return item

        enriched = await asyncio.gather(*(_enrich_one(it) for it in items[:10]))
        return [it for it in enriched if it.get("price", 0) > 0]

    async def search_lowest_prices(
        self, raw_query: str, mode: str = "smart"
    ) -> Tuple[bool, List[Dict[str, Any]], str, str]:
        """
        Возвращает: (успех, список_товаров, источник_данных, исправленный_запрос).
        """
        corrected_q = await self.autocorrect_typo(raw_query)
        session = await self.get_session()
        proxy = ProxyManager.pick_proxy()
        encoded_q = quote(corrected_q)
        wb_sort = "popular" if mode == "smart" else "priceup"

        # УРОВЕНЬ 1: Прямой запрос к шлюзам WB Search API
        direct_urls = [
            f"https://search.wb.ru/exactmatch/ru/common/v18/search?ab_testing=false&appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false",
            f"https://u-search.wb.ru/exactmatch/ru/common/v18/search?ab_testing=false&appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false",
            f"https://search.wb.ru/exactmatch/sng/common/v18/search?ab_testing=false&appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false",
            f"https://search.wb.ru/exactmatch/ru/common/v9/search?ab_testing=false&appType=1&curr=rub&dest=-1257786&query={encoded_q}&resultset=catalog&sort={wb_sort}&spp=30&suppressSpellcheck=false",
        ]

        raw_products: List[Dict[str, Any]] = []
        source_label = "WB Search API v18"

        for url in direct_urls:
            try:
                async with session.get(
                    url, headers=get_spoofed_headers(), proxy=proxy, timeout=5.0
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        raw_products = self._extract_products(data)
                        if raw_products:
                            break
            except Exception:
                continue

        # УРОВЕНЬ 2: Публичные API-ретрансляторы (если IP Render заблокирован напрямую)
        if not raw_products:
            raw_products = await self._fetch_search_via_relays(encoded_q, wb_sort)
            if raw_products:
                source_label = "WB Relay Gateway"

        if raw_products:
            parsed = [self._parse_commercial_prod(p) for p in raw_products[:25]]
            valid_direct = [p for p in parsed if p["price"] > 0]
            if not valid_direct and parsed:
                valid_direct = await self.enrich_items_with_cdn(parsed[:10])

            if valid_direct:
                if mode == "smart":
                    good = [
                        p
                        for p in valid_direct
                        if p["rating"] >= 4.4 and p["feedbacks"] >= 5
                    ]
                    pool = good if len(good) >= 3 else valid_direct
                    pool.sort(key=lambda x: x["price"])
                    top_slice = await self.enrich_items_with_cdn(pool[:7])
                    top_slice.sort(key=lambda x: x["price"])
                    return True, top_slice, source_label, corrected_q
                else:
                    valid_direct.sort(key=lambda x: x["price"])
                    top_slice = await self.enrich_items_with_cdn(valid_direct[:7])
                    top_slice.sort(key=lambda x: x["price"])
                    return True, top_slice, source_label, corrected_q

        # УРОВЕНЬ 3 & 4: Декодирующий Web-Index (POST/GET) + Локальный кэш + CDN wbbasket
        candidate_ids = db.search_cached_nm_ids(corrected_q)
        web_ids = await self._discover_ids_via_web_index(corrected_q)
        for wid in web_ids:
            if wid not in candidate_ids:
                candidate_ids.append(wid)

        if not candidate_ids and corrected_q != raw_query:
            extra_web = await self._discover_ids_via_web_index(raw_query)
            for wid in extra_web:
                if wid not in candidate_ids:
                    candidate_ids.append(wid)

        if not candidate_ids:
            return False, [], "none", corrected_q

        initial_results = await asyncio.gather(
            *(self.get_product_by_id(nm) for nm in candidate_ids[:14])
        )
        valid_items: List[Dict[str, Any]] = []
        extra_variant_ids: List[str] = []

        for ok, info in initial_results:
            if ok and info.get("price", 0) > 0:
                valid_items.append(info)
                for v_id in info.get("variants", [])[:5]:
                    if v_id not in candidate_ids and v_id not in extra_variant_ids:
                        extra_variant_ids.append(v_id)

        if len(valid_items) < 8 and extra_variant_ids:
            var_results = await asyncio.gather(
                *(self.get_product_by_id(nm) for nm in extra_variant_ids[:10])
            )
            for ok, info in var_results:
                if ok and info.get("price", 0) > 0:
                    valid_items.append(info)

        if not valid_items:
            return False, [], "none", corrected_q

        unique_map = {item["id"]: item for item in valid_items}
        sorted_items = sorted(unique_map.values(), key=lambda x: x["price"])
        return True, sorted_items[:8], "CDN + Гибридный Веб-Индекс", corrected_q


wb_engine = WildberriesEnterpriseEngine()


# ==========================================
# 4. АНАЛИТИЧЕСКОЕ ФОРМАТИРОВАНИЕ И ВИЗУАЛИЗАЦИЯ
# ==========================================
def build_price_thermometer(curr: float, h_min: float, h_max: float) -> str:
    """Строит визуальную шкалу положения текущей цены между историческим дном и пиком."""
    if curr <= 0 or h_min <= 0 or h_max <= h_min:
        return ""
    ratio = max(0.0, min(1.0, (curr - h_min) / (h_max - h_min)))
    filled = int(round(ratio * 10))
    if ratio <= 0.25:
        dot = "🟢"
    elif ratio <= 0.60:
        dot = "🟡"
    else:
        dot = "🔴"
    bar = dot * max(1, filled) + "⚪" * (10 - max(1, filled))
    pct = int(round(ratio * 100))
    return f"🌡️ Шкала цены: [{bar}] ({pct}% от дна)\n"


def build_price_verdict(info: Dict[str, Any]) -> str:
    curr = info.get("price", 0.0)
    h_min = info.get("history_min", 0.0)
    h_avg = info.get("history_avg", 0.0)

    if curr <= 0 or h_min <= 0:
        return ""
    if curr <= h_min * 1.03:
        return "💎 <b>Вердикт: Исторический минимум! Идеальный момент для покупки.</b>\n"
    if h_avg > 0 and curr <= h_avg * 0.95:
        diff_pct = round((1 - curr / h_avg) * 100)
        return f"🟢 <b>Вердикт: Выгодно (на {diff_pct}% дешевле средней цены)</b>\n"
    if h_avg > 0 and curr > h_avg * 1.10:
        over_pct = round((curr / h_avg - 1) * 100)
        return f"🔴 <b>Вердикт: Цена завышена (+{over_pct}% к средней истории)</b>\n"
    return "🟡 <b>Вердикт: Стандартная рыночная цена (рекомендуем поставить цель ₽)</b>\n"


def format_product_card(info: Dict[str, Any], target_price: float = 0.0) -> str:
    brand = html.escape(info["brand"])
    title = html.escape(info["title"])
    subj = html.escape(info.get("subj_name") or "")
    subj_line = f"📂 Категория: <i>{subj}</i>\n" if subj else ""

    if info["price"] > 0:
        price_str = f"{info['price']:,.0f}".replace(",", " ") + " ₽"
    else:
        price_str = "Нет в наличии (ожидается поставка)"

    old_str = f"{info['old_price']:,.0f}".replace(",", " ")
    discount_line = (
        f" (<s>{old_str} ₽</s> • <b>-{info['discount']}%</b>)"
        if info["discount"] > 0 and info["old_price"] > info["price"]
        else ""
    )

    h_min = info.get("history_min", 0.0)
    h_max = info.get("history_max", 0.0)
    h_avg = info.get("history_avg", 0.0)

    hist_lines = ""
    if h_min > 0:
        hm_str = f"{h_min:,.0f}".replace(",", " ")
        hist_lines += f"📉 Исторический мин: <b>{hm_str} ₽</b>"
        if h_max > h_min:
            hmax_str = f"{h_max:,.0f}".replace(",", " ")
            hist_lines += f" | Пик: {hmax_str} ₽"
        hist_lines += "\n"
    if h_avg > 0:
        havg_str = f"{h_avg:,.0f}".replace(",", " ")
        hist_lines += f"📊 Средняя по истории: <b>{havg_str} ₽</b>\n"

    thermo_line = build_price_thermometer(info["price"], h_min, h_max)
    verdict_line = build_price_verdict(info)
    target_line = (
        f"🎯 Ваша цель: <b>≤ {int(target_price)} ₽</b>\n" if target_price > 0 else ""
    )

    variants_count = len(info.get("variants", []))
    var_line = (
        f"🎨 Вариантов/цветов в линейке: <b>{variants_count} шт.</b>\n"
        if variants_count > 0
        else ""
    )

    stock_str = (
        f"🟢 В наличии ({info['qty']} шт.)"
        if info["qty"] > 0
        else "⚪ Остаток по региональному складу CDN"
    )
    rating_line = (
        f"⭐ Рейтинг: <b>{info['rating']}</b> ({info['feedbacks']} отз.)\n"
        if info["rating"] > 0
        else ""
    )

    return (
        f"📦 <b>{brand}</b> — {title}\n"
        f"{subj_line}\n"
        f"💰 <b>Цена: {price_str}</b>{discount_line}\n"
        f"{hist_lines}"
        f"{thermo_line}"
        f"{verdict_line}"
        f"{target_line}"
        f"{rating_line}"
        f"📊 Склад: {stock_str}\n"
        f"{var_line}"
        f"🏷️ Артикул: <code>{info['id']}</code>\n\n"
        f"🔗 <a href='{info['url']}'>Открыть карточку на Wildberries</a>"
    )


def format_search_results(
    raw_query: str,
    corrected_query: str,
    items: List[Dict[str, Any]],
    mode: str,
    source_label: str,
) -> str:
    safe_q = html.escape(corrected_query)
    typo_note = (
        f"✨ <i>Автоисправление опечатки: «{html.escape(raw_query)}» ➡️ «{safe_q}»</i>\n"
        if corrected_query.lower() != raw_query.strip().lower()
        else ""
    )
    mode_title = (
        "🔥 Топ выгодных предложений по возрастанию цены"
        if mode == "smart"
        else "📉 Абсолютный минимум цены"
    )
    lines = [
        f"🔎 Запрос: <b>«{safe_q}»</b>\n"
        f"{typo_note}"
        f"<i>{mode_title} ({source_label}):</i>\n"
    ]

    for idx, item in enumerate(items, start=1):
        brand = html.escape(item["brand"])
        title = html.escape(item["title"])
        price_str = f"{item['price']:,.0f}".replace(",", " ")
        h_min = item.get("history_min", 0.0)
        min_tag = f" (мин. был {int(h_min)}₽)" if h_min > 0 else ""
        rating_tag = f" | ⭐ {item['rating']}" if item["rating"] > 0 else ""

        lines.append(
            f"<b>{idx}. {price_str} ₽</b>{min_tag}{rating_tag}\n"
            f"   <b>{brand}</b> / {title}\n"
            f"   Арт: <code>{item['id']}</code> • <a href='{item['url']}'>Открыть на WB</a>\n"
        )

    lines.append(
        "💡 <i>Нажмите кнопку с номером товара ниже для мониторинга цены!</i>"
    )
    return "\n".join(lines)


# ==========================================
# 5. ПОСТРОИТЕЛИ КЛАВИАТУР (UI LAYER)
# ==========================================
def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="⭐ Мое избранное"),
                KeyboardButton(text="🔄 Проверить скидки сейчас"),
            ],
            [
                KeyboardButton(text="💾 Бэкап и Прокси"),
                KeyboardButton(text="⚙️ Статус бота"),
            ],
            [
                KeyboardButton(text="🔍 Как пользоваться ботом?"),
            ],
        ],
        resize_keyboard=True,
    )


def get_product_inline_kb(info: Dict[str, Any]) -> InlineKeyboardMarkup:
    nm_id = info["id"]
    buttons = [
        [
            InlineKeyboardButton(
                text="⭐ Отслеживать цену", callback_data=f"fav_add:{nm_id}"
            ),
            InlineKeyboardButton(
                text="🎯 Задать цель ₽", callback_data=f"target_ask:{nm_id}"
            ),
        ]
    ]
    if info.get("variants"):
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"🎨 Сравнить цвета/варианты ({len(info['variants'])} шт.)",
                    callback_data=f"variants:{nm_id}",
                )
            ]
        )
    if info.get("subj_name"):
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"📂 Искать дешевле в «{info['subj_name'][:20]}»",
                    callback_data=f"cat_search:{nm_id}",
                )
            ]
        )
    buttons.append(
        [
            InlineKeyboardButton(
                text="🔄 Обновить карточку", callback_data=f"refresh_id:{nm_id}"
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_search_results_kb(
    items: List[Dict[str, Any]], current_mode: str
) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for idx, item in enumerate(items[:6], start=1):
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


def get_favorites_kb(
    items: List[Tuple[str, str, str, float, float, float]]
) -> InlineKeyboardMarkup:
    buttons = []
    for nm_id, title, _, price, _, target_p in items[:8]:
        short_title = title[:16] + ".." if len(title) > 16 else title
        t_badge = f" 🎯{int(target_p)}₽" if target_p > 0 else ""
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"🔎 {short_title} ({int(price)}₽{t_badge})",
                    callback_data=f"open_id:{nm_id}",
                ),
                InlineKeyboardButton(
                    text="❌",
                    callback_data=f"fav_del:{nm_id}",
                ),
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
# 6. ОБРАБОТЧИКИ КОМАНД И РОУТЕРЫ TELEGRAM
# ==========================================
router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    WAITING_TARGET_PRICE.pop(message.from_user.id, None)
    text = (
        "🛍️ <b>WB Радар Цен — Enterprise Edition (10/10)</b>\n\n"
        "Отправьте в чат любой запрос:\n"
        "1️⃣ <b>Название товара</b> (например: <code>Комбинезон женский</code>) — 5-уровневый поиск самых низких цен с автоисправлением опечаток.\n"
        "2️⃣ <b>Артикул или ссылку WB</b> (например: <code>14195461</code>) — рентген истории цен, шкала выгоды и сканер дешевых цветов.\n"
        "3️⃣ <b>Артикул + желаемая цена</b> (например: <code>14195461 280</code>) — установка снайпер-цели!"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(Command("restore"))
async def cmd_restore(message: Message):
    nms = re.findall(r"\d{5,12}", message.text or "")
    if not nms:
        await message.answer(
            "❓ Пример восстановления: <code>/restore 14195461 183421364</code>",
            parse_mode="HTML",
        )
        return

    wait_msg = await message.answer(
        f"⏳ Восстанавливаю {len(nms)} товаров из резервной копии..."
    )
    restored = 0
    for nm_id in nms[:25]:
        ok, info = await wb_engine.get_product_by_id(nm_id)
        if ok:
            db.save_item(
                message.from_user.id,
                nm_id,
                info["title"],
                info["brand"],
                info["price"],
                info.get("history_min", 0.0),
            )
            restored += 1

    await wait_msg.delete()
    await message.answer(
        f"✅ Успешно восстановлено в избранное: <b>{restored} из {len(nms)}</b> товаров!",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(),
    )


@router.message(Command("proxy"))
async def cmd_proxy(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() in ("off", "clear", "none", "0"):
        db.set_setting("proxy_pool", "")
        await message.answer(
            "🌐 Пользовательский прокси отключен. Активен встроенный 5-уровневый Relay + CDN обход."
        )
        return

    raw_input = parts[1].strip()
    tokens = re.split(r"[\s,;]+", raw_input)
    normalized_list = [
        ProxyManager.normalize_proxy(t)
        for t in tokens
        if ProxyManager.normalize_proxy(t)
    ]

    if not normalized_list:
        await message.answer("❌ Не удалось распознать формат прокси.")
        return

    db.set_setting("proxy_pool", "\n".join(normalized_list))
    await message.answer(
        f"✅ В пул сохранено прокси: <b>{len(normalized_list)} шт.</b>\n"
        f"Активный шлюз: <code>{html.escape(normalized_list[0])}</code>",
        parse_mode="HTML",
    )


@router.message(F.text == "🔍 Как пользоваться ботом?")
async def cmd_help(message: Message):
    text = (
        "💡 <b>Архитектурные возможности Enterprise-версии:</b>\n\n"
        "• <b>Автоисправление опечаток:</b> пишите запросы быстро — встроенный модуль Yandex Speller сам исправит опечатки перед поиском.\n"
        "• <b>🌡️ Шкала выгоды:</b> визуальный индикатор показывает, насколько текущая цена близка к историческому дну.\n"
        "• <b>🎨 Сравнение цветов и категории:</b> кнопки под карточкой находят самый дешевый цвет в линейке или самые дешевые аналоги в той же категории.\n"
        "• <b>🎯 Снайпер цены:</b> задайте желаемую цену в рублях, и фоновый радар пришлет экстренное уведомление при пробое уровня."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "💾 Бэкап и Прокси")
async def cmd_backup_and_proxy(message: Message):
    items = db.get_user_items(message.from_user.id)
    if items:
        ids_str = " ".join(nm_id for nm_id, *_ in items)
        restore_cmd = f"<code>/restore {ids_str}</code>"
    else:
        restore_cmd = "<i>Список избранного пока пуст</i>"

    active_proxies = ProxyManager.get_active_proxies()
    proxy_label = (
        f"Активно ({len(active_proxies)} шт.): {active_proxies[0]}"
        if active_proxies
        else "Встроенный Relay Gateway + Декодер Индекса"
    )
    text = (
        "💾 <b>Резервная копия ваших товаров:</b>\n"
        "Скопируйте команду ниже для мгновенного восстановления списка после редеплоя:\n"
        f"{restore_cmd}\n\n"
        "🌐 <b>Управление прокси-пулом (принимает любой формат):</b>\n"
        f"• Режим: <code>{html.escape(proxy_label)}</code>\n"
        "• Формат 1: <code>/proxy ip:port:login:pass</code>\n"
        "• Формат 2: <code>/proxy http://login:pass@ip:port</code>\n"
        "• Отключить: <code>/proxy off</code>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "⚙️ Статус бота")
async def cmd_status(message: Message):
    items = db.get_user_items(message.from_user.id)
    active_proxies = ProxyManager.get_active_proxies()
    net_mode = (
        f"Пул прокси ({len(active_proxies)} шт.) + CDN"
        if active_proxies
        else "5-Tier (API v18 + Relay Gateways + Unquoted Web-Index + CDN)"
    )
    text = (
        "⚙️ <b>Статус системы (Enterprise 10/10):</b>\n\n"
        f"📂 База данных: <b>SQLite WAL ({DB_PATH.name})</b>\n"
        f"⭐ Товаров на мониторинге: <b>{len(items)} шт.</b>\n"
        "🧠 Корректор опечаток: <b>Yandex Speller (Активен)</b>\n"
        f"🛡️ Сетевой стек: <b>{net_mode}</b>\n"
        "🔔 Фоновый радар скидок: <b>Каждые 30 минут (24/7)</b>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "⭐ Мое избранное")
async def show_favorites(message: Message):
    items = db.get_user_items(message.from_user.id)
    if not items:
        await message.answer(
            "📭 Ваш список отслеживания пуст.\nНапишите название товара или пришлите артикул, чтобы добавить позиции!",
            reply_markup=get_main_keyboard(),
        )
        return

    lines = [f"⭐ <b>Ваши отслеживаемые товары ({len(items)} шт.):</b>\n"]
    for nm_id, title, brand, last_price, min_price, target_p in items:
        b_safe = html.escape(brand)
        t_safe = html.escape(title)
        lp_str = f"{last_price:,.0f}".replace(",", " ") if last_price > 0 else "н/д"
        mp_str = f"{min_price:,.0f}".replace(",", " ") if min_price > 0 else "н/д"
        target_str = f" | 🎯 Цель: <b>≤ {int(target_p)} ₽</b>" if target_p > 0 else ""
        lines.append(
            f"🔹 <b>{b_safe}</b> — {t_safe}\n"
            f"   Арт: <code>{nm_id}</code> | Цена: <b>{lp_str} ₽</b> (Мин: {mp_str} ₽){target_str}\n"
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
    items = db.get_user_items(message.from_user.id)
    if not items:
        await message.answer(
            "📭 Сначала добавьте товары в «⭐ Мое избранное».",
            reply_markup=get_main_keyboard(),
        )
        return

    wait_msg = await message.answer(
        f"⏳ Сверяю актуальные цены для {len(items)} товаров..."
    )
    report = ["📊 <b>Отчет по ценам избранного:</b>\n"]

    for nm_id, title, _, old_saved_price, _, target_p in items:
        ok, info = await wb_engine.get_product_by_id(nm_id)
        t_safe = html.escape(title[:35])
        if not ok or info["price"] <= 0:
            report.append(f"⚪ <code>{nm_id}</code> ({t_safe}) — нет в наличии")
            continue

        new_price = info["price"]
        db.update_price(message.from_user.id, nm_id, new_price)
        diff = new_price - old_saved_price if old_saved_price > 0 else 0
        target_hit = (
            f"\n   🎯 <b>ДОСТИГНУТА ВАША ЦЕЛЬ ({int(target_p)} ₽)!</b>"
            if (target_p > 0 and new_price <= target_p)
            else ""
        )

        if diff < -1:
            report.append(
                f"🔥 <b>ПОДЕШЕВЕЛ на {abs(int(diff))} ₽!</b>\n"
                f"   {t_safe}: было {int(old_saved_price)} ₽ ➡️ <b>стало {int(new_price)} ₽</b>{target_hit}\n"
                f"   🔗 <a href='{info['url']}'>Забрать на WB</a>"
            )
        elif diff > 1:
            report.append(
                f"📈 {t_safe}: +{int(diff)} ₽ (сейчас <b>{int(new_price)} ₽</b>){target_hit}"
            )
        else:
            report.append(
                f"🟰 {t_safe}: без изменений (<b>{int(new_price)} ₽</b> | мин. по истории: {int(info.get('history_min', 0))} ₽){target_hit}"
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
    ok, info = await wb_engine.get_product_by_id(nm_id)

    if ok:
        db.save_item(
            callback.from_user.id,
            nm_id,
            info["title"],
            info["brand"],
            info["price"],
            info.get("history_min", 0.0),
        )
        p_label = f"{int(info['price'])} ₽" if info["price"] > 0 else "мониторинг"
        await callback.answer(
            f"✅ Добавлено в избранное ({p_label})!", show_alert=False
        )
    else:
        await callback.answer("⚠️ Не удалось загрузить карточку.", show_alert=True)


@router.callback_query(F.data.startswith("target_ask:"))
async def cb_ask_target_price(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    ok, info = await wb_engine.get_product_by_id(nm_id)
    if ok:
        db.save_item(
            callback.from_user.id,
            nm_id,
            info["title"],
            info["brand"],
            info["price"],
            info.get("history_min", 0.0),
        )

    WAITING_TARGET_PRICE[callback.from_user.id] = nm_id
    await callback.answer()
    await callback.message.answer(
        f"🎯 Введите желаемую цену в рублях для артикула <code>{nm_id}</code> (например: <code>280</code>):\n"
        "<i>Как только цена опустится до этого значения или ниже, бот пришлет вам уведомление!</i>",
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("variants:"))
async def cb_check_variants(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    await callback.answer("Сканирую цены всех вариантов в линейке...")

    ok, base_info = await wb_engine.get_product_by_id(nm_id)
    if not ok or not base_info.get("variants"):
        await callback.message.answer("У этого товара нет других связанных вариантов.")
        return

    all_ids = [nm_id] + base_info["variants"][:11]
    results = await asyncio.gather(*(wb_engine.get_product_by_id(v) for v in all_ids))

    valid_variants = [
        info for ok_v, info in results if ok_v and info.get("price", 0) > 0
    ]
    if not valid_variants:
        await callback.message.answer("Не удалось получить цены связанных вариантов.")
        return

    valid_variants.sort(key=lambda x: x["price"])
    lines = [
        f"🎨 <b>Рейтинг цен всех вариантов модели ({len(valid_variants)} шт.):</b>\n"
    ]
    for idx, v in enumerate(valid_variants, start=1):
        current_mark = " 👈 <i>(выбранный)</i>" if v["id"] == nm_id else ""
        h_min = v.get("history_min", 0)
        min_str = f" | мин. был {int(h_min)}₽" if h_min > 0 else ""
        lines.append(
            f"<b>{idx}. {int(v['price'])} ₽</b>{min_str}{current_mark}\n"
            f"   {html.escape(v['title'])}\n"
            f"   Арт: <code>{v['id']}</code> • <a href='{v['url']}'>Открыть на WB</a>\n"
        )

    await callback.message.answer(
        "\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=get_search_results_kb(valid_variants, current_mode="smart"),
    )


@router.callback_query(F.data.startswith("cat_search:"))
async def cb_category_search(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    cached = db.get_cached_product(nm_id)
    subj_name = cached.get("subj_name") if cached else ""
    if not subj_name:
        ok, info = await wb_engine.get_product_by_id(nm_id)
        subj_name = info.get("subj_name", "") if ok else ""

    if not subj_name:
        await callback.answer("Категория не определена", show_alert=True)
        return

    await callback.answer(f"Ищу самые дешевые товары в категории «{subj_name}»...")
    LAST_SEARCH_QUERY[callback.from_user.id] = subj_name
    ok, items, source_label, corr_q = await wb_engine.search_lowest_prices(
        subj_name, mode="smart"
    )
    if not ok or not items:
        await callback.message.answer("Не удалось получить список товаров категории.")
        return

    await callback.message.answer(
        format_search_results(subj_name, corr_q, items, "smart", source_label),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=get_search_results_kb(items, current_mode="smart"),
    )


@router.callback_query(F.data.startswith("open_id:"))
async def cb_open_id(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    await callback.answer("Загружаю карточку...")
    ok, info = await wb_engine.get_product_by_id(nm_id)
    if ok:
        await callback.message.answer(
            format_product_card(info),
            parse_mode="HTML",
            disable_web_page_preview=False,
            reply_markup=get_product_inline_kb(info),
        )


@router.callback_query(F.data.startswith("refresh_id:"))
async def cb_refresh_card(callback: CallbackQuery):
    nm_id = callback.data.split(":")[1]
    ok, info = await wb_engine.get_product_by_id(nm_id)
    if not ok:
        await callback.answer("Товар временно недоступен", show_alert=True)
        return

    await callback.answer("Данные обновлены!")
    try:
        await callback.message.edit_text(
            format_product_card(info),
            parse_mode="HTML",
            disable_web_page_preview=False,
            reply_markup=get_product_inline_kb(info),
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
    ok, items, source_label, corr_q = await wb_engine.search_lowest_prices(
        query, mode=mode
    )
    if not ok or not items:
        await callback.answer("По этому фильтру ничего не найдено.", show_alert=True)
        return

    try:
        await callback.message.edit_text(
            format_search_results(query, corr_q, items, mode, source_label),
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
    db.delete_item(user_id, nm_id)
    await callback.answer("Товар удален")

    items = db.get_user_items(user_id)
    if not items:
        await callback.message.edit_text("📭 Ваш список отслеживания пуст.")
    else:
        await callback.message.edit_reply_markup(reply_markup=get_favorites_kb(items))


@router.callback_query(F.data == "fav_clear_all")
async def cb_clear_favorites(callback: CallbackQuery):
    db.clear_user_items(callback.from_user.id)
    await callback.answer("Список очищен!")
    await callback.message.edit_text("📭 Ваш список отслеживания пуст.")


@router.message(F.text)
async def handle_user_input(message: Message):
    text = message.text.strip()
    user_id = message.from_user.id

    # 1. Ввод целевой цены после нажатия кнопки «🎯 Задать цель ₽»
    if user_id in WAITING_TARGET_PRICE and re.fullmatch(
        r"\d{1,7}([.,]\d{1,2})?", text
    ):
        nm_id = WAITING_TARGET_PRICE.pop(user_id)
        target_val = float(text.replace(",", "."))
        db.set_target_price(user_id, nm_id, target_val)
        await message.answer(
            f"🎯 Цель <b>≤ {int(target_val)} ₽</b> для артикула <code>{nm_id}</code> сохранена!\n"
            "Бот автоматически уведомит вас при достижении этой цены.",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(),
        )
        return

    WAITING_TARGET_PRICE.pop(user_id, None)

    # 2. Формат «Артикул ЦелеваяЦена» (например: 14195461 280)
    combo_match = re.fullmatch(r"(\d{6,12})\s+(\d{2,7})", text)
    if combo_match:
        nm_id, target_str = combo_match.group(1), combo_match.group(2)
        target_val = float(target_str)
        ok, info = await wb_engine.get_product_by_id(nm_id)
        if ok:
            db.save_item(
                user_id,
                nm_id,
                info["title"],
                info["brand"],
                info["price"],
                info.get("history_min", 0.0),
                target_price=target_val,
            )
            await message.answer(
                format_product_card(info, target_price=target_val),
                parse_mode="HTML",
                reply_markup=get_product_inline_kb(info),
            )
            return

    # 3. Ссылка WB или чистый артикул
    url_match = re.search(
        r"wildberries\.[a-z]+/catalog/(\d{5,12})", text, re.IGNORECASE
    )
    pure_digits_match = re.fullmatch(r"\d{5,12}", text)

    nm_id: Optional[str] = None
    if url_match:
        nm_id = url_match.group(1)
    elif pure_digits_match:
        nm_id = pure_digits_match.group(0)

    if nm_id:
        wait_msg = await message.answer(
            f"⏳ Загружаю аналитику по артикулу <code>{nm_id}</code>...",
            parse_mode="HTML",
        )
        ok, info = await wb_engine.get_product_by_id(nm_id)
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
            reply_markup=get_product_inline_kb(info),
        )
        return

    # 4. 5-уровневый поиск самых низких цен по названию товара
    if len(text) < 2:
        await message.answer(
            "❓ Напишите название товара (от 2 букв) или пришлите артикул WB.",
            reply_markup=get_main_keyboard(),
        )
        return

    LAST_SEARCH_QUERY[user_id] = text
    wait_msg = await message.answer(
        f"🔎 Проверяю орфографию и ищу самые низкие цены по запросу <b>«{html.escape(text)}»</b>...",
        parse_mode="HTML",
    )

    ok, items, source_label, corr_q = await wb_engine.search_lowest_prices(
        text, mode="smart"
    )
    await wait_msg.delete()

    if not ok or not items:
        await message.answer(
            "😕 Не удалось найти товары по этому текстовому запросу.\n\n"
            "💡 <b>Быстрое решение:</b> пришлите <b>любой артикул</b> нужного товара из приложения WB и нажмите под карточкой <b>«🎨 Сравнить цвета/варианты»</b>!",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(),
        )
        return

    LAST_SEARCH_QUERY[user_id] = corr_q
    await message.answer(
        format_search_results(
            text, corr_q, items, mode="smart", source_label=source_label
        ),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=get_search_results_kb(items, current_mode="smart"),
    )


# ==========================================
# 7. ФОНОВЫЙ РАДАР СКИДОК + HEALTH-СЕРВЕР RENDER
# ==========================================
async def background_price_watcher(bot: Bot):
    while True:
        await asyncio.sleep(1800)  # 30 минут
        all_items = db.get_all_items()
        if not all_items:
            continue

        logger.info(f"Фоновая ревизия цен для {len(all_items)} позиций...")
        for user_id, nm_id, title, brand, last_price, target_p in all_items:
            ok, info = await wb_engine.get_product_by_id(nm_id)
            if not ok or info["price"] <= 0:
                continue

            new_price = info["price"]
            h_min = info.get("history_min", 0.0)

            target_reached = (
                target_p > 0
                and new_price <= target_p
                and (last_price > target_p or last_price == 0)
            )
            historical_bottom = (
                h_min > 0
                and new_price <= h_min
                and last_price > new_price
            )
            price_dropped = last_price > 0 and new_price < last_price - 5

            if target_reached or historical_bottom or price_dropped:
                drop = int(last_price - new_price) if last_price > new_price else 0
                db.update_price(user_id, nm_id, new_price)

                if target_reached:
                    header = f"🎯 <b>ДОСТИГНУТА ВАША ЦЕЛЕВАЯ ЦЕНА ({int(target_p)} ₽)!</b>"
                elif historical_bottom:
                    header = f"💎 <b>НОВЫЙ ИСТОРИЧЕСКИЙ МИНИМУМ (-{drop} ₽)!</b>"
                else:
                    header = f"🔥 <b>ЦЕНА СНИЗИЛАСЬ НА {drop} ₽!</b>"

                alert_text = (
                    f"{header}\n\n"
                    f"📦 <b>{html.escape(brand)}</b> — {html.escape(title)}\n"
                    f"📉 Было: <s>{int(last_price)} ₽</s> ➡️ <b>Стало: {int(new_price)} ₽</b>\n"
                    f"🏷️ Артикул: <code>{nm_id}</code>\n\n"
                    f"🔗 <a href='{info['url']}'>Успеть купить на Wildberries</a>"
                )
                try:
                    await bot.send_message(user_id, alert_text, parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"Ошибка доставки алерта {user_id}: {e}")
            elif new_price != last_price:
                db.update_price(user_id, nm_id, new_price)

            await asyncio.sleep(1.5)


async def keep_alive_ping():
    external_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not external_url:
        return
    ping_url = f"{external_url.rstrip('/')}/health"
    while True:
        await asyncio.sleep(600)
        try:
            session = await wb_engine.get_session()
            async with session.get(ping_url, timeout=10) as resp:
                logger.info(f"Keep-alive ping: HTTP {resp.status}")
        except Exception as e:
            logger.warning(f"Keep-alive ping error: {e}")


async def start_render_web_server():
    app = web.Application()
    app.router.add_get(
        "/",
        lambda request: web.Response(text="WB Enterprise Radar 10/10 is running!"),
    )
    app.router.add_get(
        "/health",
        lambda request: web.json_response({"status": "ok", "engine": "enterprise_v5"}),
    )

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health-сервер Render активен на порту {port}")


async def on_shutdown() -> None:
    logger.info("Завершение работы контейнера, закрываю пул сессий...")
    await wb_engine.close()


async def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "Не найден токен Telegram-бота в переменных окружения (BOT_TOKEN)!"
        )

    # Zero-Conflict Startup: мгновенный бинд порта и пауза для завершения старого инстанса Render
    await start_render_web_server()
    await asyncio.sleep(4)

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    dp.shutdown.register(on_shutdown)

    asyncio.create_task(keep_alive_ping())
    asyncio.create_task(background_price_watcher(bot))

    logger.info("WB Радар Цен (Enterprise 10/10) запущен!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот штатно остановлен.")
