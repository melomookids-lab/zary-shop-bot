"""
ZARY SHOP BOT — FIXED FULL VERSION (PART 1/4)
✅ aiogram 3.x
✅ aiohttp web server
✅ SQLite
✅ Orders + Cart + Admin
✅ Shop WebApp
✅ Render ready
"""

import os
import html
import asyncio
import json
import secrets
from datetime import datetime, timedelta
from calendar import monthrange
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import sqlite3
import threading
from urllib.parse import quote

from zoneinfo import ZoneInfo
from aiohttp import web

TZ = ZoneInfo("Asia/Tashkent")
web_app = web.Application()


# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не установлен!")

ADMIN_IDS: List[int] = []
for i in range(1, 4):
    v = os.getenv(f"ADMIN_ID_{i}", "").strip()
    if v and v.lstrip("-").isdigit():
        ADMIN_IDS.append(int(v))

if not ADMIN_IDS:
    old_admin = os.getenv("MANAGER_CHAT_ID", "").strip()
    if old_admin and old_admin.lstrip("-").isdigit():
        ADMIN_IDS.append(int(old_admin))

if not ADMIN_IDS:
    raise RuntimeError("❌ Нужен хотя бы один ADMIN_ID_1")

PRIMARY_ADMIN = ADMIN_IDS[0]

_channel_id = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_ID = int(_channel_id) if _channel_id and _channel_id.lstrip("-").isdigit() else None

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "zaryco_official").strip().lstrip("@")
TG_CHANNEL_URL = f"https://t.me/{CHANNEL_USERNAME}"

PHONE = os.getenv("MANAGER_PHONE", "+998771202255").strip()
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "zaryco_official").strip().lstrip("@")

PORT = int(os.getenv("PORT", "10000"))
DB_PATH = os.getenv("DB_PATH", "bot.db")

CRON_SECRET = os.getenv("CRON_SECRET", "").strip()
ADMIN_PANEL_TOKEN = os.getenv("ADMIN_PANEL_TOKEN", "").strip()
WEBAPP_SECRET = os.getenv("WEBAPP_SECRET", secrets.token_hex(16)).strip()

BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")
if not BASE_URL:
    print("⚠️ BASE_URL не установлен. WebApp кнопка магазина может работать некорректно.")

if not ADMIN_PANEL_TOKEN:
    print("⚠️ ADMIN_PANEL_TOKEN не установлен! /admin будет без защиты.")

FOLLOW_TG = "https://t.me/zaryco_official"
FOLLOW_YT = "https://www.youtube.com/@ZARYCOOFFICIAL"
FOLLOW_IG = "https://www.instagram.com/zary.co/"


# =========================
# PRODUCTS
# =========================
PRODUCTS_RU = [
    "Худи детское", "Свитшот", "Футболка", "Рубашка", "Джинсы",
    "Брюки классические", "Юбка", "Платье", "Куртка демисезонная",
    "Костюм спортивный", "Школьная форма (комплект)", "Жилет школьный",
    "Кардиган", "Пижама", "Комплект (кофта+брюки)"
]
PRODUCTS_UZ = [
    "Bolalar hudi", "Sviter", "Futbolka", "Ko'ylak", "Jinsi",
    "Klassik shim", "Yubka", "Ko'ylak (dress)", "Demisezon kurtka",
    "Sport kostyum", "Maktab formasi (komplekt)", "Maktab jileti",
    "Kardigan", "Pijama", "Komplekt (kofta+shim)"
]

SHOP_CATEGORIES = ("new", "hits", "sale", "limited", "school", "casual")


# =========================
# HELPERS
# =========================
def now_tz() -> datetime:
    return datetime.now(TZ)

def esc(s: str) -> str:
    return html.escape(str(s) if s else "")

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def size_by_age(age: int) -> str:
    mapping = {
        1: "86", 2: "92", 3: "98", 4: "104", 5: "110", 6: "116",
        7: "122", 8: "128", 9: "134", 10: "140", 11: "146",
        12: "152", 13: "158", 14: "164", 15: "164"
    }
    return mapping.get(age, "122-128")

def size_by_height(height: int) -> str:
    sizes = [86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164]
    closest = min(sizes, key=lambda x: abs(x - height))
    return str(closest)

def prev_month(dt: datetime) -> tuple[int, int]:
    first = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_last = first - timedelta(days=1)
    return prev_last.year, prev_last.month

def cron_allowed(secret: str) -> bool:
    return bool(CRON_SECRET) and secret == CRON_SECRET

def admin_panel_allowed(token: str) -> bool:
    if not ADMIN_PANEL_TOKEN:
        return True
    return token == ADMIN_PANEL_TOKEN

def product_public_photo_url(file_id: str) -> str:
    if not file_id:
        return ""
    return f"/media/{quote(file_id)}"

def money_fmt(amount: int) -> str:
    try:
        return f"{int(amount):,}".replace(",", " ")
    except Exception:
        return "0"


# =========================
# DB
# =========================
class Database:
    def __init__(self):
        self.db_path = DB_PATH
        self._local = threading.local()
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
        except Exception:
            pass
        return conn

    def _get_conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = self._connect()
        return self._local.conn

    def _init_db(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                lang TEXT DEFAULT 'ru',
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS carts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product_name TEXT,
                qty INTEGER DEFAULT 1,
                size TEXT,
                added_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                name TEXT,
                phone TEXT,
                city TEXT,
                items TEXT,
                total_amount INTEGER DEFAULT 0,
                delivery_type TEXT,
                delivery_address TEXT,
                comment TEXT,
                status TEXT DEFAULT 'new',
                manager_seen INTEGER DEFAULT 0,
                manager_id INTEGER,
                created_at TEXT,
                reminded_at TEXT
            );

            CREATE TABLE IF NOT EXISTS monthly_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                month INTEGER,
                sent_at TEXT,
                filename TEXT,
                total_orders INTEGER,
                total_amount INTEGER,
                status TEXT DEFAULT 'pending'
            );

            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dow INTEGER,
                media_type TEXT,
                file_id TEXT,
                caption TEXT,
                week_key TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                posted_at TEXT
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event_type TEXT,
                meta TEXT,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS shop_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                photo_file_id TEXT,
                title_ru TEXT NOT NULL,
                title_uz TEXT NOT NULL,
                description_ru TEXT DEFAULT '',
                description_uz TEXT DEFAULT '',
                sizes TEXT DEFAULT '',
                category_slug TEXT DEFAULT 'casual',
                price INTEGER DEFAULT 0,
                price_on_request INTEGER DEFAULT 0,
                is_published INTEGER DEFAULT 1,
                sort_order INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_carts_user ON carts(user_id);
            CREATE INDEX IF NOT EXISTS idx_sched_week_dow ON scheduled_posts(week_key, dow);
            CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, created_at);
            CREATE INDEX IF NOT EXISTS idx_shop_products_pub ON shop_products(is_published, category_slug, sort_order, id);
        """)
        conn.commit()

        existing_cols = {r["name"] for r in conn.execute("PRAGMA table_info(orders)").fetchall()}
        if "latitude" not in existing_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN latitude REAL")
        if "longitude" not in existing_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN longitude REAL")
        if "source" not in existing_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN source TEXT DEFAULT 'bot'")
        conn.commit()
        conn.close()

    def event_add(self, user_id: int, event_type: str, meta: Optional[Dict] = None):
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            "INSERT INTO events (user_id, event_type, meta, created_at) VALUES (?,?,?,?)",
            (user_id, event_type, json.dumps(meta or {}, ensure_ascii=False), ts)
        )
        conn.commit()

    def user_upsert(self, user_id: int, username: str, lang: str):
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,))
        if cur.fetchone():
            cur.execute("UPDATE users SET username=?, lang=? WHERE user_id=?", (username, lang, user_id))
        else:
            cur.execute(
                "INSERT INTO users (user_id, username, lang, created_at) VALUES (?,?,?,?)",
                (user_id, username, lang, ts)
            )
        conn.commit()

    def user_get(self, user_id: int) -> Optional[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def cart_add(self, user_id: int, product_name: str, qty: int = 1, size: str = ""):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO carts (user_id, product_name, qty, size) VALUES (?,?,?,?)",
            (user_id, product_name, qty, size)
        )
        conn.commit()
        self.event_add(user_id, "cart_add", {"product": product_name, "qty": qty})

    def cart_get(self, user_id: int) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM carts WHERE user_id=? ORDER BY id DESC", (user_id,))
        return [dict(r) for r in cur.fetchall()]

    def cart_clear(self, user_id: int):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM carts WHERE user_id=?", (user_id,))
        conn.commit()

    def cart_remove(self, cart_id: int):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM carts WHERE id=?", (cart_id,))
        conn.commit()

    def order_create(self, data: Dict) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            INSERT INTO orders (
                user_id, username, name, phone, city, items,
                total_amount, delivery_type, delivery_address,
                comment, status, created_at, latitude, longitude, source
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get("user_id"),
            data.get("username", ""),
            data.get("name", ""),
            data.get("phone", ""),
            data.get("city", ""),
            data.get("items", "[]"),
            data.get("total_amount", 0),
            data.get("delivery_type", ""),
            data.get("delivery_address", ""),
            data.get("comment", "—"),
            "new",
            ts,
            data.get("latitude"),
            data.get("longitude"),
            data.get("source", "bot"),
        ))
        conn.commit()
        order_id = cur.lastrowid

        if data.get("user_id"):
            self.event_add(data["user_id"], "order_created", {"order_id": order_id, "source": data.get("source", "bot")})

        return order_id

    def order_get(self, order_id: int) -> Optional[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE id=?", (order_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def orders_get_by_status(self, status: str, limit: int = 50) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE status=? ORDER BY created_at DESC LIMIT ?", (status, limit))
        return [dict(r) for r in cur.fetchall()]

    def orders_get_user(self, user_id: int, limit: int = 10) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE user_id=? ORDER BY id DESC LIMIT ?", (user_id, limit))
        return [dict(r) for r in cur.fetchall()]

    def order_update_status(self, order_id: int, status: str, manager_id: int = None):
        conn = self._get_conn()
        cur = conn.cursor()
        if manager_id is not None:
            cur.execute("UPDATE orders SET status=?, manager_id=?, manager_seen=1 WHERE id=?", (status, manager_id, order_id))
        else:
            cur.execute("UPDATE orders SET status=? WHERE id=?", (status, order_id))
        conn.commit()

    def order_mark_seen(self, order_id: int, manager_id: int):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE orders SET manager_seen=1, manager_id=? WHERE id=?", (manager_id, order_id))
        conn.commit()

    def orders_get_for_reminder(self) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cutoff = (now_tz() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT * FROM orders
            WHERE status='new' AND manager_seen=0
              AND created_at < ?
              AND (reminded_at IS NULL OR reminded_at < ?)
            ORDER BY created_at DESC
        """, (cutoff, cutoff))
        return [dict(r) for r in cur.fetchall()]

    def order_update_reminded(self, order_id: int):
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("UPDATE orders SET reminded_at=? WHERE id=?", (ts, order_id))
        conn.commit()

    def orders_get_monthly(self, year: int, month: int) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        start = f"{year}-{month:02d}-01 00:00:00"
        last_day = monthrange(year, month)[1]
        end = f"{year}-{month:02d}-{last_day} 23:59:59"
        cur.execute("SELECT * FROM orders WHERE created_at BETWEEN ? AND ? ORDER BY id", (start, end))
        return [dict(r) for r in cur.fetchall()]

    def report_mark_sent(self, year: int, month: int, filename: str, total_orders: int, total_amount: int):
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            INSERT INTO monthly_reports (year, month, sent_at, filename, total_orders, total_amount, status)
            VALUES (?,?,?,?,?,?,?)
        """, (year, month, ts, filename, total_orders, total_amount, "sent"))
        conn.commit()

    def report_is_sent(self, year: int, month: int) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM monthly_reports WHERE year=? AND month=? AND status='sent'", (year, month))
        return cur.fetchone() is not None

    def get_stats_all(self) -> Dict:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as new,
                SUM(CASE WHEN status='processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) as shipped,
                SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) as delivered,
                SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) as cancelled,
                COUNT(DISTINCT user_id) as unique_users
            FROM orders
        """)
        row = cur.fetchone()
        return dict(row) if row else {
            "total": 0, "new": 0, "processing": 0, "shipped": 0,
            "delivered": 0, "cancelled": 0, "unique_users": 0
        }

    def week_key_now(self, dt: datetime) -> str:
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"

    def sched_add(self, dow: int, media_type: str, file_id: str, caption: str, week_key: str):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scheduled_posts (dow, media_type, file_id, caption, week_key)
            VALUES (?,?,?,?,?)
        """, (dow, media_type, file_id, caption, week_key))
        conn.commit()

    def sched_get_for_day(self, dow: int, week_key: str) -> Optional[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM scheduled_posts
            WHERE dow=? AND week_key=? AND posted_at IS NULL
            ORDER BY id ASC
            LIMIT 1
        """, (dow, week_key))
        row = cur.fetchone()
        return dict(row) if row else None

    def sched_mark_posted(self, post_id: int):
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("UPDATE scheduled_posts SET posted_at=? WHERE id=?", (ts, post_id))
        conn.commit()

    def sched_count_week(self, week_key: str) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM scheduled_posts WHERE week_key=?", (week_key,))
        r = cur.fetchone()
        return int(r["c"]) if r else 0

    def stats_range(self, start: str, end: str) -> Dict:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as new,
                SUM(CASE WHEN status='processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) as shipped,
                SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) as delivered,
                SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) as cancelled
            FROM orders
            WHERE created_at BETWEEN ? AND ?
        """, (start, end))
        row = cur.fetchone()
        return dict(row) if row else {
            "total": 0, "new": 0, "processing": 0, "shipped": 0, "delivered": 0, "cancelled": 0
        }

    def top_products_range(self, start: str, end: str, limit: int = 10) -> List[Tuple[str, int]]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT items FROM orders WHERE created_at BETWEEN ? AND ?", (start, end))
        counter: Dict[str, int] = {}
        for r in cur.fetchall():
            try:
                items = json.loads(r["items"] or "[]")
            except Exception:
                items = []
            for it in items:
                name = (it.get("name") or it.get("product_name") or "").strip()
                qty = int(it.get("qty") or 1)
                if not name:
                    continue
                counter[name] = counter.get(name, 0) + qty
        return sorted(counter.items(), key=lambda x: x[1], reverse=True)[:limit]

    def top_cities_range(self, start: str, end: str, limit: int = 10) -> List[Tuple[str, int]]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT city, COUNT(*) as c
            FROM orders
            WHERE created_at BETWEEN ? AND ?
            GROUP BY city
            ORDER BY c DESC
            LIMIT ?
        """, (start, end, limit))
        return [(r["city"] or "—", int(r["c"])) for r in cur.fetchall()]

    def ru_vs_uz_range(self, start: str, end: str) -> Dict:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT u.lang as lang, COUNT(o.id) as c
            FROM orders o
            LEFT JOIN users u ON u.user_id = o.user_id
            WHERE o.created_at BETWEEN ? AND ?
            GROUP BY u.lang
        """, (start, end))
        res = {"ru": 0, "uz": 0, "unknown": 0}
        for r in cur.fetchall():
            lang = (r["lang"] or "unknown").lower()
            if lang not in res:
                lang = "unknown"
            res[lang] += int(r["c"])
        return res

    def funnel_range(self, start: str, end: str) -> Dict:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
              SUM(CASE WHEN event_type='cart_add' THEN 1 ELSE 0 END) as cart_add,
              SUM(CASE WHEN event_type='order_created' THEN 1 ELSE 0 END) as order_created
            FROM events
            WHERE created_at BETWEEN ? AND ?
        """, (start, end))
        r = cur.fetchone()
        cart_add = int(r["cart_add"] or 0) if r else 0
        order_created = int(r["order_created"] or 0) if r else 0
        conv = (order_created / cart_add * 100.0) if cart_add > 0 else 0.0
        return {"cart_add": cart_add, "order_created": order_created, "conversion": round(conv, 2)}

    def orders_filter(self, status: str = "", city: str = "", phone_q: str = "", limit: int = 200) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        q = "SELECT * FROM orders WHERE 1=1"
        args: List = []
        if status:
            q += " AND status=?"
            args.append(status)
        if city:
            q += " AND city LIKE ?"
            args.append(f"%{city}%")
        if phone_q:
            q += " AND phone LIKE ?"
            args.append(f"%{phone_q}%")
        q += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        cur.execute(q, tuple(args))
        return [dict(r) for r in cur.fetchall()]

    def find_orders_by_phone(self, phone_part: str, limit: int = 20) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM orders
            WHERE phone LIKE ?
            ORDER BY id DESC
            LIMIT ?
        """, (f"%{phone_part}%", limit))
        return [dict(r) for r in cur.fetchall()]

    def shop_product_add(
        self,
        photo_file_id: str,
        title_ru: str,
        title_uz: str,
        description_ru: str,
        description_uz: str,
        sizes: str,
        category_slug: str,
        price: int,
        price_on_request: int,
        is_published: int = 1,
        sort_order: int = 0,
    ) -> int:
        if category_slug not in SHOP_CATEGORIES:
            category_slug = "casual"

        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            INSERT INTO shop_products (
                photo_file_id, title_ru, title_uz,
                description_ru, description_uz,
                sizes, category_slug, price, price_on_request,
                is_published, sort_order, created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            photo_file_id, title_ru, title_uz,
            description_ru, description_uz,
            sizes, category_slug, int(price or 0), int(price_on_request or 0),
            int(is_published), int(sort_order),
            ts, ts
        ))
        conn.commit()
        return cur.lastrowid

    def shop_product_update_publish(self, product_id: int, is_published: int):
        conn = self._get_conn()
        cur = conn.cursor()
        ts = now_tz().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("UPDATE shop_products SET is_published=?, updated_at=? WHERE id=?", (int(is_published), ts, product_id))
        conn.commit()

    def shop_product_delete(self, product_id: int):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM shop_products WHERE id=?", (product_id,))
        conn.commit()

    def shop_product_get(self, product_id: int) -> Optional[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM shop_products WHERE id=?", (product_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def shop_products_list(self, published_only: bool = True, limit: int = 500) -> List[Dict]:
        conn = self._get_conn()
        cur = conn.cursor()
        if published_only:
            cur.execute("""
                SELECT * FROM shop_products
                WHERE is_published=1
                ORDER BY sort_order ASC, id DESC
                LIMIT ?
            """, (limit,))
        else:
            cur.execute("""
                SELECT * FROM shop_products
                ORDER BY sort_order ASC, id DESC
                LIMIT ?
            """, (limit,))
        return [dict(r) for r in cur.fetchall()]

    def shop_products_count(self) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM shop_products")
        row = cur.fetchone()
        return int(row["c"]) if row else 0

    def shop_seed_demo_if_empty(self):
        if self.shop_products_count() > 0:
            return
        demo = [
            {
                "photo_file_id": "",
                "title_ru": "Kids Hoodie",
                "title_uz": "Bolalar hudi",
                "description_ru": "Тёплый худи из хлопка для повседневной носки.",
                "description_uz": "Kundalik kiyish uchun issiq paxtali hudi.",
                "sizes": "98,104,110,116",
                "category_slug": "new",
                "price": 250000,
                "price_on_request": 0,
            },
            {
                "photo_file_id": "",
                "title_ru": "Mini Boss Suit",
                "title_uz": "Mini Boss kostyum",
                "description_ru": "Стильный костюм для особых дней.",
                "description_uz": "Maxsus kunlar uchun zamonaviy kostyum.",
                "sizes": "98,104,110,116,122",
                "category_slug": "hits",
                "price": 390000,
                "price_on_request": 0,
            },
            {
                "photo_file_id": "",
                "title_ru": "School Set",
                "title_uz": "Maktab formasi",
                "description_ru": "Школьная форма премиум качества.",
                "description_uz": "Premium sifatdagi maktab formasi.",
                "sizes": "110,116,122,128,134",
                "category_slug": "school",
                "price": 320000,
                "price_on_request": 0,
            },
            {
                "photo_file_id": "",
                "title_ru": "Daily Comfort",
                "title_uz": "Kundalik kiyim",
                "description_ru": "Комфортный комплект на каждый день.",
                "description_uz": "Har kun uchun qulay kiyim to‘plami.",
                "sizes": "98,104,110",
                "category_slug": "casual",
                "price": 0,
                "price_on_request": 1,
            },
        ]
        for item in demo:
            self.shop_product_add(**item)


db = Database()
db.shop_seed_demo_if_empty()


from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    WebAppInfo,
)
from aiogram.types.input_file import FSInputFile

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill


# =========================
# BOT INIT
# =========================

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher(storage=MemoryStorage())


# =========================
# STATES
# =========================

class OrderStates(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_city = State()
    waiting_address = State()
    waiting_comment = State()


class AdminAddProduct(StatesGroup):
    waiting_photo = State()
    waiting_title_ru = State()
    waiting_title_uz = State()
    waiting_desc_ru = State()
    waiting_desc_uz = State()
    waiting_sizes = State()
    waiting_category = State()
    waiting_price = State()


# =========================
# KEYBOARDS
# =========================

def main_menu_ru():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍 Магазин"), KeyboardButton(text="🛒 Корзина")],
            [KeyboardButton(text="📦 Мои заказы"), KeyboardButton(text="📏 Подбор размера")],
            [KeyboardButton(text="📞 Контакты")]
        ],
        resize_keyboard=True
    )


def main_menu_uz():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍 Do'kon"), KeyboardButton(text="🛒 Savatcha")],
            [KeyboardButton(text="📦 Buyurtmalarim"), KeyboardButton(text="📏 Razmer tanlash")],
            [KeyboardButton(text="📞 Aloqa")]
        ],
        resize_keyboard=True
    )


def shop_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Открыть магазин", web_app=WebAppInfo(url=f"{BASE_URL}/"))]
        ]
    )


def cart_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Оформить заказ", callback_data="checkout")],
            [InlineKeyboardButton(text="🗑 Очистить корзину", callback_data="cart_clear")]
        ]
    )


def admin_panel_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📦 Новые заказы", callback_data="admin_orders_new")],
            [InlineKeyboardButton(text="➕ Добавить товар", callback_data="admin_add_product")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")]
        ]
    )


# =========================
# START
# =========================

@dp.message(CommandStart())
async def cmd_start(message: Message):

    user_id = message.from_user.id
    username = message.from_user.username or ""

    db.user_upsert(user_id, username, "ru")

    text = (
        "👋 Добро пожаловать в <b>ZARY & CO</b>\n\n"
        "Детская одежда нового поколения.\n\n"
        "Выберите действие 👇"
    )

    await message.answer(text, reply_markup=main_menu_ru())


# =========================
# MENU
# =========================

@dp.message(F.text == "🛍 Магазин")
async def open_shop(message: Message):

    await message.answer(
        "🛍 Откройте магазин ниже:",
        reply_markup=shop_keyboard()
    )


@dp.message(F.text == "🛒 Корзина")
async def open_cart(message: Message):

    cart = db.cart_get(message.from_user.id)

    if not cart:
        await message.answer("🛒 Корзина пустая.")
        return

    text = "🛒 <b>Ваша корзина</b>\n\n"

    for item in cart:
        text += f"• {esc(item['product_name'])} x{item['qty']}\n"

    await message.answer(text, reply_markup=cart_keyboard())


@dp.callback_query(F.data == "cart_clear")
async def cart_clear(cb: CallbackQuery):

    db.cart_clear(cb.from_user.id)

    await cb.message.edit_text("🗑 Корзина очищена.")


# =========================
# CHECKOUT
# =========================

@dp.callback_query(F.data == "checkout")
async def checkout_start(cb: CallbackQuery, state: FSMContext):

    await cb.message.answer("Введите ваше имя:")
    await state.set_state(OrderStates.waiting_name)


@dp.message(OrderStates.waiting_name)
async def order_name(message: Message, state: FSMContext):

    await state.update_data(name=message.text)

    await message.answer("Введите номер телефона:")
    await state.set_state(OrderStates.waiting_phone)


@dp.message(OrderStates.waiting_phone)
async def order_phone(message: Message, state: FSMContext):

    await state.update_data(phone=message.text)

    await message.answer("Введите город:")
    await state.set_state(OrderStates.waiting_city)


@dp.message(OrderStates.waiting_city)
async def order_city(message: Message, state: FSMContext):

    await state.update_data(city=message.text)

    await message.answer("Введите адрес доставки:")
    await state.set_state(OrderStates.waiting_address)


@dp.message(OrderStates.waiting_address)
async def order_address(message: Message, state: FSMContext):

    await state.update_data(address=message.text)

    await message.answer("Комментарий к заказу (или -):")
    await state.set_state(OrderStates.waiting_comment)


@dp.message(OrderStates.waiting_comment)
async def order_finish(message: Message, state: FSMContext):

    data = await state.get_data()

    cart = db.cart_get(message.from_user.id)

    items = json.dumps(cart, ensure_ascii=False)

    order_id = db.order_create({
        "user_id": message.from_user.id,
        "username": message.from_user.username,
        "name": data["name"],
        "phone": data["phone"],
        "city": data["city"],
        "items": items,
        "delivery_type": "courier",
        "delivery_address": data["address"],
        "comment": message.text
    })

    db.cart_clear(message.from_user.id)

    await message.answer(
        f"✅ Заказ №{order_id} создан!\n\n"
        "Менеджер скоро свяжется с вами."
    )

    for admin in ADMIN_IDS:

        try:
            await bot.send_message(
                admin,
                f"🆕 Новый заказ #{order_id}\n\n"
                f"👤 {data['name']}\n"
                f"📞 {data['phone']}\n"
                f"🏙 {data['city']}\n"
                f"📦 {len(cart)} товаров"
            )
        except:
            pass

    await state.clear()


# =========================
# WEB SHOP API
# =========================

async def api_shop_products(request):

    products = db.shop_products_list(True)

    result = []

    for p in products:

        result.append({
            "id": p["id"],
            "title_ru": p["title_ru"],
            "title_uz": p["title_uz"],
            "description_ru": p["description_ru"],
            "description_uz": p["description_uz"],
            "sizes": p["sizes"],
            "price": p["price"],
            "price_on_request": p["price_on_request"],
            "photo": product_public_photo_url(p["photo_file_id"])
        })

    return web.json_response(result)


async def api_shop_order(request):

    data = await request.json()

    order_id = db.order_create({
        "user_id": None,
        "username": "",
        "name": data.get("name", ""),
        "phone": data.get("phone", ""),
        "city": data.get("city", ""),
        "items": json.dumps(data.get("items", []), ensure_ascii=False),
        "delivery_type": "courier",
        "delivery_address": data.get("address", ""),
        "comment": data.get("comment", ""),
        "source": "web"
    })

    for admin in ADMIN_IDS:

        try:
            await bot.send_message(
                admin,
                f"🌐 Новый WEB заказ #{order_id}"
            )
        except:
            pass

    return web.json_response({"status": "ok", "order_id": order_id})


# =========================
# MEDIA PROXY
# =========================

async def media_proxy(request):

    file_id = request.match_info.get("file_id")

    return web.Response(text="media placeholder")


# =========================
# WEB SHOP PAGE
# =========================

async def shop_index(request):

    html_page = """
<!DOCTYPE html>
<html>
<head>
<title>ZARY SHOP</title>
<meta charset="utf-8"/>
<style>
body{font-family:sans-serif;background:#f5f5f5;padding:40px}
.card{background:white;padding:20px;margin:10px;border-radius:10px}
button{padding:10px 20px;border:none;background:black;color:white;border-radius:6px}
</style>
</head>

<body>

<h1>ZARY SHOP</h1>

<div id="products"></div>

<script>

async function load(){

let res = await fetch("/api/shop/products")

let data = await res.json()

let html=""

for(let p of data){

html += `<div class="card">
<h3>${p.title_ru}</h3>
<p>${p.description_ru}</p>
<button onclick="order(${p.id})">Заказать</button>
</div>`
}

document.getElementById("products").innerHTML = html

}

async function order(id){

let name = prompt("Ваше имя")
let phone = prompt("Телефон")

await fetch("/api/shop/order",{
method:"POST",
headers:{"Content-Type":"application/json"},
body:JSON.stringify({
name:name,
phone:phone,
items:[{id:id,qty:1}]
})
})

alert("Заказ отправлен")

}

load()

</script>

</body>
</html>
"""

    return web.Response(text=html_page, content_type="text/html")


# =========================
# ADMIN PANEL
# =========================

async def admin_dashboard(request):

    token = request.query.get("token","")

    if not admin_panel_allowed(token):

        return web.Response(text="Access denied")

    stats = db.get_stats_all()

    html_page = f"""
<h1>ZARY ADMIN</h1>

<p>Всего заказов: {stats["total"]}</p>
<p>Новые: {stats["new"]}</p>
<p>В обработке: {stats["processing"]}</p>
<p>Доставлено: {stats["delivered"]}</p>
"""

    return web.Response(text=html_page, content_type="text/html")

# =========================
# ADMIN ORDERS / STATUS
# =========================

async def admin_orders_page(request):
    token = request.query.get("token", "")
    if not admin_panel_allowed(token):
        return web.Response(text="Access denied", status=403)

    rows = db.orders_filter(limit=200)

    lines = []
    for o in rows:
        try:
            items = json.loads(o.get("items") or "[]")
            items_preview = ", ".join([
                f"{(it.get('name') or it.get('product_name') or 'item')} x{it.get('qty',1)}"
                for it in items[:3]
            ])
        except Exception:
            items_preview = ""

        lines.append(
            f"<tr>"
            f"<td>#{o['id']}</td>"
            f"<td>{esc((o.get('created_at') or '')[:16])}</td>"
            f"<td>{esc(o.get('name') or '')}</td>"
            f"<td>{esc(o.get('phone') or '')}</td>"
            f"<td>{esc(o.get('city') or '')}</td>"
            f"<td>{esc(items_preview)}</td>"
            f"<td>{esc(o.get('status') or '')}</td>"
            f"</tr>"
        )

    html_page = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>ZARY Orders</title>
<style>
body{{font-family:Arial,sans-serif;background:#f5f5f5;padding:30px}}
table{{width:100%;border-collapse:collapse;background:#fff}}
th,td{{border:1px solid #ddd;padding:10px;text-align:left}}
th{{background:#111;color:#fff}}
a{{display:inline-block;margin-bottom:20px}}
</style>
</head>
<body>
<a href="/admin?token={esc(token)}">← Назад</a>
<h1>Orders</h1>
<table>
<tr>
<th>ID</th>
<th>Дата</th>
<th>Имя</th>
<th>Телефон</th>
<th>Город</th>
<th>Товары</th>
<th>Статус</th>
</tr>
{''.join(lines)}
</table>
</body>
</html>
"""
    return web.Response(text=html_page, content_type="text/html")


# =========================
# REMINDERS LOOP
# =========================

async def check_reminders():
    orders = db.orders_get_for_reminder()
    if not orders:
        return

    for admin_id in ADMIN_IDS:
        try:
            lines = [f"🆕 #{o['id']} | {esc(o['name'])} | {esc(o['phone'])}" for o in orders[:10]]
            text = "🔔 <b>Напоминание: новые заказы!</b>\n\n" + "\n".join(lines)
            await bot.send_message(admin_id, text)
        except Exception as e:
            print(f"Reminder failed for {admin_id}: {e}")

    for o in orders:
        db.order_update_reminded(o["id"])


async def reminders_loop():
    while True:
        try:
            await check_reminders()
        except Exception as e:
            print("reminders_loop error:", e)
        await asyncio.sleep(30 * 60)


# =========================
# MONTHLY REPORT
# =========================

def build_excel_report(filename: str, orders: List[Dict]) -> int:
    wb = Workbook()
    ws = wb.active
    ws.title = "Orders"

    headers = ["ID", "Дата", "Имя", "Телефон", "Город", "Товары", "Статус", "Источник"]
    ws.append(headers)

    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")

    total_amount = 0

    for o in orders:
        try:
            items = json.loads(o.get("items") or "[]")
            items_text = ", ".join([
                f"{(it.get('name') or it.get('product_name') or 'item')} x{it.get('qty',1)}"
                for it in items
            ])
        except Exception:
            items_text = ""

        ws.append([
            o.get("id"),
            o.get("created_at"),
            o.get("name"),
            o.get("phone"),
            o.get("city"),
            items_text,
            o.get("status"),
            o.get("source", "bot"),
        ])

        total_amount += int(o.get("total_amount") or 0)

    wb.save(filename)
    return total_amount


async def generate_monthly_report_to_admins():
    y, m = prev_month(now_tz())

    if db.report_is_sent(y, m):
        return

    orders = db.orders_get_monthly(y, m)
    if not orders:
        return

    Path("reports").mkdir(exist_ok=True)
    filename = f"reports/report_{y}_{m:02d}.xlsx"
    total_amount = build_excel_report(filename, orders)

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"📊 Отчет {m:02d}.{y}\n📦 Заказов: {len(orders)}\n💰 Сумма: {total_amount}"
            )
            await bot.send_document(admin_id, FSInputFile(filename))
        except Exception as e:
            print(f"Failed to send report to {admin_id}: {e}")

    db.report_mark_sent(y, m, filename, len(orders), total_amount)


# =========================
# DAILY CHANNEL POST
# =========================

async def cron_post_daily_to_channel():
    if not CHANNEL_ID:
        return

    now = now_tz()
    dow = now.isoweekday()

    if dow == 7:
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, "📌 Воскресенье: загрузите посты на новую неделю.")
            except Exception:
                pass
        return

    week_key = db.week_key_now(now)
    post = db.sched_get_for_day(dow, week_key)

    if not post:
        return

    caption = (post.get("caption") or "").strip() or "🔥 ZARY SHOP"
    media_type = post.get("media_type") or "none"
    file_id = post.get("file_id") or ""

    try:
        if media_type == "photo" and file_id:
            await bot.send_photo(CHANNEL_ID, file_id, caption=caption)
        elif media_type == "video" and file_id:
            await bot.send_video(CHANNEL_ID, file_id, caption=caption)
        else:
            await bot.send_message(CHANNEL_ID, caption)

        db.sched_mark_posted(post["id"])
    except Exception as e:
        print("Post error:", e)


# =========================
# SIMPLE HEALTH
# =========================

async def health(request):
    return web.Response(text="OK", status=200)


# =========================
# REGISTER ROUTES
# =========================

web_app.router.add_get("/", shop_index)
web_app.router.add_get("/health", health)
web_app.router.add_get("/api/shop/products", api_shop_products)
web_app.router.add_post("/api/shop/order", api_shop_order)
web_app.router.add_get("/media/{file_id}", media_proxy)
web_app.router.add_get("/admin", admin_dashboard)
web_app.router.add_get("/admin/orders", admin_orders_page)


# =========================
# START BOT + WEB SERVER
# =========================

async def main():
    print("Starting bot...")

    asyncio.create_task(reminders_loop())

    runner = web.AppRunner(web_app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    print(f"Web server started on port {PORT}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
