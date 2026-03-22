import os
import re
import json
import html
import asyncio
import logging
import sqlite3
from io import BytesIO
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timezone

from aiohttp import web
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    BufferedInputFile,
    KeyboardButton,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bot.db"
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
SHOP_BRAND = os.getenv("SHOP_BRAND", "ZARY & CO").strip() or "ZARY & CO"
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "ru").strip() or "ru"
ADMIN_PANEL_TOKEN = os.getenv("ADMIN_PANEL_TOKEN", "").strip()
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()
INSTAGRAM_LINK = os.getenv("INSTAGRAM_LINK", "").strip()
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK", "").strip()
MANAGER_PHONE = os.getenv("MANAGER_PHONE", "+998771202255").strip()
MANAGER_TG = os.getenv("MANAGER_TG", "@manager").strip()
LOW_STOCK_THRESHOLD = int(os.getenv("LOW_STOCK_THRESHOLD", "3"))

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip() and x.strip().lstrip("-").isdigit()
}

SUPPORTED_LANGS = ("ru", "uz")
CATEGORY_SLUGS = ("new", "hits", "sale", "limited", "school", "casual")
ORDER_STATUSES = ("new", "confirmed", "paid", "sent", "delivered", "cancelled")
PAYMENT_STATUSES = ("pending", "paid", "failed", "cancelled", "refunded")
DELIVERY_METHODS = ("yandex_courier", "b2b_pochta", "yandex_pvz", "pickup")
PRODUCT_BADGES = ("new", "hit", "sale", "limited", "low_stock")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("zary_shop_v2")

# ============================================================
# BOT
# ============================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
user_router = Router()
cart_router = Router()
checkout_router = Router()
orders_router = Router()
reviews_router = Router()
admin_router = Router()
web_router = web.RouteTableDef()
fallback_router = Router()

# ============================================================
# TEXTS
# ============================================================

TEXTS = {
    "ru": {
        "menu_shop": "🛍 Магазин",
        "menu_cart": "🛒 Корзина",
        "menu_orders": "📦 Мои заказы",
        "menu_reviews": "⭐ Отзывы",
        "menu_leave_review": "✍️ Оставить отзыв",
        "menu_contacts": "📞 Контакты",
        "menu_size": "📏 Размер",
        "menu_lang": "🌐 Язык",
        "menu_admin": "🛠 Админ",
        "welcome": f"Добро пожаловать в <b>{SHOP_BRAND}</b>\n\nLuxury mini ecommerce внутри Telegram.",
        "main_menu_hint": "Выберите раздел ниже.",
        "choose_lang": "Выберите язык.",
        "lang_updated": "Язык обновлён.",
        "cart_empty": "Корзина пуста.",
        "cart_added": "Товар добавлен в корзину.",
        "cart_updated": "Количество товара в корзине обновлено.",
        "cart_removed": "Позиция удалена из корзины.",
        "cart_cleared": "Корзина очищена.",
        "checkout_intro": "Начинаем оформление заказа.",
        "checkout_name": "Введите имя получателя.",
        "checkout_phone": "Введите телефон в формате +998901234567",
        "checkout_delivery": "Выберите доставку.",
        "checkout_city": "Введите город.",
        "checkout_address": "Введите адрес.",
        "checkout_payment": "Выберите способ оплаты.",
        "checkout_comment": "Введите комментарий или нажмите «Пропустить».",
        "checkout_summary": "🧾 <b>Проверка заказа</b>",
        "checkout_confirm_hint": "Напишите «Да» для подтверждения или «Нет» для отмены.",
        "checkout_done": "✅ Заказ создан.",
        "checkout_invalid_phone": "Некорректный номер телефона.",
        "cancel": "❌ Отмена",
        "skip": "Пропустить",
        "yes": "Да",
        "no": "Нет",
        "contacts_title": "📞 <b>Контакты</b>",
        "reviews_empty": "Отзывов пока нет.",
        "review_sent": "Спасибо, отзыв отправлен на модерацию.",
        "not_admin": "Это доступно только админу.",
        "admin_title": "🛠 <b>Админ меню</b>",
        "admin_stats": "📊 Статистика",
        "admin_products": "📦 Товары",
        "admin_orders": "📋 Заказы",
        "admin_reviews": "⭐ Отзывы",
        "admin_stock": "📉 Остатки",
        "admin_report": "📁 Отчёт",
        "admin_customers": "👥 Клиенты",
        "admin_back": "⬅️ В меню",
        "delivery_yandex_courier": "Яндекс Курьер",
        "delivery_b2b_pochta": "B2B Почта",
        "delivery_yandex_pvz": "Яндекс ПВЗ",
        "delivery_pickup": "Самовывоз",
        "payment_click": "Click",
        "payment_payme": "Payme",
        "payment_cash": "Наличными",
        "status_new": "Новый",
        "status_confirmed": "Подтверждён",
        "status_paid": "Оплачен",
        "status_sent": "Отправлен",
        "status_delivered": "Доставлен",
        "status_cancelled": "Отменён",
        "payment_status_pending": "Ожидает оплаты",
        "payment_status_paid": "Оплачен",
        "payment_status_failed": "Ошибка",
        "payment_status_cancelled": "Отменён",
        "payment_status_refunded": "Возврат",
        "stock_low": "⚠️ Мало на складе",
        "stock_out": "⛔ Нет в наличии",
    },
    "uz": {
        "menu_shop": "🛍 Do'kon",
        "menu_cart": "🛒 Savatcha",
        "menu_orders": "📦 Buyurtmalarim",
        "menu_reviews": "⭐ Sharhlar",
        "menu_leave_review": "✍️ Sharh qoldirish",
        "menu_contacts": "📞 Kontaktlar",
        "menu_size": "📏 O'lcham",
        "menu_lang": "🌐 Til",
        "menu_admin": "🛠 Admin",
        "welcome": f"<b>{SHOP_BRAND}</b> ga xush kelibsiz.\n\nTelegram ichidagi luxury mini ecommerce.",
        "main_menu_hint": "Quyidagi bo'limni tanlang.",
        "choose_lang": "Tilni tanlang.",
        "lang_updated": "Til yangilandi.",
        "cart_empty": "Savatcha bo'sh.",
        "cart_added": "Mahsulot savatchaga qo'shildi.",
        "cart_updated": "Savatchadagi son yangilandi.",
        "cart_removed": "Pozitsiya o'chirildi.",
        "cart_cleared": "Savatcha tozalandi.",
        "checkout_intro": "Buyurtma rasmiylashtirish boshlandi.",
        "checkout_name": "Qabul qiluvchi ismini kiriting.",
        "checkout_phone": "Telefonni +998901234567 formatida kiriting",
        "checkout_delivery": "Yetkazib berish usulini tanlang.",
        "checkout_city": "Shaharni kiriting.",
        "checkout_address": "Manzilni kiriting.",
        "checkout_payment": "To'lov usulini tanlang.",
        "checkout_comment": "Izoh kiriting yoki «Propuстить» tugmasini bosing.",
        "checkout_summary": "🧾 <b>Buyurtmani tekshirish</b>",
        "checkout_confirm_hint": "Tasdiqlash uchun «Ha», bekor qilish uchun «Yo'q» yuboring.",
        "checkout_done": "✅ Buyurtma yaratildi.",
        "checkout_invalid_phone": "Telefon noto'g'ri.",
        "cancel": "❌ Bekor qilish",
        "skip": "O'tkazib yuborish",
        "yes": "Ha",
        "no": "Yo'q",
        "contacts_title": "📞 <b>Kontaktlar</b>",
        "reviews_empty": "Hozircha sharhlar yo'q.",
        "review_sent": "Rahmat, sharh moderatsiyaga yuborildi.",
        "not_admin": "Bu faqat admin uchun.",
        "admin_title": "🛠 <b>Admin menyu</b>",
        "admin_stats": "📊 Statistika",
        "admin_products": "📦 Mahsulotlar",
        "admin_orders": "📋 Buyurtmalar",
        "admin_reviews": "⭐ Sharhlar",
        "admin_stock": "📉 Qoldiq",
        "admin_report": "📁 Hisobot",
        "admin_customers": "👥 Mijozlar",
        "admin_back": "⬅️ Menyuga",
        "delivery_yandex_courier": "Yandex Kuryer",
        "delivery_b2b_pochta": "B2B Pochta",
        "delivery_yandex_pvz": "Yandex PVZ",
        "delivery_pickup": "Olib ketish",
        "payment_click": "Click",
        "payment_payme": "Payme",
        "payment_cash": "Naqd",
        "status_new": "Yangi",
        "status_confirmed": "Tasdiqlangan",
        "status_paid": "To'langan",
        "status_sent": "Yuborilgan",
        "status_delivered": "Yetkazilgan",
        "status_cancelled": "Bekor qilingan",
        "payment_status_pending": "Kutilmoqda",
        "payment_status_paid": "To'langan",
        "payment_status_failed": "Xato",
        "payment_status_cancelled": "Bekor qilingan",
        "payment_status_refunded": "Qaytarilgan",
        "stock_low": "⚠️ Qoldiq kam",
        "stock_out": "⛔ Tugagan",
    },
}

# ============================================================
# DB
# ============================================================

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()

    cur.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL DEFAULT '',
            full_name TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            city TEXT NOT NULL DEFAULT '',
            lang TEXT NOT NULL DEFAULT 'ru',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_ru TEXT NOT NULL,
            title_uz TEXT NOT NULL,
            description_ru TEXT NOT NULL DEFAULT '',
            description_uz TEXT NOT NULL DEFAULT '',
            photo_file_id TEXT NOT NULL DEFAULT '',
            gallery_json TEXT NOT NULL DEFAULT '[]',
            category_slug TEXT NOT NULL DEFAULT 'casual',
            price INTEGER NOT NULL DEFAULT 0,
            old_price INTEGER NOT NULL DEFAULT 0,
            sizes_json TEXT NOT NULL DEFAULT '[]',
            stock_qty INTEGER NOT NULL DEFAULT 0,
            is_published INTEGER NOT NULL DEFAULT 1,
            is_new INTEGER NOT NULL DEFAULT 0,
            is_hit INTEGER NOT NULL DEFAULT 0,
            is_limited INTEGER NOT NULL DEFAULT 0,
            discount_percent INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS carts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            size TEXT NOT NULL DEFAULT '',
            qty INTEGER NOT NULL DEFAULT 1,
            added_at TEXT NOT NULL,
            UNIQUE(user_id, product_id, size)
        );

        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL DEFAULT '',
            customer_name TEXT NOT NULL DEFAULT '',
            customer_phone TEXT NOT NULL DEFAULT '',
            city TEXT NOT NULL DEFAULT '',
            items_json TEXT NOT NULL,
            total_qty INTEGER NOT NULL DEFAULT 0,
            total_amount INTEGER NOT NULL DEFAULT 0,
            delivery_method TEXT NOT NULL DEFAULT 'pickup',
            delivery_address TEXT NOT NULL DEFAULT '',
            payment_method TEXT NOT NULL DEFAULT 'cash',
            payment_status TEXT NOT NULL DEFAULT 'pending',
            status TEXT NOT NULL DEFAULT 'new',
            source TEXT NOT NULL DEFAULT 'telegram',
            comment TEXT NOT NULL DEFAULT '',
            is_new_customer INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL DEFAULT '',
            customer_name TEXT NOT NULL DEFAULT '',
            rating INTEGER NOT NULL DEFAULT 5,
            text TEXT NOT NULL DEFAULT '',
            is_published INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, product_id)
        );

        CREATE TABLE IF NOT EXISTS stock_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            stock_qty INTEGER NOT NULL,
            alert_type TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_products_published ON products(is_published, category_slug, sort_order);
        CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
        CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_pub ON reviews(is_published);
        """
    )

    conn.commit()
    conn.close()

# ============================================================
# HELPERS
# ============================================================

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def ensure_lang(lang: str) -> str:
    return lang if lang in SUPPORTED_LANGS else DEFAULT_LANGUAGE


def normalize_phone(phone: str) -> str:
    value = (phone or "").strip()
    value = value.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if value.startswith("998") and not value.startswith("+998"):
        value = "+" + value
    return value


def is_valid_phone(phone: str) -> bool:
    return bool(re.fullmatch(r"\+998\d{9}", normalize_phone(phone)))


def fmt_sum(value: Any) -> str:
    return f"{safe_int(value):,}".replace(",", " ") + " сум"


def get_user_lang(user_id: int) -> str:
    conn = get_db()
    row = conn.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row["lang"] if row and row["lang"] in SUPPORTED_LANGS else DEFAULT_LANGUAGE


def t(lang_or_user: int | str, key: str) -> str:
    lang = get_user_lang(lang_or_user) if isinstance(lang_or_user, int) else ensure_lang(lang_or_user)
    return TEXTS.get(lang, TEXTS[DEFAULT_LANGUAGE]).get(key, key)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def user_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=t(user_id, "menu_shop"), web_app=WebAppInfo(url=f"{BASE_URL}/shop?lang={get_user_lang(user_id)}"))],
        [KeyboardButton(text=t(user_id, "menu_cart")), KeyboardButton(text=t(user_id, "menu_orders"))],
        [KeyboardButton(text=t(user_id, "menu_reviews")), KeyboardButton(text=t(user_id, "menu_leave_review"))],
        [KeyboardButton(text=t(user_id, "menu_contacts")), KeyboardButton(text=t(user_id, "menu_lang"))],
    ]
    if is_admin(user_id):
        rows.append([KeyboardButton(text=t(user_id, "menu_admin"))])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_menu(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "admin_stats")), KeyboardButton(text=t(user_id, "admin_stock"))],
            [KeyboardButton(text=t(user_id, "admin_orders")), KeyboardButton(text=t(user_id, "admin_products"))],
            [KeyboardButton(text=t(user_id, "admin_customers")), KeyboardButton(text=t(user_id, "admin_reviews"))],
            [KeyboardButton(text=t(user_id, "admin_report"))],
            [KeyboardButton(text=t(user_id, "admin_back"))],
        ],
        resize_keyboard=True,
    )


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Русский", callback_data="lang:ru")],
            [InlineKeyboardButton(text="O'zbekcha", callback_data="lang:uz")],
        ]
    )


def cancel_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t(user_id, "cancel"))]], resize_keyboard=True)


def yes_no_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "yes")), KeyboardButton(text=t(user_id, "no"))]],
        resize_keyboard=True,
    )


def delivery_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "delivery_yandex_courier"))],
            [KeyboardButton(text=t(user_id, "delivery_b2b_pochta"))],
            [KeyboardButton(text=t(user_id, "delivery_yandex_pvz"))],
            [KeyboardButton(text=t(user_id, "delivery_pickup"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def payment_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "payment_click"))],
            [KeyboardButton(text=t(user_id, "payment_payme"))],
            [KeyboardButton(text=t(user_id, "payment_cash"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def skip_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "skip"))], [KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def upsert_user(user_id: int, username: str | None, full_name: str | None) -> None:
    now = utc_now_iso()
    conn = get_db()
    row = conn.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,)).fetchone()
    lang = row["lang"] if row else DEFAULT_LANGUAGE
    conn.execute(
        """
        INSERT INTO users (user_id, username, full_name, lang, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            full_name = excluded.full_name,
            updated_at = excluded.updated_at
        """,
        (user_id, username or "", full_name or "", lang, now, now),
    )
    conn.commit()
    conn.close()


def set_user_lang(user_id: int, lang: str) -> None:
    lang = ensure_lang(lang)
    now = utc_now_iso()
    conn = get_db()
    conn.execute(
        "UPDATE users SET lang = ?, updated_at = ? WHERE user_id = ?",
        (lang, now, user_id),
    )
    conn.commit()
    conn.close()


def parse_json_list(value: str) -> list[str]:
    try:
        data = json.loads(value or "[]")
    except Exception:
        data = []
    return [str(x).strip() for x in data if str(x).strip()]


def product_title(row: sqlite3.Row, lang: str) -> str:
    return row["title_uz"] if ensure_lang(lang) == "uz" else row["title_ru"]


def product_description(row: sqlite3.Row, lang: str) -> str:
    return row["description_uz"] if ensure_lang(lang) == "uz" else row["description_ru"]


def delivery_method_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {
        t(user_id, "delivery_yandex_courier"): "yandex_courier",
        t(user_id, "delivery_b2b_pochta"): "b2b_pochta",
        t(user_id, "delivery_yandex_pvz"): "yandex_pvz",
        t(user_id, "delivery_pickup"): "pickup",
    }
    return mapping.get((text or "").strip())


def payment_method_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {
        t(user_id, "payment_click"): "click",
        t(user_id, "payment_payme"): "payme",
        t(user_id, "payment_cash"): "cash",
    }
    return mapping.get((text or "").strip())


def status_label(lang_or_user: int | str, value: str) -> str:
    return t(lang_or_user, f"status_{value}")


def payment_status_label(lang_or_user: int | str, value: str) -> str:
    return t(lang_or_user, f"payment_status_{value}")


def delivery_label(lang_or_user: int | str, value: str) -> str:
    return t(lang_or_user, f"delivery_{value}")

# ============================================================
# PRODUCT / CART
# ============================================================

def get_products(published_only: bool = True) -> list[sqlite3.Row]:
    conn = get_db()
    if published_only:
        rows = conn.execute(
            "SELECT * FROM products WHERE is_published = 1 ORDER BY sort_order ASC, id DESC"
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM products ORDER BY sort_order ASC, id DESC").fetchall()
    conn.close()
    return rows


def get_product(product_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    conn.close()
    return row


def get_cart_rows(user_id: int) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT c.id as cart_id, c.user_id, c.product_id, c.size, c.qty,
               p.title_ru, p.title_uz, p.photo_file_id, p.price, p.stock_qty,
               p.old_price, p.is_new, p.is_hit, p.is_limited, p.discount_percent
        FROM carts c
        JOIN products p ON p.id = c.product_id
        ORDER BY c.id ASC
        """
    ).fetchall()
    conn.close()
    return [r for r in rows if safe_int(r["user_id"]) == user_id]


def cart_totals(user_id: int) -> tuple[int, int]:
    rows = get_cart_rows(user_id)
    total_qty = sum(safe_int(r["qty"]) for r in rows)
    total_amount = sum(safe_int(r["qty"]) * safe_int(r["price"]) for r in rows)
    return total_qty, total_amount


def add_to_cart(user_id: int, product_id: int, qty: int, size: str = "") -> tuple[bool, str]:
    product = get_product(product_id)
    if not product or not safe_int(product["is_published"], 1):
        return False, "not_found"
    if safe_int(product["stock_qty"]) <= 0:
        return False, "out_of_stock"

    allowed_sizes = parse_json_list(product["sizes_json"])
    if allowed_sizes and size not in allowed_sizes:
        return False, "bad_size"

    conn = get_db()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT * FROM carts WHERE user_id = ? AND product_id = ? AND size = ?",
        (user_id, product_id, size),
    ).fetchone()
    if row:
        new_qty = safe_int(row["qty"]) + max(1, qty)
        cur.execute("UPDATE carts SET qty = ? WHERE id = ?", (new_qty, row["id"]))
        key = "cart_updated"
    else:
        cur.execute(
            "INSERT INTO carts (user_id, product_id, size, qty, added_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, product_id, size, max(1, qty), utc_now_iso()),
        )
        key = "cart_added"
    conn.commit()
    conn.close()
    return True, key


def remove_cart_item(user_id: int, cart_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM carts WHERE id = ? AND user_id = ?", (cart_id, user_id))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def clear_cart(user_id: int) -> None:
    conn = get_db()
    conn.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def cart_api(user_id: int, lang: str) -> dict[str, Any]:
    rows = get_cart_rows(user_id)
    items = []
    for row in rows:
        price = safe_int(row["price"])
        qty = safe_int(row["qty"])
        items.append({
            "cart_id": row["cart_id"],
            "product_id": row["product_id"],
            "product_name": row["title_uz"] if ensure_lang(lang) == "uz" else row["title_ru"],
            "qty": qty,
            "size": row["size"] or "",
            "price": price,
            "subtotal": price * qty,
        })
    total_qty, total_amount = cart_totals(user_id)
    return {"items": items, "total_qty": total_qty, "total_amount": total_amount}


def stock_badges(row: sqlite3.Row) -> list[str]:
    result = []
    if safe_int(row["is_new"]):
        result.append("new")
    if safe_int(row["is_hit"]):
        result.append("hit")
    if safe_int(row["discount_percent"]):
        result.append("sale")
    if safe_int(row["is_limited"]):
        result.append("limited")
    if 0 < safe_int(row["stock_qty"]) <= LOW_STOCK_THRESHOLD:
        result.append("low_stock")
    return result

# ============================================================
# ORDERS / REPORTS
# ============================================================

def user_has_previous_orders(user_id: int) -> bool:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) AS c FROM orders WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return safe_int(row["c"]) > 0


def create_order(user_id: int, username: str, data: dict[str, Any], source: str = "telegram") -> int:
    cart_rows = get_cart_rows(user_id)
    if not cart_rows:
        raise ValueError("Cart is empty")

    conn = get_db()
    cur = conn.cursor()
    items = []
    total_qty = 0
    total_amount = 0

    for row in cart_rows:
        product = get_product(safe_int(row["product_id"]))
        if not product:
            continue
        qty = safe_int(row["qty"])
        price = safe_int(product["price"])
        subtotal = qty * price
        total_qty += qty
        total_amount += subtotal
        items.append({
            "product_id": row["product_id"],
            "product_name": product["title_ru"],
            "size": row["size"] or "",
            "qty": qty,
            "price": price,
            "subtotal": subtotal,
        })
        new_stock = max(0, safe_int(product["stock_qty"]) - qty)
        cur.execute("UPDATE products SET stock_qty = ?, updated_at = ? WHERE id = ?", (new_stock, utc_now_iso(), product["id"]))

    now = utc_now_iso()
    is_new_customer = 0 if user_has_previous_orders(user_id) else 1
    payment_method = data.get("payment_method") or "cash"
    payment_status = "pending" if payment_method in {"click", "payme"} else "paid"

    cur.execute(
        """
        INSERT INTO orders (
            user_id, username, customer_name, customer_phone, city,
            items_json, total_qty, total_amount, delivery_method,
            delivery_address, payment_method, payment_status,
            status, source, comment, is_new_customer, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            username or "",
            data.get("customer_name") or "",
            data.get("customer_phone") or "",
            data.get("city") or "",
            json.dumps(items, ensure_ascii=False),
            total_qty,
            total_amount,
            data.get("delivery_method") or "pickup",
            data.get("delivery_address") or "",
            payment_method,
            payment_status,
            "new",
            source,
            data.get("comment") or "",
            is_new_customer,
            now,
            now,
        ),
    )
    order_id = cur.lastrowid
    cur.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return order_id


def get_orders_for_user(user_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit)).fetchall()
    conn.close()
    return rows


def get_order(order_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    conn.close()
    return row


def render_items(items_json: str) -> str:
    try:
        items = json.loads(items_json or "[]")
    except Exception:
        items = []
    if not items:
        return "—"
    result = []
    for idx, item in enumerate(items, start=1):
        size = item.get("size") or ""
        size_part = f" | {size}" if size else ""
        result.append(f"{idx}. {html.escape(item.get('product_name', '—'))}{size_part} — {item.get('qty', 0)} × {fmt_sum(item.get('price', 0))}")
    return "\n".join(result)


def monthly_report(month: int, year: int) -> tuple[Path, str]:
    conn = get_db()
    cur = conn.cursor()
    month_prefix = f"{year:04d}-{month:02d}-"
    rows = cur.execute(
        "SELECT * FROM orders WHERE substr(created_at, 1, 8) = ? ORDER BY id ASC",
        (month_prefix,),
    ).fetchall()

    total_turnover = sum(safe_int(r["total_amount"]) for r in rows)
    total_orders = len(rows)
    new_customers = sum(safe_int(r["is_new_customer"]) for r in rows)

    product_counter: dict[str, int] = {}
    size_counter: dict[str, int] = {}
    delivery_counter: dict[str, int] = {}

    for row in rows:
        delivery_counter[row["delivery_method"]] = delivery_counter.get(row["delivery_method"], 0) + 1
        try:
            items = json.loads(row["items_json"] or "[]")
        except Exception:
            items = []
        for item in items:
            name = item.get("product_name") or "—"
            size = item.get("size") or "—"
            qty = safe_int(item.get("qty"))
            product_counter[name] = product_counter.get(name, 0) + qty
            size_counter[size] = size_counter.get(size, 0) + qty

    wb = Workbook()
    ws = wb.active
    ws.title = "Orders"
    headers = [
        "Номер заказа", "Дата", "Имя клиента", "Телефон", "Город", "Товары",
        "Размеры", "Количество", "Сумма", "Доставка", "Способ оплаты",
        "Статус оплаты", "Статус заказа", "Источник заказа"
    ]
    ws.append(headers)
    fill = PatternFill("solid", fgColor="D9C28A")
    font = Font(bold=True)
    for cell in ws[1]:
        cell.font = font
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for row in rows:
        try:
            items = json.loads(row["items_json"] or "[]")
        except Exception:
            items = []
        product_names = ", ".join(str(i.get("product_name") or "—") for i in items)
        sizes = ", ".join(str(i.get("size") or "—") for i in items)
        qty = sum(safe_int(i.get("qty")) for i in items)
        ws.append([
            row["id"], row["created_at"], row["customer_name"], row["customer_phone"], row["city"],
            product_names, sizes, qty, safe_int(row["total_amount"]), row["delivery_method"], row["payment_method"],
            row["payment_status"], row["status"], row["source"],
        ])

    summary = wb.create_sheet("Summary")
    summary.append(["Метрика", "Значение"])
    summary.append(["Общий оборот за месяц", total_turnover])
    summary.append(["Количество заказов", total_orders])
    summary.append(["Новых клиентов", new_customers])
    summary.append([])
    summary.append(["Топ товаров", "Количество"])
    for name, qty in sorted(product_counter.items(), key=lambda x: x[1], reverse=True)[:10]:
        summary.append([name, qty])
    summary.append([])
    summary.append(["Популярные размеры", "Количество"])
    for size, qty in sorted(size_counter.items(), key=lambda x: x[1], reverse=True)[:10]:
        summary.append([size, qty])
    summary.append([])
    summary.append(["Способ доставки", "Количество"])
    for name, qty in sorted(delivery_counter.items(), key=lambda x: x[1], reverse=True):
        summary.append([name, qty])

    for row in summary[1:2]:
        for cell in row:
            cell.font = font
            cell.fill = fill
    for col in ("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N"):
        ws.column_dimensions[col].width = 20
    summary.column_dimensions["A"].width = 30
    summary.column_dimensions["B"].width = 22

    file_path = REPORTS_DIR / f"orders_report_{year}_{month:02d}.xlsx"
    wb.save(file_path)
    conn.close()
    caption = (
        f"📁 Отчёт за {month:02d}.{year}\n"
        f"Оборот: {fmt_sum(total_turnover)}\n"
        f"Заказов: {total_orders}\n"
        f"Новых клиентов: {new_customers}"
    )
    return file_path, caption

# ============================================================
# STOCK ALERTS
# ============================================================

def low_stock_products() -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM products WHERE stock_qty <= ? ORDER BY stock_qty ASC, id DESC",
        (LOW_STOCK_THRESHOLD,),
    ).fetchall()
    conn.close()
    return rows


async def notify_admins_low_stock() -> None:
    rows = low_stock_products()
    if not rows or not ADMIN_IDS:
        return
    lines = ["📉 <b>Контроль склада</b>"]
    for row in rows[:15]:
        title = html.escape(row["title_ru"])
        stock = safe_int(row["stock_qty"])
        label = "⛔ Закончился" if stock <= 0 else f"⚠️ Осталось {stock} шт"
        lines.append(f"• #{row['id']} {title} — {label}")
    text = "\n".join(lines)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            logger.exception("Failed to send stock alert to %s", admin_id)

# ============================================================
# FSM
# ============================================================

class ReviewState(StatesGroup):
    rating = State()
    text = State()


class CheckoutState(StatesGroup):
    customer_name = State()
    customer_phone = State()
    delivery_method = State()
    city = State()
    delivery_address = State()
    payment_method = State()
    comment = State()
    confirm = State()

# ============================================================
# WEBAPP JSON
# ============================================================

async def file_url(file_id: str) -> str:
    if not file_id:
        return ""
    try:
        f = await bot.get_file(file_id)
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{f.file_path}"
    except Exception:
        return ""


def product_to_web_dict(row: sqlite3.Row, lang: str, photo_url: str) -> dict[str, Any]:
    return {
        "id": row["id"],
        "title": product_title(row, lang),
        "description": product_description(row, lang),
        "price": safe_int(row["price"]),
        "old_price": safe_int(row["old_price"]),
        "stock_qty": safe_int(row["stock_qty"]),
        "sizes_list": parse_json_list(row["sizes_json"]),
        "category_slug": row["category_slug"],
        "badges": stock_badges(row),
        "photo_url": photo_url,
        "can_buy": safe_int(row["stock_qty"]) > 0 and safe_int(row["is_published"]) == 1,
    }

# ============================================================
# USER HANDLERS
# ============================================================

@user_router.message(CommandStart())
async def start_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await message.answer(t(message.from_user.id, "welcome"), reply_markup=user_main_menu(message.from_user.id))
    await message.answer(t(message.from_user.id, "main_menu_hint"))


@user_router.message(F.text.in_([TEXTS['ru']['menu_lang'], TEXTS['uz']['menu_lang']]))
async def choose_lang_handler(message: Message) -> None:
    await message.answer(t(message.from_user.id, "choose_lang"), reply_markup=language_keyboard())


@user_router.callback_query(F.data.startswith("lang:"))
async def set_lang_callback(callback: CallbackQuery) -> None:
    lang = callback.data.split(":")[-1]
    set_user_lang(callback.from_user.id, lang)
    await callback.message.answer(t(callback.from_user.id, "lang_updated"), reply_markup=user_main_menu(callback.from_user.id))
    await callback.answer()


@user_router.message(F.text.in_([TEXTS['ru']['menu_contacts'], TEXTS['uz']['menu_contacts']]))
async def contacts_handler(message: Message) -> None:
    lang = get_user_lang(message.from_user.id)
    text = (
        f"{t(lang, 'contacts_title')}\n\n"
        f"<b>Phone:</b> {html.escape(MANAGER_PHONE)}\n"
        f"<b>Telegram:</b> {html.escape(MANAGER_TG)}\n"
        f"<b>Channel:</b> {html.escape(CHANNEL_LINK or '—')}\n"
        f"<b>Instagram:</b> {html.escape(INSTAGRAM_LINK or '—')}\n"
        f"<b>YouTube:</b> {html.escape(YOUTUBE_LINK or '—')}"
    )
    await message.answer(text, reply_markup=user_main_menu(message.from_user.id))

# ============================================================
# CART
# ============================================================

@cart_router.message(F.text.in_([TEXTS['ru']['menu_cart'], TEXTS['uz']['menu_cart']]))
async def cart_handler(message: Message) -> None:
    data = cart_api(message.from_user.id, get_user_lang(message.from_user.id))
    if not data["items"]:
        await message.answer(t(message.from_user.id, "cart_empty"))
        return
    lines = ["🛒 <b>Корзина</b>"]
    for idx, item in enumerate(data["items"], start=1):
        size = f" | {item['size']}" if item['size'] else ""
        lines.append(f"{idx}. <b>{html.escape(item['product_name'])}</b>{size} — {item['qty']} × {fmt_sum(item['price'])}")
    lines.append("")
    lines.append(f"Всего: <b>{data['total_qty']}</b>")
    lines.append(f"Сумма: <b>{fmt_sum(data['total_amount'])}</b>")
    await message.answer("\n".join(lines))


@cart_router.message(F.web_app_data)
async def webapp_data_handler(message: Message, state: FSMContext) -> None:
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    try:
        payload = json.loads(message.web_app_data.data)
    except Exception:
        await message.answer("Bad web app payload")
        return

    action = str(payload.get("action") or "").strip()
    if action == "add_to_cart":
        ok, key = add_to_cart(message.from_user.id, safe_int(payload.get("product_id")), max(1, safe_int(payload.get("qty"), 1)), str(payload.get("size") or ""))
        await message.answer(t(message.from_user.id, key if ok else "cart_empty"))
        return

    if action == "remove_from_cart":
        ok = remove_cart_item(message.from_user.id, safe_int(payload.get("cart_id")))
        await message.answer(t(message.from_user.id, "cart_removed") if ok else "Not found")
        return

    if action == "clear_cart":
        clear_cart(message.from_user.id)
        await message.answer(t(message.from_user.id, "cart_cleared"))
        return

    if action in {"checkout", "buy_now"}:
        if action == "buy_now":
            add_to_cart(message.from_user.id, safe_int(payload.get("product_id")), max(1, safe_int(payload.get("qty"), 1)), str(payload.get("size") or ""))
        if not get_cart_rows(message.from_user.id):
            await message.answer(t(message.from_user.id, "cart_empty"))
            return
        await state.clear()
        await state.set_state(CheckoutState.customer_name)
        await message.answer(f"{t(message.from_user.id, 'checkout_intro')}\n\n{t(message.from_user.id, 'checkout_name')}", reply_markup=cancel_keyboard(message.from_user.id))
        return

# ============================================================
# CHECKOUT
# ============================================================

async def maybe_cancel(message: Message, state: FSMContext) -> bool:
    if (message.text or "").strip() == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer("Отменено.", reply_markup=user_main_menu(message.from_user.id))
        return True
    return False


@checkout_router.message(CheckoutState.customer_name)
async def checkout_name_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    value = (message.text or "").strip()
    if not value:
        await message.answer(t(message.from_user.id, "checkout_name"))
        return
    await state.update_data(customer_name=value)
    await state.set_state(CheckoutState.customer_phone)
    await message.answer(t(message.from_user.id, "checkout_phone"))


@checkout_router.message(CheckoutState.customer_phone)
async def checkout_phone_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    phone = normalize_phone(message.text or "")
    if not is_valid_phone(phone):
        await message.answer(t(message.from_user.id, "checkout_invalid_phone"))
        return
    await state.update_data(customer_phone=phone)
    await state.set_state(CheckoutState.delivery_method)
    await message.answer(t(message.from_user.id, "checkout_delivery"), reply_markup=delivery_keyboard(message.from_user.id))


@checkout_router.message(CheckoutState.delivery_method)
async def checkout_delivery_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    method = delivery_method_from_label(message.from_user.id, message.text or "")
    if not method:
        await message.answer(t(message.from_user.id, "checkout_delivery"))
        return
    await state.update_data(delivery_method=method)
    await state.set_state(CheckoutState.city)
    await message.answer(t(message.from_user.id, "checkout_city"), reply_markup=cancel_keyboard(message.from_user.id))


@checkout_router.message(CheckoutState.city)
async def checkout_city_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    city = (message.text or "").strip()
    if not city:
        await message.answer(t(message.from_user.id, "checkout_city"))
        return
    await state.update_data(city=city)
    await state.set_state(CheckoutState.delivery_address)
    await message.answer(t(message.from_user.id, "checkout_address"))


@checkout_router.message(CheckoutState.delivery_address)
async def checkout_address_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    value = (message.text or "").strip()
    if not value:
        await message.answer(t(message.from_user.id, "checkout_address"))
        return
    await state.update_data(delivery_address=value)
    await state.set_state(CheckoutState.payment_method)
    await message.answer(t(message.from_user.id, "checkout_payment"), reply_markup=payment_keyboard(message.from_user.id))


@checkout_router.message(CheckoutState.payment_method)
async def checkout_payment_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    method = payment_method_from_label(message.from_user.id, message.text or "")
    if not method:
        await message.answer(t(message.from_user.id, "checkout_payment"))
        return
    await state.update_data(payment_method=method)
    await state.set_state(CheckoutState.comment)
    await message.answer(t(message.from_user.id, "checkout_comment"), reply_markup=skip_keyboard(message.from_user.id))


@checkout_router.message(CheckoutState.comment)
async def checkout_comment_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel(message, state):
        return
    comment = (message.text or "").strip()
    if comment == t(message.from_user.id, "skip"):
        comment = ""
    await state.update_data(comment=comment)
    data = await state.get_data()
    total_qty, total_amount = cart_totals(message.from_user.id)
    text = (
        f"{t(message.from_user.id, 'checkout_summary')}\n\n"
        f"<b>Имя:</b> {html.escape(data.get('customer_name', '—'))}\n"
        f"<b>Телефон:</b> {html.escape(data.get('customer_phone', '—'))}\n"
        f"<b>Город:</b> {html.escape(data.get('city', '—'))}\n"
        f"<b>Доставка:</b> {delivery_label(message.from_user.id, data.get('delivery_method', 'pickup'))}\n"
        f"<b>Адрес:</b> {html.escape(data.get('delivery_address', '—'))}\n"
        f"<b>Оплата:</b> {html.escape(data.get('payment_method', 'cash'))}\n"
        f"<b>Количество:</b> {total_qty}\n"
        f"<b>Сумма:</b> {fmt_sum(total_amount)}\n\n"
        f"{t(message.from_user.id, 'checkout_confirm_hint')}"
    )
    await state.set_state(CheckoutState.confirm)
    await message.answer(text, reply_markup=yes_no_keyboard(message.from_user.id))


@checkout_router.message(CheckoutState.confirm)
async def checkout_confirm_handler(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text == t(message.from_user.id, "no"):
        await state.clear()
        await message.answer("Оформление отменено.", reply_markup=user_main_menu(message.from_user.id))
        return
    if text != t(message.from_user.id, "yes"):
        await message.answer(t(message.from_user.id, "checkout_confirm_hint"))
        return

    data = await state.get_data()
    order_id = create_order(message.from_user.id, message.from_user.username or "", data, source="telegram")
    await state.clear()
    await message.answer(
        f"{t(message.from_user.id, 'checkout_done')}\n\n<b>Номер заказа:</b> #{order_id}",
        reply_markup=user_main_menu(message.from_user.id),
    )
    await notify_admins_new_order(order_id)

# ============================================================
# ORDERS / REVIEWS
# ============================================================

@orders_router.message(F.text.in_([TEXTS['ru']['menu_orders'], TEXTS['uz']['menu_orders']]))
async def user_orders_handler(message: Message) -> None:
    rows = get_orders_for_user(message.from_user.id, 20)
    if not rows:
        await message.answer("Заказов пока нет.")
        return
    for row in rows:
        await message.answer(
            f"<b>Заказ #{row['id']}</b>\n"
            f"Дата: {row['created_at']}\n"
            f"Статус: {status_label(message.from_user.id, row['status'])}\n"
            f"Оплата: {payment_status_label(message.from_user.id, row['payment_status'])}\n"
            f"Доставка: {delivery_label(message.from_user.id, row['delivery_method'])}\n"
            f"Сумма: {fmt_sum(row['total_amount'])}\n\n"
            f"{render_items(row['items_json'])}"
        )


@reviews_router.message(F.text.in_([TEXTS['ru']['menu_reviews'], TEXTS['uz']['menu_reviews']]))
async def reviews_list_handler(message: Message) -> None:
    conn = get_db()
    rows = conn.execute("SELECT * FROM reviews WHERE is_published = 1 ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    if not rows:
        await message.answer(t(message.from_user.id, "reviews_empty"))
        return
    for row in rows:
        await message.answer(
            f"{'⭐' * max(1, min(5, safe_int(row['rating'], 5)))}\n"
            f"<b>{html.escape(row['customer_name'] or 'Client')}</b>\n"
            f"{html.escape(row['text'] or '')}"
        )

# ============================================================
# ADMIN
# ============================================================

def get_admin_stats() -> dict[str, int]:
    conn = get_db()
    cur = conn.cursor()
    def scalar(q: str, p: tuple = ()) -> int:
        row = cur.execute(q, p).fetchone()
        return safe_int(row[0]) if row else 0
    result = {
        "orders": scalar("SELECT COUNT(*) FROM orders"),
        "users": scalar("SELECT COUNT(*) FROM users"),
        "products": scalar("SELECT COUNT(*) FROM products"),
        "reviews": scalar("SELECT COUNT(*) FROM reviews WHERE is_published = 1"),
        "new_orders": scalar("SELECT COUNT(*) FROM orders WHERE status = 'new'"),
        "low_stock": scalar("SELECT COUNT(*) FROM products WHERE stock_qty <= ?", (LOW_STOCK_THRESHOLD,)),
    }
    conn.close()
    return result


async def notify_admins_new_order(order_id: int) -> None:
    order = get_order(order_id)
    if not order:
        return
    text = (
        f"📦 <b>Новый заказ #{order['id']}</b>\n\n"
        f"<b>Клиент:</b> {html.escape(order['customer_name'])}\n"
        f"<b>Телефон:</b> {html.escape(order['customer_phone'])}\n"
        f"<b>Город:</b> {html.escape(order['city'])}\n"
        f"<b>Доставка:</b> {delivery_label('ru', order['delivery_method'])}\n"
        f"<b>Оплата:</b> {html.escape(order['payment_method'])} / {payment_status_label('ru', order['payment_status'])}\n"
        f"<b>Сумма:</b> {fmt_sum(order['total_amount'])}\n\n"
        f"<b>Товары:</b>\n{render_items(order['items_json'])}"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            logger.exception("Failed to notify admin %s", admin_id)


@admin_router.message(F.text.in_([TEXTS['ru']['menu_admin'], TEXTS['uz']['menu_admin']]))
async def admin_open_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    await message.answer(t(message.from_user.id, "admin_title"), reply_markup=admin_menu(message.from_user.id))


@admin_router.message(F.text.in_([TEXTS['ru']['admin_back'], TEXTS['uz']['admin_back']]))
async def admin_back_handler(message: Message) -> None:
    await message.answer(t(message.from_user.id, "main_menu_hint"), reply_markup=user_main_menu(message.from_user.id))


@admin_router.message(F.text.in_([TEXTS['ru']['admin_stats'], TEXTS['uz']['admin_stats']]))
async def admin_stats_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    stats = get_admin_stats()
    await message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"Заказы: <b>{stats['orders']}</b>\n"
        f"Новые заказы: <b>{stats['new_orders']}</b>\n"
        f"Пользователи: <b>{stats['users']}</b>\n"
        f"Товары: <b>{stats['products']}</b>\n"
        f"Опубликованные отзывы: <b>{stats['reviews']}</b>\n"
        f"Мало на складе: <b>{stats['low_stock']}</b>"
    )


@admin_router.message(F.text.in_([TEXTS['ru']['admin_stock'], TEXTS['uz']['admin_stock']]))
async def admin_stock_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    rows = low_stock_products()
    if not rows:
        await message.answer("На складе всё нормально.")
        return
    lines = ["📉 <b>Остатки</b>"]
    for row in rows[:30]:
        lines.append(f"#{row['id']} {html.escape(row['title_ru'])} — {row['stock_qty']} шт")
    lines.append("\nКоманда: /set_stock PRODUCT_ID QTY")
    await message.answer("\n".join(lines))


@admin_router.message(Command("set_stock"))
async def admin_set_stock(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Используй: /set_stock 12 7")
        return
    product_id = safe_int(parts[1])
    qty = max(0, safe_int(parts[2]))
    conn = get_db()
    conn.execute("UPDATE products SET stock_qty = ?, updated_at = ? WHERE id = ?", (qty, utc_now_iso(), product_id))
    conn.commit()
    conn.close()
    await message.answer(f"Остаток товара #{product_id} обновлён: {qty} шт")


@admin_router.message(F.text.in_([TEXTS['ru']['admin_orders'], TEXTS['uz']['admin_orders']]))
async def admin_orders_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    if not rows:
        await message.answer("Пока заказов нет.")
        return
    for row in rows:
        await message.answer(
            f"📦 <b>Заказ #{row['id']}</b>\n"
            f"{html.escape(row['customer_name'])} | {html.escape(row['customer_phone'])}\n"
            f"{status_label('ru', row['status'])} | {payment_status_label('ru', row['payment_status'])}\n"
            f"{delivery_label('ru', row['delivery_method'])}\n"
            f"{fmt_sum(row['total_amount'])}\n\n"
            f"{render_items(row['items_json'])}\n\n"
            f"Команда статуса: /set_order_status {row['id']} paid"
        )


@admin_router.message(Command("set_order_status"))
async def admin_set_order_status(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Используй: /set_order_status 15 sent")
        return
    order_id = safe_int(parts[1])
    status = parts[2].strip()
    if status not in ORDER_STATUSES:
        await message.answer("Допустимые статусы: new, confirmed, paid, sent, delivered, cancelled")
        return
    conn = get_db()
    conn.execute("UPDATE orders SET status = ?, updated_at = ? WHERE id = ?", (status, utc_now_iso(), order_id))
    conn.commit()
    conn.close()
    await message.answer(f"Статус заказа #{order_id} обновлён: {status}")


@admin_router.message(Command("set_payment_status"))
async def admin_set_payment_status(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Используй: /set_payment_status 15 paid")
        return
    order_id = safe_int(parts[1])
    payment_status = parts[2].strip()
    if payment_status not in PAYMENT_STATUSES:
        await message.answer("Допустимые статусы: pending, paid, failed, cancelled, refunded")
        return
    conn = get_db()
    conn.execute("UPDATE orders SET payment_status = ?, updated_at = ? WHERE id = ?", (payment_status, utc_now_iso(), order_id))
    conn.commit()
    conn.close()
    await message.answer(f"Статус оплаты заказа #{order_id} обновлён: {payment_status}")


@admin_router.message(F.text.in_([TEXTS['ru']['admin_products'], TEXTS['uz']['admin_products']]))
async def admin_products_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    rows = get_products(published_only=False)[:30]
    if not rows:
        await message.answer("Товаров пока нет.")
        return
    for row in rows:
        badges = ", ".join(stock_badges(row)) or "—"
        await message.answer(
            f"📦 <b>Товар #{row['id']}</b>\n"
            f"RU: {html.escape(row['title_ru'])}\n"
            f"UZ: {html.escape(row['title_uz'])}\n"
            f"Цена: {fmt_sum(row['price'])}\n"
            f"Старая цена: {fmt_sum(row['old_price']) if safe_int(row['old_price']) else '—'}\n"
            f"Остаток: {row['stock_qty']}\n"
            f"Категория: {html.escape(row['category_slug'])}\n"
            f"Бейджи: {html.escape(badges)}\n"
            f"Опубликован: {'Да' if safe_int(row['is_published']) else 'Нет'}\n\n"
            f"Команды:\n/set_price {row['id']} 399000\n/toggle_product {row['id']}"
        )


@admin_router.message(Command("set_price"))
async def admin_set_price(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Используй: /set_price 3 299000")
        return
    product_id = safe_int(parts[1])
    price = max(0, safe_int(parts[2]))
    conn = get_db()
    conn.execute("UPDATE products SET price = ?, updated_at = ? WHERE id = ?", (price, utc_now_iso(), product_id))
    conn.commit()
    conn.close()
    await message.answer(f"Цена товара #{product_id} обновлена: {fmt_sum(price)}")


@admin_router.message(Command("toggle_product"))
async def admin_toggle_product(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Используй: /toggle_product 3")
        return
    product_id = safe_int(parts[1])
    conn = get_db()
    row = conn.execute("SELECT is_published FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        await message.answer("Товар не найден.")
        return
    new_value = 0 if safe_int(row["is_published"]) else 1
    conn.execute("UPDATE products SET is_published = ?, updated_at = ? WHERE id = ?", (new_value, utc_now_iso(), product_id))
    conn.commit()
    conn.close()
    await message.answer(f"Товар #{product_id} теперь {'опубликован' if new_value else 'скрыт'}")


@admin_router.message(F.text.in_([TEXTS['ru']['admin_customers'], TEXTS['uz']['admin_customers']]))
async def admin_customers_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    conn = get_db()
    rows = conn.execute(
        "SELECT customer_phone, customer_name, COUNT(*) AS orders_count, SUM(total_amount) AS total_spent FROM orders GROUP BY customer_phone ORDER BY orders_count DESC, total_spent DESC LIMIT 30"
    ).fetchall()
    conn.close()
    if not rows:
        await message.answer("Клиентов пока нет.")
        return
    lines = ["👥 <b>Клиенты</b>"]
    for row in rows:
        lines.append(f"• {html.escape(row['customer_name'] or '—')} | {html.escape(row['customer_phone'] or '—')} | заказов: {row['orders_count']} | сумма: {fmt_sum(row['total_spent'] or 0)}")
    await message.answer("\n".join(lines))


@admin_router.message(Command("find_customer"))
async def admin_find_customer(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    phone = normalize_phone((message.text or "").replace("/find_customer", "", 1).strip())
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders WHERE customer_phone = ? ORDER BY id DESC LIMIT 20", (phone,)).fetchall()
    conn.close()
    if not rows:
        await message.answer("Клиент не найден.")
        return
    for row in rows:
        await message.answer(
            f"#{row['id']} | {html.escape(row['customer_name'])} | {fmt_sum(row['total_amount'])} | {status_label('ru', row['status'])}"
        )


@admin_router.message(F.text.in_([TEXTS['ru']['admin_reviews'], TEXTS['uz']['admin_reviews']]))
async def admin_reviews_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    conn = get_db()
    rows = conn.execute("SELECT * FROM reviews ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    if not rows:
        await message.answer("Отзывов пока нет.")
        return
    for row in rows:
        await message.answer(
            f"⭐ <b>Отзыв #{row['id']}</b>\n"
            f"Имя: {html.escape(row['customer_name'] or '—')}\n"
            f"Оценка: {'⭐' * max(1, min(5, safe_int(row['rating'], 5)))}\n"
            f"Статус: {'Опубликован' if safe_int(row['is_published']) else 'На модерации'}\n\n"
            f"{html.escape(row['text'] or '')}\n\n"
            f"Команда: /publish_review {row['id']}"
        )


@admin_router.message(Command("publish_review"))
async def admin_publish_review(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Используй: /publish_review 5")
        return
    review_id = safe_int(parts[1])
    conn = get_db()
    conn.execute("UPDATE reviews SET is_published = 1, updated_at = ? WHERE id = ?", (utc_now_iso(), review_id))
    conn.commit()
    conn.close()
    await message.answer(f"Отзыв #{review_id} опубликован.")


@admin_router.message(F.text.in_([TEXTS['ru']['admin_report'], TEXTS['uz']['admin_report']]))
async def admin_report_hint_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    await message.answer("Команда: /report 2026 03")


@admin_router.message(Command("report"))
async def admin_report_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Используй: /report 2026 03")
        return
    year = safe_int(parts[1])
    month = safe_int(parts[2])
    if year < 2024 or not (1 <= month <= 12):
        await message.answer("Неверный месяц или год.")
        return
    file_path, caption = monthly_report(month, year)
    await message.answer_document(BufferedInputFile(file_path.read_bytes(), filename=file_path.name), caption=caption)

# ============================================================
# WEB HTML
# ============================================================

def build_shop_html() -> str:
    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(SHOP_BRAND)}</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root {{
  --bg:#fbf6ef; --card:rgba(255,255,255,.74); --stroke:rgba(207,177,110,.25);
  --text:#211812; --muted:#7f6d60; --gold:#c6a463; --gold2:#9d772f; --dark:#18120e;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:Inter,Arial,sans-serif; color:var(--text); background:linear-gradient(135deg,#fff9f2,#fff3f7,#fff8ee); }}
.wrap {{ max-width:1180px; margin:0 auto; padding:18px 14px 30px; }}
.hero {{ padding:24px; border-radius:0 0 28px 28px; background:rgba(255,255,255,.62); backdrop-filter:blur(14px); box-shadow:0 18px 48px rgba(101,77,38,.08); }}
.brand {{ font-size:36px; font-weight:900; letter-spacing:.12em; text-transform:uppercase; background:linear-gradient(180deg,#f6e3ae,#c6a463,#9d772f); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.sub {{ color:var(--muted); margin-top:6px; }}
.layout {{ display:grid; grid-template-columns:1.6fr .95fr; gap:16px; margin-top:16px; }}
.panel {{ background:var(--card); border:1px solid rgba(255,255,255,.65); border-radius:24px; box-shadow:0 12px 30px rgba(89,67,34,.06); }}
.head {{ padding:18px 18px 8px; font-size:24px; font-weight:900; }}
.subhead {{ padding:0 18px 14px; color:var(--muted); font-size:14px; }}
.filters,.tools {{ display:flex; gap:8px; flex-wrap:wrap; padding:0 16px 12px; }}
.btn,.chip {{ border:none; cursor:pointer; border-radius:999px; padding:10px 14px; font-weight:700; }}
.chip {{ background:#fff; border:1px solid var(--stroke); }}
.chip.active {{ background:linear-gradient(180deg,var(--gold),var(--gold2)); color:#fff; }}
.search {{ width:100%; padding:12px 14px; border-radius:16px; border:1px solid var(--stroke); background:#fff; }}
.grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; padding:0 14px 16px; }}
.card {{ background:rgba(255,255,255,.78); border:1px solid rgba(255,255,255,.76); border-radius:22px; overflow:hidden; box-shadow:0 12px 24px rgba(89,67,34,.06); }}
.card-inner {{ padding:14px; display:flex; flex-direction:column; gap:12px; }}
.photo {{ aspect-ratio:1/1.08; border-radius:18px; overflow:hidden; background:linear-gradient(135deg,#fff,#f7eadf); }}
.photo img {{ width:100%; height:100%; object-fit:cover; display:block; }}
.badges {{ display:flex; gap:6px; flex-wrap:wrap; position:absolute; top:12px; left:12px; }}
.badge {{ padding:6px 10px; border-radius:999px; font-size:11px; font-weight:900; background:#111; color:#fff; }}
.photo-wrap {{ position:relative; }}
.title {{ font-size:17px; font-weight:900; line-height:1.3; }}
.desc {{ color:var(--muted); font-size:13px; line-height:1.45; min-height:38px; }}
.price-row {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
.price {{ font-size:21px; font-weight:900; }}
.old {{ color:#9a887c; text-decoration:line-through; font-size:13px; }}
.meta {{ font-size:12px; color:#6f6156; }}
.sizes {{ display:flex; gap:7px; flex-wrap:wrap; }}
.size-btn {{ border:none; cursor:pointer; padding:8px 12px; border-radius:999px; background:#fff; border:1px solid var(--stroke); font-weight:700; }}
.size-btn.active {{ background:linear-gradient(180deg,var(--gold),var(--gold2)); color:#fff; }}
.action-row {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
.black-btn,.gold-btn,.ghost-btn {{ border:none; cursor:pointer; border-radius:16px; padding:13px 14px; font-weight:800; }}
.black-btn {{ background:var(--dark); color:#fff; }}
.gold-btn {{ background:linear-gradient(180deg,var(--gold),var(--gold2)); color:#fff; }}
.ghost-btn {{ background:#fff; border:1px solid var(--stroke); }}
.cart-list {{ display:flex; flex-direction:column; gap:10px; padding:0 16px 16px; }}
.cart-item {{ background:#fff; border:1px solid rgba(207,177,110,.18); border-radius:18px; padding:12px; }}
.summary {{ padding:0 16px 16px; }}
.summary-row {{ display:flex; justify-content:space-between; margin-bottom:8px; }}
.notice {{ position:fixed; left:50%; bottom:18px; transform:translateX(-50%); background:#18120e; color:#fff; padding:12px 16px; border-radius:999px; opacity:0; transition:.2s; }}
.notice.show {{ opacity:1; }}
@media(max-width:940px) {{ .layout {{ grid-template-columns:1fr; }} }}
@media(max-width:640px) {{ .grid {{ grid-template-columns:1fr; }} .action-row {{ grid-template-columns:1fr; }} .brand {{ font-size:30px; }} }}
</style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <div class="brand">{html.escape(SHOP_BRAND)}</div>
    <div class="sub">Premium spring / Navruz style inside Telegram</div>
  </div>
  <div class="layout">
    <div class="panel">
      <div class="head">Каталог</div>
      <div class="subhead">Поиск, фильтры, размеры, избранное, статусы товара.</div>
      <div class="tools"><input id="searchInput" class="search" placeholder="Поиск по названию"></div>
      <div class="filters" id="filters"></div>
      <div class="grid" id="productGrid"></div>
    </div>
    <div class="panel">
      <div class="head">Корзина</div>
      <div class="subhead">Оформление продолжается в боте.</div>
      <div class="cart-list" id="cartList"></div>
      <div class="summary">
        <div class="summary-row"><span>Всего</span><b id="sumQty">0</b></div>
        <div class="summary-row"><span>Сумма</span><b id="sumAmount">0 сум</b></div>
        <button class="black-btn" style="width:100%;margin-top:8px" id="checkoutBtn">Оформить</button>
        <button class="ghost-btn" style="width:100%;margin-top:8px" id="clearBtn">Очистить корзину</button>
      </div>
    </div>
  </div>
</div>
<div class="notice" id="notice"></div>
<script>
const tg = window.Telegram?.WebApp || null;
if (tg) {{ tg.ready(); tg.expand(); }}
const params = new URLSearchParams(window.location.search);
const lang = params.get('lang') || 'ru';
const state = {{ products: [], filtered: [], cart: [], category: 'all', search: '' }};
const I18N = {{
  badgeMap: {{ new:'Новинка', hit:'Хит', sale:'Скидка', limited:'Limited', low_stock:'Скоро закончится' }},
  categories: [
    {{key:'all', title:'Все'}}, {{key:'new', title:'Новинки'}}, {{key:'hits', title:'Хиты'}},
    {{key:'sale', title:'Скидки'}}, {{key:'limited', title:'Limited'}}, {{key:'school', title:'Школа'}}, {{key:'casual', title:'Casual'}}
  ]
}};
function esc(v) {{ return String(v ?? '').replace(/[&<>"']/g, s => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}}[s])); }}
function fmtSum(v) {{ return Number(v||0).toLocaleString('ru-RU') + ' сум'; }}
function notice(text) {{ const n=document.getElementById('notice'); n.textContent=text; n.classList.add('show'); setTimeout(()=>n.classList.remove('show'),1800); }}
function sendData(payload) {{ if (!tg) return notice('Открой через Telegram'); tg.sendData(JSON.stringify(payload)); }}
async function loadProducts() {{ const r=await fetch(`/api/shop/products?lang=${{encodeURIComponent(lang)}}`); state.products=await r.json(); applyFilter(); buildFilters(); }}
async function loadCart() {{ const userId=tg?.initDataUnsafe?.user?.id; if (!userId) return; const r=await fetch(`/api/shop/cart?user_id=${{encodeURIComponent(userId)}}&lang=${{encodeURIComponent(lang)}}`); const data=await r.json(); state.cart=data.items||[]; renderCart(); }}
function buildFilters() {{ const box=document.getElementById('filters'); box.innerHTML=''; I18N.categories.forEach(cat=>{{ const b=document.createElement('button'); b.className='chip'+(state.category===cat.key?' active':''); b.textContent=cat.title; b.onclick=()=>{{ state.category=cat.key; applyFilter(); buildFilters(); }}; box.appendChild(b); }}); }}
function applyFilter() {{ const s=(state.search||'').toLowerCase().trim(); state.filtered=state.products.filter(p=>{{ const byCat=state.category==='all'||p.category_slug===state.category; const bySearch=!s||String(p.title||'').toLowerCase().includes(s); return byCat&&bySearch; }}); renderProducts(); }}
document.getElementById('searchInput').addEventListener('input', e=>{{ state.search=e.target.value||''; applyFilter(); }});
function badgeHtml(list) {{ return (list||[]).map(x=>`<span class="badge">${{esc(I18N.badgeMap[x]||x)}}</span>`).join(''); }}
function renderProducts() {{ const grid=document.getElementById('productGrid'); grid.innerHTML=''; if (!state.filtered.length) {{ grid.innerHTML='<div class="card"><div class="card-inner">Ничего не найдено</div></div>'; return; }} state.filtered.forEach(product=>{{ const sizes=Array.isArray(product.sizes_list)?product.sizes_list:[]; const card=document.createElement('div'); card.className='card'; card.innerHTML=`<div class="card-inner"><div class="photo-wrap"><div class="badges">${{badgeHtml(product.badges)}}</div><div class="photo">${{product.photo_url?`<img src="${{esc(product.photo_url)}}">`:'<div style="padding:24px;color:#8a7666">Без фото</div>'}}</div></div><div class="title">${{esc(product.title)}}</div><div class="desc">${{esc(product.description||'')}}</div><div class="price-row"><div class="price">${{fmtSum(product.price)}}</div>${{Number(product.old_price)>0?`<div class="old">${{fmtSum(product.old_price)}}</div>`:''}}</div><div class="meta">Остаток: ${{product.stock_qty}}</div><div class="sizes">${{sizes.map((s,i)=>`<button class="size-btn${{i===0?' active':''}}" data-size="${{esc(s)}}">${{esc(s)}}</button>`).join('')}}</div><div class="action-row"><button class="black-btn addBtn" ${{product.can_buy?'':'disabled'}}>В корзину</button><button class="gold-btn buyBtn" ${{product.can_buy?'':'disabled'}}>Купить сейчас</button></div></div>`; let activeSize=sizes[0]||''; card.querySelectorAll('.size-btn').forEach(btn=>btn.onclick=()=>{{ card.querySelectorAll('.size-btn').forEach(x=>x.classList.remove('active')); btn.classList.add('active'); activeSize=btn.dataset.size||''; }}); card.querySelector('.addBtn').onclick=()=>{{ sendData({{action:'add_to_cart',product_id:product.id,qty:1,size:activeSize}}); notice('Добавлено в корзину'); setTimeout(loadCart,400); }}; card.querySelector('.buyBtn').onclick=()=>{{ sendData({{action:'buy_now',product_id:product.id,qty:1,size:activeSize}}); notice('Переходим к оформлению'); setTimeout(loadCart,400); }}; grid.appendChild(card); }}); }}
function renderCart() {{ const box=document.getElementById('cartList'); const qtyEl=document.getElementById('sumQty'); const amountEl=document.getElementById('sumAmount'); box.innerHTML=''; let qty=0, amount=0; (state.cart||[]).forEach(item=>{{ qty+=Number(item.qty||0); amount+=Number(item.subtotal||0); const div=document.createElement('div'); div.className='cart-item'; div.innerHTML=`<div><b>${{esc(item.product_name)}}</b></div><div style="color:#7f6d60;font-size:13px;margin-top:4px">${{item.size?esc(item.size)+' | ':''}}${{item.qty}} × ${{fmtSum(item.price)}}</div><div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px"><b>${{fmtSum(item.subtotal)}}</b><button class="ghost-btn">Удалить</button></div>`; div.querySelector('button').onclick=()=>{{ sendData({{action:'remove_from_cart',cart_id:item.cart_id}}); notice('Удалено'); setTimeout(loadCart,400); }}; box.appendChild(div); }}); if (!state.cart.length) box.innerHTML='<div class="cart-item">Корзина пуста</div>'; qtyEl.textContent=String(qty); amountEl.textContent=fmtSum(amount); }}
document.getElementById('checkoutBtn').onclick=()=>sendData({{action:'checkout'}});
document.getElementById('clearBtn').onclick=()=>{{ sendData({{action:'clear_cart'}}); notice('Корзина очищена'); setTimeout(loadCart,400); }};
loadProducts(); loadCart();
</script>
</body>
</html>
"""

# ============================================================
# WEB ROUTES
# ============================================================

@web_router.get("/shop")
async def shop_page(_: web.Request) -> web.Response:
    return web.Response(text=build_shop_html(), content_type="text/html")


@web_router.get("/api/shop/products")
async def api_products(request: web.Request) -> web.Response:
    lang = ensure_lang(request.query.get("lang", DEFAULT_LANGUAGE))
    rows = get_products(published_only=True)
    result = []
    for row in rows:
        result.append(product_to_web_dict(row, lang, await file_url(row["photo_file_id"] or "")))
    return web.json_response(result)


@web_router.get("/api/shop/cart")
async def api_cart(request: web.Request) -> web.Response:
    user_id = safe_int(request.query.get("user_id"))
    lang = ensure_lang(request.query.get("lang", DEFAULT_LANGUAGE))
    return web.json_response(cart_api(user_id, lang))


@web_router.get("/api/shop/reviews")
async def api_reviews(_: web.Request) -> web.Response:
    conn = get_db()
    rows = conn.execute("SELECT * FROM reviews WHERE is_published = 1 ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    return web.json_response([
        {
            "id": row["id"],
            "customer_name": row["customer_name"],
            "rating": row["rating"],
            "text": row["text"],
        }
        for row in rows
    ])


@web_router.get("/health")
async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})

# ============================================================
# SIMPLE WEB ADMIN
# ============================================================

def admin_allowed(request: web.Request) -> bool:
    token = request.query.get("token", "").strip()
    return bool(ADMIN_PANEL_TOKEN) and token == ADMIN_PANEL_TOKEN


def admin_template(title: str, body: str) -> str:
    token = html.escape(ADMIN_PANEL_TOKEN)
    return f"""
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>{html.escape(title)}</title>
<style>
body{{margin:0;font-family:Arial,sans-serif;background:linear-gradient(135deg,#fff9f2,#fff3f7,#fff8ee);color:#231812}}
.wrap{{max-width:1240px;margin:0 auto;padding:20px}}
.top{{background:rgba(255,255,255,.78);border-radius:24px;padding:18px 20px;box-shadow:0 12px 28px rgba(89,67,34,.06)}}
.brand{{font-size:34px;font-weight:900;letter-spacing:.14em;background:linear-gradient(180deg,#f6e3ae,#c6a463,#9d772f);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.nav{{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}} .nav a{{text-decoration:none;color:#fff;background:linear-gradient(180deg,#c6a463,#9d772f);padding:10px 14px;border-radius:999px;font-weight:700}}
.card{{background:rgba(255,255,255,.82);border-radius:22px;padding:18px;margin-top:16px;box-shadow:0 12px 24px rgba(89,67,34,.05)}}
.stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}} .stat{{background:#fff;border-radius:16px;padding:14px;border:1px solid #eee4d2}} .label{{font-size:12px;color:#7a6d61}} .value{{font-size:26px;font-weight:900;margin-top:8px}}
table{{width:100%;border-collapse:collapse}} th,td{{padding:10px 8px;border-bottom:1px solid #eee4d2;text-align:left;vertical-align:top}} th{{background:#faf4ea}}
@media(max-width:900px){{.stats{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
</style></head><body><div class="wrap"><div class="top"><div class="brand">{html.escape(SHOP_BRAND)} ADMIN</div><div class="nav"><a href="/admin?token={token}">Dashboard</a><a href="/admin/orders?token={token}">Orders</a><a href="/admin/products?token={token}">Products</a><a href="/admin/reviews?token={token}">Reviews</a></div></div>{body}</div></body></html>
"""


@web_router.get("/admin")
async def admin_dashboard(request: web.Request) -> web.Response:
    if not admin_allowed(request):
        return web.Response(text="Access denied", status=403)
    stats = get_admin_stats()
    body = f"<div class='card'><div class='stats'><div class='stat'><div class='label'>Заказы</div><div class='value'>{stats['orders']}</div></div><div class='stat'><div class='label'>Пользователи</div><div class='value'>{stats['users']}</div></div><div class='stat'><div class='label'>Товары</div><div class='value'>{stats['products']}</div></div><div class='stat'><div class='label'>Мало на складе</div><div class='value'>{stats['low_stock']}</div></div></div></div>"
    return web.Response(text=admin_template("Dashboard", body), content_type="text/html")


@web_router.get("/admin/orders")
async def admin_orders_page(request: web.Request) -> web.Response:
    if not admin_allowed(request):
        return web.Response(text="Access denied", status=403)
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 200").fetchall()
    conn.close()
    tr = []
    for row in rows:
        tr.append(f"<tr><td>#{row['id']}</td><td>{html.escape(row['customer_name'])}<br>{html.escape(row['customer_phone'])}</td><td>{html.escape(row['city'])}</td><td>{delivery_label('ru', row['delivery_method'])}</td><td>{html.escape(row['payment_method'])}<br>{payment_status_label('ru', row['payment_status'])}</td><td>{fmt_sum(row['total_amount'])}</td><td>{status_label('ru', row['status'])}</td><td>{row['created_at']}</td></tr>")
    body = "<div class='card'><table><thead><tr><th>ID</th><th>Клиент</th><th>Город</th><th>Доставка</th><th>Оплата</th><th>Сумма</th><th>Статус</th><th>Дата</th></tr></thead><tbody>" + "".join(tr) + "</tbody></table></div>"
    return web.Response(text=admin_template("Orders", body), content_type="text/html")


@web_router.get("/admin/products")
async def admin_products_page(request: web.Request) -> web.Response:
    if not admin_allowed(request):
        return web.Response(text="Access denied", status=403)
    rows = get_products(published_only=False)
    tr = []
    for row in rows:
        tr.append(f"<tr><td>#{row['id']}</td><td>{html.escape(row['title_ru'])}</td><td>{html.escape(row['category_slug'])}</td><td>{fmt_sum(row['price'])}</td><td>{row['stock_qty']}</td><td>{'Да' if safe_int(row['is_published']) else 'Нет'}</td></tr>")
    body = "<div class='card'><table><thead><tr><th>ID</th><th>Товар</th><th>Категория</th><th>Цена</th><th>Остаток</th><th>Опубликован</th></tr></thead><tbody>" + "".join(tr) + "</tbody></table></div>"
    return web.Response(text=admin_template("Products", body), content_type="text/html")


@web_router.get("/admin/reviews")
async def admin_reviews_page(request: web.Request) -> web.Response:
    if not admin_allowed(request):
        return web.Response(text="Access denied", status=403)
    conn = get_db()
    rows = conn.execute("SELECT * FROM reviews ORDER BY id DESC LIMIT 200").fetchall()
    conn.close()
    body = "<div class='card'>" + "".join(
        f"<div style='padding:12px 0;border-bottom:1px solid #eee4d2'><b>#{row['id']}</b> | {html.escape(row['customer_name'])} | {'⭐' * max(1, min(5, safe_int(row['rating'], 5)))}<br>{html.escape(row['text'])}<br><small>{'Опубликован' if safe_int(row['is_published']) else 'Модерация'}</small></div>"
        for row in rows
    ) + "</div>"
    return web.Response(text=admin_template("Reviews", body), content_type="text/html")

# ============================================================
# SEED
# ============================================================

def seed_products_if_empty() -> None:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) AS c FROM products").fetchone()
    if safe_int(row["c"]) > 0:
        conn.close()
        return
    now = utc_now_iso()
    demo = [
        ("Классический школьный костюм", "Maktab klassik kostyumi", "Премиальная школьная классика.", "Premium maktab klassikasi.", "", "[]", "school", 289000, 329000, json.dumps(["110", "116", "122", "128"]), 15, 1, 1, 0, 0, 12, 10, now, now),
        ("Весенний casual комплект", "Bahorgi casual to'plam", "Мягкий повседневный комплект.", "Yumshoq kundalik to'plam.", "", "[]", "casual", 249000, 0, json.dumps(["116", "122", "128", "134"]), 9, 1, 0, 1, 0, 0, 20, now, now),
        ("Limited white collection", "Limit oq kolleksiya", "Лимитированная коллекция для premium дропа.", "Limit premium kolleksiya.", "", "[]", "limited", 359000, 399000, json.dumps(["122", "128", "134", "140"]), 3, 1, 1, 1, 1, 10, 30, now, now),
    ]
    conn.executemany(
        """
        INSERT INTO products (
            title_ru, title_uz, description_ru, description_uz, photo_file_id, gallery_json,
            category_slug, price, old_price, sizes_json, stock_qty, is_published,
            is_new, is_hit, is_limited, discount_percent, sort_order, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        demo,
    )
    conn.commit()
    conn.close()

# ============================================================
# FALLBACK / STARTUP
# ============================================================

@fallback_router.message()
async def fallback_handler(message: Message) -> None:
    await message.answer(t(message.from_user.id, "main_menu_hint"), reply_markup=user_main_menu(message.from_user.id))


def create_web_app() -> web.Application:
    app = web.Application()
    app.add_routes(web_router)
    return app


def register_routers() -> None:
    dp.include_router(user_router)
    dp.include_router(cart_router)
    dp.include_router(checkout_router)
    dp.include_router(orders_router)
    dp.include_router(reviews_router)
    dp.include_router(admin_router)
    dp.include_router(fallback_router)


async def stock_watch_loop() -> None:
    while True:
        try:
            await asyncio.sleep(3600)
            await notify_admins_low_stock()
        except Exception:
            logger.exception("stock_watch_loop failed")


async def main() -> None:
    init_db()
    seed_products_if_empty()
    register_routers()

    app = create_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Web server started on port %s", PORT)

    asyncio.create_task(stock_watch_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
