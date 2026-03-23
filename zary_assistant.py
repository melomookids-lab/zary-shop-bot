import os
import re
import json
import math
import html
import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timezone

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("zary_shop_bot")

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

CHANNEL_ID_RAW = os.getenv("CHANNEL_ID", "0").strip()
CHANNEL_ID = int(CHANNEL_ID_RAW) if CHANNEL_ID_RAW.lstrip("-").isdigit() else 0

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in ADMIN_IDS_RAW.split(",")
    if x.strip() and x.strip().lstrip("-").isdigit()
}

SUPPORTED_LANGS = ("ru", "uz")
CATEGORY_SLUGS = ("new", "hits", "sale", "limited", "school", "casual")
ORDER_STATUSES = ("new", "processing", "confirmed", "paid", "sent", "delivered", "cancelled")
PAYMENT_STATUSES = ("pending", "paid", "failed", "cancelled", "refunded")
SIZE_BY_AGE = {3: "98", 4: "104", 5: "110", 6: "116", 7: "122", 8: "128", 9: "134", 10: "140"}
SIZE_BY_HEIGHT = {98: "98", 104: "104", 110: "110", 116: "116", 122: "122", 128: "128", 134: "134", 140: "140", 146: "146"}

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not BASE_URL:
    logger.warning("BASE_URL is empty. WebApp button will not work correctly until you set it.")


# ============================================================
# BOT / ROUTERS
# ============================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

user_router = Router()
cart_router = Router()
checkout_router = Router()
reviews_router = Router()
orders_router = Router()
admin_router = Router()
fallback_router = Router()
web_router = web.RouteTableDef()


# ============================================================
# TEXTS
# ============================================================

TEXTS: dict[str, dict[str, str]] = {
    "ru": {
        "menu_shop": "🛍 Магазин",
        "menu_cart": "🛒 Корзина",
        "menu_orders": "📦 Мои заказы",
        "menu_reviews": "⭐ Отзывы",
        "menu_leave_review": "✍️ Оставить отзыв",
        "menu_size": "📏 Подбор размера",
        "menu_contacts": "📞 Контакты",
        "menu_lang": "🌐 Язык",
        "menu_admin": "🛠 Админ",
        "welcome": f"Добро пожаловать в <b>{SHOP_BRAND}</b>\n\nПремиальный магазин одежды внутри Telegram.",
        "main_menu_hint": "Выберите нужный раздел в меню ниже.",
        "choose_lang": "Выберите язык.",
        "lang_updated": "Язык обновлён.",
        "cart_empty": "Ваша корзина пуста.",
        "cart_item_added": "Товар добавлен в корзину.",
        "cart_same_item_merged": "Количество товара в корзине обновлено.",
        "cart_removed": "Позиция удалена из корзины.",
        "cart_cleared": "Корзина очищена.",
        "cart_bad_payload": "Не удалось обработать данные магазина.",
        "cart_item_not_found": "Товар не найден.",
        "cart_item_no_stock": "Товар временно недоступен.",
        "cart_size_required": "Нужно выбрать размер.",
        "cart_invalid_qty": "Некорректное количество.",
        "cart_empty_for_checkout": "Корзина пуста. Сначала добавьте товар.",
        "cart_title": "🛒 <b>Ваша корзина</b>",
        "cart_total_qty": "Всего товаров",
        "cart_total_amount": "Сумма",
        "cart_checkout": "Оформить заказ",
        "cart_clear": "Очистить корзину",
        "order_number": "Номер заказа",
        "order_date": "Дата",
        "order_status": "Статус заказа",
        "order_payment_method": "Способ оплаты",
        "order_payment_status": "Статус оплаты",
        "order_delivery_service": "Способ доставки",
        "order_total_amount": "Сумма",
        "order_items": "Товары",
        "my_orders_title": "📦 <b>Мои заказы</b>",
        "my_orders_empty": "У вас пока нет заказов.",
        "order_created_title": "✅ <b>Спасибо за покупку!</b>",
        "order_created_text": "Мы рады, что вы с нами. Носите с удовольствием и с любовью от ZARY & CO.",
        "order_links_text": "Чтобы не потерять нас, подпишитесь на наши каналы.",
        "checkout_intro": "Начинаем оформление заказа.",
        "checkout_name": "Введите имя получателя.",
        "checkout_phone": "Введите телефон в формате +998...",
        "checkout_delivery": "Выберите способ доставки.",
        "checkout_address_type": "Как указать адрес?",
        "checkout_city": "Введите город.",
        "checkout_address": "Введите адрес вручную.",
        "checkout_location": "Отправьте локацию кнопкой ниже.",
        "checkout_payment": "Выберите способ оплаты.",
        "checkout_comment": "Введите комментарий или нажмите «Пропустить».",
        "checkout_invalid_phone": "Телефон выглядит некорректно. Пример: +998901234567",
        "checkout_invalid_choice": "Выберите один из предложенных вариантов.",
        "checkout_need_location": "Нужно отправить именно локацию.",
        "checkout_summary": "🧾 <b>Проверка заказа</b>",
        "checkout_confirm_hint": "Напишите «Да» для подтверждения или «Нет» для отмены.",
        "checkout_cancelled": "Оформление заказа отменено.",
        "checkout_confirm_yes": "Да",
        "checkout_confirm_no": "Нет",
        "checkout_name_label": "Имя",
        "checkout_phone_label": "Телефон",
        "checkout_city_label": "Город",
        "checkout_delivery_label": "Доставка",
        "checkout_address_type_label": "Тип адреса",
        "checkout_address_label": "Адрес",
        "checkout_location_label": "Локация",
        "checkout_payment_label": "Оплата",
        "checkout_comment_label": "Комментарий",
        "checkout_total_label": "Сумма",
        "checkout_items_label": "Товары",
        "delivery_courier": "🚚 Курьер",
        "delivery_pickup": "🏪 Самовывоз / ПВЗ",
        "address_location": "📍 Отправить локацию",
        "address_manual": "✍️ Ввести адрес вручную",
        "payment_click": "Click",
        "payment_payme": "Payme",
        "payment_cash": "Наличными",
        "review_rating_ask": "Выберите оценку от 1 до 5.",
        "review_text_ask": "Напишите текст отзыва.",
        "review_sent": "Спасибо. Ваш отзыв отправлен на модерацию.",
        "review_bad_rating": "Нужно выбрать оценку от 1 до 5.",
        "reviews_title": "⭐ <b>Отзывы покупателей</b>",
        "reviews_empty": "Пока отзывов нет.",
        "cancel": "❌ Отмена",
        "skip": "Пропустить",
        "yes": "Да",
        "no": "Нет",
        "action_cancelled": "Действие отменено.",
        "size_intro": "Подбор размера работает по возрасту или росту.\n\nПримеры:\n• 5\n• 128",
        "size_result_age": "По возрасту рекомендуемый размер",
        "size_result_height": "По росту рекомендуемый размер",
        "size_not_found": "Не смог подобрать размер. Введите возраст 3–10 или рост 98–146.",
        "size_hint_extra": "Если сомневаетесь, лучше взять размер чуть больше.",
        "contacts_title": "📞 <b>Контакты</b>",
        "contacts_phone": "Телефон",
        "contacts_manager": "Telegram менеджера",
        "contacts_channel": "Telegram канал",
        "contacts_instagram": "Instagram",
        "contacts_youtube": "YouTube",
        "send_start_again": "Отправьте /start, чтобы открыть меню.",
        "not_admin": "Эта команда доступна только администратору.",
        "admin_title": "🛠 <b>Админ панель</b>",
        "admin_stats": "📊 Статистика",
        "admin_products": "📦 Товары",
        "admin_orders": "📋 Заказы",
        "admin_reviews": "⭐ Отзывы",
        "admin_back_to_user": "⬅️ В меню",
        "status_new": "Новый",
        "status_processing": "В обработке",
        "status_confirmed": "Подтверждён",
        "status_paid": "Оплачен",
        "status_sent": "Отправлен",
        "status_delivered": "Доставлен",
        "status_cancelled": "Отменён",
        "payment_status_pending": "Ожидает оплаты",
        "payment_status_paid": "Оплачен",
        "payment_status_failed": "Ошибка оплаты",
        "payment_status_cancelled": "Отменён",
        "payment_status_refunded": "Возврат",
    },
    "uz": {
        "menu_shop": "🛍 Do'kon",
        "menu_cart": "🛒 Savatcha",
        "menu_orders": "📦 Buyurtmalarim",
        "menu_reviews": "⭐ Sharhlar",
        "menu_leave_review": "✍️ Sharh qoldirish",
        "menu_size": "📏 O'lcham tanlash",
        "menu_contacts": "📞 Kontaktlar",
        "menu_lang": "🌐 Til",
        "menu_admin": "🛠 Admin",
        "welcome": f"<b>{SHOP_BRAND}</b> ga xush kelibsiz.\n\nTelegram ichidagi premium kiyim do'koni.",
        "main_menu_hint": "Quyidagi menyudan kerakli bo'limni tanlang.",
        "choose_lang": "Tilni tanlang.",
        "lang_updated": "Til yangilandi.",
        "cart_empty": "Savatchangiz bo'sh.",
        "cart_item_added": "Mahsulot savatchaga qo'shildi.",
        "cart_same_item_merged": "Savatchadagi mahsulot soni yangilandi.",
        "cart_removed": "Pozitsiya savatchadan o'chirildi.",
        "cart_cleared": "Savatcha tozalandi.",
        "cart_bad_payload": "Do'kon ma'lumotlarini qayta ishlab bo'lmadi.",
        "cart_item_not_found": "Mahsulot topilmadi.",
        "cart_item_no_stock": "Mahsulot vaqtincha mavjud emas.",
        "cart_size_required": "O'lcham tanlanishi kerak.",
        "cart_invalid_qty": "Noto'g'ri son.",
        "cart_empty_for_checkout": "Savatcha bo'sh. Avval mahsulot qo'shing.",
        "cart_title": "🛒 <b>Savatchangiz</b>",
        "cart_total_qty": "Jami mahsulot",
        "cart_total_amount": "Summa",
        "cart_checkout": "Buyurtma berish",
        "cart_clear": "Savatchani tozalash",
        "order_number": "Buyurtma raqami",
        "order_date": "Sana",
        "order_status": "Buyurtma holati",
        "order_payment_method": "To'lov usuli",
        "order_payment_status": "To'lov holati",
        "order_delivery_service": "Yetkazib berish",
        "order_total_amount": "Summa",
        "order_items": "Mahsulotlar",
        "my_orders_title": "📦 <b>Buyurtmalarim</b>",
        "my_orders_empty": "Sizda hozircha buyurtmalar yo'q.",
        "order_created_title": "✅ <b>Xaridingiz uchun rahmat!</b>",
        "order_created_text": "Siz biz bilan ekaningizdan xursandmiz. Mahsulotni mamnuniyat bilan kiying.",
        "order_links_text": "Bizni yo'qotib qo'ymaslik uchun kanallarimizga obuna bo'ling.",
        "checkout_intro": "Buyurtma rasmiylashtirishni boshlaymiz.",
        "checkout_name": "Qabul qiluvchi ismini kiriting.",
        "checkout_phone": "Telefon raqamini +998... ko'rinishida kiriting.",
        "checkout_delivery": "Yetkazib berish usulini tanlang.",
        "checkout_address_type": "Manzilni qanday kiritasiz?",
        "checkout_city": "Shaharni kiriting.",
        "checkout_address": "Manzilni qo'lda kiriting.",
        "checkout_location": "Quyidagi tugma orqali lokatsiya yuboring.",
        "checkout_payment": "To'lov usulini tanlang.",
        "checkout_comment": "Izoh yozing yoki «Propuстить» o'rniga tugmani bosing.",
        "checkout_invalid_phone": "Telefon noto'g'ri ko'rinadi. Misol: +998901234567",
        "checkout_invalid_choice": "Taklif qilingan variantlardan birini tanlang.",
        "checkout_need_location": "Aynan lokatsiya yuborilishi kerak.",
        "checkout_summary": "🧾 <b>Buyurtmani tekshirish</b>",
        "checkout_confirm_hint": "Tasdiqlash uchun «Ha», bekor qilish uchun «Yo'q» deb yozing.",
        "checkout_cancelled": "Buyurtma rasmiylashtirish bekor qilindi.",
        "checkout_confirm_yes": "Ha",
        "checkout_confirm_no": "Yo'q",
        "checkout_name_label": "Ism",
        "checkout_phone_label": "Telefon",
        "checkout_city_label": "Shahar",
        "checkout_delivery_label": "Yetkazib berish",
        "checkout_address_type_label": "Manzil turi",
        "checkout_address_label": "Manzil",
        "checkout_location_label": "Lokatsiya",
        "checkout_payment_label": "To'lov",
        "checkout_comment_label": "Izoh",
        "checkout_total_label": "Summa",
        "checkout_items_label": "Mahsulotlar",
        "delivery_courier": "🚚 Kuryer",
        "delivery_pickup": "🏪 Olib ketish / PVZ",
        "address_location": "📍 Lokatsiya yuborish",
        "address_manual": "✍️ Manzilni qo'lda kiritish",
        "payment_click": "Click",
        "payment_payme": "Payme",
        "payment_cash": "Naqd",
        "review_rating_ask": "1 dan 5 gacha bahoni tanlang.",
        "review_text_ask": "Sharh matnini yozing.",
        "review_sent": "Rahmat. Sharhingiz moderatsiyaga yuborildi.",
        "review_bad_rating": "1 dan 5 gacha bahoni tanlash kerak.",
        "reviews_title": "⭐ <b>Xaridorlar sharhlari</b>",
        "reviews_empty": "Hozircha sharhlar yo'q.",
        "cancel": "❌ Bekor qilish",
        "skip": "O'tkazib yuborish",
        "yes": "Ha",
        "no": "Yo'q",
        "action_cancelled": "Amal bekor qilindi.",
        "size_intro": "O'lcham tanlash yosh yoki bo'y bo'yicha ishlaydi.\n\nMisollar:\n• 5\n• 128",
        "size_result_age": "Yosh bo'yicha tavsiya etilgan o'lcham",
        "size_result_height": "Bo'y bo'yicha tavsiya etilgan o'lcham",
        "size_not_found": "O'lcham topilmadi. 3–10 yosh yoki 98–146 bo'yni kiriting.",
        "size_hint_extra": "Ikki o'lcham oralig'ida bo'lsa, kattaroq variantni oling.",
        "contacts_title": "📞 <b>Kontaktlar</b>",
        "contacts_phone": "Telefon",
        "contacts_manager": "Telegram menejer",
        "contacts_channel": "Telegram kanal",
        "contacts_instagram": "Instagram",
        "contacts_youtube": "YouTube",
        "send_start_again": "Menyuni ochish uchun /start yuboring.",
        "not_admin": "Bu bo'lim faqat administrator uchun.",
        "admin_title": "🛠 <b>Admin panel</b>",
        "admin_stats": "📊 Statistika",
        "admin_products": "📦 Mahsulotlar",
        "admin_orders": "📋 Buyurtmalar",
        "admin_reviews": "⭐ Sharhlar",
        "admin_back_to_user": "⬅️ Menyuga",
        "status_new": "Yangi",
        "status_processing": "Jarayonda",
        "status_confirmed": "Tasdiqlangan",
        "status_paid": "To'langan",
        "status_sent": "Yuborilgan",
        "status_delivered": "Yetkazilgan",
        "status_cancelled": "Bekor qilingan",
        "payment_status_pending": "To'lov kutilmoqda",
        "payment_status_paid": "To'langan",
        "payment_status_failed": "To'lov xatosi",
        "payment_status_cancelled": "Bekor qilingan",
        "payment_status_refunded": "Qaytarilgan",
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            lang TEXT NOT NULL DEFAULT 'ru',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shop_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            photo_file_id TEXT NOT NULL DEFAULT '',
            title_ru TEXT NOT NULL,
            title_uz TEXT NOT NULL,
            description_ru TEXT NOT NULL DEFAULT '',
            description_uz TEXT NOT NULL DEFAULT '',
            sizes TEXT NOT NULL DEFAULT '',
            category_slug TEXT NOT NULL DEFAULT 'casual',
            price INTEGER NOT NULL DEFAULT 0,
            old_price INTEGER NOT NULL DEFAULT 0,
            stock_qty INTEGER NOT NULL DEFAULT 0,
            is_published INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS carts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            price INTEGER NOT NULL DEFAULT 0,
            qty INTEGER NOT NULL DEFAULT 1,
            size TEXT NOT NULL DEFAULT '',
            photo_file_id TEXT NOT NULL DEFAULT '',
            added_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            customer_name TEXT,
            customer_phone TEXT,
            city TEXT,
            items TEXT NOT NULL,
            total_qty INTEGER NOT NULL DEFAULT 0,
            total_amount INTEGER NOT NULL DEFAULT 0,
            delivery_service TEXT,
            delivery_type TEXT,
            delivery_address TEXT,
            latitude REAL,
            longitude REAL,
            payment_method TEXT,
            payment_status TEXT NOT NULL DEFAULT 'pending',
            payment_provider_url TEXT NOT NULL DEFAULT '',
            comment TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            manager_seen INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            customer_name TEXT,
            rating INTEGER NOT NULL DEFAULT 5,
            text TEXT NOT NULL,
            is_published INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_pub ON shop_products(is_published)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_carts_user ON carts(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_pub ON reviews(is_published)")

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


def get_user_lang(user_id: int) -> str:
    conn = get_db()
    row = conn.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row["lang"] if row and row["lang"] in SUPPORTED_LANGS else DEFAULT_LANGUAGE


def t(user_id_or_lang: int | str, key: str) -> str:
    lang = get_user_lang(user_id_or_lang) if isinstance(user_id_or_lang, int) else ensure_lang(user_id_or_lang)
    return TEXTS.get(lang, TEXTS[DEFAULT_LANGUAGE]).get(key, key)


def fmt_sum(value: Any) -> str:
    return f"{safe_int(value):,}".replace(",", " ") + " сум"


def parse_sizes_string(sizes: str) -> list[str]:
    if not sizes:
        return []
    return [x.strip() for x in sizes.replace(";", ",").split(",") if x.strip()]


def sizes_to_string(sizes: list[str]) -> str:
    return ", ".join(x.strip() for x in sizes if x.strip())


def normalize_phone(phone: str) -> str:
    value = (phone or "").strip()
    value = value.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if value.startswith("998") and not value.startswith("+998"):
        value = "+" + value
    return value


def is_valid_phone(phone: str) -> bool:
    return bool(re.fullmatch(r"\+998\d{9}", normalize_phone(phone)))


def product_title_by_lang(row: sqlite3.Row | dict[str, Any], lang: str) -> str:
    return row["title_uz"] if ensure_lang(lang) == "uz" else row["title_ru"]


def product_desc_by_lang(row: sqlite3.Row | dict[str, Any], lang: str) -> str:
    return row["description_uz"] if ensure_lang(lang) == "uz" else row["description_ru"]


def stars_text(value: int) -> str:
    n = max(1, min(5, safe_int(value, 5)))
    return "⭐" * n


def status_label(lang_or_user: int | str, status: str) -> str:
    return t(lang_or_user, f"status_{status}")


def payment_status_label(lang_or_user: int | str, status: str) -> str:
    return t(lang_or_user, f"payment_status_{status}")


def payment_method_label(lang_or_user: int | str, method: str) -> str:
    mapping = {"click": "payment_click", "payme": "payment_payme", "cash": "payment_cash"}
    return t(lang_or_user, mapping.get(method, "payment_cash"))


def delivery_label(lang_or_user: int | str, service: str) -> str:
    mapping = {"courier": "delivery_courier", "pickup": "delivery_pickup"}
    return t(lang_or_user, mapping.get(service, "delivery_courier"))


def address_type_label(lang_or_user: int | str, value: str) -> str:
    mapping = {"location": "address_location", "manual": "address_manual"}
    return t(lang_or_user, mapping.get(value, "address_manual"))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def mask_username(username: Optional[str]) -> str:
    if not username:
        return "—"
    return "@" + username.strip().lstrip("@")


def upsert_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    now = utc_now_iso()
    conn = get_db()
    existing = conn.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,)).fetchone()
    lang = existing["lang"] if existing else DEFAULT_LANGUAGE
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
    existing = conn.execute("SELECT username, full_name, created_at FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE users SET lang = ?, updated_at = ? WHERE user_id = ?",
            (lang, now, user_id),
        )
    else:
        conn.execute(
            "INSERT INTO users (user_id, username, full_name, lang, created_at, updated_at) VALUES (?, '', '', ?, ?, ?)",
            (user_id, lang, now, now),
        )
    conn.commit()
    conn.close()


# ============================================================
# FSM
# ============================================================

class SizePickerStates(StatesGroup):
    waiting_for_value = State()


class ReviewStates(StatesGroup):
    rating = State()
    text = State()


class CheckoutStates(StatesGroup):
    customer_name = State()
    customer_phone = State()
    delivery_service = State()
    delivery_type = State()
    city = State()
    address = State()
    location = State()
    payment_method = State()
    comment = State()
    confirm = State()


# ============================================================
# KEYBOARDS
# ============================================================


def user_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=t(user_id, "menu_shop"), web_app=WebAppInfo(url=f"{BASE_URL}/shop?lang={get_user_lang(user_id)}"))],
        [KeyboardButton(text=t(user_id, "menu_cart"))],
        [KeyboardButton(text=t(user_id, "menu_orders"))],
        [KeyboardButton(text=t(user_id, "menu_reviews"))],
        [KeyboardButton(text=t(user_id, "menu_leave_review"))],
        [KeyboardButton(text=t(user_id, "menu_size"))],
        [KeyboardButton(text=t(user_id, "menu_contacts"))],
        [KeyboardButton(text=t(user_id, "menu_lang"))],
    ]
    if is_admin(user_id):
        rows.append([KeyboardButton(text=t(user_id, "menu_admin"))])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def cart_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(user_id, "cart_checkout"), callback_data="cart:checkout")],
            [InlineKeyboardButton(text=t(user_id, "cart_clear"), callback_data="cart:clear")],
        ]
    )


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Русский", callback_data="lang:set:ru")],
            [InlineKeyboardButton(text="O'zbekcha", callback_data="lang:set:uz")],
        ]
    )


def cancel_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t(user_id, "cancel"))]], resize_keyboard=True)


def review_rating_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")], [KeyboardButton(text="4"), KeyboardButton(text="5")], [KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def checkout_delivery_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "delivery_courier"))], [KeyboardButton(text=t(user_id, "delivery_pickup"))], [KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def checkout_address_type_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "address_location"), request_location=True)], [KeyboardButton(text=t(user_id, "address_manual"))], [KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def checkout_payment_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "payment_click"))], [KeyboardButton(text=t(user_id, "payment_payme"))], [KeyboardButton(text=t(user_id, "payment_cash"))], [KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def checkout_comment_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "skip"))], [KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def checkout_confirm_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "checkout_confirm_yes")), KeyboardButton(text=t(user_id, "checkout_confirm_no"))]],
        resize_keyboard=True,
    )


def admin_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "admin_stats"))],
            [KeyboardButton(text=t(user_id, "admin_orders"))],
            [KeyboardButton(text=t(user_id, "admin_products"))],
            [KeyboardButton(text=t(user_id, "admin_reviews"))],
            [KeyboardButton(text=t(user_id, "admin_back_to_user"))],
        ],
        resize_keyboard=True,
    )


def social_links_keyboard() -> Optional[InlineKeyboardMarkup]:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    if CHANNEL_LINK:
        row.append(InlineKeyboardButton(text="Telegram", url=CHANNEL_LINK))
    if INSTAGRAM_LINK:
        row.append(InlineKeyboardButton(text="Instagram", url=INSTAGRAM_LINK))
    if YOUTUBE_LINK:
        row.append(InlineKeyboardButton(text="YouTube", url=YOUTUBE_LINK))
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


# ============================================================
# PRODUCT / CART / ORDER HELPERS
# ============================================================


def get_product_by_id(product_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute("SELECT * FROM shop_products WHERE id = ?", (product_id,)).fetchone()
    conn.close()
    return row


def get_published_products() -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM shop_products WHERE is_published = 1 ORDER BY sort_order ASC, id DESC").fetchall()
    conn.close()
    return rows


def get_all_products(limit: int = 200) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM shop_products ORDER BY sort_order ASC, id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


def get_cart_rows(user_id: int) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM carts WHERE user_id = ? ORDER BY id ASC", (user_id,)).fetchall()
    conn.close()
    return rows


def get_cart_totals(user_id: int) -> tuple[int, int]:
    rows = get_cart_rows(user_id)
    qty = sum(safe_int(r["qty"]) for r in rows)
    amount = sum(safe_int(r["qty"]) * safe_int(r["price"]) for r in rows)
    return qty, amount


def clear_cart_for_user(user_id: int) -> None:
    conn = get_db()
    conn.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def remove_cart_item(cart_id: int, user_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM carts WHERE id = ? AND user_id = ?", (cart_id, user_id))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def add_to_cart(*, user_id: int, product_id: int, qty: int, size: str = "") -> tuple[bool, str]:
    product = get_product_by_id(product_id)
    if not product:
        return False, "cart_item_not_found"
    if safe_int(product["stock_qty"]) <= 0:
        return False, "cart_item_no_stock"
    if qty <= 0:
        return False, "cart_invalid_qty"

    allowed_sizes = parse_sizes_string(product["sizes"])
    size = (size or "").strip()
    if allowed_sizes and not size:
        return False, "cart_size_required"
    if allowed_sizes and size not in allowed_sizes:
        return False, "cart_size_required"

    conn = get_db()
    cur = conn.cursor()
    existing = cur.execute(
        "SELECT * FROM carts WHERE user_id = ? AND product_id = ? AND size = ? LIMIT 1",
        (user_id, product_id, size),
    ).fetchone()

    if existing:
        new_qty = safe_int(existing["qty"]) + qty
        cur.execute("UPDATE carts SET qty = ? WHERE id = ?", (new_qty, existing["id"]))
        conn.commit()
        conn.close()
        return True, "cart_same_item_merged"

    cur.execute(
        """
        INSERT INTO carts (user_id, product_id, product_name, price, qty, size, photo_file_id, added_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            product_id,
            product["title_ru"],
            safe_int(product["price"]),
            qty,
            size,
            product["photo_file_id"] or "",
            utc_now_iso(),
        ),
    )
    conn.commit()
    conn.close()
    return True, "cart_item_added"


def order_items_from_cart(user_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in get_cart_rows(user_id):
        price = safe_int(row["price"])
        qty = safe_int(row["qty"])
        items.append(
            {
                "cart_id": row["id"],
                "product_id": row["product_id"],
                "product_name": row["product_name"],
                "price": price,
                "qty": qty,
                "size": row["size"] or "",
                "photo_file_id": row["photo_file_id"] or "",
                "subtotal": price * qty,
            }
        )
    return items


def cart_items_api(user_id: int) -> dict[str, Any]:
    items = order_items_from_cart(user_id)
    qty, amount = get_cart_totals(user_id)
    return {"items": items, "total_qty": qty, "total_amount": amount}


def cart_text(user_id: int) -> str:
    rows = get_cart_rows(user_id)
    if not rows:
        return t(user_id, "cart_empty")
    total_qty, total_amount = get_cart_totals(user_id)
    lines = [t(user_id, "cart_title"), ""]
    for i, row in enumerate(rows, start=1):
        subtotal = safe_int(row["price"]) * safe_int(row["qty"])
        size_part = f" | {row['size']}" if row["size"] else ""
        lines.append(f"{i}. <b>{html.escape(row['product_name'])}</b>{size_part}\n   {fmt_sum(row['price'])} × {row['qty']} = <b>{fmt_sum(subtotal)}</b>")
    lines += ["", f"{t(user_id, 'cart_total_qty')}: <b>{total_qty}</b>", f"{t(user_id, 'cart_total_amount')}: <b>{fmt_sum(total_amount)}</b>"]
    return "\n".join(lines)


def create_order_from_checkout(*, user_id: int, username: str, checkout_data: dict[str, Any], source: str = "telegram") -> int:
    items = order_items_from_cart(user_id)
    total_qty, total_amount = get_cart_totals(user_id)
    now = utc_now_iso()
    payment_method = checkout_data.get("payment_method") or "cash"
    payment_url = ""
    if payment_method == "click":
        payment_url = f"{BASE_URL}/pay/click/pending"
    elif payment_method == "payme":
        payment_url = f"{BASE_URL}/pay/payme/pending"

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (
            user_id, username, customer_name, customer_phone, city, items,
            total_qty, total_amount, delivery_service, delivery_type,
            delivery_address, latitude, longitude, payment_method,
            payment_status, payment_provider_url, comment, status,
            manager_seen, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
        """,
        (
            user_id,
            username or "",
            checkout_data.get("customer_name") or "",
            checkout_data.get("customer_phone") or "",
            checkout_data.get("city") or "",
            json.dumps(items, ensure_ascii=False),
            total_qty,
            total_amount,
            checkout_data.get("delivery_service") or "courier",
            checkout_data.get("delivery_type") or "manual",
            checkout_data.get("delivery_address") or "",
            checkout_data.get("latitude"),
            checkout_data.get("longitude"),
            payment_method,
            "pending" if payment_method in {"click", "payme"} else "paid",
            payment_url,
            checkout_data.get("comment") or "",
            "new",
            now,
            now,
        ),
    )
    order_id = cur.lastrowid
    if payment_method == "click":
        payment_url = f"{BASE_URL}/pay/click/{order_id}"
    elif payment_method == "payme":
        payment_url = f"{BASE_URL}/pay/payme/{order_id}"
    cur.execute("UPDATE orders SET payment_provider_url = ?, updated_at = ? WHERE id = ?", (payment_url, utc_now_iso(), order_id))
    cur.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return order_id


def get_order_by_id(order_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    conn.close()
    return row


def get_orders_for_user(user_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit)).fetchall()
    conn.close()
    return rows


def render_order_items(items_json: str) -> str:
    try:
        items = json.loads(items_json or "[]")
    except Exception:
        items = []
    if not items:
        return "—"
    lines: list[str] = []
    for i, item in enumerate(items, start=1):
        name = item.get("product_name") or "—"
        qty = safe_int(item.get("qty"), 1)
        price = safe_int(item.get("price"), 0)
        subtotal = safe_int(item.get("subtotal"), price * qty)
        size = item.get("size") or ""
        size_part = f" | {size}" if size else ""
        lines.append(f"{i}. {html.escape(name)}{size_part} — {qty} × {fmt_sum(price)} = {fmt_sum(subtotal)}")
    return "\n".join(lines)


def build_checkout_summary(user_id: int, data: dict[str, Any]) -> str:
    items = order_items_from_cart(user_id)
    total_qty, total_amount = get_cart_totals(user_id)
    lines = [
        t(user_id, "checkout_summary"),
        "",
        f"<b>{t(user_id, 'checkout_name_label')}:</b> {html.escape(data.get('customer_name') or '—')}",
        f"<b>{t(user_id, 'checkout_phone_label')}:</b> {html.escape(data.get('customer_phone') or '—')}",
        f"<b>{t(user_id, 'checkout_city_label')}:</b> {html.escape(data.get('city') or '—')}",
        f"<b>{t(user_id, 'checkout_delivery_label')}:</b> {delivery_label(user_id, data.get('delivery_service') or '')}",
        f"<b>{t(user_id, 'checkout_address_type_label')}:</b> {address_type_label(user_id, data.get('delivery_type') or '')}",
        f"<b>{t(user_id, 'checkout_address_label')}:</b> {html.escape(data.get('delivery_address') or '—')}",
    ]
    if data.get("latitude") is not None and data.get("longitude") is not None:
        lines.append(f"<b>{t(user_id, 'checkout_location_label')}:</b> {data['latitude']}, {data['longitude']}")
    lines += [
        f"<b>{t(user_id, 'checkout_payment_label')}:</b> {payment_method_label(user_id, data.get('payment_method') or '')}",
        f"<b>{t(user_id, 'checkout_comment_label')}:</b> {html.escape(data.get('comment') or '—')}",
        "",
        f"<b>{t(user_id, 'checkout_items_label')}:</b>",
    ]
    if items:
        for i, item in enumerate(items, start=1):
            size_part = f" | {item['size']}" if item["size"] else ""
            lines.append(f"{i}. {html.escape(item['product_name'])}{size_part} — {item['qty']} × {fmt_sum(item['price'])} = <b>{fmt_sum(item['subtotal'])}</b>")
    else:
        lines.append("—")
    lines += ["", f"<b>{t(user_id, 'cart_total_qty')}:</b> {total_qty}", f"<b>{t(user_id, 'checkout_total_label')}:</b> {fmt_sum(total_amount)}", "", t(user_id, "checkout_confirm_hint")]
    return "\n".join(lines)


def get_published_reviews(limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM reviews WHERE is_published = 1 ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


def get_all_reviews(limit: int = 50) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM reviews ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


def create_review(*, user_id: int, username: str, customer_name: str, rating: int, text: str) -> int:
    conn = get_db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute(
        "INSERT INTO reviews (user_id, username, customer_name, rating, text, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
        (user_id, username or "", customer_name or "", max(1, min(5, safe_int(rating, 5))), text.strip(), now, now),
    )
    review_id = cur.lastrowid
    conn.commit()
    conn.close()
    return review_id


def get_basic_stats() -> dict[str, int]:
    conn = get_db()
    cur = conn.cursor()
    def scalar(query: str, params: tuple = ()) -> int:
        row = cur.execute(query, params).fetchone()
        return safe_int(row[0]) if row else 0
    stats = {
        "total_orders": scalar("SELECT COUNT(*) FROM orders"),
        "new": scalar("SELECT COUNT(*) FROM orders WHERE status = 'new'"),
        "processing": scalar("SELECT COUNT(*) FROM orders WHERE status = 'processing'"),
        "confirmed": scalar("SELECT COUNT(*) FROM orders WHERE status = 'confirmed'"),
        "paid": scalar("SELECT COUNT(*) FROM orders WHERE status = 'paid'"),
        "sent": scalar("SELECT COUNT(*) FROM orders WHERE status = 'sent'"),
        "delivered": scalar("SELECT COUNT(*) FROM orders WHERE status = 'delivered'"),
        "cancelled": scalar("SELECT COUNT(*) FROM orders WHERE status = 'cancelled'"),
        "users": scalar("SELECT COUNT(*) FROM users"),
        "products": scalar("SELECT COUNT(*) FROM shop_products"),
        "reviews": scalar("SELECT COUNT(*) FROM reviews WHERE is_published = 1"),
    }
    conn.close()
    return stats


def request_lang(request: web.Request) -> str:
    return ensure_lang(request.query.get("lang", DEFAULT_LANGUAGE))


def product_row_to_api_dict(row: sqlite3.Row, lang: str, photo_url: str = "") -> dict[str, Any]:
    return {
        "id": row["id"],
        "photo_file_id": row["photo_file_id"] or "",
        "photo_url": photo_url,
        "title": product_title_by_lang(row, lang),
        "description": product_desc_by_lang(row, lang),
        "sizes": row["sizes"] or "",
        "sizes_list": parse_sizes_string(row["sizes"] or ""),
        "category_slug": row["category_slug"] or "casual",
        "price": safe_int(row["price"]),
        "old_price": safe_int(row["old_price"]),
        "stock_qty": safe_int(row["stock_qty"]),
    }


def product_card_text(row: sqlite3.Row) -> str:
    return (
        f"🧷 <b>Товар #{row['id']}</b>\n\n"
        f"<b>RU:</b> {html.escape(row['title_ru'])}\n"
        f"<b>UZ:</b> {html.escape(row['title_uz'])}\n"
        f"<b>Цена:</b> {fmt_sum(row['price'])}\n"
        f"<b>Размеры:</b> {html.escape(row['sizes'] or '—')}\n"
        f"<b>Категория:</b> {html.escape(row['category_slug'])}\n"
        f"<b>Остаток:</b> {row['stock_qty']}\n"
        f"<b>Опубликован:</b> {'Да' if safe_int(row['is_published']) else 'Нет'}"
    )


def seed_demo_products_if_empty() -> None:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) FROM shop_products").fetchone()
    if safe_int(row[0]) > 0:
        conn.close()
        return
    now = utc_now_iso()
    demo = [
        ("", "Школьный классический костюм", "Maktab klassik kostyumi", "Премиальная школьная классика для детей.", "Bolalar uchun premium maktab klassikasi.", "110, 116, 122, 128", "school", 289000, 329000, 15, 1, 10, now, now),
        ("", "Повседневный мягкий комплект", "Kundalik yumshoq to'plam", "Удобный комплект на каждый день.", "Har kun uchun qulay to'plam.", "116, 122, 128, 134", "casual", 249000, 0, 12, 1, 20, now, now),
        ("", "Лимитированная белая коллекция", "Limitlangan oq kolleksiya", "Лимитированная премиальная коллекция.", "Limitlangan premium kolleksiya.", "122, 128, 134, 140", "limited", 359000, 399000, 7, 1, 30, now, now),
    ]
    conn.executemany(
        """
        INSERT INTO shop_products (
            photo_file_id, title_ru, title_uz, description_ru, description_uz,
            sizes, category_slug, price, old_price, stock_qty,
            is_published, sort_order, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        demo,
    )
    conn.commit()
    conn.close()


# ============================================================
# COMMON ASYNC HELPERS
# ============================================================

async def maybe_cancel_state(message: Message, state: FSMContext, admin_back: bool = False) -> bool:
    if (message.text or "").strip() == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=admin_main_menu(message.from_user.id) if admin_back else user_main_menu(message.from_user.id),
        )
        return True
    return False


async def get_file_url_by_file_id(file_id: str) -> str:
    if not file_id:
        return ""
    try:
        file = await bot.get_file(file_id)
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    except Exception:
        return ""


async def send_order_success_to_user(message: Message, order_id: int) -> None:
    text = (
        f"{t(message.from_user.id, 'order_created_title')}\n\n"
        f"{t(message.from_user.id, 'order_created_text')}\n\n"
        f"<b>{t(message.from_user.id, 'order_number')}:</b> #{order_id}\n\n"
        f"{t(message.from_user.id, 'order_links_text')}"
    )
    await message.answer(text, reply_markup=user_main_menu(message.from_user.id))
    kb = social_links_keyboard()
    if kb:
        await message.answer("Наши ссылки:", reply_markup=kb)


async def notify_admins_about_order(order_id: int) -> None:
    order = get_order_by_id(order_id)
    if not order:
        return
    text = (
        f"📦 <b>Новый заказ #{order['id']}</b>\n\n"
        f"<b>Имя:</b> {html.escape(order['customer_name'] or '—')}\n"
        f"<b>Телефон:</b> {html.escape(order['customer_phone'] or '—')}\n"
        f"<b>Город:</b> {html.escape(order['city'] or '—')}\n"
        f"<b>Сумма:</b> {fmt_sum(order['total_amount'])}\n"
        f"<b>Доставка:</b> {delivery_label('ru', order['delivery_service'] or '')}\n"
        f"<b>Оплата:</b> {payment_method_label('ru', order['payment_method'] or '')} / {payment_status_label('ru', order['payment_status'])}\n\n"
        f"<b>Товары:</b>\n{render_order_items(order['items'])}"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            logger.exception("Failed to notify admin %s about order", admin_id)


# ============================================================
# USER HANDLERS
# ============================================================

@user_router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await message.answer(t(message.from_user.id, "welcome"), reply_markup=user_main_menu(message.from_user.id))
    await message.answer(t(message.from_user.id, "main_menu_hint"))


@user_router.message(Command("menu"))
async def cmd_menu(message: Message) -> None:
    await message.answer(t(message.from_user.id, "main_menu_hint"), reply_markup=user_main_menu(message.from_user.id))


@user_router.message(F.text.in_([TEXTS['ru']['menu_lang'], TEXTS['uz']['menu_lang']]))
async def choose_language(message: Message) -> None:
    await message.answer(t(message.from_user.id, "choose_lang"), reply_markup=language_keyboard())


@user_router.callback_query(F.data.startswith("lang:set:"))
async def set_language_callback(callback: CallbackQuery) -> None:
    lang = callback.data.split(":")[-1]
    if lang in SUPPORTED_LANGS:
        set_user_lang(callback.from_user.id, lang)
        await callback.message.answer(t(lang, "lang_updated"), reply_markup=user_main_menu(callback.from_user.id))
    await callback.answer()


@user_router.message(F.text.in_([TEXTS['ru']['menu_contacts'], TEXTS['uz']['menu_contacts']]))
async def contacts_handler(message: Message) -> None:
    lang = get_user_lang(message.from_user.id)
    text = (
        f"{t(lang, 'contacts_title')}\n\n"
        f"<b>{t(lang, 'contacts_phone')}:</b> {html.escape(MANAGER_PHONE)}\n"
        f"<b>{t(lang, 'contacts_manager')}:</b> {html.escape(MANAGER_TG)}\n"
        f"<b>{t(lang, 'contacts_channel')}:</b> {html.escape(CHANNEL_LINK or '—')}\n"
        f"<b>{t(lang, 'contacts_instagram')}:</b> {html.escape(INSTAGRAM_LINK or '—')}\n"
        f"<b>{t(lang, 'contacts_youtube')}:</b> {html.escape(YOUTUBE_LINK or '—')}"
    )
    await message.answer(text, reply_markup=social_links_keyboard())


@user_router.message(F.text.in_([TEXTS['ru']['menu_size'], TEXTS['uz']['menu_size']]))
async def size_picker_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SizePickerStates.waiting_for_value)
    await message.answer(t(message.from_user.id, "size_intro"), reply_markup=cancel_keyboard(message.from_user.id))


@user_router.message(SizePickerStates.waiting_for_value)
async def size_picker_value(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(t(message.from_user.id, "action_cancelled"), reply_markup=user_main_menu(message.from_user.id))
        return
    if not text.isdigit():
        await message.answer(t(message.from_user.id, "size_not_found"))
        return
    value = int(text)
    if value in SIZE_BY_AGE:
        await state.clear()
        await message.answer(f"{t(message.from_user.id, 'size_result_age')}: <b>{SIZE_BY_AGE[value]}</b>\n\n{t(message.from_user.id, 'size_hint_extra')}", reply_markup=user_main_menu(message.from_user.id))
        return
    if value in SIZE_BY_HEIGHT:
        await state.clear()
        await message.answer(f"{t(message.from_user.id, 'size_result_height')}: <b>{SIZE_BY_HEIGHT[value]}</b>\n\n{t(message.from_user.id, 'size_hint_extra')}", reply_markup=user_main_menu(message.from_user.id))
        return
    await message.answer(t(message.from_user.id, "size_not_found"))


# ============================================================
# REVIEWS
# ============================================================

@reviews_router.message(F.text.in_([TEXTS['ru']['menu_reviews'], TEXTS['uz']['menu_reviews']]))
async def reviews_list_handler(message: Message) -> None:
    rows = get_published_reviews(limit=20)
    if not rows:
        await message.answer(t(message.from_user.id, "reviews_empty"))
        return
    await message.answer(t(message.from_user.id, "reviews_title"))
    for row in rows:
        await message.answer(f"{stars_text(row['rating'])}\n<b>{html.escape(row['customer_name'] or 'Клиент')}</b>\n{html.escape(row['text'])}")


@reviews_router.message(F.text.in_([TEXTS['ru']['menu_leave_review'], TEXTS['uz']['menu_leave_review']]))
async def review_start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ReviewStates.rating)
    await message.answer(t(message.from_user.id, "review_rating_ask"), reply_markup=review_rating_keyboard(message.from_user.id))


@reviews_router.message(ReviewStates.rating)
async def review_rating_handler(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(t(message.from_user.id, "action_cancelled"), reply_markup=user_main_menu(message.from_user.id))
        return
    if text not in {"1", "2", "3", "4", "5"}:
        await message.answer(t(message.from_user.id, "review_bad_rating"))
        return
    await state.update_data(rating=int(text))
    await state.set_state(ReviewStates.text)
    await message.answer(t(message.from_user.id, "review_text_ask"), reply_markup=cancel_keyboard(message.from_user.id))


@reviews_router.message(ReviewStates.text)
async def review_text_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer(t(message.from_user.id, "review_text_ask"))
        return
    data = await state.get_data()
    create_review(user_id=message.from_user.id, username=message.from_user.username or "", customer_name=message.from_user.full_name or "", rating=safe_int(data.get("rating"), 5), text=text)
    await state.clear()
    await message.answer(t(message.from_user.id, "review_sent"), reply_markup=user_main_menu(message.from_user.id))


# ============================================================
# CART / WEBAPP DATA
# ============================================================

@cart_router.message(F.text.in_([TEXTS['ru']['menu_cart'], TEXTS['uz']['menu_cart']]))
async def cart_view_handler(message: Message) -> None:
    rows = get_cart_rows(message.from_user.id)
    if not rows:
        await message.answer(t(message.from_user.id, "cart_empty"))
        return
    await message.answer(cart_text(message.from_user.id), reply_markup=cart_keyboard(message.from_user.id))


@cart_router.callback_query(F.data == "cart:clear")
async def cart_clear_callback(callback: CallbackQuery) -> None:
    clear_cart_for_user(callback.from_user.id)
    await callback.message.answer(t(callback.from_user.id, "cart_cleared"), reply_markup=user_main_menu(callback.from_user.id))
    await callback.answer()


@cart_router.message(F.web_app_data)
async def web_app_data_handler(message: Message, state: FSMContext) -> None:
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    raw = message.web_app_data.data if message.web_app_data else ""
    try:
        payload = json.loads(raw)
    except Exception:
        await message.answer(t(message.from_user.id, "cart_bad_payload"))
        return

    action = (payload.get("action") or "").strip()

    if action == "add_to_cart":
        ok, key = add_to_cart(
            user_id=message.from_user.id,
            product_id=safe_int(payload.get("product_id")),
            qty=max(1, safe_int(payload.get("qty"), 1)),
            size=(payload.get("size") or "").strip(),
        )
        await message.answer(t(message.from_user.id, key))
        return

    if action == "buy_now":
        ok, key = add_to_cart(
            user_id=message.from_user.id,
            product_id=safe_int(payload.get("product_id")),
            qty=max(1, safe_int(payload.get("qty"), 1)),
            size=(payload.get("size") or "").strip(),
        )
        await message.answer(t(message.from_user.id, key))
        if ok:
            await state.clear()
            await state.set_state(CheckoutStates.customer_name)
            await message.answer(
                f"{t(message.from_user.id, 'checkout_intro')}\n\n{t(message.from_user.id, 'checkout_name')}",
                reply_markup=cancel_keyboard(message.from_user.id),
            )
        return

    if action == "remove_from_cart":
        removed = remove_cart_item(safe_int(payload.get("cart_id")), message.from_user.id)
        await message.answer(t(message.from_user.id, "cart_removed") if removed else t(message.from_user.id, "cart_item_not_found"))
        return

    if action == "clear_cart":
        clear_cart_for_user(message.from_user.id)
        await message.answer(t(message.from_user.id, "cart_cleared"))
        return

    if action == "checkout":
        rows = get_cart_rows(message.from_user.id)
        if not rows:
            await message.answer(t(message.from_user.id, "cart_empty_for_checkout"))
            return
        await state.clear()
        await state.set_state(CheckoutStates.customer_name)
        await message.answer(
            f"{t(message.from_user.id, 'checkout_intro')}\n\n{t(message.from_user.id, 'checkout_name')}",
            reply_markup=cancel_keyboard(message.from_user.id),
        )
        return

    await message.answer(t(message.from_user.id, "cart_bad_payload"))


# ============================================================
# CHECKOUT
# ============================================================

@checkout_router.callback_query(F.data == "cart:checkout")
async def checkout_start_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not get_cart_rows(callback.from_user.id):
        await callback.message.answer(t(callback.from_user.id, "cart_empty_for_checkout"))
        await callback.answer()
        return
    await state.clear()
    await state.set_state(CheckoutStates.customer_name)
    await callback.message.answer(f"{t(callback.from_user.id, 'checkout_intro')}\n\n{t(callback.from_user.id, 'checkout_name')}", reply_markup=cancel_keyboard(callback.from_user.id))
    await callback.answer()


@checkout_router.message(Command("checkout"))
async def checkout_command(message: Message, state: FSMContext) -> None:
    if not get_cart_rows(message.from_user.id):
        await message.answer(t(message.from_user.id, "cart_empty_for_checkout"))
        return
    await state.clear()
    await state.set_state(CheckoutStates.customer_name)
    await message.answer(f"{t(message.from_user.id, 'checkout_intro')}\n\n{t(message.from_user.id, 'checkout_name')}", reply_markup=cancel_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.customer_name)
async def checkout_name_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    value = (message.text or "").strip()
    if not value:
        await message.answer(t(message.from_user.id, "checkout_name"))
        return
    await state.update_data(customer_name=value)
    await state.set_state(CheckoutStates.customer_phone)
    await message.answer(t(message.from_user.id, "checkout_phone"))


@checkout_router.message(CheckoutStates.customer_phone)
async def checkout_phone_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    phone = normalize_phone(message.text or "")
    if not is_valid_phone(phone):
        await message.answer(t(message.from_user.id, "checkout_invalid_phone"))
        return
    await state.update_data(customer_phone=phone)
    await state.set_state(CheckoutStates.delivery_service)
    await message.answer(t(message.from_user.id, "checkout_delivery"), reply_markup=checkout_delivery_keyboard(message.from_user.id))


def delivery_service_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {t(user_id, "delivery_courier"): "courier", t(user_id, "delivery_pickup"): "pickup"}
    return mapping.get((text or "").strip())


def address_type_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {t(user_id, "address_location"): "location", t(user_id, "address_manual"): "manual"}
    return mapping.get((text or "").strip())


def payment_method_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {t(user_id, "payment_click"): "click", t(user_id, "payment_payme"): "payme", t(user_id, "payment_cash"): "cash"}
    return mapping.get((text or "").strip())


@checkout_router.message(CheckoutStates.delivery_service)
async def checkout_delivery_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    delivery_service = delivery_service_from_label(message.from_user.id, message.text or "")
    if not delivery_service:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return
    await state.update_data(delivery_service=delivery_service)
    await state.set_state(CheckoutStates.delivery_type)
    await message.answer(t(message.from_user.id, "checkout_address_type"), reply_markup=checkout_address_type_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.delivery_type)
async def checkout_delivery_type_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    delivery_type = address_type_from_label(message.from_user.id, message.text or "")
    if not delivery_type:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return
    await state.update_data(delivery_type=delivery_type)
    await state.set_state(CheckoutStates.city)
    await message.answer(t(message.from_user.id, "checkout_city"), reply_markup=cancel_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.city)
async def checkout_city_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    city = (message.text or "").strip()
    if not city:
        await message.answer(t(message.from_user.id, "checkout_city"))
        return
    await state.update_data(city=city)
    data = await state.get_data()
    if data.get("delivery_type") == "location":
        await state.set_state(CheckoutStates.location)
        await message.answer(
            t(message.from_user.id, "checkout_location"),
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=t(message.from_user.id, "address_location"), request_location=True)], [KeyboardButton(text=t(message.from_user.id, "cancel"))]],
                resize_keyboard=True,
            ),
        )
        return
    await state.set_state(CheckoutStates.address)
    await message.answer(t(message.from_user.id, "checkout_address"), reply_markup=cancel_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.location)
async def checkout_location_handler(message: Message, state: FSMContext) -> None:
    if message.text and await maybe_cancel_state(message, state):
        return
    if not message.location:
        await message.answer(t(message.from_user.id, "checkout_need_location"))
        return
    await state.update_data(latitude=message.location.latitude, longitude=message.location.longitude, delivery_address="")
    await state.set_state(CheckoutStates.payment_method)
    await message.answer(t(message.from_user.id, "checkout_payment"), reply_markup=checkout_payment_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.address)
async def checkout_address_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    value = (message.text or "").strip()
    if not value:
        await message.answer(t(message.from_user.id, "checkout_address"))
        return
    await state.update_data(delivery_address=value)
    await state.set_state(CheckoutStates.payment_method)
    await message.answer(t(message.from_user.id, "checkout_payment"), reply_markup=checkout_payment_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.payment_method)
async def checkout_payment_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    value = payment_method_from_label(message.from_user.id, message.text or "")
    if not value:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return
    await state.update_data(payment_method=value)
    await state.set_state(CheckoutStates.comment)
    await message.answer(t(message.from_user.id, "checkout_comment"), reply_markup=checkout_comment_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.comment)
async def checkout_comment_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state):
        return
    text = (message.text or "").strip()
    if text == t(message.from_user.id, "skip"):
        text = ""
    await state.update_data(comment=text)
    data = await state.get_data()
    await state.set_state(CheckoutStates.confirm)
    await message.answer(build_checkout_summary(message.from_user.id, data), reply_markup=checkout_confirm_keyboard(message.from_user.id))


@checkout_router.message(CheckoutStates.confirm)
async def checkout_confirm_handler(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    yes_text = t(message.from_user.id, "checkout_confirm_yes")
    no_text = t(message.from_user.id, "checkout_confirm_no")
    if text == no_text:
        await state.clear()
        await message.answer(t(message.from_user.id, "checkout_cancelled"), reply_markup=user_main_menu(message.from_user.id))
        return
    if text != yes_text:
        await message.answer(t(message.from_user.id, "checkout_confirm_hint"))
        return
    if not get_cart_rows(message.from_user.id):
        await state.clear()
        await message.answer(t(message.from_user.id, "cart_empty_for_checkout"), reply_markup=user_main_menu(message.from_user.id))
        return
    data = await state.get_data()
    order_id = create_order_from_checkout(user_id=message.from_user.id, username=message.from_user.username or "", checkout_data=data, source="telegram")
    await state.clear()
    await send_order_success_to_user(message, order_id)
    await notify_admins_about_order(order_id)


# ============================================================
# ORDERS
# ============================================================

@orders_router.message(F.text.in_([TEXTS['ru']['menu_orders'], TEXTS['uz']['menu_orders']]))
async def my_orders_handler(message: Message) -> None:
    rows = get_orders_for_user(message.from_user.id, 15)
    if not rows:
        await message.answer(t(message.from_user.id, "my_orders_empty"))
        return
    await message.answer(t(message.from_user.id, "my_orders_title"))
    for row in rows:
        text = (
            f"<b>{t(message.from_user.id, 'order_number')}:</b> #{row['id']}\n"
            f"<b>{t(message.from_user.id, 'order_date')}:</b> {row['created_at']}\n"
            f"<b>{t(message.from_user.id, 'order_status')}:</b> {status_label(message.from_user.id, row['status'])}\n"
            f"<b>{t(message.from_user.id, 'order_payment_method')}:</b> {payment_method_label(message.from_user.id, row['payment_method'] or '')}\n"
            f"<b>{t(message.from_user.id, 'order_payment_status')}:</b> {payment_status_label(message.from_user.id, row['payment_status'])}\n"
            f"<b>{t(message.from_user.id, 'order_delivery_service')}:</b> {delivery_label(message.from_user.id, row['delivery_service'] or '')}\n"
            f"<b>{t(message.from_user.id, 'order_total_amount')}:</b> {fmt_sum(row['total_amount'])}\n"
            f"<b>{t(message.from_user.id, 'order_items')}:</b>\n{render_order_items(row['items'])}"
        )
        await message.answer(text)


# ============================================================
# ADMIN
# ============================================================

class AdminProductStates(StatesGroup):
    title_ru = State()
    title_uz = State()
    description_ru = State()
    description_uz = State()
    sizes = State()
    category_slug = State()
    price = State()
    old_price = State()
    stock_qty = State()
    photo = State()
    is_published = State()
    sort_order = State()


def admin_products_toolbar_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить новый товар", callback_data="admin_product:add")]
        ]
    )


def admin_product_row_keyboard(product_id: int, is_published: int) -> InlineKeyboardMarkup:
    pub_text = "🙈 Скрыть" if safe_int(is_published) else "👁 Опубликовать"
    pub_action = "unpublish" if safe_int(is_published) else "publish"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"admin_product:edit:{product_id}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_product:delete:{product_id}"),
            ],
            [
                InlineKeyboardButton(text=pub_text, callback_data=f"admin_product:{pub_action}:{product_id}")
            ],
        ]
    )


def admin_delete_confirm_keyboard(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_product:delete_yes:{product_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"admin_product:delete_no:{product_id}"),
            ]
        ]
    )


def normalize_optional_admin_text(value: str) -> str:
    value = (value or "").strip()
    if value in {"-", "—"}:
        return ""
    return value


def parse_admin_publish_value(value: str) -> Optional[int]:
    v = (value or "").strip().lower()
    if v in {"1", "да", "yes", "y", "опубликовать", "publish"}:
        return 1
    if v in {"0", "нет", "no", "n", "скрыть", "hide"}:
        return 0
    return None


def create_product_record(data: dict[str, Any]) -> int:
    conn = get_db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute(
        """
        INSERT INTO shop_products (
            photo_file_id,
            title_ru,
            title_uz,
            description_ru,
            description_uz,
            sizes,
            category_slug,
            price,
            old_price,
            stock_qty,
            is_published,
            sort_order,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data.get("photo_file_id", ""),
            data.get("title_ru", "").strip(),
            data.get("title_uz", "").strip(),
            data.get("description_ru", "").strip(),
            data.get("description_uz", "").strip(),
            data.get("sizes", "").strip(),
            data.get("category_slug", "casual").strip(),
            safe_int(data.get("price"), 0),
            safe_int(data.get("old_price"), 0),
            safe_int(data.get("stock_qty"), 0),
            1 if safe_int(data.get("is_published"), 0) else 0,
            safe_int(data.get("sort_order"), 100),
            now,
            now,
        ),
    )
    product_id = cur.lastrowid
    conn.commit()
    conn.close()
    return product_id


def update_product_record(product_id: int, data: dict[str, Any]) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE shop_products
        SET
            photo_file_id = ?,
            title_ru = ?,
            title_uz = ?,
            description_ru = ?,
            description_uz = ?,
            sizes = ?,
            category_slug = ?,
            price = ?,
            old_price = ?,
            stock_qty = ?,
            is_published = ?,
            sort_order = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            data.get("photo_file_id", ""),
            data.get("title_ru", "").strip(),
            data.get("title_uz", "").strip(),
            data.get("description_ru", "").strip(),
            data.get("description_uz", "").strip(),
            data.get("sizes", "").strip(),
            data.get("category_slug", "casual").strip(),
            safe_int(data.get("price"), 0),
            safe_int(data.get("old_price"), 0),
            safe_int(data.get("stock_qty"), 0),
            1 if safe_int(data.get("is_published"), 0) else 0,
            safe_int(data.get("sort_order"), 100),
            utc_now_iso(),
            product_id,
        ),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def delete_product_record(product_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM shop_products WHERE id = ?", (product_id,))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def set_product_published(product_id: int, value: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE shop_products SET is_published = ?, updated_at = ? WHERE id = ?",
        (1 if safe_int(value) else 0, utc_now_iso(), product_id),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def admin_edit_intro_text(row: sqlite3.Row) -> str:
    return (
        f"✏️ <b>Редактирование товара #{row['id']}</b>\n\n"
        f"<b>Текущее название RU:</b> {html.escape(row['title_ru'])}\n"
        f"<b>Текущее название UZ:</b> {html.escape(row['title_uz'])}\n"
        f"<b>Цена:</b> {fmt_sum(row['price'])}\n"
        f"<b>Размеры:</b> {html.escape(row['sizes'] or '—')}\n"
        f"<b>Категория:</b> {html.escape(row['category_slug'])}\n"
        f"<b>Остаток:</b> {row['stock_qty']}\n"
        f"<b>Публикация:</b> {'Да' if safe_int(row['is_published']) else 'Нет'}\n\n"
        f"Сейчас начнётся полное редактирование товара по всем полям."
    )


def build_product_payload_from_state(data: dict[str, Any]) -> dict[str, Any]:
    sizes_raw = normalize_optional_admin_text(data.get("sizes", ""))
    sizes_value = sizes_to_string(parse_sizes_string(sizes_raw)) if sizes_raw else ""

    return {
        "photo_file_id": data.get("photo_file_id", ""),
        "title_ru": (data.get("title_ru") or "").strip(),
        "title_uz": (data.get("title_uz") or "").strip(),
        "description_ru": normalize_optional_admin_text(data.get("description_ru", "")),
        "description_uz": normalize_optional_admin_text(data.get("description_uz", "")),
        "sizes": sizes_value,
        "category_slug": (data.get("category_slug") or "casual").strip(),
        "price": safe_int(data.get("price"), 0),
        "old_price": safe_int(data.get("old_price"), 0),
        "stock_qty": safe_int(data.get("stock_qty"), 0),
        "is_published": 1 if safe_int(data.get("is_published"), 0) else 0,
        "sort_order": safe_int(data.get("sort_order"), 100),
    }


@admin_router.message(F.text.in_([TEXTS['ru']['menu_admin'], TEXTS['uz']['menu_admin']]))
async def admin_menu_open(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return
    await message.answer(t(message.from_user.id, "admin_title"), reply_markup=admin_main_menu(message.from_user.id))


@admin_router.message(F.text.in_([TEXTS['ru']['admin_back_to_user'], TEXTS['uz']['admin_back_to_user']]))
async def admin_back_to_user_menu(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(t(message.from_user.id, "main_menu_hint"), reply_markup=user_main_menu(message.from_user.id))


@admin_router.message(F.text.in_([TEXTS['ru']['admin_stats'], TEXTS['uz']['admin_stats']]))
async def admin_stats_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    stats = get_basic_stats()
    await message.answer(
        f"📊 <b>{t(message.from_user.id, 'admin_stats')}</b>\n\n"
        f"Orders: <b>{stats['total_orders']}</b>\n"
        f"Users: <b>{stats['users']}</b>\n"
        f"Products: <b>{stats['products']}</b>\n"
        f"Published reviews: <b>{stats['reviews']}</b>\n"
        f"New: <b>{stats['new']}</b>\n"
        f"Processing: <b>{stats['processing']}</b>\n"
        f"Confirmed: <b>{stats['confirmed']}</b>\n"
        f"Paid: <b>{stats['paid']}</b>\n"
        f"Sent: <b>{stats['sent']}</b>\n"
        f"Delivered: <b>{stats['delivered']}</b>\n"
        f"Cancelled: <b>{stats['cancelled']}</b>"
    )


@admin_router.message(F.text.in_([TEXTS['ru']['admin_orders'], TEXTS['uz']['admin_orders']]))
async def admin_orders_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    conn = get_db()
    rows = conn.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 20").fetchall()
    conn.execute("UPDATE orders SET manager_seen = 1 WHERE manager_seen = 0")
    conn.commit()
    conn.close()

    if not rows:
        await message.answer("Пока заказов нет.")
        return

    for row in rows:
        await message.answer(
            f"📦 <b>Заказ #{row['id']}</b>\n"
            f"{html.escape(row['customer_name'] or '—')}\n"
            f"{fmt_sum(row['total_amount'])}\n"
            f"{status_label('ru', row['status'])}\n\n"
            f"{render_order_items(row['items'])}"
        )


@admin_router.message(F.text.in_([TEXTS['ru']['admin_products'], TEXTS['uz']['admin_products']]))
async def admin_products_handler(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    await state.clear()

    rows = get_all_products(limit=50)
    await message.answer(
        "📦 <b>Управление товарами</b>\n\n"
        "Здесь ты можешь:\n"
        "• добавить новый товар\n"
        "• полностью редактировать товар\n"
        "• удалить товар\n"
        "• скрыть или опубликовать товар",
        reply_markup=admin_products_toolbar_keyboard(),
    )

    if not rows:
        await message.answer("Товаров пока нет. Нажми «Добавить новый товар».")
        return

    for row in rows:
        await message.answer(
            product_card_text(row),
            reply_markup=admin_product_row_keyboard(row["id"], row["is_published"]),
        )


@admin_router.callback_query(F.data == "admin_product:add")
async def admin_product_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(mode="create", current_product={})
    await state.set_state(AdminProductStates.title_ru)

    await callback.message.answer(
        "➕ <b>Создание нового товара</b>\n\n"
        "1/11. Введите название товара на русском."
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin_product:edit:"))
async def admin_product_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    product_id = safe_int(callback.data.split(":")[-1])
    row = get_product_by_id(product_id)
    if not row:
        await callback.answer("Товар не найден", show_alert=True)
        return

    await state.clear()
    await state.update_data(
        mode="edit",
        product_id=product_id,
        current_product=dict(row),
    )
    await state.set_state(AdminProductStates.title_ru)

    await callback.message.answer(admin_edit_intro_text(row))
    await callback.message.answer("1/11. Введите новое название товара на русском.")
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin_product:delete_yes:"))
async def admin_product_delete_yes(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    product_id = safe_int(callback.data.split(":")[-1])
    ok = delete_product_record(product_id)
    if ok:
        await callback.message.answer(f"🗑 Товар #{product_id} удалён.")
    else:
        await callback.message.answer("Товар не найден.")
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin_product:delete_no:"))
async def admin_product_delete_no(callback: CallbackQuery) -> None:
    await callback.message.answer("Удаление отменено.")
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin_product:delete:"))
async def admin_product_delete_ask(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    product_id = safe_int(callback.data.split(":")[-1])
    row = get_product_by_id(product_id)
    if not row:
        await callback.answer("Товар не найден", show_alert=True)
        return

    await callback.message.answer(
        f"Ты точно хочешь удалить товар #{product_id}?\n\n"
        f"<b>{html.escape(row['title_ru'])}</b>",
        reply_markup=admin_delete_confirm_keyboard(product_id),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin_product:publish:"))
async def admin_product_publish(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    product_id = safe_int(callback.data.split(":")[-1])
    ok = set_product_published(product_id, 1)
    await callback.message.answer(f"Товар #{product_id} опубликован." if ok else "Товар не найден.")
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin_product:unpublish:"))
async def admin_product_unpublish(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    product_id = safe_int(callback.data.split(":")[-1])
    ok = set_product_published(product_id, 0)
    await callback.message.answer(f"Товар #{product_id} скрыт." if ok else "Товар не найден.")
    await callback.answer()


@admin_router.message(AdminProductStates.title_ru)
async def admin_product_title_ru_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = (message.text or "").strip()
    if not value:
        await message.answer("Название RU не может быть пустым. Введи название товара на русском.")
        return

    await state.update_data(title_ru=value)
    await state.set_state(AdminProductStates.title_uz)
    await message.answer("2/11. Введите название товара на узбекском.")


@admin_router.message(AdminProductStates.title_uz)
async def admin_product_title_uz_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = (message.text or "").strip()
    if not value:
        await message.answer("Название UZ не может быть пустым. Введи название товара на узбекском.")
        return

    await state.update_data(title_uz=value)
    await state.set_state(AdminProductStates.description_ru)
    await message.answer("3/11. Введите описание на русском или отправьте '-' если пусто.")


@admin_router.message(AdminProductStates.description_ru)
async def admin_product_description_ru_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = normalize_optional_admin_text(message.text or "")
    await state.update_data(description_ru=value)
    await state.set_state(AdminProductStates.description_uz)
    await message.answer("4/11. Введите описание на узбекском или отправьте '-' если пусто.")


@admin_router.message(AdminProductStates.description_uz)
async def admin_product_description_uz_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = normalize_optional_admin_text(message.text or "")
    await state.update_data(description_uz=value)
    await state.set_state(AdminProductStates.sizes)
    await message.answer(
        "5/11. Введите размеры через запятую.\n\n"
        "Пример:\n"
        "<code>110, 116, 122, 128</code>\n\n"
        "Если размеров нет — отправьте '-'."
    )


@admin_router.message(AdminProductStates.sizes)
async def admin_product_sizes_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = normalize_optional_admin_text(message.text or "")
    await state.update_data(sizes=value)
    await state.set_state(AdminProductStates.category_slug)
    await message.answer(
        "6/11. Введите категорию.\n\n"
        f"Допустимые значения:\n<code>{', '.join(CATEGORY_SLUGS)}</code>"
    )


@admin_router.message(AdminProductStates.category_slug)
async def admin_product_category_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = (message.text or "").strip().lower()
    if value not in CATEGORY_SLUGS:
        await message.answer(f"Неверная категория.\nДопустимо только: <code>{', '.join(CATEGORY_SLUGS)}</code>")
        return

    await state.update_data(category_slug=value)
    await state.set_state(AdminProductStates.price)
    await message.answer("7/11. Введите цену товара числом. Пример: <code>289000</code>")


@admin_router.message(AdminProductStates.price)
async def admin_product_price_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = safe_int(message.text, -1)
    if value < 0:
        await message.answer("Цена должна быть числом 0 или больше. Пример: <code>289000</code>")
        return

    await state.update_data(price=value)
    await state.set_state(AdminProductStates.old_price)
    await message.answer("8/11. Введите старую цену числом. Если старой цены нет — отправьте <code>0</code>")


@admin_router.message(AdminProductStates.old_price)
async def admin_product_old_price_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = safe_int(message.text, -1)
    if value < 0:
        await message.answer("Старая цена должна быть числом 0 или больше.")
        return

    await state.update_data(old_price=value)
    await state.set_state(AdminProductStates.stock_qty)
    await message.answer("9/11. Введите остаток на складе. Пример: <code>15</code>")


@admin_router.message(AdminProductStates.stock_qty)
async def admin_product_stock_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = safe_int(message.text, -1)
    if value < 0:
        await message.answer("Остаток должен быть числом 0 или больше.")
        return

    await state.update_data(stock_qty=value)
    await state.set_state(AdminProductStates.photo)

    data = await state.get_data()
    mode = data.get("mode", "create")
    if mode == "edit":
        await message.answer(
            "10/11. Отправьте новое фото товара.\n\n"
            "Либо отправьте:\n"
            "• <code>skip</code> — оставить текущее фото\n"
            "• <code>-</code> — удалить фото"
        )
    else:
        await message.answer(
            "10/11. Отправьте фото товара.\n\n"
            "Если фото пока нет — отправьте <code>-</code>"
        )


@admin_router.message(AdminProductStates.photo)
async def admin_product_photo_handler(message: Message, state: FSMContext) -> None:
    if message.text and await maybe_cancel_state(message, state, admin_back=True):
        return

    data = await state.get_data()
    mode = data.get("mode", "create")
    current_product = data.get("current_product") or {}
    photo_file_id = ""

    if message.photo:
        photo_file_id = message.photo[-1].file_id
    else:
        text = (message.text or "").strip().lower()
        if mode == "edit" and text == "skip":
            photo_file_id = current_product.get("photo_file_id", "") or ""
        elif text in {"-", "—"}:
            photo_file_id = ""
        else:
            if mode == "edit":
                await message.answer("Отправь фото, либо <code>skip</code> чтобы оставить текущее, либо <code>-</code> чтобы удалить.")
            else:
                await message.answer("Отправь фото товара или <code>-</code>, если фото пока нет.")
            return

    await state.update_data(photo_file_id=photo_file_id)
    await state.set_state(AdminProductStates.is_published)
    await message.answer("11/11. Публикация товара: отправьте <code>1</code> для публикации или <code>0</code> чтобы скрыть.")


@admin_router.message(AdminProductStates.is_published)
async def admin_product_is_published_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = parse_admin_publish_value(message.text or "")
    if value is None:
        await message.answer("Отправь <code>1</code> чтобы опубликовать или <code>0</code> чтобы скрыть товар.")
        return

    await state.update_data(is_published=value)
    await state.set_state(AdminProductStates.sort_order)
    await message.answer("Финал. Введите sort_order. Пример: <code>10</code>")


@admin_router.message(AdminProductStates.sort_order)
async def admin_product_sort_order_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, admin_back=True):
        return

    value = safe_int(message.text, -1)
    if value < 0:
        await message.answer("sort_order должен быть числом 0 или больше.")
        return

    await state.update_data(sort_order=value)
    data = await state.get_data()
    payload = build_product_payload_from_state(data)

    if data.get("mode") == "edit":
        product_id = safe_int(data.get("product_id"))
        ok = update_product_record(product_id, payload)
        await state.clear()

        if not ok:
            await message.answer("Не удалось обновить товар.", reply_markup=admin_main_menu(message.from_user.id))
            return

        row = get_product_by_id(product_id)
        await message.answer(f"✅ Товар #{product_id} обновлён.", reply_markup=admin_main_menu(message.from_user.id))
        if row:
            await message.answer(product_card_text(row), reply_markup=admin_product_row_keyboard(row["id"], row["is_published"]))
        return

    product_id = create_product_record(payload)
    await state.clear()

    row = get_product_by_id(product_id)
    await message.answer(f"✅ Новый товар #{product_id} создан.", reply_markup=admin_main_menu(message.from_user.id))
    if row:
        await message.answer(product_card_text(row), reply_markup=admin_product_row_keyboard(row["id"], row["is_published"]))


@admin_router.message(F.text.in_([TEXTS['ru']['admin_reviews'], TEXTS['uz']['admin_reviews']]))
async def admin_reviews_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_all_reviews(limit=30)
    if not rows:
        await message.answer("Отзывов пока нет.")
        return

    for row in rows:
        await message.answer(
            f"⭐ <b>Отзыв #{row['id']}</b>\n\n"
            f"Имя: {html.escape(row['customer_name'] or '—')}\n"
            f"Username: {mask_username(row['username'])}\n"
            f"Оценка: {stars_text(row['rating'])}\n"
            f"Статус: {'Опубликован' if safe_int(row['is_published']) else 'На модерации'}\n\n"
            f"{html.escape(row['text'])}"
        )


@admin_router.message(Command("publish_review"))
async def admin_publish_review_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Используй: /publish_review 5")
        return

    review_id = int(parts[1])
    conn = get_db()
    conn.execute("UPDATE reviews SET is_published = 1, updated_at = ? WHERE id = ?", (utc_now_iso(), review_id))
    conn.commit()
    conn.close()
    await message.answer(f"Отзыв #{review_id} опубликован.")

# ============================================================
# FALLBACK
# ============================================================

@fallback_router.message()
async def fallback_handler(message: Message) -> None:
    await message.answer(t(message.from_user.id, "send_start_again"), reply_markup=user_main_menu(message.from_user.id))


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
  --bg-1: #fffaf3;
  --bg-2: #fff5ea;
  --bg-3: #fff9f5;
  --glass: rgba(255,255,255,.62);
  --glass-strong: rgba(255,255,255,.80);
  --line: rgba(68, 120, 96, .16);
  --text: #241b14;
  --muted: #7f7167;

  --brand: #1f4b3a;
  --brand-dark: #163629;
  --brand-mid: #2f6a53;
  --brand-soft: #dfeee8;
  --brand-soft-2: #edf7f2;

  --shadow: 0 18px 50px rgba(70, 86, 73, .10);
  --shadow-soft: 0 10px 28px rgba(70, 86, 73, .07);

  --radius-xl: 28px;
  --radius-lg: 22px;
  --radius-md: 16px;
}}

* {{
  box-sizing: border-box;
}}

html, body {{
  min-height: 100%;
}}

body {{
  margin: 0;
  color: var(--text);
  font-family: Inter, Arial, Helvetica, sans-serif;
  background:
    radial-gradient(circle at 12% 12%, rgba(255,228,170,.22), transparent 22%),
    radial-gradient(circle at 86% 20%, rgba(196,222,210,.20), transparent 22%),
    radial-gradient(circle at 78% 84%, rgba(232,214,255,.12), transparent 18%),
    linear-gradient(135deg, var(--bg-1) 0%, var(--bg-2) 48%, var(--bg-3) 100%);
  overflow-x: hidden;
}}

body::before {{
  content: "";
  position: fixed;
  inset: -15%;
  background:
    radial-gradient(circle at 30% 35%, rgba(255,255,255,.78), transparent 16%),
    radial-gradient(circle at 75% 22%, rgba(239,248,243,.30), transparent 17%);
  filter: blur(30px);
  pointer-events: none;
  z-index: 0;
}}

.flower-field {{
  position: fixed;
  inset: 0;
  pointer-events: none;
  overflow: hidden;
  z-index: 1;
}}

.flower {{
  position: absolute;
  top: -12vh;
  will-change: transform, opacity;
  animation-name: daisyFall;
  animation-timing-function: linear;
  animation-iteration-count: infinite;
  filter: drop-shadow(0 6px 12px rgba(0,0,0,.08));
  opacity: .92;
}}

.flower svg {{
  display: block;
  width: 100%;
  height: 100%;
}}

@keyframes daisyFall {{
  0% {{
    transform: translate3d(0, -12vh, 0) rotate(0deg) scale(var(--scale));
    opacity: 0;
  }}
  10% {{
    opacity: .95;
  }}
  100% {{
    transform: translate3d(var(--drift), 112vh, 0) rotate(320deg) scale(var(--scale));
    opacity: .10;
  }}
}}

.wrap {{
  max-width: 1180px;
  margin: 0 auto;
  padding: 20px 14px 28px;
  position: relative;
  z-index: 2;
}}

.hero {{
  position: relative;
  overflow: hidden;
  padding: 28px 20px 22px;
  border-radius: 0 0 34px 34px;
  background:
    linear-gradient(135deg, rgba(255,255,255,.86), rgba(255,248,236,.78)),
    radial-gradient(circle at 82% 18%, rgba(196,222,210,.18), transparent 38%);
  backdrop-filter: blur(18px) saturate(160%);
  -webkit-backdrop-filter: blur(18px) saturate(160%);
  border: 1px solid rgba(255,255,255,.66);
  box-shadow: var(--shadow);
}}

.hero::before {{
  content: "";
  position: absolute;
  right: -50px;
  top: -40px;
  width: 240px;
  height: 240px;
  background: radial-gradient(circle, rgba(203,230,217,.18), transparent 68%);
  filter: blur(14px);
}}

.hero-top {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}}

.brand-wrap {{
  display: flex;
  flex-direction: column;
  gap: 8px;
}}

.brand {{
  font-size: clamp(34px, 6.2vw, 50px);
  font-weight: 950;
  letter-spacing: .08em;
  text-transform: uppercase;
  line-height: .95;
  position: relative;
  display: inline-block;
  padding: 2px 0 6px;
  color: var(--brand);
  -webkit-text-fill-color: var(--brand);
  background: none;
  text-shadow:
    0 1px 0 rgba(255,255,255,.95),
    0 2px 0 rgba(235,245,240,.90),
    0 4px 10px rgba(18,66,49,.18),
    0 10px 18px rgba(18,66,49,.10);
  filter: saturate(1.05) contrast(1.04);
}}

.brand::after {{
  content: "";
  position: absolute;
  left: 4%;
  right: 4%;
  bottom: 5px;
  height: 10px;
  background: radial-gradient(circle, rgba(31,75,58,.16) 0%, rgba(31,75,58,0) 74%);
  filter: blur(6px);
  z-index: -1;
  pointer-events: none;
}}

.brand-sub {{
  color: var(--muted);
  font-size: 15px;
  max-width: 560px;
  line-height: 1.45;
  font-weight: 500;
}}

.badge {{
  min-width: 38px;
  height: 38px;
  padding: 0 12px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  color: #ffffff;
  font-weight: 900;
  font-size: 13px;
  background: linear-gradient(180deg, var(--brand-mid), var(--brand-dark));
  box-shadow: 0 12px 24px rgba(31,75,58,.24);
}}

.layout {{
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(320px, .95fr);
  gap: 16px;
  margin-top: 16px;
}}

.panel, .reviews-box, .social-box {{
  background: var(--glass);
  backdrop-filter: blur(16px) saturate(165%);
  -webkit-backdrop-filter: blur(16px) saturate(165%);
  border: 1px solid rgba(255,255,255,.64);
  box-shadow: var(--shadow-soft);
  border-radius: var(--radius-xl);
}}

.panel-head {{
  padding: 18px 18px 8px;
  font-size: 26px;
  font-weight: 900;
}}

.panel-sub {{
  padding: 0 18px 16px;
  font-size: 14px;
  color: var(--muted);
  line-height: 1.5;
}}

.filters {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  padding: 0 16px 14px;
}}

.filter-btn {{
  border: none;
  cursor: pointer;
  padding: 10px 14px;
  border-radius: 999px;
  font-weight: 700;
  font-size: 13px;
  background: rgba(255,255,255,.86);
  color: #4f4034;
  border: 1px solid rgba(68,120,96,.16);
  box-shadow: 0 6px 18px rgba(95,120,108,.07);
  transition: .24s ease;
}}

.filter-btn:hover {{
  transform: translateY(-1px);
}}

.filter-btn.active {{
  background: linear-gradient(180deg, var(--brand-mid), var(--brand-dark));
  color: #ffffff;
  border-color: rgba(31,75,58,.24);
}}

.catalog {{
  padding: 0 14px 16px;
}}

.grid {{
  display: grid;
  grid-template-columns: repeat(2, minmax(0,1fr));
  gap: 14px;
}}

.card {{
  background: var(--glass-strong);
  border: 1px solid rgba(255,255,255,.74);
  border-radius: 22px;
  overflow: hidden;
  box-shadow: 0 14px 34px rgba(133,96,49,.08);
  transition: transform .28s ease, box-shadow .28s ease;
}}

.card:hover {{
  transform: translateY(-3px);
  box-shadow: 0 20px 40px rgba(133,96,49,.14);
}}

.card-inner {{
  padding: 14px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}}

.photo {{
  aspect-ratio: 1 / 1.08;
  border-radius: 18px;
  overflow: hidden;
  background: linear-gradient(135deg, rgba(255,255,255,.86), rgba(247,238,231,.96));
  border: 1px solid rgba(68,120,96,.10);
}}

.photo img {{
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}}

.photo-placeholder {{
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  color: #9e8f86;
  font-weight: 700;
  font-size: 15px;
}}

.card-title {{
  font-size: 17px;
  font-weight: 900;
  line-height: 1.3;
}}

.card-desc {{
  color: var(--muted);
  font-size: 13px;
  line-height: 1.5;
  min-height: 40px;
}}

.price-row {{
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}}

.price {{
  font-size: 22px;
  font-weight: 900;
}}

.old-price {{
  color: #99887c;
  text-decoration: line-through;
  font-size: 13px;
}}

.meta {{
  display: flex;
  flex-direction: column;
  gap: 5px;
  color: #6e615a;
  font-size: 12px;
}}

.sizes {{
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
}}

.size-btn {{
  border: none;
  cursor: pointer;
  padding: 8px 12px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  background: rgba(255,255,255,.86);
  color: #493626;
  border: 1px solid rgba(68,120,96,.16);
  transition: .2s ease;
}}

.size-btn.active {{
  background: linear-gradient(180deg, var(--brand-mid), var(--brand-dark));
  color: #fff;
}}

.qty-row {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
}}

.qty-box {{
  display: flex;
  align-items: center;
  border-radius: 999px;
  background: rgba(255,255,255,.84);
  border: 1px solid rgba(68,120,96,.14);
  overflow: hidden;
}}

.qty-btn {{
  width: 38px;
  height: 38px;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 19px;
  color: #37291f;
}}

.qty-value {{
  min-width: 36px;
  text-align: center;
  font-weight: 800;
}}

.action-row {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
}}

.buy-btn, .quick-btn, .checkout-btn, .clear-btn {{
  border: none;
  cursor: pointer;
  border-radius: 16px;
  padding: 13px 14px;
  font-weight: 800;
  font-size: 14px;
  transition: transform .22s ease, box-shadow .22s ease, opacity .22s ease;
}}

.buy-btn:hover, .quick-btn:hover, .checkout-btn:hover, .clear-btn:hover {{
  transform: translateY(-1px);
}}

.buy-btn {{
  background: #181311;
  color: #fff;
  box-shadow: 0 12px 24px rgba(0,0,0,.12);
}}

.quick-btn {{
  background: linear-gradient(180deg, var(--brand-mid), var(--brand-dark));
  color: #fff;
  box-shadow: 0 12px 24px rgba(31,75,58,.20);
}}

.checkout-btn {{
  width: 100%;
  background: linear-gradient(180deg, var(--brand-mid), var(--brand-dark));
  color: #fff;
  box-shadow: 0 12px 24px rgba(31,75,58,.20);
}}

.clear-btn {{
  width: 100%;
  margin-top: 10px;
  background: rgba(255,255,255,.86);
  color: #2a2018;
  border: 1px solid rgba(68,120,96,.16);
}}

.buy-btn:disabled, .quick-btn:disabled {{
  opacity: .55;
  cursor: not-allowed;
}}

.cart-wrap {{
  padding: 16px;
}}

.cart-list {{
  display: flex;
  flex-direction: column;
  gap: 10px;
}}

.cart-item {{
  background: rgba(255,255,255,.80);
  border: 1px solid rgba(68,120,96,.14);
  border-radius: 18px;
  padding: 12px;
}}

.cart-item-top {{
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: flex-start;
}}

.cart-name {{
  font-weight: 800;
  font-size: 14px;
}}

.cart-meta {{
  color: #6d615a;
  font-size: 12px;
  line-height: 1.45;
}}

.cart-remove {{
  border: none;
  cursor: pointer;
  background: rgba(255,255,255,.92);
  width: 34px;
  height: 34px;
  border-radius: 12px;
  box-shadow: 0 6px 12px rgba(0,0,0,.05);
}}

.cart-empty {{
  color: #7f7268;
  font-size: 14px;
  padding: 6px 2px 14px;
}}

.summary {{
  border-top: 1px solid rgba(68,120,96,.12);
  margin-top: 14px;
  padding-top: 14px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}}

.summary-row {{
  display: flex;
  justify-content: space-between;
  gap: 12px;
  font-size: 14px;
}}

.checkout-box {{
  margin-top: 16px;
  border-top: 1px solid rgba(68,120,96,.12);
  padding-top: 14px;
}}

.checkout-box-title {{
  font-size: 18px;
  font-weight: 900;
  margin-bottom: 8px;
}}

.checkout-box-text {{
  color: var(--muted);
  font-size: 13px;
  line-height: 1.5;
}}

.reviews-box, .social-box {{
  margin-top: 16px;
  padding: 18px;
}}

.reviews-title, .social-title {{
  font-size: 22px;
  font-weight: 900;
  margin-bottom: 12px;
}}

.reviews-list {{
  display: grid;
  grid-template-columns: repeat(2, minmax(0,1fr));
  gap: 12px;
}}

.review-card {{
  background: rgba(255,255,255,.80);
  border: 1px solid rgba(68,120,96,.14);
  border-radius: 18px;
  padding: 14px;
}}

.review-stars {{
  font-size: 17px;
  margin-bottom: 8px;
}}

.review-name {{
  font-weight: 800;
  margin-bottom: 6px;
}}

.review-text {{
  color: #685b54;
  line-height: 1.5;
  font-size: 13px;
}}

.social-links {{
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}}

.social-links a {{
  text-decoration: none;
  padding: 11px 16px;
  border-radius: 999px;
  font-weight: 800;
  font-size: 13px;
  background: linear-gradient(180deg, var(--brand-mid), var(--brand-dark));
  color: #fff;
  box-shadow: 0 12px 24px rgba(31,75,58,.16);
}}

.notice {{
  position: fixed;
  left: 50%;
  bottom: 18px;
  transform: translateX(-50%) translateY(10px);
  opacity: 0;
  pointer-events: none;
  background: #181311;
  color: #fff;
  padding: 12px 16px;
  border-radius: 999px;
  font-size: 13px;
  z-index: 999;
  box-shadow: 0 18px 34px rgba(0,0,0,.18);
  transition: .22s ease;
}}

.notice.show {{
  opacity: 1;
  transform: translateX(-50%) translateY(0);
}}

@media (max-width: 920px) {{
  .layout {{
    grid-template-columns: 1fr;
  }}
}}

@media (max-width: 640px) {{
  .grid, .reviews-list {{
    grid-template-columns: 1fr;
  }}

  .action-row {{
    grid-template-columns: 1fr;
  }}

  .hero {{
    border-radius: 0 0 24px 24px;
    padding: 20px 16px 18px;
  }}

  .panel-head {{
    font-size: 22px;
  }}
}}
</style>
</head>
<body>
<div class="flower-field" id="flowerField"></div>

<div class="wrap">
  <div class="hero">
    <div class="hero-top">
      <div class="brand-wrap">
        <div class="brand">{html.escape(SHOP_BRAND)}</div>
        <div class="brand-sub" id="heroSub">Премиальный магазин внутри Telegram</div>
      </div>
      <div class="badge" id="cartBadge">0</div>
    </div>
  </div>

  <div class="layout">
    <div class="panel">
      <div class="panel-head" id="catalogTitle">Каталог</div>
      <div class="panel-sub" id="catalogSub">Выберите размер, количество и добавьте товар в корзину или купите сразу.</div>
      <div class="filters" id="filters"></div>
      <div class="catalog">
        <div class="grid" id="productGrid"></div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-head" id="cartTitle">Корзина</div>
      <div class="cart-wrap">
        <div class="cart-list" id="cartList"></div>
        <div class="cart-empty" id="cartEmpty">Ваша корзина пуста</div>

        <div class="summary">
          <div class="summary-row">
            <span id="summaryQtyLabel">Всего товаров</span>
            <b id="summaryQty">0</b>
          </div>
          <div class="summary-row">
            <span id="summaryAmountLabel">Сумма</span>
            <b id="summaryAmount">0 сум</b>
          </div>
        </div>

        <div class="checkout-box">
          <div class="checkout-box-title" id="checkoutBoxTitle">Оформление заказа</div>
          <div class="checkout-box-text" id="checkoutBoxText">Сначала добавьте товары в корзину. Затем нажмите кнопку ниже и бот продолжит оформление заказа.</div>
          <button class="checkout-btn" id="checkoutBtn">Оформить через бот</button>
          <button class="clear-btn" id="clearBtn">Очистить корзину</button>
        </div>
      </div>
    </div>
  </div>

  <div class="reviews-box">
    <div class="reviews-title" id="reviewsTitle">Отзывы покупателей</div>
    <div class="reviews-list" id="reviewsList"></div>
  </div>

  <div class="social-box">
    <div class="social-title" id="socialTitle">Наши каналы</div>
    <div class="social-links" id="socialLinks"></div>
  </div>
</div>

<div class="notice" id="notice"></div>

<script>
const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
if (tg) {{
  tg.ready();
  tg.expand();
}}

const params = new URLSearchParams(window.location.search);
const lang = params.get("lang") || "ru";

const I18N = {{
  ru: {{
    heroSub: "Премиальный магазин внутри Telegram",
    catalogTitle: "Каталог",
    catalogSub: "Выберите размер, количество и добавьте товар в корзину или купите сразу.",
    cartTitle: "Корзина",
    cartEmpty: "Ваша корзина пуста",
    summaryQtyLabel: "Всего товаров",
    summaryAmountLabel: "Сумма",
    checkoutBoxTitle: "Оформление заказа",
    checkoutBoxText: "Сначала добавьте товары в корзину. Затем нажмите кнопку ниже и бот продолжит оформление заказа.",
    checkoutBtn: "Оформить через бот",
    clearBtn: "Очистить корзину",
    addToCart: "В корзину",
    buyNow: "Купить сейчас",
    sizes: "Размеры",
    stock: "Остаток",
    qty: "Кол-во",
    added: "Товар добавлен в корзину",
    removed: "Позиция удалена",
    cleared: "Корзина очищена",
    chooseSize: "Выберите размер",
    noProducts: "Товаров пока нет",
    noPhoto: "Без фото",
    startCheckoutMsg: "Вернитесь в чат бота и продолжите оформление.",
    reviewsTitle: "Отзывы покупателей",
    noReviews: "Пока отзывов нет",
    socialTitle: "Наши каналы",
    category_all: "Все",
    category_new: "Новинки",
    category_hits: "Хиты",
    category_sale: "Скидки",
    category_limited: "Лимит",
    category_school: "Школа",
    category_casual: "Повседневное"
  }},
  uz: {{
    heroSub: "Telegram ichidagi premium do'kon",
    catalogTitle: "Katalog",
    catalogSub: "O'lcham va sonni tanlab mahsulotni savatchaga qo'shing yoki darhol sotib oling.",
    cartTitle: "Savatcha",
    cartEmpty: "Savatchangiz bo'sh",
    summaryQtyLabel: "Jami mahsulot",
    summaryAmountLabel: "Summa",
    checkoutBoxTitle: "Buyurtma rasmiylashtirish",
    checkoutBoxText: "Avval mahsulotlarni savatchaga qo'shing. Keyin tugmani bosing va bot buyurtmani davom ettiradi.",
    checkoutBtn: "Bot orqali rasmiylashtirish",
    clearBtn: "Savatchani tozalash",
    addToCart: "Savatchaga",
    buyNow: "Hozir sotib olish",
    sizes: "O'lchamlar",
    stock: "Qoldiq",
    qty: "Soni",
    added: "Mahsulot savatchaga qo'shildi",
    removed: "Pozitsiya o'chirildi",
    cleared: "Savatcha tozalandi",
    chooseSize: "O'lchamni tanlang",
    noProducts: "Hozircha mahsulotlar yo'q",
    noPhoto: "Rasmsiz",
    startCheckoutMsg: "Bot chatiga qaytib buyurtmani davom ettiring.",
    reviewsTitle: "Xaridorlar sharhlari",
    noReviews: "Hozircha sharhlar yo'q",
    socialTitle: "Kanallarimiz",
    category_all: "Barchasi",
    category_new: "Yangi",
    category_hits: "Xitlar",
    category_sale: "Chegirma",
    category_limited: "Limit",
    category_school: "Maktab",
    category_casual: "Kundalik"
  }}
}};

const TXT = I18N[lang] || I18N.ru;
const state = {{
  products: [],
  filteredProducts: [],
  cart: [],
  reviews: [],
  activeCategory: "all"
}};

function esc(v) {{
  return String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}}

function fmtSum(value) {{
  return Number(value || 0).toLocaleString("ru-RU") + " сум";
}}

function showNotice(text) {{
  const n = document.getElementById("notice");
  n.textContent = text;
  n.classList.add("show");
  setTimeout(() => n.classList.remove("show"), 1800);
}}

function applyTexts() {{
  document.getElementById("heroSub").textContent = TXT.heroSub;
  document.getElementById("catalogTitle").textContent = TXT.catalogTitle;
  document.getElementById("catalogSub").textContent = TXT.catalogSub;
  document.getElementById("cartTitle").textContent = TXT.cartTitle;
  document.getElementById("cartEmpty").textContent = TXT.cartEmpty;
  document.getElementById("summaryQtyLabel").textContent = TXT.summaryQtyLabel;
  document.getElementById("summaryAmountLabel").textContent = TXT.summaryAmountLabel;
  document.getElementById("checkoutBoxTitle").textContent = TXT.checkoutBoxTitle;
  document.getElementById("checkoutBoxText").textContent = TXT.checkoutBoxText;
  document.getElementById("checkoutBtn").textContent = TXT.checkoutBtn;
  document.getElementById("clearBtn").textContent = TXT.clearBtn;
  document.getElementById("reviewsTitle").textContent = TXT.reviewsTitle;
  document.getElementById("socialTitle").textContent = TXT.socialTitle;
}}

function daisySVG(petalColor, centerColor) {{
  return `
    <svg viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">
      <g>
        <ellipse cx="32" cy="10" rx="7" ry="15" fill="${{petalColor}}"/>
        <ellipse cx="32" cy="54" rx="7" ry="15" fill="${{petalColor}}"/>
        <ellipse cx="10" cy="32" rx="15" ry="7" fill="${{petalColor}}"/>
        <ellipse cx="54" cy="32" rx="15" ry="7" fill="${{petalColor}}"/>
        <ellipse cx="17" cy="17" rx="7" ry="14" transform="rotate(-45 17 17)" fill="${{petalColor}}"/>
        <ellipse cx="47" cy="17" rx="7" ry="14" transform="rotate(45 47 17)" fill="${{petalColor}}"/>
        <ellipse cx="17" cy="47" rx="7" ry="14" transform="rotate(45 17 47)" fill="${{petalColor}}"/>
        <ellipse cx="47" cy="47" rx="7" ry="14" transform="rotate(-45 47 47)" fill="${{petalColor}}"/>
        <circle cx="32" cy="32" r="10" fill="${{centerColor}}"/>
        <circle cx="32" cy="32" r="5" fill="#b97b10"/>
      </g>
    </svg>
  `;
}}

function buildFlowers() {{
  const root = document.getElementById("flowerField");
  root.innerHTML = "";

  const palettes = [
    {{ petal: "#ffd84d", center: "#f2b705" }},
    {{ petal: "#ff6b6b", center: "#ffd166" }},
    {{ petal: "#6ec5ff", center: "#ffd166" }},
    {{ petal: "#9b7bff", center: "#ffd166" }},
    {{ petal: "#6bcf8a", center: "#ffd166" }}
  ];

  for (let i = 0; i < 30; i += 1) {{
    const item = palettes[Math.floor(Math.random() * palettes.length)];
    const node = document.createElement("div");
    node.className = "flower";
    const size = Math.random() * 24 + 18;
    node.style.left = (Math.random() * 100).toFixed(2) + "%";
    node.style.width = size.toFixed(0) + "px";
    node.style.height = size.toFixed(0) + "px";
    node.style.animationDuration = (8 + Math.random() * 8).toFixed(2) + "s";
    node.style.animationDelay = (-Math.random() * 16).toFixed(2) + "s";
    node.style.setProperty("--drift", ((Math.random() * 180) - 90).toFixed(0) + "px");
    node.style.setProperty("--scale", (0.75 + Math.random() * 1.15).toFixed(2));
    node.innerHTML = daisySVG(item.petal, item.center);
    root.appendChild(node);
  }}
}}

function buildFilters() {{
  const box = document.getElementById("filters");
  const categories = [
    {{key:"all", name:TXT.category_all}},
    {{key:"new", name:TXT.category_new}},
    {{key:"hits", name:TXT.category_hits}},
    {{key:"sale", name:TXT.category_sale}},
    {{key:"limited", name:TXT.category_limited}},
    {{key:"school", name:TXT.category_school}},
    {{key:"casual", name:TXT.category_casual}}
  ];

  box.innerHTML = "";

  categories.forEach(cat => {{
    const btn = document.createElement("button");
    btn.className = "filter-btn" + (state.activeCategory === cat.key ? " active" : "");
    btn.textContent = cat.name;
    btn.onclick = () => {{
      state.activeCategory = cat.key;
      buildFilters();
      applyFilter();
    }};
    box.appendChild(btn);
  }});
}}

function applyFilter() {{
  state.filteredProducts = state.activeCategory === "all"
    ? [...state.products]
    : state.products.filter(p => p.category_slug === state.activeCategory);
  renderProducts();
}}

async function loadProducts() {{
  const res = await fetch(`/api/shop/products?lang=${{encodeURIComponent(lang)}}`);
  state.products = await res.json();
  buildFilters();
  applyFilter();
}}

async function loadCart() {{
  if (!tg || !tg.initDataUnsafe || !tg.initDataUnsafe.user) {{
    renderCart();
    return;
  }}

  const userId = tg.initDataUnsafe.user.id;
  const res = await fetch(`/api/shop/cart?user_id=${{encodeURIComponent(userId)}}`);
  const data = await res.json();
  state.cart = data.items || [];
  renderCart();
}}

async function loadReviews() {{
  const res = await fetch(`/api/shop/reviews?lang=${{encodeURIComponent(lang)}}`);
  state.reviews = await res.json();
  renderReviews();
}}

function sendPayload(payload) {{
  if (!tg) {{
    showNotice(TXT.startCheckoutMsg);
    return;
  }}
  tg.sendData(JSON.stringify(payload));
}}

function renderProducts() {{
  const grid = document.getElementById("productGrid");
  grid.innerHTML = "";

  if (!state.filteredProducts.length) {{
    const empty = document.createElement("div");
    empty.className = "card";
    empty.innerHTML = `<div class="card-inner"><div class="card-desc">${{TXT.noProducts}}</div></div>`;
    grid.appendChild(empty);
    return;
  }}

  state.filteredProducts.forEach(product => {{
    const card = document.createElement("div");
    card.className = "card";

    const sizes = Array.isArray(product.sizes_list) ? product.sizes_list : [];
    const img = product.photo_url
      ? `<img src="${{esc(product.photo_url)}}" alt="">`
      : `<div class="photo-placeholder">${{TXT.noPhoto}}</div>`;

    const sizesHtml = sizes.length
      ? `<div class="sizes">${{sizes.map(s => `<button class="size-btn" data-size="${{esc(s)}}">${{esc(s)}}</button>`).join("")}}</div>`
      : "";

    card.innerHTML = `
      <div class="card-inner">
        <div class="photo">${{img}}</div>
        <div class="card-title">${{esc(product.title)}}</div>
        <div class="card-desc">${{esc(product.description || "")}}</div>
        <div class="price-row">
          <div class="price current-total">${{fmtSum(product.price)}}</div>
          ${{Number(product.old_price || 0) > 0 ? `<div class="old-price">${{fmtSum(product.old_price)}}</div>` : ""}}
        </div>
        <div class="meta">
          <div>${{TXT.stock}}: ${{Number(product.stock_qty || 0)}}</div>
          <div>${{TXT.sizes}}: ${{sizes.length ? sizes.map(esc).join(", ") : "—"}}</div>
        </div>
        ${{sizesHtml}}
        <div class="qty-row">
          <div class="qty-box">
            <button class="qty-btn minus">−</button>
            <div class="qty-value">1</div>
            <button class="qty-btn plus">+</button>
          </div>
          <div class="meta">${{TXT.qty}}</div>
        </div>
        <div class="action-row">
          <button class="buy-btn" ${{Number(product.stock_qty) <= 0 ? "disabled" : ""}}>${{TXT.addToCart}}</button>
          <button class="quick-btn" ${{Number(product.stock_qty) <= 0 ? "disabled" : ""}}>${{TXT.buyNow}}</button>
        </div>
      </div>
    `;

    let qty = 1;
    let activeSize = sizes.length ? String(sizes[0]) : "";

    const qtyValue = card.querySelector(".qty-value");
    const currentTotal = card.querySelector(".current-total");
    const minus = card.querySelector(".minus");
    const plus = card.querySelector(".plus");

    function recalc() {{
      currentTotal.textContent = fmtSum(Number(product.price || 0) * qty);
      qtyValue.textContent = String(qty);
    }}

    minus.onclick = () => {{
      qty = Math.max(1, qty - 1);
      recalc();
    }};

    plus.onclick = () => {{
      qty = Math.min(99, qty + 1);
      recalc();
    }};

    const sizeButtons = card.querySelectorAll(".size-btn");
    sizeButtons.forEach((btn, index) => {{
      if (index === 0) btn.classList.add("active");
      btn.onclick = () => {{
        sizeButtons.forEach(x => x.classList.remove("active"));
        btn.classList.add("active");
        activeSize = btn.dataset.size || "";
      }};
    }});

    card.querySelector(".buy-btn").onclick = () => {{
      if (sizes.length && !activeSize) {{
        showNotice(TXT.chooseSize);
        return;
      }}
      sendPayload({{
        action: "add_to_cart",
        product_id: product.id,
        qty: qty,
        size: activeSize
      }});
      showNotice(TXT.added);
      setTimeout(loadCart, 500);
    }};

    card.querySelector(".quick-btn").onclick = () => {{
      if (sizes.length && !activeSize) {{
        showNotice(TXT.chooseSize);
        return;
      }}
      sendPayload({{
        action: "buy_now",
        product_id: product.id,
        qty: qty,
        size: activeSize
      }});
      showNotice(TXT.added);
      setTimeout(loadCart, 500);
    }};

    grid.appendChild(card);
  }});
}}

function renderCart() {{
  const list = document.getElementById("cartList");
  const empty = document.getElementById("cartEmpty");
  const badge = document.getElementById("cartBadge");
  const qtyEl = document.getElementById("summaryQty");
  const amountEl = document.getElementById("summaryAmount");

  list.innerHTML = "";

  let totalQty = 0;
  let totalAmount = 0;

  (state.cart || []).forEach(item => {{
    totalQty += Number(item.qty || 0);
    totalAmount += Number(item.subtotal || 0);

    const div = document.createElement("div");
    div.className = "cart-item";
    div.innerHTML = `
      <div class="cart-item-top">
        <div>
          <div class="cart-name">${{esc(item.product_name)}}</div>
          <div class="cart-meta">${{item.size ? esc(item.size) + " | " : ""}}${{item.qty}} × ${{fmtSum(item.price)}}</div>
        </div>
        <button class="cart-remove">×</button>
      </div>
      <div class="cart-meta">${{fmtSum(item.subtotal)}}</div>
    `;

    div.querySelector(".cart-remove").onclick = () => {{
      sendPayload({{
        action: "remove_from_cart",
        cart_id: item.cart_id
      }});
      showNotice(TXT.removed);
      setTimeout(loadCart, 500);
    }};

    list.appendChild(div);
  }});

  empty.style.display = state.cart.length ? "none" : "block";
  badge.textContent = String(totalQty);
  qtyEl.textContent = String(totalQty);
  amountEl.textContent = fmtSum(totalAmount);
}}

function renderReviews() {{
  const box = document.getElementById("reviewsList");
  box.innerHTML = "";

  if (!state.reviews.length) {{
    const div = document.createElement("div");
    div.className = "review-card";
    div.innerHTML = `<div class="review-text">${{TXT.noReviews}}</div>`;
    box.appendChild(div);
    return;
  }}

  state.reviews.forEach(item => {{
    const div = document.createElement("div");
    div.className = "review-card";
    div.innerHTML = `
      <div class="review-stars">${{esc(item.stars || "")}}</div>
      <div class="review-name">${{esc(item.customer_name || "Client")}}</div>
      <div class="review-text">${{esc(item.text || "")}}</div>
    `;
    box.appendChild(div);
  }});
}}

function renderSocials() {{
  const box = document.getElementById("socialLinks");
  box.innerHTML = "";

  const links = [
    {{title:"Telegram", url:{json.dumps(CHANNEL_LINK or "")}}},
    {{title:"Instagram", url:{json.dumps(INSTAGRAM_LINK or "")}}},
    {{title:"YouTube", url:{json.dumps(YOUTUBE_LINK or "")}}}
  ].filter(x => x.url);

  links.forEach(item => {{
    const a = document.createElement("a");
    a.href = item.url;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.textContent = item.title;
    box.appendChild(a);
  }});
}}

document.getElementById("clearBtn").onclick = () => {{
  sendPayload({{ action: "clear_cart" }});
  showNotice(TXT.cleared);
  setTimeout(loadCart, 500);
}};

document.getElementById("checkoutBtn").onclick = () => {{
  sendPayload({{ action: "checkout" }});
}};

applyTexts();
buildFlowers();
renderSocials();
loadProducts();
loadCart();
loadReviews();
</script>
</body>
</html>
"""


# ============================================================
# WEB ROUTES
# ============================================================

@web_router.get("/shop")
async def shop_page(request: web.Request) -> web.Response:
    return web.Response(text=build_shop_html(), content_type="text/html")


@web_router.get("/api/shop/products")
async def api_shop_products(request: web.Request) -> web.Response:
    lang = request_lang(request)
    rows = get_published_products()
    result: list[dict[str, Any]] = []
    for row in rows:
        photo_url = await get_file_url_by_file_id(row["photo_file_id"] or "")
        result.append(product_row_to_api_dict(row, lang, photo_url=photo_url))
    return web.json_response(result)


@web_router.get("/api/shop/cart")
async def api_shop_cart(request: web.Request) -> web.Response:
    user_id = safe_int(request.query.get("user_id"))
    return web.json_response(cart_items_api(user_id))


@web_router.get("/api/shop/reviews")
async def api_shop_reviews(request: web.Request) -> web.Response:
    rows = get_published_reviews(limit=12)
    return web.json_response([
        {
            "id": row["id"],
            "customer_name": row["customer_name"] or "Клиент",
            "rating": safe_int(row["rating"], 5),
            "stars": stars_text(row["rating"]),
            "text": row["text"] or "",
            "created_at": row["created_at"],
        }
        for row in rows
    ])


@web_router.get("/health")
async def health_route(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


@web_router.get("/pay/click/{order_id}")
async def pay_click_route(request: web.Request) -> web.Response:
    order_id = safe_int(request.match_info.get("order_id"))
    return web.Response(text=f"<html><body style='font-family:Arial;padding:30px;background:#fff8f2'><h2>Click payment</h2><p>Order ID: <b>{order_id}</b></p><p>Сюда подключается интеграция Click.</p></body></html>", content_type="text/html")


@web_router.get("/pay/payme/{order_id}")
async def pay_payme_route(request: web.Request) -> web.Response:
    order_id = safe_int(request.match_info.get("order_id"))
    return web.Response(text=f"<html><body style='font-family:Arial;padding:30px;background:#fff8f2'><h2>Payme payment</h2><p>Order ID: <b>{order_id}</b></p><p>Сюда подключается интеграция Payme.</p></body></html>", content_type="text/html")


# ============================================================
# WEB ADMIN
# ============================================================


def admin_access_ok(request: web.Request) -> bool:
    token = request.query.get("token", "").strip()
    return bool(ADMIN_PANEL_TOKEN) and token == ADMIN_PANEL_TOKEN


def admin_page_template(title: str, body: str) -> str:
    token_q = html.escape(ADMIN_PANEL_TOKEN)
    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(title)}</title>
<style>
body {{ margin:0; font-family:Arial,Helvetica,sans-serif; background:linear-gradient(135deg,#fff8f2,#fff0f5,#fff8ee); color:#1b1714; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:20px; }}
.top {{ background:rgba(255,255,255,.72); backdrop-filter:blur(16px); border:1px solid rgba(255,255,255,.6); border-radius:22px; padding:18px 20px; margin-bottom:18px; box-shadow:0 10px 28px rgba(0,0,0,.08); }}
.brand {{ font-size:34px; font-weight:900; letter-spacing:.14em; background:linear-gradient(180deg,#f4dfae,#c8a96b,#a06f21); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.nav {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:12px; }}
.nav a {{ color:#fff; text-decoration:none; padding:9px 14px; background:linear-gradient(180deg,#c8a96b,#9e7b36); border-radius:999px; font-weight:700; font-size:13px; }}
.card {{ background:rgba(255,255,255,.78); border-radius:20px; padding:18px; box-shadow:0 8px 24px rgba(0,0,0,.06); margin-bottom:16px; border:1px solid rgba(255,255,255,.6); }}
.stats {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:12px; }}
.stat {{ background:#fffdfa; border-radius:16px; padding:16px; box-shadow:0 8px 24px rgba(0,0,0,.05); border:1px solid #efe3cf; }}
.stat-label {{ color:#756d60; font-size:12px; margin-bottom:8px; }}
.stat-value {{ font-size:26px; font-weight:900; }}
table {{ width:100%; border-collapse:collapse; }}
th, td {{ border-bottom:1px solid #ece1cf; text-align:left; padding:10px 8px; font-size:14px; vertical-align:top; }}
th {{ background:#faf4ea; }}
.muted {{ color:#7b7468; font-size:13px; }}
.review-card {{ background:#fff; border:1px solid #eee2cf; border-radius:16px; padding:14px; margin-bottom:12px; }}
@media (max-width: 1080px) {{ .stats {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} }}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div class="brand">{html.escape(SHOP_BRAND)} ADMIN</div>
    <div class="nav">
      <a href="/admin?token={token_q}">Dashboard</a>
      <a href="/admin/orders?token={token_q}">Orders</a>
      <a href="/admin/products?token={token_q}">Products</a>
      <a href="/admin/reviews?token={token_q}">Reviews</a>
    </div>
  </div>
  {body}
</div>
</body>
</html>
"""


@web_router.get("/admin")
async def admin_dashboard_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)
    stats = get_basic_stats()
    body = f"""
<div class="stats">
  <div class="stat"><div class="stat-label">Всего заказов</div><div class="stat-value">{stats['total_orders']}</div></div>
  <div class="stat"><div class="stat-label">Новые</div><div class="stat-value">{stats['new']}</div></div>
  <div class="stat"><div class="stat-label">В обработке</div><div class="stat-value">{stats['processing']}</div></div>
  <div class="stat"><div class="stat-label">Подтверждённые</div><div class="stat-value">{stats['confirmed']}</div></div>
  <div class="stat"><div class="stat-label">Оплаченные</div><div class="stat-value">{stats['paid']}</div></div>
  <div class="stat"><div class="stat-label">Пользователи</div><div class="stat-value">{stats['users']}</div></div>
  <div class="stat"><div class="stat-label">Товары</div><div class="stat-value">{stats['products']}</div></div>
  <div class="stat"><div class="stat-label">Отзывы</div><div class="stat-value">{stats['reviews']}</div></div>
</div>
"""
    return web.Response(text=admin_page_template("Admin dashboard", body), content_type="text/html")


@web_router.get("/admin/orders")
async def admin_orders_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 200").fetchall()
    conn.close()
    table_rows = []
    for row in rows:
        table_rows.append(
            f"<tr><td>#{row['id']}</td><td>{html.escape(row['customer_name'] or '—')}<div class='muted'>{html.escape(row['customer_phone'] or '—')}</div></td><td>{mask_username(row['username'])}<div class='muted'>{row['user_id']}</div></td><td>{html.escape(row['city'] or '—')}</td><td>{delivery_label('ru', row['delivery_service'] or '')}</td><td>{payment_method_label('ru', row['payment_method'] or '')}<div class='muted'>{payment_status_label('ru', row['payment_status'])}</div></td><td>{fmt_sum(row['total_amount'])}</td><td>{status_label('ru', row['status'])}</td><td>{row['created_at']}</td></tr>"
        )
    body = "<div class='card'><table><thead><tr><th>ID</th><th>Клиент</th><th>Telegram</th><th>Город</th><th>Доставка</th><th>Оплата</th><th>Сумма</th><th>Статус</th><th>Дата</th></tr></thead><tbody>" + "".join(table_rows) + "</tbody></table></div>"
    return web.Response(text=admin_page_template("Admin orders", body), content_type="text/html")


@web_router.get("/admin/products")
async def admin_products_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)
    rows = get_all_products(limit=300)
    table_rows = []
    for row in rows:
        table_rows.append(
            f"<tr><td>#{row['id']}</td><td>{html.escape(row['title_ru'])}</td><td>{html.escape(row['title_uz'])}</td><td>{html.escape(row['category_slug'])}</td><td>{fmt_sum(row['price'])}</td><td>{fmt_sum(row['old_price']) if safe_int(row['old_price']) > 0 else '—'}</td><td>{html.escape(row['sizes'] or '—')}</td><td>{row['stock_qty']}</td><td>{'Да' if safe_int(row['is_published']) else 'Нет'}</td><td>{row['sort_order']}</td></tr>"
        )
    body = "<div class='card'><table><thead><tr><th>ID</th><th>Title RU</th><th>Title UZ</th><th>Category</th><th>Price</th><th>Old price</th><th>Sizes</th><th>Stock</th><th>Published</th><th>Sort</th></tr></thead><tbody>" + "".join(table_rows) + "</tbody></table></div>"
    return web.Response(text=admin_page_template("Admin products", body), content_type="text/html")


@web_router.get("/admin/reviews")
async def admin_reviews_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)
    rows = get_all_reviews(limit=200)
    if not rows:
        return web.Response(text=admin_page_template("Admin reviews", "<div class='card'>Отзывов нет.</div>"), content_type="text/html")
    blocks = []
    for row in rows:
        blocks.append(
            f"<div class='review-card'><div><b>{stars_text(row['rating'])}</b></div><div style='margin:8px 0 6px;font-weight:800'>{html.escape(row['customer_name'] or '—')} | {mask_username(row['username'])}</div><div>{html.escape(row['text'] or '')}</div><div class='muted' style='margin-top:8px'>ID: #{row['id']} | {'Опубликован' if safe_int(row['is_published']) else 'На модерации'} | {row['created_at']}</div></div>"
        )
    return web.Response(text=admin_page_template("Admin reviews", "<div class='card'>" + "".join(blocks) + "</div>"), content_type="text/html")


# ============================================================
# APP / STARTUP
# ============================================================


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


async def remind_admins_about_unseen_orders_loop() -> None:
    while True:
        await asyncio.sleep(1800)
        try:
            conn = get_db()
            count_row = conn.execute("SELECT COUNT(*) FROM orders WHERE manager_seen = 0").fetchone()
            conn.close()
            unseen = safe_int(count_row[0]) if count_row else 0
            if unseen:
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, f"⚠️ Есть непросмотренные новые заказы: {unseen}")
                    except Exception:
                        logger.exception("Failed sending unseen reminder to %s", admin_id)
        except Exception:
            logger.exception("remind_admins_about_unseen_orders_loop error")


async def main() -> None:
    init_db()
    seed_demo_products_if_empty()
    register_routers()

    app = create_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Web server started on port %s", PORT)

    asyncio.create_task(remind_admins_about_unseen_orders_loop())
    logger.info("Bot polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
