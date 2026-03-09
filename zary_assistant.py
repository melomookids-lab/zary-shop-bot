# ============================================================
# ZARY SHOP BOT
# FULL CLEAN REBUILD FROM SCRATCH
# PART 1 / 10
# core imports, config, constants, texts, database, base helpers
# Python 3.11+
# aiogram 3.x
# aiohttp
# SQLite
# openpyxl
# ============================================================

import os
import re
import hmac
import json
import math
import base64
import hashlib
import asyncio
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
    BufferedInputFile,
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from aiohttp import web
from openpyxl import Workbook


# ============================================================
# PATHS / LOGGING
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


# ============================================================
# ENV CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))

ADMIN_PANEL_TOKEN = os.getenv("ADMIN_PANEL_TOKEN", "").strip()

CHANNEL_ID_RAW = os.getenv("CHANNEL_ID", "0").strip()
CHANNEL_ID = int(CHANNEL_ID_RAW) if CHANNEL_ID_RAW.lstrip("-").isdigit() else 0

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in ADMIN_IDS_RAW.split(",")
    if x.strip() and x.strip().lstrip("-").isdigit()
}

SHOP_BRAND = os.getenv("SHOP_BRAND", "ZARY & CO").strip() or "ZARY & CO"
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "ru").strip() or "ru"

MANAGER_PHONE = os.getenv("MANAGER_PHONE", "+998771202255").strip()
MANAGER_TG = os.getenv("MANAGER_TG", "@manager").strip()
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()
INSTAGRAM_LINK = os.getenv("INSTAGRAM_LINK", "").strip()
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK", "").strip()

CLICK_SERVICE_ID = os.getenv("CLICK_SERVICE_ID", "").strip()
CLICK_MERCHANT_ID = os.getenv("CLICK_MERCHANT_ID", "").strip()
CLICK_SECRET_KEY = os.getenv("CLICK_SECRET_KEY", "").strip()

PAYME_MERCHANT_ID = os.getenv("PAYME_MERCHANT_ID", "").strip()
PAYME_SECRET_KEY = os.getenv("PAYME_SECRET_KEY", "").strip()

SUPPORTED_LANGS = ("ru", "uz")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")


# ============================================================
# BOT / DISPATCHER / ROUTERS
# ============================================================

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

dp = Dispatcher()

user_router = Router()
cart_router = Router()
checkout_router = Router()
orders_router = Router()
reviews_router = Router()
admin_router = Router()
web_router = web.RouteTableDef()


# ============================================================
# CONSTANTS
# ============================================================

CATEGORY_SLUGS = (
    "new",
    "hits",
    "sale",
    "limited",
    "school",
    "casual",
)

ORDER_STATUSES = (
    "new",
    "processing",
    "confirmed",
    "paid",
    "sent",
    "delivered",
    "cancelled",
)

PAYMENT_STATUSES = (
    "pending",
    "paid",
    "failed",
    "cancelled",
    "refunded",
)

DELIVERY_SERVICES = (
    "yandex_courier",
    "b2b_post",
    "yandex_pvz",
)

ADDRESS_TYPES = (
    "location",
    "manual",
)

PAYMENT_METHODS = (
    "click",
    "payme",
)

SIZE_BY_AGE = {
    3: "98",
    4: "104",
    5: "110",
    6: "116",
    7: "122",
    8: "128",
    9: "134",
    10: "140",
}

SIZE_BY_HEIGHT = {
    98: "98",
    104: "104",
    110: "110",
    116: "116",
    122: "122",
    128: "128",
    134: "134",
    140: "140",
    146: "146",
}


# ============================================================
# I18N TEXTS
# ============================================================

TEXTS: dict[str, dict[str, str]] = {
    "ru": {
        # Main menu
        "menu_shop": "🛍 Магазин",
        "menu_cart": "🛒 Корзина",
        "menu_orders": "📦 Мои заказы",
        "menu_size": "📏 Подбор размера",
        "menu_contacts": "📞 Контакты",
        "menu_lang": "🌐 Язык",
        "menu_reviews": "⭐ Отзывы",
        "menu_leave_review": "✍️ Оставить отзыв",
        "menu_admin": "🛠 Админ",

        # General
        "ok": "Готово.",
        "back": "⬅️ Назад",
        "cancel": "❌ Отмена",
        "yes": "Да",
        "no": "Нет",
        "skip": "Пропустить",
        "close": "Закрыть",
        "unknown": "Неизвестно",
        "empty": "Пока пусто.",
        "not_admin": "Эта команда доступна только администратору.",
        "action_cancelled": "Действие отменено.",
        "choose_lang": "Выберите язык.",
        "lang_updated": "Язык обновлён.",
        "welcome": (
            f"Добро пожаловать в <b>{SHOP_BRAND}</b>\n\n"
            "Премиальный магазин одежды внутри Telegram."
        ),
        "main_menu_hint": "Выберите нужный раздел в меню ниже.",
        "send_start_again": "Отправьте /start, чтобы открыть меню.",
        "coming_soon": "Этот раздел скоро будет готов.",

        # Contacts
        "contacts_title": "📞 <b>Контакты</b>",
        "contacts_phone": "Телефон",
        "contacts_manager": "Telegram менеджера",
        "contacts_channel": "Telegram канал",
        "contacts_instagram": "Instagram",
        "contacts_youtube": "YouTube",

        # Size picker
        "size_intro": (
            "Подбор размера работает по возрасту или по росту.\n\n"
            "Примеры:\n"
            "• 5\n"
            "• 128"
        ),
        "size_result_age": "По возрасту рекомендуемый размер",
        "size_result_height": "По росту рекомендуемый размер",
        "size_not_found": "Не смог подобрать размер. Введите возраст 3–10 или рост 98–146.",
        "size_hint_extra": "Если сомневаетесь, лучше взять размер чуть больше.",

        # Shop / cart
        "webapp_open_hint": "Откройте магазин кнопкой «🛍 Магазин».",
        "cart_empty": "Ваша корзина пуста.",
        "cart_title": "🛒 <b>Ваша корзина</b>",
        "cart_total_qty": "Всего товаров",
        "cart_total_amount": "Сумма",
        "cart_checkout": "Оформить заказ",
        "cart_clear": "Очистить корзину",
        "cart_removed": "Позиция удалена из корзины.",
        "cart_cleared": "Корзина очищена.",
        "cart_item_added": "Товар добавлен в корзину.",
        "cart_item_updated": "Корзина обновлена.",
        "cart_bad_payload": "Не удалось обработать данные магазина.",
        "cart_item_not_found": "Товар не найден.",
        "cart_item_no_stock": "Товар временно недоступен.",
        "cart_size_required": "Нужно выбрать размер.",
        "cart_invalid_qty": "Некорректное количество.",
        "cart_same_item_merged": "Количество товара в корзине обновлено.",
        "cart_empty_for_checkout": "Корзина пуста. Сначала добавьте товар.",

        # Orders
        "my_orders_empty": "У вас пока нет заказов.",
        "my_orders_title": "📦 <b>Мои заказы</b>",
        "order_number": "Номер заказа",
        "order_date": "Дата",
        "order_status": "Статус заказа",
        "order_payment_method": "Способ оплаты",
        "order_payment_status": "Статус оплаты",
        "order_delivery_service": "Способ доставки",
        "order_total_amount": "Сумма",
        "order_source": "Источник",
        "order_items": "Товары",
        "order_comment": "Комментарий",
        "order_created_title": "✅ <b>Спасибо за покупку!</b>",
        "order_created_text": (
            "Мы рады, что вы с нами.\n"
            "Носите с удовольствием и с любовью от ZARY & CO."
        ),
        "order_links_text": (
            "Чтобы не потерять нас и не пропустить новые коллекции, "
            "подпишитесь на наши каналы."
        ),
        "order_send_again": "Подтвердите заказ сообщением «Да» или «Нет».",

        # Checkout
        "checkout_intro": "Начинаем оформление заказа.",
        "checkout_name": "Введите имя получателя.",
        "checkout_phone": "Введите телефон в формате +998...",
        "checkout_delivery": "Выберите способ доставки.",
        "checkout_address_type": "Как указать адрес?",
        "checkout_city": "Введите город.",
        "checkout_address": "Введите адрес вручную.",
        "checkout_location": "Отправьте локацию кнопкой ниже.",
        "checkout_pvz_mode": "Для Яндекс ПВЗ введите адрес ПВЗ или код ПВЗ.",
        "checkout_payment": "Выберите способ оплаты.",
        "checkout_comment": "Введите комментарий к заказу или нажмите «Пропустить».",
        "checkout_confirm": "Проверьте данные заказа и подтвердите.",
        "checkout_invalid_phone": "Телефон выглядит некорректно. Пример: +998901234567",
        "checkout_invalid_choice": "Выберите один из предложенных вариантов.",
        "checkout_need_location": "Нужно отправить именно локацию.",
        "checkout_confirm_yes": "Да",
        "checkout_confirm_no": "Нет",
        "checkout_cancelled": "Оформление заказа отменено.",
        "checkout_summary": "🧾 <b>Проверка заказа</b>",
        "checkout_name_label": "Имя",
        "checkout_phone_label": "Телефон",
        "checkout_city_label": "Город",
        "checkout_delivery_label": "Доставка",
        "checkout_address_type_label": "Тип адреса",
        "checkout_address_label": "Адрес",
        "checkout_location_label": "Локация",
        "checkout_pvz_label": "ПВЗ",
        "checkout_payment_label": "Оплата",
        "checkout_comment_label": "Комментарий",
        "checkout_total_label": "Сумма",
        "checkout_items_label": "Товары",
        "checkout_confirm_hint": "Напишите «Да» для подтверждения или «Нет» для отмены.",

        # Delivery values
        "delivery_yandex_courier": "🚚 Яндекс курьер",
        "delivery_b2b_post": "📦 B2B почта",
        "delivery_yandex_pvz": "🏪 Яндекс ПВЗ",

        # Address type values
        "address_location": "📍 Отправить локацию",
        "address_manual": "✍️ Ввести адрес вручную",

        # Payment values
        "payment_click": "Click",
        "payment_payme": "Payme",

        # Reviews
        "reviews_title": "⭐ <b>Отзывы покупателей</b>",
        "reviews_empty": "Пока отзывов нет.",
        "review_leave_intro": "Вы можете оставить отзыв о покупке.",
        "review_rating_ask": "Выберите оценку от 1 до 5.",
        "review_text_ask": "Напишите текст отзыва.",
        "review_sent": "Спасибо. Ваш отзыв отправлен на модерацию.",
        "review_bad_rating": "Нужно выбрать оценку от 1 до 5.",
        "review_admin_title": "⭐ <b>Отзывы на модерации</b>",
        "review_admin_empty": "Новых отзывов нет.",
        "review_published": "Отзыв опубликован.",
        "review_hidden": "Отзыв скрыт.",
        "review_deleted": "Отзыв удалён.",

        # Admin menu
        "admin_title": "🛠 <b>Админ панель</b>",
        "admin_new_orders": "📦 Новые заказы",
        "admin_all_orders": "📋 Все заказы",
        "admin_crm_orders": "🧾 CRM Заказы",
        "admin_find_client": "🔎 Поиск клиента",
        "admin_clients": "👥 Клиенты",
        "admin_add_product": "➕ Добавить товар",
        "admin_edit_product": "📝 Редактировать товар",
        "admin_delete_product": "🗑 Удалить товар",
        "admin_products_browser": "📦 Товары",
        "admin_reviews": "⭐ Отзывы",
        "admin_stats": "📊 Статистика",
        "admin_excel": "🧾 Excel отчёт",
        "admin_schedule_post": "📝 Запланировать пост",
        "admin_back_to_user": "⬅️ В меню",
        "admin_orders_empty": "Новых заказов нет.",
        "admin_no_products": "Товаров пока нет.",
        "admin_choose_product_id": "Отправьте ID товара.",
        "admin_invalid_id": "Нужно отправить числовой ID.",
        "admin_product_deleted": "Товар удалён.",
        "admin_product_not_found": "Товар не найден.",
        "admin_order_not_found": "Заказ не найден.",
        "admin_status_updated": "Статус заказа обновлён.",
        "admin_payment_updated": "Статус оплаты обновлён.",

        # Product FSM
        "product_send_photo": "Отправьте фото товара.",
        "product_title_ru": "Введите название товара на русском.",
        "product_title_uz": "Введите название товара на узбекском.",
        "product_desc_ru": "Введите описание товара на русском.",
        "product_desc_uz": "Введите описание товара на узбекском.",
        "product_sizes": "Введите размеры через запятую. Пример: 110, 116, 122",
        "product_category": "Введите категорию: new / hits / sale / limited / school / casual",
        "product_price": "Введите цену.",
        "product_old_price": "Введите старую цену или 0.",
        "product_stock": "Введите остаток товара.",
        "product_publish": "Опубликовать товар сразу?",
        "product_sort_order": "Введите сортировку. Пример: 10",
        "product_saved": "Товар сохранён.",
        "product_invalid_price": "Нужно ввести число.",
        "product_invalid_category": "Категория должна быть одной из: new, hits, sale, limited, school, casual.",
        "product_publish_yes": "Опубликовать",
        "product_publish_no": "Скрыть",
        "product_edit_choose": "Отправьте ID товара для редактирования.",
        "product_edit_field": "Выберите поле для редактирования.",
        "product_field_updated": "Поле обновлено.",
        "product_hide": "Скрыть",
        "product_show": "Показать",
        "product_publish_updated": "Статус публикации обновлён.",

        # Stats labels
        "stats_total_orders": "Всего заказов",
        "stats_new": "Новые",
        "stats_processing": "В обработке",
        "stats_confirmed": "Подтверждённые",
        "stats_paid": "Оплаченные",
        "stats_sent": "Отправленные",
        "stats_delivered": "Доставленные",
        "stats_cancelled": "Отменённые",
        "stats_unique_users": "Уникальные пользователи",
        "stats_products": "Товары в базе",
        "stats_reviews": "Отзывы",

        # Status labels
        "status_new": "Новый",
        "status_processing": "В обработке",
        "status_confirmed": "Подтверждён",
        "status_paid": "Оплачен",
        "status_sent": "Отправлен",
        "status_delivered": "Доставлен",
        "status_cancelled": "Отменён",

        # Payment status labels
        "payment_status_pending": "Ожидает оплаты",
        "payment_status_paid": "Оплачен",
        "payment_status_failed": "Ошибка оплаты",
        "payment_status_cancelled": "Отменён",
        "payment_status_refunded": "Возврат",

        # Source labels
        "source_telegram": "Telegram",
        "source_webapp": "WebApp",
    },

    "uz": {
        # Main menu
        "menu_shop": "🛍 Do‘kon",
        "menu_cart": "🛒 Savatcha",
        "menu_orders": "📦 Buyurtmalarim",
        "menu_size": "📏 O‘lcham tanlash",
        "menu_contacts": "📞 Kontaktlar",
        "menu_lang": "🌐 Til",
        "menu_reviews": "⭐ Sharhlar",
        "menu_leave_review": "✍️ Sharh qoldirish",
        "menu_admin": "🛠 Admin",

        # General
        "ok": "Tayyor.",
        "back": "⬅️ Orqaga",
        "cancel": "❌ Bekor qilish",
        "yes": "Ha",
        "no": "Yo‘q",
        "skip": "O‘tkazib yuborish",
        "close": "Yopish",
        "unknown": "Noma’lum",
        "empty": "Hozircha bo‘sh.",
        "not_admin": "Bu bo‘lim faqat administrator uchun.",
        "action_cancelled": "Amal bekor qilindi.",
        "choose_lang": "Tilni tanlang.",
        "lang_updated": "Til yangilandi.",
        "welcome": (
            f"<b>{SHOP_BRAND}</b> ga xush kelibsiz.\n\n"
            "Telegram ichidagi premium kiyim do‘koni."
        ),
        "main_menu_hint": "Quyidagi menyudan kerakli bo‘limni tanlang.",
        "send_start_again": "Menyuni ochish uchun /start yuboring.",
        "coming_soon": "Bu bo‘lim tez orada tayyor bo‘ladi.",

        # Contacts
        "contacts_title": "📞 <b>Kontaktlar</b>",
        "contacts_phone": "Telefon",
        "contacts_manager": "Telegram menejer",
        "contacts_channel": "Telegram kanal",
        "contacts_instagram": "Instagram",
        "contacts_youtube": "YouTube",

        # Size picker
        "size_intro": (
            "O‘lcham tanlash yosh yoki bo‘y bo‘yicha ishlaydi.\n\n"
            "Misollar:\n"
            "• 5\n"
            "• 128"
        ),
        "size_result_age": "Yosh bo‘yicha tavsiya etilgan o‘lcham",
        "size_result_height": "Bo‘y bo‘yicha tavsiya etilgan o‘lcham",
        "size_not_found": "O‘lcham topilmadi. 3–10 yosh yoki 98–146 bo‘yni kiriting.",
        "size_hint_extra": "Ikki o‘lcham oralig‘ida bo‘lsa, kattaroq variantni oling.",

        # Shop / cart
        "webapp_open_hint": "Do‘konni «🛍 Do‘kon» tugmasi orqali oching.",
        "cart_empty": "Savatchangiz bo‘sh.",
        "cart_title": "🛒 <b>Savatchangiz</b>",
        "cart_total_qty": "Jami mahsulot",
        "cart_total_amount": "Summa",
        "cart_checkout": "Buyurtma berish",
        "cart_clear": "Savatchani tozalash",
        "cart_removed": "Mahsulot savatchadan o‘chirildi.",
        "cart_cleared": "Savatcha tozalandi.",
        "cart_item_added": "Mahsulot savatchaga qo‘shildi.",
        "cart_item_updated": "Savatcha yangilandi.",
        "cart_bad_payload": "Do‘kon ma’lumotlarini qayta ishlab bo‘lmadi.",
        "cart_item_not_found": "Mahsulot topilmadi.",
        "cart_item_no_stock": "Mahsulot vaqtincha mavjud emas.",
        "cart_size_required": "O‘lcham tanlanishi kerak.",
        "cart_invalid_qty": "Noto‘g‘ri son.",
        "cart_same_item_merged": "Savatchadagi mahsulot soni yangilandi.",
        "cart_empty_for_checkout": "Savatcha bo‘sh. Avval mahsulot qo‘shing.",

        # Orders
        "my_orders_empty": "Sizda hozircha buyurtmalar yo‘q.",
        "my_orders_title": "📦 <b>Buyurtmalarim</b>",
        "order_number": "Buyurtma raqami",
        "order_date": "Sana",
        "order_status": "Buyurtma holati",
        "order_payment_method": "To‘lov usuli",
        "order_payment_status": "To‘lov holati",
        "order_delivery_service": "Yetkazib berish",
        "order_total_amount": "Summa",
        "order_source": "Manba",
        "order_items": "Mahsulotlar",
        "order_comment": "Izoh",
        "order_created_title": "✅ <b>Xaridingiz uchun rahmat!</b>",
        "order_created_text": (
            "Siz biz bilan ekaningizdan xursandmiz.\n"
            "Mahsulotni mamnuniyat bilan kiying."
        ),
        "order_links_text": (
            "Bizni yo‘qotib qo‘ymaslik va yangi kolleksiyalarni "
            "o‘tkazib yubormaslik uchun kanallarimizga obuna bo‘ling."
        ),
        "order_send_again": "Tasdiqlash uchun «Ha» yoki «Yo‘q» deb yozing.",

        # Checkout
        "checkout_intro": "Buyurtma rasmiylashtirishni boshlaymiz.",
        "checkout_name": "Qabul qiluvchi ismini kiriting.",
        "checkout_phone": "Telefon raqamini +998... ko‘rinishida kiriting.",
        "checkout_delivery": "Yetkazib berish usulini tanlang.",
        "checkout_address_type": "Manzilni qanday kiritasiz?",
        "checkout_city": "Shaharni kiriting.",
        "checkout_address": "Manzilni qo‘lda kiriting.",
        "checkout_location": "Quyidagi tugma orqali lokatsiya yuboring.",
        "checkout_pvz_mode": "Yandex PVZ uchun PVZ manzilini yoki kodini kiriting.",
        "checkout_payment": "To‘lov usulini tanlang.",
        "checkout_comment": "Izoh yozing yoki «O‘tkazib yuborish» tugmasini bosing.",
        "checkout_confirm": "Buyurtma ma’lumotlarini tekshirib, tasdiqlang.",
        "checkout_invalid_phone": "Telefon noto‘g‘ri ko‘rinadi. Misol: +998901234567",
        "checkout_invalid_choice": "Taklif qilingan variantlardan birini tanlang.",
        "checkout_need_location": "Aynan lokatsiya yuborilishi kerak.",
        "checkout_confirm_yes": "Ha",
        "checkout_confirm_no": "Yo‘q",
        "checkout_cancelled": "Buyurtma rasmiylashtirish bekor qilindi.",
        "checkout_summary": "🧾 <b>Buyurtmani tekshirish</b>",
        "checkout_name_label": "Ism",
        "checkout_phone_label": "Telefon",
        "checkout_city_label": "Shahar",
        "checkout_delivery_label": "Yetkazib berish",
        "checkout_address_type_label": "Manzil turi",
        "checkout_address_label": "Manzil",
        "checkout_location_label": "Lokatsiya",
        "checkout_pvz_label": "PVZ",
        "checkout_payment_label": "To‘lov",
        "checkout_comment_label": "Izoh",
        "checkout_total_label": "Summa",
        "checkout_items_label": "Mahsulotlar",
        "checkout_confirm_hint": "Tasdiqlash uchun «Ha», bekor qilish uchun «Yo‘q» deb yozing.",

        # Delivery values
        "delivery_yandex_courier": "🚚 Yandex kuryer",
        "delivery_b2b_post": "📦 B2B pochta",
        "delivery_yandex_pvz": "🏪 Yandex PVZ",

        # Address type values
        "address_location": "📍 Lokatsiya yuborish",
        "address_manual": "✍️ Manzilni qo‘lda kiritish",

        # Payment values
        "payment_click": "Click",
        "payment_payme": "Payme",

        # Reviews
        "reviews_title": "⭐ <b>Xaridorlar sharhlari</b>",
        "reviews_empty": "Hozircha sharhlar yo‘q.",
        "review_leave_intro": "Xarid haqida sharhingizni qoldirishingiz mumkin.",
        "review_rating_ask": "1 dan 5 gacha bahoni tanlang.",
        "review_text_ask": "Sharh matnini yozing.",
        "review_sent": "Rahmat. Sharhingiz moderatsiyaga yuborildi.",
        "review_bad_rating": "1 dan 5 gacha bahoni tanlash kerak.",
        "review_admin_title": "⭐ <b>Moderatsiyadagi sharhlar</b>",
        "review_admin_empty": "Yangi sharhlar yo‘q.",
        "review_published": "Sharh chop etildi.",
        "review_hidden": "Sharh yashirildi.",
        "review_deleted": "Sharh o‘chirildi.",

        # Admin menu
        "admin_title": "🛠 <b>Admin panel</b>",
        "admin_new_orders": "📦 Yangi buyurtmalar",
        "admin_all_orders": "📋 Barcha buyurtmalar",
        "admin_crm_orders": "🧾 CRM Buyurtmalar",
        "admin_find_client": "🔎 Mijoz qidirish",
        "admin_clients": "👥 Mijozlar",
        "admin_add_product": "➕ Mahsulot qo‘shish",
        "admin_edit_product": "📝 Mahsulotni tahrirlash",
        "admin_delete_product": "🗑 Mahsulotni o‘chirish",
        "admin_products_browser": "📦 Mahsulotlar",
        "admin_reviews": "⭐ Sharhlar",
        "admin_stats": "📊 Statistika",
        "admin_excel": "🧾 Excel hisobot",
        "admin_schedule_post": "📝 Post rejalash",
        "admin_back_to_user": "⬅️ Menyuga qaytish",
        "admin_orders_empty": "Yangi buyurtmalar yo‘q.",
        "admin_no_products": "Hozircha mahsulotlar yo‘q.",
        "admin_choose_product_id": "Mahsulot ID sini yuboring.",
        "admin_invalid_id": "Raqamli ID yuborilishi kerak.",
        "admin_product_deleted": "Mahsulot o‘chirildi.",
        "admin_product_not_found": "Mahsulot topilmadi.",
        "admin_order_not_found": "Buyurtma topilmadi.",
        "admin_status_updated": "Buyurtma holati yangilandi.",
        "admin_payment_updated": "To‘lov holati yangilandi.",

        # Product FSM
        "product_send_photo": "Mahsulot rasmini yuboring.",
        "product_title_ru": "Mahsulot nomini rus tilida kiriting.",
        "product_title_uz": "Mahsulot nomini o‘zbek tilida kiriting.",
        "product_desc_ru": "Mahsulot tavsifini rus tilida kiriting.",
        "product_desc_uz": "Mahsulot tavsifini o‘zbek tilida kiriting.",
        "product_sizes": "O‘lchamlarni vergul bilan kiriting. Masalan: 110, 116, 122",
        "product_category": "Kategoriya kiriting: new / hits / sale / limited / school / casual",
        "product_price": "Narxni kiriting.",
        "product_old_price": "Eski narxni kiriting yoki 0.",
        "product_stock": "Qoldiq sonini kiriting.",
        "product_publish": "Mahsulotni darhol e’lon qilamizmi?",
        "product_sort_order": "Saralash raqamini kiriting. Masalan: 10",
        "product_saved": "Mahsulot saqlandi.",
        "product_invalid_price": "Raqam kiritilishi kerak.",
        "product_invalid_category": "Kategoriya quyidagilardan biri bo‘lishi kerak: new, hits, sale, limited, school, casual.",
        "product_publish_yes": "E’lon qilish",
        "product_publish_no": "Yashirish",
        "product_edit_choose": "Tahrirlash uchun mahsulot ID sini yuboring.",
        "product_edit_field": "Tahrirlash uchun maydonni tanlang.",
        "product_field_updated": "Maydon yangilandi.",
        "product_hide": "Yashirish",
        "product_show": "Ko‘rsatish",
        "product_publish_updated": "E’lon holati yangilandi.",

        # Stats labels
        "stats_total_orders": "Jami buyurtmalar",
        "stats_new": "Yangi",
        "stats_processing": "Jarayonda",
        "stats_confirmed": "Tasdiqlangan",
        "stats_paid": "To‘langan",
        "stats_sent": "Yuborilgan",
        "stats_delivered": "Yetkazilgan",
        "stats_cancelled": "Bekor qilingan",
        "stats_unique_users": "Unikal foydalanuvchilar",
        "stats_products": "Bazadagi mahsulotlar",
        "stats_reviews": "Sharhlar",

        # Status labels
        "status_new": "Yangi",
        "status_processing": "Jarayonda",
        "status_confirmed": "Tasdiqlangan",
        "status_paid": "To‘langan",
        "status_sent": "Yuborilgan",
        "status_delivered": "Yetkazilgan",
        "status_cancelled": "Bekor qilingan",

        # Payment status labels
        "payment_status_pending": "To‘lov kutilmoqda",
        "payment_status_paid": "To‘langan",
        "payment_status_failed": "To‘lov xatosi",
        "payment_status_cancelled": "Bekor qilingan",
        "payment_status_refunded": "Qaytarilgan",

        # Source labels
        "source_telegram": "Telegram",
        "source_webapp": "WebApp",
    },
}


# ============================================================
# DATABASE
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
            photo_file_id TEXT,
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
            photo_file_id TEXT,
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
            pvz_code TEXT,
            pvz_address TEXT,
            payment_method TEXT,
            payment_status TEXT NOT NULL DEFAULT 'pending',
            payment_provider_invoice_id TEXT,
            payment_provider_url TEXT,
            comment TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            manager_seen INTEGER NOT NULL DEFAULT 0,
            manager_id INTEGER,
            source TEXT NOT NULL DEFAULT 'telegram',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            reminded_at TEXT
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
            admin_seen INTEGER NOT NULL DEFAULT 0,
            order_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            event_type TEXT NOT NULL,
            data TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduled_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            media TEXT,
            post_time TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,
            file_path TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_lang ON users(lang)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_published ON shop_products(is_published)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_sort ON shop_products(sort_order)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_carts_user_id ON carts(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_payment_status ON orders(payment_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_seen ON orders(manager_seen)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_published ON reviews(is_published)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_seen ON reviews(admin_seen)")

    conn.commit()
    conn.close()


# ============================================================
# HELPERS
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def fmt_sum(value: int | float | None) -> str:
    amount = safe_int(value or 0)
    return f"{amount:,}".replace(",", " ") + " сум"


def normalize_phone(phone: str) -> str:
    phone = phone.strip()
    phone = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone.startswith("998") and not phone.startswith("+998"):
        phone = "+" + phone
    return phone


def is_valid_phone(phone: str) -> bool:
    return bool(re.fullmatch(r"\+998\d{9}", normalize_phone(phone)))


def parse_sizes_string(sizes: str) -> list[str]:
    if not sizes:
        return []
    parts = [x.strip() for x in sizes.split(",")]
    return [x for x in parts if x]


def sizes_to_string(sizes: list[str]) -> str:
    return ", ".join([x.strip() for x in sizes if x.strip()])


def ensure_lang(lang: str) -> str:
    return lang if lang in SUPPORTED_LANGS else DEFAULT_LANGUAGE


def get_user_lang(user_id: int) -> str:
    conn = get_db()
    row = conn.execute(
        "SELECT lang FROM users WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    conn.close()
    if row and row["lang"] in SUPPORTED_LANGS:
        return row["lang"]
    return DEFAULT_LANGUAGE


def t(user_id_or_lang: int | str, key: str) -> str:
    if isinstance(user_id_or_lang, int):
        lang = get_user_lang(user_id_or_lang)
    else:
        lang = ensure_lang(user_id_or_lang)
    return TEXTS.get(lang, TEXTS[DEFAULT_LANGUAGE]).get(key, key)


def upsert_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    now = utc_now_iso()
    conn = get_db()
    conn.execute(
        """
        INSERT INTO users (user_id, username, full_name, lang, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            full_name = excluded.full_name,
            updated_at = excluded.updated_at
        """,
        (
            user_id,
            username or "",
            full_name or "",
            DEFAULT_LANGUAGE,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()


def set_user_lang(user_id: int, lang: str) -> None:
    lang = ensure_lang(lang)
    now = utc_now_iso()
    conn = get_db()
    conn.execute(
        """
        INSERT INTO users (user_id, username, full_name, lang, created_at, updated_at)
        VALUES (?, '', '', ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            lang = excluded.lang,
            updated_at = excluded.updated_at
        """,
        (user_id, lang, now, now),
    )
    conn.commit()
    conn.close()


def log_event(user_id: Optional[int], event_type: str, data: dict[str, Any] | None = None) -> None:
    conn = get_db()
    conn.execute(
        """
        INSERT INTO events (user_id, event_type, data, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            user_id,
            event_type,
            json.dumps(data or {}, ensure_ascii=False),
            utc_now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def status_label(lang_or_user: int | str, status: str) -> str:
    return t(lang_or_user, f"status_{status}")


def payment_status_label(lang_or_user: int | str, status: str) -> str:
    return t(lang_or_user, f"payment_status_{status}")


def source_label(lang_or_user: int | str, source: str) -> str:
    return t(lang_or_user, f"source_{source}")


def delivery_label(lang_or_user: int | str, service: str) -> str:
    mapping = {
        "yandex_courier": "delivery_yandex_courier",
        "b2b_post": "delivery_b2b_post",
        "yandex_pvz": "delivery_yandex_pvz",
    }
    return t(lang_or_user, mapping.get(service, "unknown"))


def address_type_label(lang_or_user: int | str, address_type: str) -> str:
    mapping = {
        "location": "address_location",
        "manual": "address_manual",
    }
    return t(lang_or_user, mapping.get(address_type, "unknown"))


def payment_method_label(lang_or_user: int | str, method: str) -> str:
    mapping = {
        "click": "payment_click",
        "payme": "payment_payme",
    }
    return t(lang_or_user, mapping.get(method, "unknown"))


def product_title_by_lang(product_row: sqlite3.Row | dict[str, Any], lang: str) -> str:
    lang = ensure_lang(lang)
    return product_row["title_uz"] if lang == "uz" else product_row["title_ru"]


def product_desc_by_lang(product_row: sqlite3.Row | dict[str, Any], lang: str) -> str:
    lang = ensure_lang(lang)
    return product_row["description_uz"] if lang == "uz" else product_row["description_ru"]


def mask_username(username: str | None) -> str:
    if not username:
        return "—"
    username = username.strip().lstrip("@")
    return f"@{username}" if username else "—"


def stars_text(value: int) -> str:
    value = max(1, min(5, safe_int(value, 5)))
    return "⭐" * value

# ============================================================
# PART 2 / 10
# fsm states, keyboards, product/cart/order/review helpers
# ============================================================

# ============================================================
# FSM STATES
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
    address_or_pvz = State()
    location = State()
    payment_method = State()
    comment = State()
    confirm = State()


class AdminAddProductStates(StatesGroup):
    photo = State()
    title_ru = State()
    title_uz = State()
    description_ru = State()
    description_uz = State()
    sizes = State()
    category_slug = State()
    price = State()
    old_price = State()
    stock_qty = State()
    publish = State()
    sort_order = State()
    confirm = State()


class AdminEditProductStates(StatesGroup):
    product_id = State()
    field_name = State()
    field_value = State()


class AdminDeleteProductStates(StatesGroup):
    product_id = State()
    confirm = State()


class AdminFindClientStates(StatesGroup):
    waiting_phone = State()


# ============================================================
# KEYBOARDS
# ============================================================

def user_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [
            KeyboardButton(
                text=t(user_id, "menu_shop"),
                web_app=WebAppInfo(url=f"{BASE_URL}/shop?lang={get_user_lang(user_id)}"),
            )
        ],
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


def admin_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=t(user_id, "admin_new_orders"))],
        [KeyboardButton(text=t(user_id, "admin_all_orders"))],
        [KeyboardButton(text=t(user_id, "admin_crm_orders"))],
        [KeyboardButton(text=t(user_id, "admin_find_client"))],
        [KeyboardButton(text=t(user_id, "admin_clients"))],
        [KeyboardButton(text=t(user_id, "admin_add_product"))],
        [KeyboardButton(text=t(user_id, "admin_edit_product"))],
        [KeyboardButton(text=t(user_id, "admin_delete_product"))],
        [KeyboardButton(text=t(user_id, "admin_products_browser"))],
        [KeyboardButton(text=t(user_id, "admin_reviews"))],
        [KeyboardButton(text=t(user_id, "admin_stats"))],
        [KeyboardButton(text=t(user_id, "admin_excel"))],
        [KeyboardButton(text=t(user_id, "admin_schedule_post"))],
        [KeyboardButton(text=t(user_id, "admin_back_to_user"))],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Русский", callback_data="lang:set:ru")],
            [InlineKeyboardButton(text="O‘zbekcha", callback_data="lang:set:uz")],
        ]
    )


def cart_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=t(user_id, "cart_checkout"),
                    callback_data="cart:checkout",
                )
            ],
            [
                InlineKeyboardButton(
                    text=t(user_id, "cart_clear"),
                    callback_data="cart:clear",
                )
            ],
        ]
    )


def social_links_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    line: list[InlineKeyboardButton] = []

    if CHANNEL_LINK:
        line.append(InlineKeyboardButton(text="Telegram", url=CHANNEL_LINK))
    if INSTAGRAM_LINK:
        line.append(InlineKeyboardButton(text="Instagram", url=INSTAGRAM_LINK))
    if YOUTUBE_LINK:
        line.append(InlineKeyboardButton(text="YouTube", url=YOUTUBE_LINK))

    if line:
        rows.append(line)

    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="OK", callback_data="noop")]])


def size_picker_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def review_rating_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5")],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def review_admin_actions_keyboard(review_id: int, is_published: int) -> InlineKeyboardMarkup:
    publish_text = "Скрыть" if safe_int(is_published) else "Опубликовать"
    publish_value = 0 if safe_int(is_published) else 1

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=publish_text,
                    callback_data=f"review:toggle:{review_id}:{publish_value}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Удалить",
                    callback_data=f"review:delete:{review_id}",
                )
            ],
        ]
    )


def checkout_cancel_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def checkout_delivery_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "delivery_yandex_courier"))],
            [KeyboardButton(text=t(user_id, "delivery_b2b_post"))],
            [KeyboardButton(text=t(user_id, "delivery_yandex_pvz"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def checkout_address_type_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "address_location"), request_location=True)],
            [KeyboardButton(text=t(user_id, "address_manual"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def checkout_payment_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "payment_click"))],
            [KeyboardButton(text=t(user_id, "payment_payme"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def checkout_comment_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "skip"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def checkout_confirm_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "checkout_confirm_yes"))],
            [KeyboardButton(text=t(user_id, "checkout_confirm_no"))],
        ],
        resize_keyboard=True,
    )


def admin_cancel_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(user_id, "cancel"))]],
        resize_keyboard=True,
    )


def admin_publish_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "product_publish_yes"))],
            [KeyboardButton(text=t(user_id, "product_publish_no"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def admin_yes_no_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "yes")), KeyboardButton(text=t(user_id, "no"))],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def admin_category_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="new"), KeyboardButton(text="hits")],
            [KeyboardButton(text="sale"), KeyboardButton(text="limited")],
            [KeyboardButton(text="school"), KeyboardButton(text="casual")],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def admin_edit_fields_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="photo_file_id")],
            [KeyboardButton(text="title_ru"), KeyboardButton(text="title_uz")],
            [KeyboardButton(text="description_ru"), KeyboardButton(text="description_uz")],
            [KeyboardButton(text="sizes"), KeyboardButton(text="category_slug")],
            [KeyboardButton(text="price"), KeyboardButton(text="old_price")],
            [KeyboardButton(text="stock_qty"), KeyboardButton(text="sort_order")],
            [KeyboardButton(text="is_published")],
            [KeyboardButton(text=t(user_id, "cancel"))],
        ],
        resize_keyboard=True,
    )


def admin_order_actions_keyboard(order_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="В работу", callback_data=f"order:set:{order_id}:processing"),
                InlineKeyboardButton(text="Подтвердить", callback_data=f"order:set:{order_id}:confirmed"),
            ],
            [
                InlineKeyboardButton(text="Оплачен", callback_data=f"order:set:{order_id}:paid"),
                InlineKeyboardButton(text="Отправлен", callback_data=f"order:set:{order_id}:sent"),
            ],
            [
                InlineKeyboardButton(text="Доставлен", callback_data=f"order:set:{order_id}:delivered"),
                InlineKeyboardButton(text="Отменён", callback_data=f"order:set:{order_id}:cancelled"),
            ],
            [
                InlineKeyboardButton(text="Pay pending", callback_data=f"order:pay:{order_id}:pending"),
                InlineKeyboardButton(text="Pay paid", callback_data=f"order:pay:{order_id}:paid"),
            ],
            [
                InlineKeyboardButton(text="Pay failed", callback_data=f"order:pay:{order_id}:failed"),
                InlineKeyboardButton(text="Pay refunded", callback_data=f"order:pay:{order_id}:refunded"),
            ],
            [
                InlineKeyboardButton(text="Написать клиенту", url=f"tg://user?id={user_id}")
            ],
        ]
    )


def product_manage_keyboard(product_id: int, is_published: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Скрыть" if safe_int(is_published) else "Показать",
                    callback_data=f"product:toggle:{product_id}:{0 if safe_int(is_published) else 1}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Удалить",
                    callback_data=f"product:delete:{product_id}",
                )
            ],
        ]
    )


# ============================================================
# PRODUCT HELPERS
# ============================================================

def get_product_by_id(product_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM shop_products WHERE id = ?",
        (product_id,),
    ).fetchone()
    conn.close()
    return row


def get_published_products() -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM shop_products
        WHERE is_published = 1
        ORDER BY sort_order ASC, id DESC
        """
    ).fetchall()
    conn.close()
    return rows


def get_all_products(limit: int = 100) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM shop_products
        ORDER BY sort_order ASC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def get_products_page(page: int = 1, per_page: int = 10) -> tuple[list[sqlite3.Row], int]:
    page = max(1, safe_int(page, 1))
    per_page = max(1, safe_int(per_page, 10))
    offset = (page - 1) * per_page

    conn = get_db()
    total_row = conn.execute("SELECT COUNT(*) FROM shop_products").fetchone()
    total = safe_int(total_row[0]) if total_row else 0

    rows = conn.execute(
        """
        SELECT *
        FROM shop_products
        ORDER BY sort_order ASC, id DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset),
    ).fetchall()
    conn.close()

    total_pages = max(1, math.ceil(total / per_page)) if total else 1
    return rows, total_pages


def create_product(data: dict[str, Any]) -> int:
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
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data.get("photo_file_id", ""),
            data.get("title_ru", ""),
            data.get("title_uz", ""),
            data.get("description_ru", ""),
            data.get("description_uz", ""),
            data.get("sizes", ""),
            data.get("category_slug", "casual"),
            safe_int(data.get("price", 0)),
            safe_int(data.get("old_price", 0)),
            safe_int(data.get("stock_qty", 0)),
            safe_int(data.get("is_published", 1)),
            safe_int(data.get("sort_order", 100)),
            now,
            now,
        ),
    )
    product_id = cur.lastrowid
    conn.commit()
    conn.close()
    return product_id


def update_product_field(product_id: int, field_name: str, field_value: Any) -> None:
    conn = get_db()
    conn.execute(
        f"UPDATE shop_products SET {field_name} = ?, updated_at = ? WHERE id = ?",
        (field_value, utc_now_iso(), product_id),
    )
    conn.commit()
    conn.close()


def delete_product(product_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM shop_products WHERE id = ?", (product_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def normalize_sizes_input(text: str) -> str:
    text = (text or "").replace(";", ",")
    return sizes_to_string(parse_sizes_string(text))


def product_card_text(product_row: sqlite3.Row) -> str:
    return (
        f"🧷 <b>Товар #{product_row['id']}</b>\n\n"
        f"<b>RU:</b> {product_row['title_ru']}\n"
        f"<b>UZ:</b> {product_row['title_uz']}\n"
        f"<b>Описание RU:</b> {product_row['description_ru'] or '—'}\n"
        f"<b>Описание UZ:</b> {product_row['description_uz'] or '—'}\n"
        f"<b>Размеры:</b> {product_row['sizes'] or '—'}\n"
        f"<b>Категория:</b> {product_row['category_slug']}\n"
        f"<b>Цена:</b> {fmt_sum(product_row['price'])}\n"
        f"<b>Старая цена:</b> {fmt_sum(product_row['old_price']) if safe_int(product_row['old_price']) > 0 else '—'}\n"
        f"<b>Остаток:</b> {product_row['stock_qty']}\n"
        f"<b>Опубликован:</b> {'Да' if safe_int(product_row['is_published']) else 'Нет'}\n"
        f"<b>Sort order:</b> {product_row['sort_order']}\n"
    )


def products_browser_keyboard(page: int, total_pages: int, rows: list[sqlite3.Row]) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []

    for row in rows:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"#{row['id']} {row['title_ru'][:24]}",
                    callback_data=f"product:open:{row['id']}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"products:page:{page-1}"))

    nav_row.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))

    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"products:page:{page+1}"))

    keyboard.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# ============================================================
# CART HELPERS
# ============================================================

def get_cart_rows(user_id: int) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM carts
        WHERE user_id = ?
        ORDER BY id ASC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def get_cart_totals(user_id: int) -> tuple[int, int]:
    rows = get_cart_rows(user_id)
    total_qty = 0
    total_amount = 0

    for row in rows:
        qty = safe_int(row["qty"])
        price = safe_int(row["price"])
        total_qty += qty
        total_amount += price * qty

    return total_qty, total_amount


def clear_cart_for_user(user_id: int) -> None:
    conn = get_db()
    conn.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def remove_cart_item(cart_id: int, user_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM carts WHERE id = ? AND user_id = ?", (cart_id, user_id))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def add_to_cart(*, user_id: int, product_id: int, qty: int, size: str) -> tuple[bool, str]:
    product = get_product_by_id(product_id)
    if not product:
        return False, "cart_item_not_found"

    if safe_int(product["stock_qty"]) <= 0:
        return False, "cart_item_no_stock"

    if qty <= 0:
        return False, "cart_invalid_qty"

    size = (size or "").strip()
    allowed_sizes = parse_sizes_string(product["sizes"])

    if allowed_sizes and not size:
        return False, "cart_size_required"

    if allowed_sizes and size not in allowed_sizes:
        return False, "cart_size_required"

    conn = get_db()
    cur = conn.cursor()

    existing = cur.execute(
        """
        SELECT * FROM carts
        WHERE user_id = ? AND product_id = ? AND size = ?
        LIMIT 1
        """,
        (user_id, product_id, size),
    ).fetchone()

    if existing:
        new_qty = safe_int(existing["qty"]) + qty
        cur.execute(
            "UPDATE carts SET qty = ? WHERE id = ?",
            (new_qty, existing["id"]),
        )
        conn.commit()
        conn.close()
        return True, "cart_same_item_merged"

    cur.execute(
        """
        INSERT INTO carts (
            user_id,
            product_id,
            product_name,
            price,
            qty,
            size,
            photo_file_id,
            added_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            product_id,
            product["title_ru"],
            safe_int(product["price"]),
            qty,
            size,
            product["photo_file_id"],
            utc_now_iso(),
        ),
    )
    conn.commit()
    conn.close()
    return True, "cart_item_added"


def order_items_from_cart(user_id: int) -> list[dict[str, Any]]:
    rows = get_cart_rows(user_id)
    items: list[dict[str, Any]] = []

    for row in rows:
        price = safe_int(row["price"])
        qty = safe_int(row["qty"])
        subtotal = price * qty
        items.append(
            {
                "cart_id": row["id"],
                "product_id": row["product_id"],
                "product_name": row["product_name"],
                "price": price,
                "qty": qty,
                "size": row["size"] or "",
                "photo_file_id": row["photo_file_id"] or "",
                "subtotal": subtotal,
            }
        )
    return items


def cart_items_api(user_id: int) -> dict[str, Any]:
    items = order_items_from_cart(user_id)
    total_qty, total_amount = get_cart_totals(user_id)
    return {
        "items": items,
        "total_qty": total_qty,
        "total_amount": total_amount,
    }


def cart_text(user_id: int) -> str:
    lang = get_user_lang(user_id)
    rows = get_cart_rows(user_id)

    if not rows:
        return t(lang, "cart_empty")

    total_qty, total_amount = get_cart_totals(user_id)
    lines = [t(lang, "cart_title"), ""]

    for idx, row in enumerate(rows, start=1):
        size_part = f" | {row['size']}" if row["size"] else ""
        subtotal = safe_int(row["price"]) * safe_int(row["qty"])
        lines.append(
            f"{idx}. <b>{row['product_name']}</b>{size_part}\n"
            f"   {fmt_sum(row['price'])} × {row['qty']} = <b>{fmt_sum(subtotal)}</b>"
        )

    lines += [
        "",
        f"{t(lang, 'cart_total_qty')}: <b>{total_qty}</b>",
        f"{t(lang, 'cart_total_amount')}: <b>{fmt_sum(total_amount)}</b>",
    ]
    return "\n".join(lines)


# ============================================================
# ORDER HELPERS
# ============================================================

def get_order_by_id(order_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM orders WHERE id = ?",
        (order_id,),
    ).fetchone()
    conn.close()
    return row


def get_orders_for_user(user_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (user_id, limit),
    ).fetchall()
    conn.close()
    return rows


def get_unseen_orders(limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE manager_seen = 0
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def get_all_orders(limit: int = 50) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM orders
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def mark_order_seen(order_id: int, manager_id: int) -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET manager_seen = 1, manager_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (manager_id, utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()


def render_order_items(items_json: str) -> str:
    try:
        items = json.loads(items_json or "[]")
    except Exception:
        items = []

    if not items:
        return "—"

    lines: list[str] = []
    for idx, item in enumerate(items, start=1):
        name = item.get("product_name") or item.get("name") or "—"
        qty = safe_int(item.get("qty"), 1)
        price = safe_int(item.get("price"), 0)
        subtotal = safe_int(item.get("subtotal"), price * qty)
        size = item.get("size") or ""
        size_part = f" | {size}" if size else ""
        lines.append(f"{idx}. {name}{size_part} — {qty} × {fmt_sum(price)} = {fmt_sum(subtotal)}")
    return "\n".join(lines)


def build_order_summary_for_user(order_row: sqlite3.Row, lang: str) -> str:
    return "\n".join(
        [
            f"<b>{t(lang, 'order_number')}:</b> #{order_row['id']}",
            f"<b>{t(lang, 'order_date')}:</b> {order_row['created_at']}",
            f"<b>{t(lang, 'order_status')}:</b> {status_label(lang, order_row['status'])}",
            f"<b>{t(lang, 'order_payment_method')}:</b> {payment_method_label(lang, order_row['payment_method'] or '')}",
            f"<b>{t(lang, 'order_payment_status')}:</b> {payment_status_label(lang, order_row['payment_status'])}",
            f"<b>{t(lang, 'order_delivery_service')}:</b> {delivery_label(lang, order_row['delivery_service'] or '')}",
            f"<b>{t(lang, 'order_total_amount')}:</b> {fmt_sum(order_row['total_amount'])}",
        ]
    )


def admin_order_card_text(order_row: sqlite3.Row, lang: str = "ru") -> str:
    username_text = mask_username(order_row["username"])

    if order_row["latitude"] is not None and order_row["longitude"] is not None:
        location_text = f"{order_row['latitude']}, {order_row['longitude']}"
    else:
        location_text = "—"

    return (
        f"📦 <b>Заказ #{order_row['id']}</b>\n\n"
        f"<b>Имя:</b> {order_row['customer_name'] or '—'}\n"
        f"<b>Телефон:</b> {order_row['customer_phone'] or '—'}\n"
        f"<b>Username:</b> {username_text}\n"
        f"<b>User ID:</b> {order_row['user_id']}\n"
        f"<b>Город:</b> {order_row['city'] or '—'}\n"
        f"<b>Доставка:</b> {delivery_label(lang, order_row['delivery_service'] or '')}\n"
        f"<b>Тип адреса:</b> {address_type_label(lang, order_row['delivery_type'] or '')}\n"
        f"<b>Адрес:</b> {order_row['delivery_address'] or '—'}\n"
        f"<b>Локация:</b> {location_text}\n"
        f"<b>ПВЗ код:</b> {order_row['pvz_code'] or '—'}\n"
        f"<b>ПВЗ адрес:</b> {order_row['pvz_address'] or '—'}\n"
        f"<b>Способ оплаты:</b> {payment_method_label(lang, order_row['payment_method'] or '')}\n"
        f"<b>Статус оплаты:</b> {payment_status_label(lang, order_row['payment_status'])}\n"
        f"<b>Комментарий:</b> {order_row['comment'] or '—'}\n"
        f"<b>Список товаров:</b>\n{render_order_items(order_row['items'])}\n\n"
        f"<b>Количество:</b> {order_row['total_qty']}\n"
        f"<b>Сумма:</b> {fmt_sum(order_row['total_amount'])}\n"
        f"<b>Источник:</b> {source_label(lang, order_row['source'])}\n"
        f"<b>Статус заказа:</b> {status_label(lang, order_row['status'])}\n"
    )


def create_order_from_checkout(
    *,
    user_id: int,
    username: str,
    checkout_data: dict[str, Any],
    source: str = "telegram",
) -> int:
    items = order_items_from_cart(user_id)
    total_qty, total_amount = get_cart_totals(user_id)

    payment_method = checkout_data.get("payment_method") or ""
    payment_provider_url = ""

    now = utc_now_iso()

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (
            user_id,
            username,
            customer_name,
            customer_phone,
            city,
            items,
            total_qty,
            total_amount,
            delivery_service,
            delivery_type,
            delivery_address,
            latitude,
            longitude,
            pvz_code,
            pvz_address,
            payment_method,
            payment_status,
            payment_provider_invoice_id,
            payment_provider_url,
            comment,
            status,
            manager_seen,
            manager_id,
            source,
            created_at,
            updated_at,
            reminded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            checkout_data.get("delivery_service") or "",
            checkout_data.get("delivery_type") or "",
            checkout_data.get("delivery_address") or "",
            checkout_data.get("latitude"),
            checkout_data.get("longitude"),
            checkout_data.get("pvz_code") or "",
            checkout_data.get("pvz_address") or "",
            payment_method,
            "pending",
            "",
            payment_provider_url,
            checkout_data.get("comment") or "",
            "new",
            0,
            None,
            source,
            now,
            now,
            None,
        ),
    )
    order_id = cur.lastrowid

    if payment_method == "click":
        payment_provider_url = f"{BASE_URL}/pay/click/{order_id}"
    elif payment_method == "payme":
        payment_provider_url = f"{BASE_URL}/pay/payme/{order_id}"

    cur.execute(
        """
        UPDATE orders
        SET payment_provider_url = ?, updated_at = ?
        WHERE id = ?
        """,
        (payment_provider_url, utc_now_iso(), order_id),
    )

    cur.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

    log_event(
        user_id,
        "order_created",
        {
            "order_id": order_id,
            "source": source,
            "payment_method": payment_method,
            "delivery_service": checkout_data.get("delivery_service"),
            "total_amount": total_amount,
            "total_qty": total_qty,
        },
    )

    return order_id


def delivery_service_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {
        t(user_id, "delivery_yandex_courier"): "yandex_courier",
        t(user_id, "delivery_b2b_post"): "b2b_post",
        t(user_id, "delivery_yandex_pvz"): "yandex_pvz",
    }
    return mapping.get((text or "").strip())


def address_type_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {
        t(user_id, "address_location"): "location",
        t(user_id, "address_manual"): "manual",
    }
    return mapping.get((text or "").strip())


def payment_method_from_label(user_id: int, text: str) -> Optional[str]:
    mapping = {
        t(user_id, "payment_click"): "click",
        t(user_id, "payment_payme"): "payme",
    }
    return mapping.get((text or "").strip())


def build_checkout_summary(user_id: int, data: dict[str, Any]) -> str:
    items = order_items_from_cart(user_id)
    total_qty, total_amount = get_cart_totals(user_id)

    lines = [
        t(user_id, "checkout_summary"),
        "",
        f"<b>{t(user_id, 'checkout_name_label')}:</b> {data.get('customer_name') or '—'}",
        f"<b>{t(user_id, 'checkout_phone_label')}:</b> {data.get('customer_phone') or '—'}",
        f"<b>{t(user_id, 'checkout_city_label')}:</b> {data.get('city') or '—'}",
        f"<b>{t(user_id, 'checkout_delivery_label')}:</b> {delivery_label(user_id, data.get('delivery_service') or '')}",
        f"<b>{t(user_id, 'checkout_address_type_label')}:</b> {address_type_label(user_id, data.get('delivery_type') or '')}",
    ]

    if data.get("delivery_service") == "yandex_pvz":
        pvz_value = data.get("pvz_code") or data.get("pvz_address") or "—"
        lines.append(f"<b>{t(user_id, 'checkout_pvz_label')}:</b> {pvz_value}")
    else:
        lines.append(f"<b>{t(user_id, 'checkout_address_label')}:</b> {data.get('delivery_address') or '—'}")

    if data.get("latitude") is not None and data.get("longitude") is not None:
        lines.append(
            f"<b>{t(user_id, 'checkout_location_label')}:</b> "
            f"{data.get('latitude')}, {data.get('longitude')}"
        )

    lines += [
        f"<b>{t(user_id, 'checkout_payment_label')}:</b> {payment_method_label(user_id, data.get('payment_method') or '')}",
        f"<b>{t(user_id, 'checkout_comment_label')}:</b> {data.get('comment') or '—'}",
        "",
        f"<b>{t(user_id, 'checkout_items_label')}:</b>",
    ]

    if not items:
        lines.append("—")
    else:
        for idx, item in enumerate(items, start=1):
            size_part = f" | {item['size']}" if item["size"] else ""
            lines.append(
                f"{idx}. {item['product_name']}{size_part} — "
                f"{item['qty']} × {fmt_sum(item['price'])} = <b>{fmt_sum(item['subtotal'])}</b>"
            )

    lines += [
        "",
        f"<b>{t(user_id, 'cart_total_qty')}:</b> {total_qty}",
        f"<b>{t(user_id, 'checkout_total_label')}:</b> {fmt_sum(total_amount)}",
        "",
        t(user_id, "checkout_confirm_hint"),
    ]
    return "\n".join(lines)


# ============================================================
# REVIEW HELPERS
# ============================================================

def create_review(
    *,
    user_id: int,
    username: str,
    customer_name: str,
    rating: int,
    text: str,
    order_id: int | None = None,
) -> int:
    conn = get_db()
    cur = conn.cursor()
    now = utc_now_iso()

    cur.execute(
        """
        INSERT INTO reviews (
            user_id,
            username,
            customer_name,
            rating,
            text,
            is_published,
            admin_seen,
            order_id,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
        """,
        (
            user_id,
            username or "",
            customer_name or "",
            safe_int(rating, 5),
            text.strip(),
            order_id,
            now,
            now,
        ),
    )
    review_id = cur.lastrowid
    conn.commit()
    conn.close()
    return review_id


def get_published_reviews(limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM reviews
        WHERE is_published = 1
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def get_pending_reviews(limit: int = 30) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM reviews
        WHERE admin_seen = 0 OR is_published = 0
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def get_review_by_id(review_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM reviews WHERE id = ?",
        (review_id,),
    ).fetchone()
    conn.close()
    return row


def set_review_published(review_id: int, value: int) -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE reviews
        SET is_published = ?, admin_seen = 1, updated_at = ?
        WHERE id = ?
        """,
        (1 if safe_int(value) else 0, utc_now_iso(), review_id),
    )
    conn.commit()
    conn.close()


def delete_review(review_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM reviews WHERE id = ?", (review_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def review_card_text(review_row: sqlite3.Row) -> str:
    name = review_row["customer_name"] or "—"
    username = mask_username(review_row["username"])
    order_part = f"\n<b>Order ID:</b> #{review_row['order_id']}" if review_row["order_id"] else ""

    return (
        f"⭐ <b>Отзыв #{review_row['id']}</b>\n\n"
        f"<b>Имя:</b> {name}\n"
        f"<b>Username:</b> {username}\n"
        f"<b>User ID:</b> {review_row['user_id']}\n"
        f"<b>Оценка:</b> {stars_text(review_row['rating'])} ({review_row['rating']}/5)\n"
        f"<b>Текст:</b>\n{review_row['text']}\n"
        f"<b>Опубликован:</b> {'Да' if safe_int(review_row['is_published']) else 'Нет'}\n"
        f"<b>Просмотрен:</b> {'Да' if safe_int(review_row['admin_seen']) else 'Нет'}"
        f"{order_part}\n"
        f"<b>Дата:</b> {review_row['created_at']}"
    )


# ============================================================
# STATS HELPERS
# ============================================================

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
        "unique_users": scalar("SELECT COUNT(*) FROM users"),
        "products": scalar("SELECT COUNT(*) FROM shop_products"),
        "reviews": scalar("SELECT COUNT(*) FROM reviews WHERE is_published = 1"),
    }
    conn.close()
    return stats


# ============================================================
# FILE HELPERS
# ============================================================

async def get_file_url_by_file_id(file_id: str) -> str:
    if not file_id:
        return ""
    try:
        file = await bot.get_file(file_id)
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    except Exception:
        return ""


# ============================================================
# SMALL COMMON HELPERS
# ============================================================

async def maybe_cancel_state(message: Message, state: FSMContext, back_to_admin: bool = False) -> bool:
    if (message.text or "").strip() == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=admin_main_menu(message.from_user.id) if back_to_admin else user_main_menu(message.from_user.id),
        )
        return True
    return False


def request_lang(request: web.Request) -> str:
    raw = request.query.get("lang", DEFAULT_LANGUAGE)
    return ensure_lang(raw)


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
        "is_published": safe_int(row["is_published"]),
        "sort_order": safe_int(row["sort_order"]),
    }


async def send_order_success_to_user(message: Message, order_id: int) -> None:
    lang = get_user_lang(message.from_user.id)
    text = (
        f"{t(lang, 'order_created_title')}\n\n"
        f"{t(lang, 'order_created_text')}\n\n"
        f"<b>{t(lang, 'order_number')}:</b> #{order_id}\n\n"
        f"{t(lang, 'order_links_text')}"
    )
    await message.answer(
        text,
        reply_markup=user_main_menu(message.from_user.id),
    )
    if CHANNEL_LINK or INSTAGRAM_LINK or YOUTUBE_LINK:
        await message.answer("Наши ссылки:", reply_markup=social_links_keyboard())


async def notify_admins_about_order(order_id: int) -> None:
    order = get_order_by_id(order_id)
    if not order:
        return

    text = admin_order_card_text(order, "ru")
    kb = admin_order_actions_keyboard(order["id"], order["user_id"])

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception as exc:
            logger.exception("Failed to send order notification to admin %s: %s", admin_id, exc)


async def notify_admins_about_review(review_id: int) -> None:
    review = get_review_by_id(review_id)
    if not review:
        return

    text = review_card_text(review)
    kb = review_admin_actions_keyboard(review["id"], safe_int(review["is_published"]))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception as exc:
            logger.exception("Failed to send review notification to admin %s: %s", admin_id, exc)


async def notify_user_about_order_update(order_row: sqlite3.Row) -> None:
    if not order_row or not order_row["user_id"]:
        return
    try:
        text = (
            f"📦 <b>Заказ #{order_row['id']}</b>\n\n"
            f"<b>Статус заказа:</b> {status_label('ru', order_row['status'])}\n"
            f"<b>Статус оплаты:</b> {payment_status_label('ru', order_row['payment_status'])}\n"
            f"<b>Сумма:</b> {fmt_sum(order_row['total_amount'])}"
        )
        await bot.send_message(order_row["user_id"], text)
    except Exception:
        pass


def seed_demo_products_if_empty() -> None:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) FROM shop_products").fetchone()
    count = safe_int(row[0]) if row else 0
    if count > 0:
        conn.close()
        return

    now = utc_now_iso()
    demo = [
        (
            "",
            "Школьный классический костюм",
            "Maktab klassik kostyumi",
            "Премиальная школьная классика для детей.",
            "Bolalar uchun premium maktab klassikasi.",
            "110, 116, 122, 128",
            "school",
            289000,
            329000,
            15,
            1,
            10,
            now,
            now,
        ),
        (
            "",
            "Повседневный мягкий комплект",
            "Kundalik yumshoq to'plam",
            "Удобный комплект на каждый день.",
            "Har kun uchun qulay to'plam.",
            "116, 122, 128, 134",
            "casual",
            249000,
            0,
            12,
            1,
            20,
            now,
            now,
        ),
        (
            "",
            "Лимитированная белая коллекция",
            "Limitlangan oq kolleksiya",
            "Лимитированная премиальная коллекция.",
            "Limitlangan premium kolleksiya.",
            "122, 128, 134, 140",
            "limited",
            359000,
            399000,
            7,
            1,
            30,
            now,
            now,
        ),
    ]

    conn.executemany(
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
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        demo,
    )
    conn.commit()
    conn.close()


def patch_database_defaults() -> None:
    conn = get_db()

    conn.execute(
        """
        UPDATE orders
        SET updated_at = COALESCE(updated_at, created_at, ?)
        """,
        (utc_now_iso(),),
    )
    conn.execute(
        """
        UPDATE orders
        SET status = COALESCE(NULLIF(status, ''), 'new')
        """
    )
    conn.execute(
        """
        UPDATE orders
        SET payment_status = COALESCE(NULLIF(payment_status, ''), 'pending')
        """
    )
    conn.execute(
        """
        UPDATE shop_products
        SET updated_at = COALESCE(updated_at, created_at, ?)
        """,
        (utc_now_iso(),),
    )
    conn.execute(
        """
        UPDATE shop_products
        SET category_slug = COALESCE(NULLIF(category_slug, ''), 'casual')
        """
    )
    conn.execute(
        """
        UPDATE shop_products
        SET sizes = COALESCE(sizes, '')
        """
    )

    conn.commit()
    conn.close()

# ============================================================
# PART 3 / 10
# user handlers, language, contacts, size picker, reviews,
# user orders, cart view, admin entry points
# ============================================================

# ============================================================
# USER / START / MENU
# ============================================================

@user_router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    upsert_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )
    log_event(message.from_user.id, "start")

    await message.answer(
        t(message.from_user.id, "welcome"),
        reply_markup=user_main_menu(message.from_user.id),
    )
    await message.answer(t(message.from_user.id, "main_menu_hint"))


@user_router.message(Command("menu"))
async def cmd_menu(message: Message) -> None:
    upsert_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )
    await message.answer(
        t(message.from_user.id, "main_menu_hint"),
        reply_markup=user_main_menu(message.from_user.id),
    )


@user_router.message(Command("id"))
async def show_user_id_command(message: Message) -> None:
    await message.answer(f"Ваш user_id: <code>{message.from_user.id}</code>")


# ============================================================
# LANGUAGE
# ============================================================

@user_router.message(F.text.in_([TEXTS["ru"]["menu_lang"], TEXTS["uz"]["menu_lang"]]))
async def choose_language(message: Message) -> None:
    await message.answer(
        t(message.from_user.id, "choose_lang"),
        reply_markup=language_keyboard(),
    )


@user_router.callback_query(F.data.startswith("lang:set:"))
async def set_language_callback(callback: CallbackQuery) -> None:
    parts = callback.data.split(":")
    lang = parts[-1]
    if lang not in SUPPORTED_LANGS:
        await callback.answer()
        return

    set_user_lang(callback.from_user.id, lang)
    await callback.message.answer(
        t(lang, "lang_updated"),
        reply_markup=user_main_menu(callback.from_user.id),
    )
    await callback.answer()


# ============================================================
# CONTACTS
# ============================================================

@user_router.message(F.text.in_([TEXTS["ru"]["menu_contacts"], TEXTS["uz"]["menu_contacts"]]))
async def contacts_handler(message: Message) -> None:
    lang = get_user_lang(message.from_user.id)
    text = (
        f"{t(lang, 'contacts_title')}\n\n"
        f"<b>{t(lang, 'contacts_phone')}:</b> {MANAGER_PHONE}\n"
        f"<b>{t(lang, 'contacts_manager')}:</b> {MANAGER_TG}\n"
        f"<b>{t(lang, 'contacts_channel')}:</b> {CHANNEL_LINK or '—'}\n"
        f"<b>{t(lang, 'contacts_instagram')}:</b> {INSTAGRAM_LINK or '—'}\n"
        f"<b>{t(lang, 'contacts_youtube')}:</b> {YOUTUBE_LINK or '—'}"
    )
    await message.answer(text, reply_markup=social_links_keyboard() if (CHANNEL_LINK or INSTAGRAM_LINK or YOUTUBE_LINK) else None)


@user_router.message(Command("links"))
async def links_command(message: Message) -> None:
    text = (
        f"Telegram: {CHANNEL_LINK or '—'}\n"
        f"Instagram: {INSTAGRAM_LINK or '—'}\n"
        f"YouTube: {YOUTUBE_LINK or '—'}"
    )
    await message.answer(text, reply_markup=social_links_keyboard() if (CHANNEL_LINK or INSTAGRAM_LINK or YOUTUBE_LINK) else None)


# ============================================================
# SIZE PICKER
# ============================================================

@user_router.message(F.text.in_([TEXTS["ru"]["menu_size"], TEXTS["uz"]["menu_size"]]))
async def size_picker_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SizePickerStates.waiting_for_value)
    await message.answer(
        t(message.from_user.id, "size_intro"),
        reply_markup=size_picker_keyboard(message.from_user.id),
    )


@user_router.message(SizePickerStates.waiting_for_value)
async def size_picker_value(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()

    if text == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=user_main_menu(message.from_user.id),
        )
        return

    if not text.isdigit():
        await message.answer(t(message.from_user.id, "size_not_found"))
        return

    value = int(text)

    if value in SIZE_BY_AGE:
        result = (
            f"{t(message.from_user.id, 'size_result_age')}: "
            f"<b>{SIZE_BY_AGE[value]}</b>\n\n"
            f"{t(message.from_user.id, 'size_hint_extra')}"
        )
        await state.clear()
        await message.answer(result, reply_markup=user_main_menu(message.from_user.id))
        return

    if value in SIZE_BY_HEIGHT:
        result = (
            f"{t(message.from_user.id, 'size_result_height')}: "
            f"<b>{SIZE_BY_HEIGHT[value]}</b>\n\n"
            f"{t(message.from_user.id, 'size_hint_extra')}"
        )
        await state.clear()
        await message.answer(result, reply_markup=user_main_menu(message.from_user.id))
        return

    await message.answer(t(message.from_user.id, "size_not_found"))


# ============================================================
# REVIEWS / USER
# ============================================================

@reviews_router.message(F.text.in_([TEXTS["ru"]["menu_reviews"], TEXTS["uz"]["menu_reviews"]]))
async def reviews_list_handler(message: Message) -> None:
    lang = get_user_lang(message.from_user.id)
    rows = get_published_reviews(limit=20)

    if not rows:
        await message.answer(t(lang, "reviews_empty"))
        return

    await message.answer(t(lang, "reviews_title"))
    for row in rows:
        name = row["customer_name"] or "Клиент"
        text = (
            f"{stars_text(row['rating'])}\n"
            f"<b>{name}</b>\n"
            f"{row['text']}"
        )
        await message.answer(text)


@reviews_router.message(F.text.in_([TEXTS["ru"]["menu_leave_review"], TEXTS["uz"]["menu_leave_review"]]))
async def review_start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ReviewStates.rating)
    await message.answer(
        t(message.from_user.id, "review_rating_ask"),
        reply_markup=review_rating_keyboard(message.from_user.id),
    )


@reviews_router.message(ReviewStates.rating)
async def review_rating_handler(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()

    if text == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=user_main_menu(message.from_user.id),
        )
        return

    if text not in {"1", "2", "3", "4", "5"}:
        await message.answer(t(message.from_user.id, "review_bad_rating"))
        return

    await state.update_data(rating=int(text))
    await state.set_state(ReviewStates.text)
    await message.answer(
        t(message.from_user.id, "review_text_ask"),
        reply_markup=checkout_cancel_keyboard(message.from_user.id),
    )


@reviews_router.message(ReviewStates.text)
async def review_text_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer(t(message.from_user.id, "review_text_ask"))
        return

    data = await state.get_data()
    rating = safe_int(data.get("rating"), 5)

    review_id = create_review(
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        customer_name=message.from_user.full_name or "",
        rating=rating,
        text=text,
        order_id=None,
    )

    await state.clear()
    await message.answer(
        t(message.from_user.id, "review_sent"),
        reply_markup=user_main_menu(message.from_user.id),
    )

    await notify_admins_about_review(review_id)


# ============================================================
# USER ORDERS
# ============================================================

@orders_router.message(F.text.in_([TEXTS["ru"]["menu_orders"], TEXTS["uz"]["menu_orders"]]))
async def my_orders_handler(message: Message) -> None:
    lang = get_user_lang(message.from_user.id)
    orders = get_orders_for_user(message.from_user.id, limit=15)

    if not orders:
        await message.answer(t(lang, "my_orders_empty"))
        return

    await message.answer(t(lang, "my_orders_title"))

    for row in orders:
        text = (
            f"<b>{t(lang, 'order_number')}:</b> #{row['id']}\n"
            f"<b>{t(lang, 'order_date')}:</b> {row['created_at']}\n"
            f"<b>{t(lang, 'order_status')}:</b> {status_label(lang, row['status'])}\n"
            f"<b>{t(lang, 'order_payment_method')}:</b> {payment_method_label(lang, row['payment_method'] or '')}\n"
            f"<b>{t(lang, 'order_payment_status')}:</b> {payment_status_label(lang, row['payment_status'])}\n"
            f"<b>{t(lang, 'order_delivery_service')}:</b> {delivery_label(lang, row['delivery_service'] or '')}\n"
            f"<b>{t(lang, 'order_total_amount')}:</b> {fmt_sum(row['total_amount'])}\n"
            f"<b>{t(lang, 'order_items')}:</b>\n{render_order_items(row['items'])}"
        )
        await message.answer(text)


# ============================================================
# USER CART VIEW
# ============================================================

@cart_router.message(F.text.in_([TEXTS["ru"]["menu_cart"], TEXTS["uz"]["menu_cart"]]))
async def cart_view_handler(message: Message) -> None:
    rows = get_cart_rows(message.from_user.id)
    if not rows:
        await message.answer(t(message.from_user.id, "cart_empty"))
        return

    await message.answer(
        cart_text(message.from_user.id),
        reply_markup=cart_keyboard(message.from_user.id),
    )


@cart_router.callback_query(F.data == "cart:clear")
async def cart_clear_callback(callback: CallbackQuery) -> None:
    clear_cart_for_user(callback.from_user.id)
    await callback.message.answer(
        t(callback.from_user.id, "cart_cleared"),
        reply_markup=user_main_menu(callback.from_user.id),
    )
    await callback.answer()


# ============================================================
# WEBAPP DATA HANDLER
# ============================================================

@cart_router.message(F.web_app_data)
async def web_app_data_handler(message: Message) -> None:
    upsert_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    raw = message.web_app_data.data if message.web_app_data else ""
    try:
        payload = json.loads(raw)
    except Exception:
        await message.answer(t(message.from_user.id, "cart_bad_payload"))
        return

    action = (payload.get("action") or "").strip()

    if action == "add_to_cart":
        product_id = safe_int(payload.get("product_id"))
        qty = safe_int(payload.get("qty"), 1)
        size = (payload.get("size") or "").strip()

        ok, msg_key = add_to_cart(
            user_id=message.from_user.id,
            product_id=product_id,
            qty=qty,
            size=size,
        )

        log_event(
            message.from_user.id,
            "webapp_add_to_cart",
            {"product_id": product_id, "qty": qty, "size": size, "ok": ok},
        )
        await message.answer(t(message.from_user.id, msg_key))
        return

    if action == "buy_now":
        product_id = safe_int(payload.get("product_id"))
        qty = safe_int(payload.get("qty"), 1)
        size = (payload.get("size") or "").strip()

        ok, msg_key = add_to_cart(
            user_id=message.from_user.id,
            product_id=product_id,
            qty=qty,
            size=size,
        )

        await message.answer(t(message.from_user.id, msg_key))
        if ok:
            await message.answer(
                cart_text(message.from_user.id),
                reply_markup=cart_keyboard(message.from_user.id),
            )
        return

    if action == "remove_from_cart":
        cart_id = safe_int(payload.get("cart_id"))
        removed = remove_cart_item(cart_id=cart_id, user_id=message.from_user.id)
        if removed:
            await message.answer(t(message.from_user.id, "cart_removed"))
        else:
            await message.answer(t(message.from_user.id, "cart_item_not_found"))
        return

    if action == "clear_cart":
        clear_cart_for_user(message.from_user.id)
        await message.answer(t(message.from_user.id, "cart_cleared"))
        return

    await message.answer(t(message.from_user.id, "cart_bad_payload"))


# ============================================================
# ADMIN ENTRY
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["menu_admin"], TEXTS["uz"]["menu_admin"]]))
async def admin_menu_open(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    await message.answer(
        t(message.from_user.id, "admin_title"),
        reply_markup=admin_main_menu(message.from_user.id),
    )


@admin_router.message(F.text.in_([TEXTS["ru"]["admin_back_to_user"], TEXTS["uz"]["admin_back_to_user"]]))
async def admin_back_to_user_menu(message: Message) -> None:
    await message.answer(
        t(message.from_user.id, "main_menu_hint"),
        reply_markup=user_main_menu(message.from_user.id),
    )


# ============================================================
# ADMIN STATS
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_stats"], TEXTS["uz"]["admin_stats"]]))
async def admin_stats_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    lang = get_user_lang(message.from_user.id)
    stats = get_basic_stats()

    text = (
        f"📊 <b>{t(lang, 'admin_stats')}</b>\n\n"
        f"<b>{t(lang, 'stats_total_orders')}:</b> {stats['total_orders']}\n"
        f"<b>{t(lang, 'stats_new')}:</b> {stats['new']}\n"
        f"<b>{t(lang, 'stats_processing')}:</b> {stats['processing']}\n"
        f"<b>{t(lang, 'stats_confirmed')}:</b> {stats['confirmed']}\n"
        f"<b>{t(lang, 'stats_paid')}:</b> {stats['paid']}\n"
        f"<b>{t(lang, 'stats_sent')}:</b> {stats['sent']}\n"
        f"<b>{t(lang, 'stats_delivered')}:</b> {stats['delivered']}\n"
        f"<b>{t(lang, 'stats_cancelled')}:</b> {stats['cancelled']}\n"
        f"<b>{t(lang, 'stats_unique_users')}:</b> {stats['unique_users']}\n"
        f"<b>{t(lang, 'stats_products')}:</b> {stats['products']}\n"
        f"<b>{t(lang, 'stats_reviews')}:</b> {stats['reviews']}"
    )
    await message.answer(text)


# ============================================================
# ADMIN ORDERS PREVIEW
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_new_orders"], TEXTS["uz"]["admin_new_orders"]]))
async def admin_new_orders_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_unseen_orders(limit=20)
    if not rows:
        await message.answer(t(message.from_user.id, "admin_orders_empty"))
        return

    for row in rows:
        mark_order_seen(row["id"], message.from_user.id)
        preview = (
            f"📦 <b>Заказ #{row['id']}</b>\n"
            f"{row['customer_name'] or '—'}\n"
            f"{fmt_sum(row['total_amount'])}\n"
            f"{status_label(message.from_user.id, row['status'])}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Открыть",
                        callback_data=f"admin:order:open:{row['id']}",
                    )
                ]
            ]
        )
        await message.answer(preview, reply_markup=kb)


@admin_router.message(F.text.in_([TEXTS["ru"]["admin_all_orders"], TEXTS["uz"]["admin_all_orders"]]))
async def admin_all_orders_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_all_orders(limit=20)
    if not rows:
        await message.answer(t(message.from_user.id, "empty"))
        return

    for row in rows:
        preview = (
            f"📦 <b>Заказ #{row['id']}</b>\n"
            f"{row['customer_name'] or '—'}\n"
            f"{fmt_sum(row['total_amount'])}\n"
            f"{status_label(message.from_user.id, row['status'])}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Открыть",
                        callback_data=f"admin:order:open:{row['id']}",
                    )
                ]
            ]
        )
        await message.answer(preview, reply_markup=kb)


@admin_router.callback_query(F.data.startswith("admin:order:open:"))
async def admin_open_order_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    order_id = safe_int(callback.data.split(":")[-1])
    order = get_order_by_id(order_id)
    if not order:
        await callback.message.answer(t(callback.from_user.id, "admin_order_not_found"))
        await callback.answer()
        return

    mark_order_seen(order_id, callback.from_user.id)
    await callback.message.answer(
        admin_order_card_text(order, "ru"),
        reply_markup=admin_order_actions_keyboard(order["id"], order["user_id"]),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("order:set:"))
async def admin_set_order_status_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer()
        return

    order_id = safe_int(parts[2])
    status = parts[3]

    if status not in ORDER_STATUSES:
        await callback.answer()
        return

    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET status = ?, manager_seen = 1, manager_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, callback.from_user.id, utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()

    order = get_order_by_id(order_id)
    if order:
        await notify_user_about_order_update(order)

    await callback.message.answer(t(callback.from_user.id, "admin_status_updated"))
    await callback.answer()


@admin_router.callback_query(F.data.startswith("order:pay:"))
async def admin_set_payment_status_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer()
        return

    order_id = safe_int(parts[2])
    payment_status = parts[3]

    if payment_status not in PAYMENT_STATUSES:
        await callback.answer()
        return

    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET payment_status = ?, manager_seen = 1, manager_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (payment_status, callback.from_user.id, utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()

    order = get_order_by_id(order_id)
    if order:
        await notify_user_about_order_update(order)

    await callback.message.answer(t(callback.from_user.id, "admin_payment_updated"))
    await callback.answer()


# ============================================================
# ADMIN REVIEWS
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_reviews"], TEXTS["uz"]["admin_reviews"]]))
async def admin_reviews_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_pending_reviews(limit=20)
    if not rows:
        await message.answer(t(message.from_user.id, "review_admin_empty"))
        return

    await message.answer(t(message.from_user.id, "review_admin_title"))
    for row in rows:
        await message.answer(
            review_card_text(row),
            reply_markup=review_admin_actions_keyboard(row["id"], safe_int(row["is_published"])),
        )


@admin_router.callback_query(F.data.startswith("review:toggle:"))
async def review_toggle_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer()
        return

    review_id = safe_int(parts[2])
    new_value = safe_int(parts[3], 1)

    set_review_published(review_id, new_value)
    await callback.message.answer(
        t(callback.from_user.id, "review_published") if new_value else t(callback.from_user.id, "review_hidden")
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("review:delete:"))
async def review_delete_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    review_id = safe_int(callback.data.split(":")[-1])
    ok = delete_review(review_id)

    await callback.message.answer(
        t(callback.from_user.id, "review_deleted") if ok else t(callback.from_user.id, "unknown")
    )
    await callback.answer()


# ============================================================
# SMALL COMMANDS FOR ADMIN
# ============================================================

@admin_router.message(Command("health"))
async def admin_health_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    text = (
        "✅ <b>Health</b>\n\n"
        f"BOT_TOKEN: {'set' if BOT_TOKEN else 'missing'}\n"
        f"BASE_URL: {BASE_URL or 'missing'}\n"
        f"CHANNEL_ID: {CHANNEL_ID}\n"
        f"ADMIN_IDS: {', '.join(str(x) for x in sorted(ADMIN_IDS)) if ADMIN_IDS else 'missing'}\n"
        f"ADMIN_PANEL_TOKEN: {'set' if ADMIN_PANEL_TOKEN else 'missing'}\n"
        f"PORT: {PORT}\n"
        f"DB_PATH: {DB_PATH}\n"
    )
    await message.answer(text)

# ============================================================
# PART 4 / 10
# full checkout flow
# ============================================================

# ============================================================
# CHECKOUT START
# ============================================================

@checkout_router.callback_query(F.data == "cart:checkout")
async def checkout_start_callback(callback: CallbackQuery, state: FSMContext) -> None:
    rows = get_cart_rows(callback.from_user.id)
    if not rows:
        await callback.message.answer(t(callback.from_user.id, "cart_empty_for_checkout"))
        await callback.answer()
        return

    await state.clear()
    await state.set_state(CheckoutStates.customer_name)

    await callback.message.answer(
        f"{t(callback.from_user.id, 'checkout_intro')}\n\n{t(callback.from_user.id, 'checkout_name')}",
        reply_markup=checkout_cancel_keyboard(callback.from_user.id),
    )
    await callback.answer()


# ============================================================
# CHECKOUT STEP 1 / NAME
# ============================================================

@checkout_router.message(CheckoutStates.customer_name)
async def checkout_name_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    customer_name = (message.text or "").strip()
    if not customer_name:
        await message.answer(t(message.from_user.id, "checkout_name"))
        return

    await state.update_data(customer_name=customer_name)
    await state.set_state(CheckoutStates.customer_phone)
    await message.answer(t(message.from_user.id, "checkout_phone"))


# ============================================================
# CHECKOUT STEP 2 / PHONE
# ============================================================

@checkout_router.message(CheckoutStates.customer_phone)
async def checkout_phone_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    phone = normalize_phone(message.text or "")
    if not is_valid_phone(phone):
        await message.answer(t(message.from_user.id, "checkout_invalid_phone"))
        return

    await state.update_data(customer_phone=phone)
    await state.set_state(CheckoutStates.delivery_service)
    await message.answer(
        t(message.from_user.id, "checkout_delivery"),
        reply_markup=checkout_delivery_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 3 / DELIVERY SERVICE
# ============================================================

@checkout_router.message(CheckoutStates.delivery_service)
async def checkout_delivery_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    delivery_service = delivery_service_from_label(message.from_user.id, message.text or "")
    if not delivery_service:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return

    await state.update_data(delivery_service=delivery_service)
    await state.set_state(CheckoutStates.delivery_type)
    await message.answer(
        t(message.from_user.id, "checkout_address_type"),
        reply_markup=checkout_address_type_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 4 / ADDRESS TYPE
# ============================================================

@checkout_router.message(CheckoutStates.delivery_type)
async def checkout_delivery_type_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    delivery_type = address_type_from_label(message.from_user.id, message.text or "")
    if not delivery_type:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return

    await state.update_data(delivery_type=delivery_type)
    await state.set_state(CheckoutStates.city)
    await message.answer(
        t(message.from_user.id, "checkout_city"),
        reply_markup=checkout_cancel_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 5 / CITY
# ============================================================

@checkout_router.message(CheckoutStates.city)
async def checkout_city_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    city = (message.text or "").strip()
    if not city:
        await message.answer(t(message.from_user.id, "checkout_city"))
        return

    await state.update_data(city=city)
    data = await state.get_data()

    if data.get("delivery_service") == "yandex_pvz":
        await state.set_state(CheckoutStates.address_or_pvz)
        await message.answer(
            t(message.from_user.id, "checkout_pvz_mode"),
            reply_markup=checkout_cancel_keyboard(message.from_user.id),
        )
        return

    if data.get("delivery_type") == "location":
        await state.set_state(CheckoutStates.location)
        await message.answer(
            t(message.from_user.id, "checkout_location"),
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text=t(message.from_user.id, "address_location"), request_location=True)],
                    [KeyboardButton(text=t(message.from_user.id, "cancel"))],
                ],
                resize_keyboard=True,
            ),
        )
        return

    await state.set_state(CheckoutStates.address_or_pvz)
    await message.answer(
        t(message.from_user.id, "checkout_address"),
        reply_markup=checkout_cancel_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 6A / LOCATION
# ============================================================

@checkout_router.message(CheckoutStates.location)
async def checkout_location_handler(message: Message, state: FSMContext) -> None:
    if message.text and await maybe_cancel_state(message, state, back_to_admin=False):
        return

    if not message.location:
        await message.answer(t(message.from_user.id, "checkout_need_location"))
        return

    await state.update_data(
        latitude=message.location.latitude,
        longitude=message.location.longitude,
        delivery_address="",
    )

    await state.set_state(CheckoutStates.payment_method)
    await message.answer(
        t(message.from_user.id, "checkout_payment"),
        reply_markup=checkout_payment_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 6B / ADDRESS OR PVZ
# ============================================================

@checkout_router.message(CheckoutStates.address_or_pvz)
async def checkout_address_or_pvz_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    raw_value = (message.text or "").strip()
    if not raw_value:
        data = await state.get_data()
        if data.get("delivery_service") == "yandex_pvz":
            await message.answer(t(message.from_user.id, "checkout_pvz_mode"))
        else:
            await message.answer(t(message.from_user.id, "checkout_address"))
        return

    data = await state.get_data()

    if data.get("delivery_service") == "yandex_pvz":
        if len(raw_value) <= 32 and " " not in raw_value:
            await state.update_data(
                pvz_code=raw_value,
                pvz_address="",
                delivery_address="",
            )
        else:
            await state.update_data(
                pvz_code="",
                pvz_address=raw_value,
                delivery_address="",
            )
    else:
        await state.update_data(
            delivery_address=raw_value,
            pvz_code="",
            pvz_address="",
        )

    await state.set_state(CheckoutStates.payment_method)
    await message.answer(
        t(message.from_user.id, "checkout_payment"),
        reply_markup=checkout_payment_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 7 / PAYMENT
# ============================================================

@checkout_router.message(CheckoutStates.payment_method)
async def checkout_payment_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    payment_method = payment_method_from_label(message.from_user.id, message.text or "")
    if not payment_method:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return

    await state.update_data(payment_method=payment_method)
    await state.set_state(CheckoutStates.comment)
    await message.answer(
        t(message.from_user.id, "checkout_comment"),
        reply_markup=checkout_comment_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 8 / COMMENT
# ============================================================

@checkout_router.message(CheckoutStates.comment)
async def checkout_comment_handler(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=False):
        return

    text = (message.text or "").strip()
    if text == t(message.from_user.id, "skip"):
        text = ""

    await state.update_data(comment=text)

    data = await state.get_data()
    await state.set_state(CheckoutStates.confirm)
    await message.answer(
        build_checkout_summary(message.from_user.id, data),
        reply_markup=checkout_confirm_keyboard(message.from_user.id),
    )


# ============================================================
# CHECKOUT STEP 9 / CONFIRM
# ============================================================

@checkout_router.message(CheckoutStates.confirm)
async def checkout_confirm_handler(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    yes_text = t(message.from_user.id, "checkout_confirm_yes")
    no_text = t(message.from_user.id, "checkout_confirm_no")

    if text == no_text:
        await state.clear()
        await message.answer(
            t(message.from_user.id, "checkout_cancelled"),
            reply_markup=user_main_menu(message.from_user.id),
        )
        return

    if text != yes_text:
        await message.answer(t(message.from_user.id, "order_send_again"))
        return

    rows = get_cart_rows(message.from_user.id)
    if not rows:
        await state.clear()
        await message.answer(
            t(message.from_user.id, "cart_empty_for_checkout"),
            reply_markup=user_main_menu(message.from_user.id),
        )
        return

    data = await state.get_data()

    order_id = create_order_from_checkout(
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        checkout_data=data,
        source="telegram",
    )

    await state.clear()
    await send_order_success_to_user(message, order_id)
    await notify_admins_about_order(order_id)


# ============================================================
# EXTRA CHECKOUT COMMAND
# ============================================================

@checkout_router.message(Command("checkout"))
async def checkout_command(message: Message, state: FSMContext) -> None:
    rows = get_cart_rows(message.from_user.id)
    if not rows:
        await message.answer(t(message.from_user.id, "cart_empty_for_checkout"))
        return

    await state.clear()
    await state.set_state(CheckoutStates.customer_name)
    await message.answer(
        f"{t(message.from_user.id, 'checkout_intro')}\n\n{t(message.from_user.id, 'checkout_name')}",
        reply_markup=checkout_cancel_keyboard(message.from_user.id),
    )

# ============================================================
# PART 5 / 10
# admin product CRUD, product browser, toggle/delete
# ============================================================

# ============================================================
# ADMIN PRODUCT ADD
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_add_product"], TEXTS["uz"]["admin_add_product"]]))
async def admin_add_product_start(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    await state.clear()
    await state.set_state(AdminAddProductStates.photo)
    await message.answer(
        t(message.from_user.id, "product_send_photo"),
        reply_markup=admin_cancel_keyboard(message.from_user.id),
    )


@admin_router.message(AdminAddProductStates.photo)
async def admin_add_product_photo(message: Message, state: FSMContext) -> None:
    if message.text and await maybe_cancel_state(message, state, back_to_admin=True):
        return

    if not message.photo:
        await message.answer(t(message.from_user.id, "product_send_photo"))
        return

    photo_file_id = message.photo[-1].file_id
    await state.update_data(photo_file_id=photo_file_id)
    await state.set_state(AdminAddProductStates.title_ru)
    await message.answer(t(message.from_user.id, "product_title_ru"))


@admin_router.message(AdminAddProductStates.title_ru)
async def admin_add_product_title_ru(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    value = (message.text or "").strip()
    if not value:
        await message.answer(t(message.from_user.id, "product_title_ru"))
        return

    await state.update_data(title_ru=value)
    await state.set_state(AdminAddProductStates.title_uz)
    await message.answer(t(message.from_user.id, "product_title_uz"))


@admin_router.message(AdminAddProductStates.title_uz)
async def admin_add_product_title_uz(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    value = (message.text or "").strip()
    if not value:
        await message.answer(t(message.from_user.id, "product_title_uz"))
        return

    await state.update_data(title_uz=value)
    await state.set_state(AdminAddProductStates.description_ru)
    await message.answer(t(message.from_user.id, "product_desc_ru"))


@admin_router.message(AdminAddProductStates.description_ru)
async def admin_add_product_desc_ru(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    await state.update_data(description_ru=(message.text or "").strip())
    await state.set_state(AdminAddProductStates.description_uz)
    await message.answer(t(message.from_user.id, "product_desc_uz"))


@admin_router.message(AdminAddProductStates.description_uz)
async def admin_add_product_desc_uz(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    await state.update_data(description_uz=(message.text or "").strip())
    await state.set_state(AdminAddProductStates.sizes)
    await message.answer(t(message.from_user.id, "product_sizes"))


@admin_router.message(AdminAddProductStates.sizes)
async def admin_add_product_sizes(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    sizes = normalize_sizes_input(message.text or "")
    await state.update_data(sizes=sizes)
    await state.set_state(AdminAddProductStates.category_slug)
    await message.answer(
        t(message.from_user.id, "product_category"),
        reply_markup=admin_category_keyboard(message.from_user.id),
    )


@admin_router.message(AdminAddProductStates.category_slug)
async def admin_add_product_category(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    category_slug = (message.text or "").strip().lower()
    if category_slug not in CATEGORY_SLUGS:
        await message.answer(t(message.from_user.id, "product_invalid_category"))
        return

    await state.update_data(category_slug=category_slug)
    await state.set_state(AdminAddProductStates.price)
    await message.answer(
        t(message.from_user.id, "product_price"),
        reply_markup=admin_cancel_keyboard(message.from_user.id),
    )


@admin_router.message(AdminAddProductStates.price)
async def admin_add_product_price(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    value = (message.text or "").strip()
    if not value.isdigit():
        await message.answer(t(message.from_user.id, "product_invalid_price"))
        return

    await state.update_data(price=int(value))
    await state.set_state(AdminAddProductStates.old_price)
    await message.answer(t(message.from_user.id, "product_old_price"))


@admin_router.message(AdminAddProductStates.old_price)
async def admin_add_product_old_price(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    value = (message.text or "").strip()
    if not value.isdigit():
        await message.answer(t(message.from_user.id, "product_invalid_price"))
        return

    await state.update_data(old_price=int(value))
    await state.set_state(AdminAddProductStates.stock_qty)
    await message.answer(t(message.from_user.id, "product_stock"))


@admin_router.message(AdminAddProductStates.stock_qty)
async def admin_add_product_stock(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    value = (message.text or "").strip()
    if not value.isdigit():
        await message.answer(t(message.from_user.id, "product_invalid_price"))
        return

    await state.update_data(stock_qty=int(value))
    await state.set_state(AdminAddProductStates.publish)
    await message.answer(
        t(message.from_user.id, "product_publish"),
        reply_markup=admin_publish_keyboard(message.from_user.id),
    )


@admin_router.message(AdminAddProductStates.publish)
async def admin_add_product_publish(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    text = (message.text or "").strip()
    yes_text = t(message.from_user.id, "product_publish_yes")
    no_text = t(message.from_user.id, "product_publish_no")

    if text not in (yes_text, no_text):
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return

    await state.update_data(is_published=1 if text == yes_text else 0)
    await state.set_state(AdminAddProductStates.sort_order)
    await message.answer(
        t(message.from_user.id, "product_sort_order"),
        reply_markup=admin_cancel_keyboard(message.from_user.id),
    )


@admin_router.message(AdminAddProductStates.sort_order)
async def admin_add_product_sort_order(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    value = (message.text or "").strip()
    if not value.isdigit():
        await message.answer(t(message.from_user.id, "product_invalid_price"))
        return

    await state.update_data(sort_order=int(value))
    data = await state.get_data()

    summary = (
        f"🧷 <b>Проверка товара</b>\n\n"
        f"<b>RU:</b> {data.get('title_ru','')}\n"
        f"<b>UZ:</b> {data.get('title_uz','')}\n"
        f"<b>Описание RU:</b> {data.get('description_ru','') or '—'}\n"
        f"<b>Описание UZ:</b> {data.get('description_uz','') or '—'}\n"
        f"<b>Размеры:</b> {data.get('sizes','') or '—'}\n"
        f"<b>Категория:</b> {data.get('category_slug','')}\n"
        f"<b>Цена:</b> {fmt_sum(data.get('price', 0))}\n"
        f"<b>Старая цена:</b> {fmt_sum(data.get('old_price', 0)) if safe_int(data.get('old_price',0)) > 0 else '—'}\n"
        f"<b>Остаток:</b> {data.get('stock_qty', 0)}\n"
        f"<b>Опубликован:</b> {'Да' if safe_int(data.get('is_published', 1)) else 'Нет'}\n"
        f"<b>Sort order:</b> {data.get('sort_order', 100)}\n\n"
        f"Напишите <b>{t(message.from_user.id, 'yes')}</b> для сохранения или <b>{t(message.from_user.id, 'no')}</b> для отмены."
    )

    await state.set_state(AdminAddProductStates.confirm)
    await message.answer(
        summary,
        reply_markup=admin_yes_no_keyboard(message.from_user.id),
    )


@admin_router.message(AdminAddProductStates.confirm)
async def admin_add_product_confirm(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    yes_text = t(message.from_user.id, "yes")
    no_text = t(message.from_user.id, "no")

    if text == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=admin_main_menu(message.from_user.id),
        )
        return

    if text == no_text:
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=admin_main_menu(message.from_user.id),
        )
        return

    if text != yes_text:
        await message.answer(f"Напишите {yes_text} или {no_text}.")
        return

    data = await state.get_data()
    product_id = create_product(data)
    product = get_product_by_id(product_id)

    await state.clear()

    if product:
        if product["photo_file_id"]:
            try:
                await bot.send_photo(
                    chat_id=message.from_user.id,
                    photo=product["photo_file_id"],
                    caption=product_card_text(product),
                    reply_markup=product_manage_keyboard(product["id"], safe_int(product["is_published"])),
                )
            except Exception:
                await message.answer(
                    product_card_text(product),
                    reply_markup=product_manage_keyboard(product["id"], safe_int(product["is_published"])),
                )
        else:
            await message.answer(
                product_card_text(product),
                reply_markup=product_manage_keyboard(product["id"], safe_int(product["is_published"])),
            )

    await message.answer(
        t(message.from_user.id, "product_saved"),
        reply_markup=admin_main_menu(message.from_user.id),
    )


# ============================================================
# ADMIN PRODUCT EDIT
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_edit_product"], TEXTS["uz"]["admin_edit_product"]]))
async def admin_edit_product_start(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_all_products(limit=40)
    if not rows:
        await message.answer(t(message.from_user.id, "admin_no_products"))
        return

    lines = ["<b>Товары для редактирования:</b>", ""]
    for row in rows:
        pub = "ON" if safe_int(row["is_published"]) else "OFF"
        lines.append(
            f"#{row['id']} | {row['title_ru']} | {fmt_sum(row['price'])} | stock {row['stock_qty']} | {pub}"
        )

    lines += ["", t(message.from_user.id, "product_edit_choose")]

    await state.clear()
    await state.set_state(AdminEditProductStates.product_id)
    await message.answer(
        "\n".join(lines),
        reply_markup=admin_cancel_keyboard(message.from_user.id),
    )


@admin_router.message(AdminEditProductStates.product_id)
async def admin_edit_product_select(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    product_id_text = (message.text or "").strip()
    if not product_id_text.isdigit():
        await message.answer(t(message.from_user.id, "admin_invalid_id"))
        return

    product_id = int(product_id_text)
    product = get_product_by_id(product_id)
    if not product:
        await message.answer(t(message.from_user.id, "admin_product_not_found"))
        return

    await state.update_data(edit_product_id=product_id)
    await state.set_state(AdminEditProductStates.field_name)
    await message.answer(
        product_card_text(product) + "\n\n" + t(message.from_user.id, "product_edit_field"),
        reply_markup=admin_edit_fields_keyboard(message.from_user.id),
    )


@admin_router.message(AdminEditProductStates.field_name)
async def admin_edit_product_field_name(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    field_name = (message.text or "").strip()
    allowed = {
        "photo_file_id",
        "title_ru",
        "title_uz",
        "description_ru",
        "description_uz",
        "sizes",
        "category_slug",
        "price",
        "old_price",
        "stock_qty",
        "sort_order",
        "is_published",
    }

    if field_name not in allowed:
        await message.answer(t(message.from_user.id, "checkout_invalid_choice"))
        return

    await state.update_data(field_name=field_name)
    await state.set_state(AdminEditProductStates.field_value)

    hints = {
        "photo_file_id": "Отправьте новое фото товара.",
        "title_ru": "Введите новое название RU.",
        "title_uz": "Введите новое название UZ.",
        "description_ru": "Введите новое описание RU.",
        "description_uz": "Введите новое описание UZ.",
        "sizes": "Введите новые размеры через запятую.",
        "category_slug": "Введите категорию: new / hits / sale / limited / school / casual",
        "price": "Введите новую цену.",
        "old_price": "Введите новую старую цену или 0.",
        "stock_qty": "Введите новый остаток.",
        "sort_order": "Введите новый sort order.",
        "is_published": "Введите 1 для публикации или 0 для скрытия.",
    }

    await message.answer(
        hints.get(field_name, "Введите новое значение."),
        reply_markup=admin_cancel_keyboard(message.from_user.id),
    )


@admin_router.message(AdminEditProductStates.field_value)
async def admin_edit_product_field_value(message: Message, state: FSMContext) -> None:
    if message.text and await maybe_cancel_state(message, state, back_to_admin=True):
        return

    data = await state.get_data()
    product_id = safe_int(data.get("edit_product_id"))
    field_name = (data.get("field_name") or "").strip()

    product = get_product_by_id(product_id)
    if not product:
        await state.clear()
        await message.answer(
            t(message.from_user.id, "admin_product_not_found"),
            reply_markup=admin_main_menu(message.from_user.id),
        )
        return

    if field_name == "photo_file_id":
        if not message.photo:
            await message.answer("Нужно отправить фото.")
            return
        db_value: Any = message.photo[-1].file_id
    else:
        raw = (message.text or "").strip()

        if field_name in {"price", "old_price", "stock_qty", "sort_order", "is_published"}:
            if not raw.isdigit():
                await message.answer(t(message.from_user.id, "product_invalid_price"))
                return
            db_value = int(raw)

        elif field_name == "category_slug":
            if raw.lower() not in CATEGORY_SLUGS:
                await message.answer(t(message.from_user.id, "product_invalid_category"))
                return
            db_value = raw.lower()

        elif field_name == "sizes":
            db_value = normalize_sizes_input(raw)

        else:
            db_value = raw

    update_product_field(product_id, field_name, db_value)
    updated = get_product_by_id(product_id)

    await state.clear()

    if updated:
        if updated["photo_file_id"]:
            try:
                await bot.send_photo(
                    chat_id=message.from_user.id,
                    photo=updated["photo_file_id"],
                    caption=product_card_text(updated),
                    reply_markup=product_manage_keyboard(updated["id"], safe_int(updated["is_published"])),
                )
            except Exception:
                await message.answer(
                    product_card_text(updated),
                    reply_markup=product_manage_keyboard(updated["id"], safe_int(updated["is_published"])),
                )
        else:
            await message.answer(
                product_card_text(updated),
                reply_markup=product_manage_keyboard(updated["id"], safe_int(updated["is_published"])),
            )

    await message.answer(
        t(message.from_user.id, "product_field_updated"),
        reply_markup=admin_main_menu(message.from_user.id),
    )


# ============================================================
# ADMIN PRODUCT DELETE
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_delete_product"], TEXTS["uz"]["admin_delete_product"]]))
async def admin_delete_product_start(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_all_products(limit=40)
    if not rows:
        await message.answer(t(message.from_user.id, "admin_no_products"))
        return

    lines = ["<b>Товары для удаления:</b>", ""]
    for row in rows:
        pub = "ON" if safe_int(row["is_published"]) else "OFF"
        lines.append(
            f"#{row['id']} | {row['title_ru']} | {fmt_sum(row['price'])} | stock {row['stock_qty']} | {pub}"
        )

    lines += ["", t(message.from_user.id, "admin_choose_product_id")]

    await state.clear()
    await state.set_state(AdminDeleteProductStates.product_id)
    await message.answer(
        "\n".join(lines),
        reply_markup=admin_cancel_keyboard(message.from_user.id),
    )


@admin_router.message(AdminDeleteProductStates.product_id)
async def admin_delete_product_pick_id(message: Message, state: FSMContext) -> None:
    if await maybe_cancel_state(message, state, back_to_admin=True):
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer(t(message.from_user.id, "admin_invalid_id"))
        return

    product_id = int(text)
    product = get_product_by_id(product_id)
    if not product:
        await message.answer(t(message.from_user.id, "admin_product_not_found"))
        return

    await state.update_data(delete_product_id=product_id)
    await state.set_state(AdminDeleteProductStates.confirm)
    await message.answer(
        product_card_text(product) + f"\n\nУдалить этот товар? Напишите {t(message.from_user.id, 'yes')} или {t(message.from_user.id, 'no')}.",
        reply_markup=admin_yes_no_keyboard(message.from_user.id),
    )


@admin_router.message(AdminDeleteProductStates.confirm)
async def admin_delete_product_confirm(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    yes_text = t(message.from_user.id, "yes")
    no_text = t(message.from_user.id, "no")

    if text == t(message.from_user.id, "cancel"):
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=admin_main_menu(message.from_user.id),
        )
        return

    if text == no_text:
        await state.clear()
        await message.answer(
            t(message.from_user.id, "action_cancelled"),
            reply_markup=admin_main_menu(message.from_user.id),
        )
        return

    if text != yes_text:
        await message.answer(f"Напишите {yes_text} или {no_text}.")
        return

    data = await state.get_data()
    product_id = safe_int(data.get("delete_product_id"))
    ok = delete_product(product_id)

    await state.clear()
    await message.answer(
        t(message.from_user.id, "admin_product_deleted") if ok else t(message.from_user.id, "admin_product_not_found"),
        reply_markup=admin_main_menu(message.from_user.id),
    )


# ============================================================
# PRODUCT BROWSER
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_products_browser"], TEXTS["uz"]["admin_products_browser"]]))
async def admin_products_browser_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows, total_pages = get_products_page(page=1, per_page=10)
    if not rows:
        await message.answer(t(message.from_user.id, "admin_no_products"))
        return

    await message.answer(
        "📦 <b>Браузер товаров</b>\nВыберите товар ниже.",
        reply_markup=products_browser_keyboard(1, total_pages, rows),
    )


@admin_router.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery) -> None:
    await callback.answer()


@admin_router.callback_query(F.data.startswith("products:page:"))
async def admin_products_page_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    page = safe_int(callback.data.split(":")[-1], 1)
    rows, total_pages = get_products_page(page=page, per_page=10)

    if not rows:
        await callback.answer()
        return

    await callback.message.edit_reply_markup(
        reply_markup=products_browser_keyboard(page, total_pages, rows)
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("product:open:"))
async def admin_product_open_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    product_id = safe_int(callback.data.split(":")[-1])
    product = get_product_by_id(product_id)

    if not product:
        await callback.message.answer(t(callback.from_user.id, "admin_product_not_found"))
        await callback.answer()
        return

    text = product_card_text(product)
    kb = product_manage_keyboard(product["id"], safe_int(product["is_published"]))

    if product["photo_file_id"]:
        try:
            await bot.send_photo(
                chat_id=callback.from_user.id,
                photo=product["photo_file_id"],
                caption=text,
                reply_markup=kb,
            )
        except Exception:
            await callback.message.answer(text, reply_markup=kb)
    else:
        await callback.message.answer(text, reply_markup=kb)

    await callback.answer()


@admin_router.callback_query(F.data.startswith("product:toggle:"))
async def admin_product_toggle_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer()
        return

    product_id = safe_int(parts[2])
    new_value = safe_int(parts[3], 1)

    update_product_field(product_id, "is_published", 1 if new_value else 0)
    product = get_product_by_id(product_id)

    if product:
        await callback.message.answer(
            product_card_text(product),
            reply_markup=product_manage_keyboard(product["id"], safe_int(product["is_published"])),
        )

    await callback.answer(t(callback.from_user.id, "product_publish_updated"))


@admin_router.callback_query(F.data.startswith("product:delete:"))
async def admin_product_delete_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    product_id = safe_int(callback.data.split(":")[-1])
    ok = delete_product(product_id)

    await callback.message.answer(
        f"Удалён товар #{product_id}" if ok else t(callback.from_user.id, "admin_product_not_found")
    )
    await callback.answer()

# ============================================================
# PART 6 / 10
# CRM orders, clients, reports, scheduled posts, system tools
# ============================================================

# ============================================================
# CRM HELPERS
# ============================================================

def crm_get_orders(
    *,
    page: int = 1,
    per_page: int = 10,
    status: str = "",
    payment_status: str = "",
    unseen_only: bool = False,
) -> tuple[list[sqlite3.Row], int]:
    page = max(1, safe_int(page, 1))
    per_page = max(1, safe_int(per_page, 10))
    offset = (page - 1) * per_page

    where = []
    params: list[Any] = []

    if status and status in ORDER_STATUSES:
        where.append("status = ?")
        params.append(status)

    if payment_status and payment_status in PAYMENT_STATUSES:
        where.append("payment_status = ?")
        params.append(payment_status)

    if unseen_only:
        where.append("manager_seen = 0")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    conn = get_db()
    total_row = conn.execute(
        f"SELECT COUNT(*) FROM orders {where_sql}",
        tuple(params),
    ).fetchone()
    total = safe_int(total_row[0]) if total_row else 0

    rows = conn.execute(
        f"""
        SELECT *
        FROM orders
        {where_sql}
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        tuple(params + [per_page, offset]),
    ).fetchall()
    conn.close()

    total_pages = max(1, math.ceil(total / per_page)) if total else 1
    return rows, total_pages


def crm_order_short_line(row: sqlite3.Row) -> str:
    return (
        f"#{row['id']} | "
        f"{row['customer_name'] or '—'} | "
        f"{fmt_sum(row['total_amount'])} | "
        f"{status_label('ru', row['status'])}"
    )


def crm_orders_page_keyboard(
    *,
    rows: list[sqlite3.Row],
    page: int,
    total_pages: int,
    mode: str = "all",
    status: str = "",
    payment_status: str = "",
    unseen_only: int = 0,
) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []

    for row in rows:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"#{row['id']} {row['customer_name'] or '—'}",
                    callback_data=f"crm:open:{row['id']}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"crm:page:{page-1}:{mode}:{status or '-'}:{payment_status or '-'}:{unseen_only}",
            )
        )

    nav_row.append(
        InlineKeyboardButton(
            text=f"{page}/{total_pages}",
            callback_data="crm:noop",
        )
    )

    if page < total_pages:
        nav_row.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"crm:page:{page+1}:{mode}:{status or '-'}:{payment_status or '-'}:{unseen_only}",
            )
        )

    keyboard.append(nav_row)

    keyboard.append(
        [
            InlineKeyboardButton(text="Все", callback_data="crm:filter:all"),
            InlineKeyboardButton(text="Новые", callback_data="crm:filter:status:new"),
            InlineKeyboardButton(text="Оплаченные", callback_data="crm:filter:status:paid"),
        ]
    )

    keyboard.append(
        [
            InlineKeyboardButton(text="Непросмотренные", callback_data="crm:filter:unseen"),
            InlineKeyboardButton(text="Pay pending", callback_data="crm:filter:pay:pending"),
            InlineKeyboardButton(text="Pay paid", callback_data="crm:filter:pay:paid"),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def crm_order_card_keyboard(order_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="В работу", callback_data=f"crm:setstatus:{order_id}:processing"),
                InlineKeyboardButton(text="Подтвердить", callback_data=f"crm:setstatus:{order_id}:confirmed"),
            ],
            [
                InlineKeyboardButton(text="Оплачен", callback_data=f"crm:setstatus:{order_id}:paid"),
                InlineKeyboardButton(text="Отправлен", callback_data=f"crm:setstatus:{order_id}:sent"),
            ],
            [
                InlineKeyboardButton(text="Доставлен", callback_data=f"crm:setstatus:{order_id}:delivered"),
                InlineKeyboardButton(text="Отменён", callback_data=f"crm:setstatus:{order_id}:cancelled"),
            ],
            [
                InlineKeyboardButton(text="Pay pending", callback_data=f"crm:setpay:{order_id}:pending"),
                InlineKeyboardButton(text="Pay paid", callback_data=f"crm:setpay:{order_id}:paid"),
            ],
            [
                InlineKeyboardButton(text="Pay failed", callback_data=f"crm:setpay:{order_id}:failed"),
                InlineKeyboardButton(text="Pay refunded", callback_data=f"crm:setpay:{order_id}:refunded"),
            ],
            [
                InlineKeyboardButton(text="Открыть снова", callback_data=f"crm:open:{order_id}"),
                InlineKeyboardButton(text="Написать клиенту", url=f"tg://user?id={user_id}"),
            ],
        ]
    )


def crm_full_order_text(order_row: sqlite3.Row) -> str:
    username = mask_username(order_row["username"])
    location = (
        f"{order_row['latitude']}, {order_row['longitude']}"
        if order_row["latitude"] is not None and order_row["longitude"] is not None
        else "—"
    )

    return (
        f"📦 <b>CRM Заказ #{order_row['id']}</b>\n\n"
        f"<b>Имя:</b> {order_row['customer_name'] or '—'}\n"
        f"<b>Телефон:</b> {order_row['customer_phone'] or '—'}\n"
        f"<b>Username:</b> {username}\n"
        f"<b>User ID:</b> {order_row['user_id']}\n"
        f"<b>Город:</b> {order_row['city'] or '—'}\n"
        f"<b>Доставка:</b> {delivery_label('ru', order_row['delivery_service'] or '')}\n"
        f"<b>Тип адреса:</b> {address_type_label('ru', order_row['delivery_type'] or '')}\n"
        f"<b>Адрес:</b> {order_row['delivery_address'] or '—'}\n"
        f"<b>Локация:</b> {location}\n"
        f"<b>ПВЗ код:</b> {order_row['pvz_code'] or '—'}\n"
        f"<b>ПВЗ адрес:</b> {order_row['pvz_address'] or '—'}\n"
        f"<b>Оплата:</b> {payment_method_label('ru', order_row['payment_method'] or '')}\n"
        f"<b>Статус оплаты:</b> {payment_status_label('ru', order_row['payment_status'])}\n"
        f"<b>Комментарий:</b> {order_row['comment'] or '—'}\n"
        f"<b>Товары:</b>\n{render_order_items(order_row['items'])}\n\n"
        f"<b>Количество:</b> {order_row['total_qty']}\n"
        f"<b>Сумма:</b> {fmt_sum(order_row['total_amount'])}\n"
        f"<b>Источник:</b> {source_label('ru', order_row['source'])}\n"
        f"<b>Статус заказа:</b> {status_label('ru', order_row['status'])}\n"
        f"<b>Просмотрен:</b> {'Да' if safe_int(order_row['manager_seen']) else 'Нет'}\n"
        f"<b>Manager ID:</b> {order_row['manager_id'] or '—'}\n"
        f"<b>Created:</b> {order_row['created_at']}\n"
        f"<b>Updated:</b> {order_row['updated_at']}\n"
        f"<b>Payment URL:</b> {order_row['payment_provider_url'] or '—'}"
    )


def crm_mark_order_seen(order_id: int, manager_id: int) -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET manager_seen = 1, manager_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (manager_id, utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()


def crm_set_order_status(order_id: int, status: str, manager_id: int) -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET status = ?, manager_seen = 1, manager_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, manager_id, utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()


def crm_set_payment_status(order_id: int, payment_status: str, manager_id: int) -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET payment_status = ?, manager_seen = 1, manager_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (payment_status, manager_id, utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()


def crm_clients_page(page: int = 1, per_page: int = 15) -> tuple[list[sqlite3.Row], int]:
    page = max(1, safe_int(page, 1))
    per_page = max(1, safe_int(per_page, 15))
    offset = (page - 1) * per_page

    conn = get_db()
    total_row = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT user_id FROM orders GROUP BY user_id
        )
        """
    ).fetchone()
    total = safe_int(total_row[0]) if total_row else 0

    rows = conn.execute(
        """
        SELECT
            user_id,
            MAX(username) AS username,
            MAX(customer_name) AS customer_name,
            MAX(customer_phone) AS customer_phone,
            COUNT(*) AS orders_count,
            MAX(created_at) AS last_order_at
        FROM orders
        GROUP BY user_id
        ORDER BY last_order_at DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset),
    ).fetchall()
    conn.close()

    total_pages = max(1, math.ceil(total / per_page)) if total else 1
    return rows, total_pages


def crm_clients_keyboard(page: int, total_pages: int, rows: list[sqlite3.Row]) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []

    for row in rows:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"{row['customer_name'] or '—'} | {row['orders_count']}",
                    callback_data=f"crm:client:{row['user_id']}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"crm:clients:page:{page-1}",
            )
        )

    nav_row.append(
        InlineKeyboardButton(
            text=f"{page}/{total_pages}",
            callback_data="crm:noop",
        )
    )

    if page < total_pages:
        nav_row.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"crm:clients:page:{page+1}",
            )
        )

    keyboard.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def crm_find_orders_by_phone(phone_part: str, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE customer_phone LIKE ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (f"%{phone_part}%", limit),
    ).fetchall()
    conn.close()
    return rows


# ============================================================
# REPORT HELPERS
# ============================================================

def month_string_now() -> str:
    now = datetime.now()
    return f"{now.year:04d}-{now.month:02d}"


def generate_monthly_excel_report(month: Optional[str] = None) -> Path:
    if not month:
        month = month_string_now()

    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE substr(created_at, 1, 7) = ?
        ORDER BY id DESC
        """,
        (month,),
    ).fetchall()
    conn.close()

    wb = Workbook()
    ws = wb.active
    ws.title = "Orders"

    ws.append([
        "ID",
        "date",
        "name",
        "phone",
        "city",
        "items",
        "sum",
        "status",
        "payment_method",
        "payment_status",
        "source",
    ])

    for row in rows:
        ws.append([
            row["id"],
            row["created_at"],
            row["customer_name"],
            row["customer_phone"],
            row["city"],
            render_order_items(row["items"]),
            safe_int(row["total_amount"]),
            row["status"],
            row["payment_method"],
            row["payment_status"],
            row["source"],
        ])

    file_path = REPORTS_DIR / f"orders_report_{month}.xlsx"
    wb.save(file_path)

    conn = get_db()
    conn.execute(
        """
        INSERT INTO monthly_reports (month, file_path, created_at)
        VALUES (?, ?, ?)
        """,
        (month, str(file_path), utc_now_iso()),
    )
    conn.commit()
    conn.close()

    return file_path


def get_monthly_report_history(limit: int = 20) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM monthly_reports
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


# ============================================================
# SCHEDULED POSTS HELPERS
# ============================================================

def validate_scheduled_datetime(text: str) -> str | None:
    try:
        dt = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return None


def create_scheduled_post(text: str, post_time: str, media: str = "") -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scheduled_posts (text, media, post_time, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (text, media, post_time, utc_now_iso()),
    )
    post_id = cur.lastrowid
    conn.commit()
    conn.close()
    return post_id


def get_scheduled_posts(limit: int = 30) -> list[sqlite3.Row]:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM scheduled_posts
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def delete_scheduled_post(post_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM scheduled_posts WHERE id = ?", (post_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def get_system_summary_text() -> str:
    stats = get_basic_stats()
    reports = get_monthly_report_history(limit=5)
    posts = get_scheduled_posts(limit=5)

    lines = [
        "🧩 <b>System summary</b>",
        "",
        f"<b>Orders:</b> {stats['total_orders']}",
        f"<b>Users:</b> {stats['unique_users']}",
        f"<b>Products:</b> {stats['products']}",
        f"<b>Reviews:</b> {stats['reviews']}",
        f"<b>New:</b> {stats['new']}",
        f"<b>Processing:</b> {stats['processing']}",
        f"<b>Confirmed:</b> {stats['confirmed']}",
        f"<b>Paid:</b> {stats['paid']}",
        f"<b>Sent:</b> {stats['sent']}",
        f"<b>Delivered:</b> {stats['delivered']}",
        f"<b>Cancelled:</b> {stats['cancelled']}",
        "",
        "<b>Last reports:</b>",
    ]

    if reports:
        for row in reports:
            lines.append(f"• {row['month']} | {row['file_path']}")
    else:
        lines.append("• —")

    lines += ["", "<b>Scheduled posts:</b>"]
    if posts:
        for row in posts:
            lines.append(f"• #{row['id']} | {row['post_time']} | {(row['text'] or '')[:50]}")
    else:
        lines.append("• —")

    return "\n".join(lines)


# ============================================================
# CRM MENU HANDLERS
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_crm_orders"], TEXTS["uz"]["admin_crm_orders"]]))
async def crm_orders_menu_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows, total_pages = crm_get_orders(page=1, per_page=10)
    if not rows:
        await message.answer("Заказов пока нет.")
        return

    text = "🧾 <b>CRM Заказы</b>\n\n" + "\n".join(crm_order_short_line(r) for r in rows)
    await message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="all",
        ),
    )


@admin_router.message(F.text.in_([TEXTS["ru"]["admin_clients"], TEXTS["uz"]["admin_clients"]]))
async def crm_clients_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows, total_pages = crm_clients_page(page=1, per_page=15)
    if not rows:
        await message.answer("Клиентов пока нет.")
        return

    text = "👥 <b>Клиенты</b>\n\nВыберите клиента ниже."
    await message.answer(
        text,
        reply_markup=crm_clients_keyboard(1, total_pages, rows),
    )


@admin_router.message(F.text.in_([TEXTS["ru"]["admin_find_client"], TEXTS["uz"]["admin_find_client"]]))
async def crm_search_help_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    await message.answer(
        "Для поиска клиента по телефону используй команду:\n"
        "<code>/find 90</code>\n"
        "<code>/find 99890</code>"
    )


# ============================================================
# CRM COMMANDS
# ============================================================

@admin_router.message(Command("find"))
async def crm_find_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй: /find 90")
        return

    query = parts[1].strip()
    rows = crm_find_orders_by_phone(query)

    if not rows:
        await message.answer("Ничего не найдено.")
        return

    text_lines = ["🔎 <b>Результаты поиска</b>", ""]
    for row in rows:
        text_lines.append(
            f"#{row['id']} | {row['customer_name'] or '—'} | {row['customer_phone'] or '—'} | {fmt_sum(row['total_amount'])}"
        )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Открыть #{row['id']}", callback_data=f"crm:open:{row['id']}")]
            for row in rows[:10]
        ]
    )

    await message.answer("\n".join(text_lines), reply_markup=keyboard)


@admin_router.message(Command("crm"))
async def crm_command_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows, total_pages = crm_get_orders(page=1, per_page=10)
    if not rows:
        await message.answer("Заказов пока нет.")
        return

    text = "🧾 <b>CRM Заказы</b>\n\n" + "\n".join(crm_order_short_line(r) for r in rows)
    await message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="all",
        ),
    )


@admin_router.message(Command("ordercard"))
async def crm_ordercard_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Используй: /ordercard 15")
        return

    order_id = int(parts[1])
    order = get_order_by_id(order_id)
    if not order:
        await message.answer("Заказ не найден.")
        return

    crm_mark_order_seen(order_id, message.from_user.id)
    await message.answer(
        crm_full_order_text(order),
        reply_markup=crm_order_card_keyboard(order["id"], order["user_id"]),
    )


@admin_router.message(Command("report"))
async def admin_report_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    file_path = generate_monthly_excel_report()
    with open(file_path, "rb") as f:
        data = f.read()

    await message.answer_document(
        BufferedInputFile(data, filename=file_path.name),
        caption="Месячный Excel отчёт.",
    )


@admin_router.message(F.text.in_([TEXTS["ru"]["admin_excel"], TEXTS["uz"]["admin_excel"]]))
async def admin_excel_report_button(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    file_path = generate_monthly_excel_report()
    with open(file_path, "rb") as f:
        data = f.read()

    await message.answer_document(
        BufferedInputFile(data, filename=file_path.name),
        caption="Excel отчёт сформирован.",
    )


@admin_router.message(Command("reports"))
async def admin_reports_history_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_monthly_report_history(limit=20)
    if not rows:
        await message.answer("История отчётов пуста.")
        return

    lines = ["🧾 <b>История Excel отчётов</b>", ""]
    for row in rows:
        lines.append(f"#{row['id']} | {row['month']} | {row['file_path']}")

    await message.answer("\n".join(lines))


@admin_router.message(Command("mkreport"))
async def admin_make_report_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split(maxsplit=1)
    month = month_string_now()
    if len(parts) == 2:
        month = parts[1].strip()

    file_path = generate_monthly_excel_report(month=month)
    with open(file_path, "rb") as f:
        data = f.read()

    await message.answer_document(
        BufferedInputFile(data, filename=file_path.name),
        caption=f"Excel отчёт за {month}",
    )


@admin_router.message(Command("system"))
async def admin_system_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    await message.answer(get_system_summary_text())


@admin_router.message(Command("unseen"))
async def admin_unseen_orders_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows, total_pages = crm_get_orders(page=1, per_page=15, unseen_only=True)
    if not rows:
        await message.answer("Непросмотренных заказов нет.")
        return

    text = "🧾 <b>Непросмотренные заказы</b>\n\n" + "\n".join(crm_order_short_line(r) for r in rows)
    await message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="all",
            unseen_only=1,
        ),
    )


# ============================================================
# SCHEDULED POSTS COMMANDS
# ============================================================

@admin_router.message(F.text.in_([TEXTS["ru"]["admin_schedule_post"], TEXTS["uz"]["admin_schedule_post"]]))
async def admin_schedule_post_help(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    await message.answer(
        "Чтобы запланировать пост, используй команду:\n\n"
        "<code>/schedpost 2026-03-10 18:00 | Новый пост бренда ZARY & CO</code>"
    )


@admin_router.message(Command("schedpost"))
async def admin_schedule_post_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    raw = (message.text or "").replace("/schedpost", "", 1).strip()
    if "|" not in raw:
        await message.answer(
            "Формат:\n"
            "/schedpost 2026-03-10 18:00 | Текст поста"
        )
        return

    left, right = raw.split("|", 1)
    post_time = validate_scheduled_datetime(left.strip())
    post_text = right.strip()

    if not post_time:
        await message.answer("Дата должна быть в формате YYYY-MM-DD HH:MM")
        return

    if not post_text:
        await message.answer("Текст поста пустой.")
        return

    create_scheduled_post(post_text, post_time, media="")
    await message.answer(
        f"Пост запланирован.\n\n"
        f"<b>Время:</b> {post_time}\n"
        f"<b>Текст:</b> {post_text}"
    )


@admin_router.message(Command("schedlist"))
async def admin_schedlist_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    rows = get_scheduled_posts(limit=30)
    if not rows:
        await message.answer("Запланированных постов нет.")
        return

    lines = ["📝 <b>Запланированные посты</b>", ""]
    for row in rows:
        preview = (row["text"] or "").replace("\n", " ")
        lines.append(f"#{row['id']} | {row['post_time']} | {preview[:70]}")

    await message.answer("\n".join(lines))


@admin_router.message(Command("scheddelete"))
async def admin_scheddelete_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Используй: /scheddelete 12")
        return

    post_id = int(parts[1])
    ok = delete_scheduled_post(post_id)
    await message.answer("Пост удалён." if ok else "Пост не найден.")


# ============================================================
# CRM CALLBACKS
# ============================================================

@admin_router.callback_query(F.data == "crm:noop")
async def crm_noop_callback(callback: CallbackQuery) -> None:
    await callback.answer()


@admin_router.callback_query(F.data == "crm:filter:all")
async def crm_filter_all_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    rows, total_pages = crm_get_orders(page=1, per_page=10)
    text = "🧾 <b>CRM Заказы</b>\n\n" + ("\n".join(crm_order_short_line(r) for r in rows) if rows else "Пусто.")
    await callback.message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="all",
        ),
    )
    await callback.answer()


@admin_router.callback_query(F.data == "crm:filter:unseen")
async def crm_filter_unseen_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    rows, total_pages = crm_get_orders(page=1, per_page=10, unseen_only=True)
    text = "🧾 <b>Непросмотренные заказы</b>\n\n" + ("\n".join(crm_order_short_line(r) for r in rows) if rows else "Пусто.")
    await callback.message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="all",
            unseen_only=1,
        ),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("crm:filter:status:"))
async def crm_filter_status_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    status = callback.data.split(":")[-1]
    rows, total_pages = crm_get_orders(page=1, per_page=10, status=status)
    text = f"🧾 <b>Заказы: {status}</b>\n\n" + ("\n".join(crm_order_short_line(r) for r in rows) if rows else "Пусто.")
    await callback.message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="status",
            status=status,
        ),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("crm:filter:pay:"))
async def crm_filter_pay_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    payment_status = callback.data.split(":")[-1]
    rows, total_pages = crm_get_orders(page=1, per_page=10, payment_status=payment_status)
    text = f"🧾 <b>Заказы по оплате: {payment_status}</b>\n\n" + ("\n".join(crm_order_short_line(r) for r in rows) if rows else "Пусто.")
    await callback.message.answer(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=1,
            total_pages=total_pages,
            mode="pay",
            payment_status=payment_status,
        ),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("crm:page:"))
async def crm_page_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 7:
        await callback.answer()
        return

    page = safe_int(parts[2], 1)
    mode = parts[3]
    status = "" if parts[4] == "-" else parts[4]
    payment_status = "" if parts[5] == "-" else parts[5]
    unseen_only = safe_int(parts[6], 0) == 1

    rows, total_pages = crm_get_orders(
        page=page,
        per_page=10,
        status=status,
        payment_status=payment_status,
        unseen_only=unseen_only,
    )

    title = "🧾 <b>CRM Заказы</b>"
    if status:
        title = f"🧾 <b>Заказы: {status}</b>"
    if payment_status:
        title = f"🧾 <b>Заказы по оплате: {payment_status}</b>"
    if unseen_only:
        title = "🧾 <b>Непросмотренные заказы</b>"

    text = title + "\n\n" + ("\n".join(crm_order_short_line(r) for r in rows) if rows else "Пусто.")
    await callback.message.edit_text(
        text,
        reply_markup=crm_orders_page_keyboard(
            rows=rows,
            page=page,
            total_pages=total_pages,
            mode=mode,
            status=status,
            payment_status=payment_status,
            unseen_only=1 if unseen_only else 0,
        ),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("crm:open:"))
async def crm_open_order_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    order_id = safe_int(callback.data.split(":")[-1])
    order = get_order_by_id(order_id)
    if not order:
        await callback.message.answer("Заказ не найден.")
        await callback.answer()
        return

    crm_mark_order_seen(order_id, callback.from_user.id)
    await callback.message.answer(
        crm_full_order_text(order),
        reply_markup=crm_order_card_keyboard(order["id"], order["user_id"]),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("crm:setstatus:"))
async def crm_set_status_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer()
        return

    order_id = safe_int(parts[2])
    status = parts[3]

    if status not in ORDER_STATUSES:
        await callback.answer()
        return

    crm_set_order_status(order_id, status, callback.from_user.id)
    order = get_order_by_id(order_id)

    if order:
        await callback.message.answer(
            crm_full_order_text(order),
            reply_markup=crm_order_card_keyboard(order["id"], order["user_id"]),
        )
        await notify_user_about_order_update(order)

    await callback.answer("Статус заказа обновлён")


@admin_router.callback_query(F.data.startswith("crm:setpay:"))
async def crm_set_pay_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer()
        return

    order_id = safe_int(parts[2])
    payment_status = parts[3]

    if payment_status not in PAYMENT_STATUSES:
        await callback.answer()
        return

    crm_set_payment_status(order_id, payment_status, callback.from_user.id)
    order = get_order_by_id(order_id)

    if order:
        await callback.message.answer(
            crm_full_order_text(order),
            reply_markup=crm_order_card_keyboard(order["id"], order["user_id"]),
        )
        await notify_user_about_order_update(order)

    await callback.answer("Статус оплаты обновлён")


@admin_router.callback_query(F.data.startswith("crm:clients:page:"))
async def crm_clients_page_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    page = safe_int(callback.data.split(":")[-1], 1)
    rows, total_pages = crm_clients_page(page=page, per_page=15)

    await callback.message.edit_text(
        "👥 <b>Клиенты</b>\n\nВыберите клиента ниже.",
        reply_markup=crm_clients_keyboard(page, total_pages, rows),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("crm:client:"))
async def crm_client_open_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    user_id = safe_int(callback.data.split(":")[-1])
    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 20
        """,
        (user_id,),
    ).fetchall()
    conn.close()

    if not rows:
        await callback.message.answer("Заказы клиента не найдены.")
        await callback.answer()
        return

    first = rows[0]
    text_lines = [
        "👤 <b>Карточка клиента</b>",
        "",
        f"<b>Имя:</b> {first['customer_name'] or '—'}",
        f"<b>Телефон:</b> {first['customer_phone'] or '—'}",
        f"<b>User ID:</b> {first['user_id']}",
        f"<b>Username:</b> {mask_username(first['username'])}",
        "",
        "<b>Последние заказы:</b>",
    ]

    for row in rows:
        text_lines.append(
            f"#{row['id']} | {fmt_sum(row['total_amount'])} | {status_label('ru', row['status'])}"
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Открыть #{row['id']}", callback_data=f"crm:open:{row['id']}")]
            for row in rows[:10]
        ] + [
            [InlineKeyboardButton(text="Написать клиенту", url=f"tg://user?id={user_id}")]
        ]
    )

    await callback.message.answer("\n".join(text_lines), reply_markup=kb)
    await callback.answer()


# ============================================================
# BACKGROUND LOOPS
# ============================================================

async def remind_admins_about_unseen_orders_loop() -> None:
    while True:
        try:
            await asyncio.sleep(1800)

            conn = get_db()
            rows = conn.execute(
                """
                SELECT id
                FROM orders
                WHERE manager_seen = 0
                ORDER BY id DESC
                """
            ).fetchall()

            if rows:
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, "⚠️ Есть непросмотренные новые заказы.")
                    except Exception as exc:
                        logger.exception("Reminder send failed to admin %s: %s", admin_id, exc)

                conn.execute(
                    """
                    UPDATE orders
                    SET reminded_at = ?, updated_at = ?
                    WHERE manager_seen = 0
                    """,
                    (utc_now_iso(), utc_now_iso()),
                )
                conn.commit()

            conn.close()
        except Exception as exc:
            logger.exception("remind_admins_about_unseen_orders_loop error: %s", exc)


async def scheduled_posts_loop() -> None:
    while True:
        try:
            await asyncio.sleep(300)

            conn = get_db()
            rows = conn.execute(
                """
                SELECT *
                FROM scheduled_posts
                WHERE post_time IS NOT NULL
                  AND post_time <= ?
                ORDER BY id ASC
                """,
                (utc_now_iso(),),
            ).fetchall()

            for row in rows:
                text = row["text"] or ""
                media = row["media"] or ""

                try:
                    if CHANNEL_ID:
                        if media:
                            await bot.send_photo(CHANNEL_ID, media, caption=text)
                        else:
                            await bot.send_message(CHANNEL_ID, text)
                except Exception as exc:
                    logger.exception("Scheduled post send failed: %s", exc)
                    continue

                conn.execute("DELETE FROM scheduled_posts WHERE id = ?", (row["id"],))
                conn.commit()

            conn.close()
        except Exception as exc:
            logger.exception("scheduled_posts_loop error: %s", exc)


async def sunday_admin_reminder_loop() -> None:
    while True:
        try:
            await asyncio.sleep(3600)

            now = datetime.now()
            if now.weekday() == 6 and now.hour == 9:
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, "Напоминание: загрузите контент на неделю.")
                    except Exception as exc:
                        logger.exception("Sunday reminder send failed: %s", exc)
        except Exception as exc:
            logger.exception("sunday_admin_reminder_loop error: %s", exc)


async def daily_channel_post_loop() -> None:
    last_run_key = ""
    while True:
        try:
            await asyncio.sleep(300)
            now = datetime.now()
            key = f"{now.year:04d}-{now.month:02d}-{now.day:02d}-{now.hour:02d}"

            if now.hour == 18 and last_run_key != key:
                last_run_key = key
                if CHANNEL_ID:
                    text = (
                        f"🖤 <b>{SHOP_BRAND}</b>\n\n"
                        "Новые товары уже в магазине.\n"
                        "Откройте Telegram shop и выберите свою модель."
                    )
                    try:
                        await bot.send_message(CHANNEL_ID, text)
                    except Exception as exc:
                        logger.exception("daily_channel_post_loop send failed: %s", exc)
        except Exception as exc:
            logger.exception("daily_channel_post_loop error: %s", exc)


async def weekly_scheduled_posts_seed_reminder_loop() -> None:
    last_run_key = ""
    while True:
        try:
            await asyncio.sleep(1800)
            now = datetime.now()
            key = f"{now.year:04d}-{now.month:02d}-{now.day:02d}"

            if now.weekday() == 6 and now.hour == 10 and last_run_key != key:
                last_run_key = key
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, "Напоминание: загрузите контент на неделю.")
                    except Exception as exc:
                        logger.exception("weekly seed reminder failed: %s", exc)
        except Exception as exc:
            logger.exception("weekly_scheduled_posts_seed_reminder_loop error: %s", exc)

# ============================================================
# PART 7 / 10
# WEBAPP HTML + shop ui + reviews block + light premium design
# ============================================================

SHOP_HTML = r"""
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ZARY & CO</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root{
  --bg:#f7f4ee;
  --bg2:#f3eee4;
  --card:#fffdfa;
  --text:#1b1b1b;
  --muted:#7b7468;
  --line:#e7dcc8;
  --gold:#c8a96b;
  --gold-dark:#9e7b36;
  --black:#111111;
  --shadow:0 10px 30px rgba(0,0,0,.08);
  --radius:20px;
}
*{box-sizing:border-box}
body{
  margin:0;
  background:linear-gradient(180deg,var(--bg) 0%,var(--bg2) 100%);
  color:var(--text);
  font-family:Arial,Helvetica,sans-serif;
}
.wrap{
  max-width:1180px;
  margin:0 auto;
  padding:0 14px 18px;
}
.hero{
  background:linear-gradient(180deg,#f8f3ea 0%,#efe6d7 100%);
  color:#111;
  border-bottom-left-radius:24px;
  border-bottom-right-radius:24px;
  padding:24px 18px 20px;
  box-shadow:var(--shadow);
  border-bottom:1px solid #e8dbc2;
}
.hero-top{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
}
.brand-wrap{
  display:flex;
  flex-direction:column;
  gap:8px;
}
.brand{
  font-size:34px;
  font-weight:900;
  letter-spacing:.09em;
  color:var(--gold);
  text-shadow:
    0 1px 0 #fff3d6,
    0 2px 0 #ecd3a2,
    0 3px 8px rgba(0,0,0,.12);
}
.brand-sub{
  font-size:14px;
  color:#6a6358;
}
.badge{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-width:24px;
  height:24px;
  padding:0 8px;
  border-radius:999px;
  background:linear-gradient(180deg,var(--gold) 0%,var(--gold-dark) 100%);
  color:#fff;
  font-size:12px;
  font-weight:800;
  box-shadow:0 6px 16px rgba(158,123,54,.28);
}
.layout{
  display:grid;
  grid-template-columns:1.65fr .95fr;
  gap:16px;
  margin-top:16px;
}
.panel{
  background:var(--card);
  border-radius:var(--radius);
  box-shadow:var(--shadow);
  overflow:hidden;
  border:1px solid #efe3cf;
}
.panel-head{
  padding:16px 16px 8px;
  font-size:20px;
  font-weight:800;
}
.panel-sub{
  padding:0 16px 14px;
  color:var(--muted);
  font-size:13px;
  line-height:1.4;
}
.catalog{
  padding:0 12px 14px;
}
.grid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:12px;
}
.card{
  background:#fff;
  border:1px solid var(--line);
  border-radius:18px;
  padding:12px;
  box-shadow:0 6px 18px rgba(0,0,0,.04);
  display:flex;
  flex-direction:column;
  gap:10px;
}
.photo{
  width:100%;
  aspect-ratio:1/1;
  border-radius:14px;
  background:#f4efe6;
  overflow:hidden;
  display:flex;
  align-items:center;
  justify-content:center;
  border:1px solid #eee2cf;
}
.photo img{
  width:100%;
  height:100%;
  object-fit:cover;
  display:block;
}
.photo-placeholder{
  color:#9b927f;
  font-size:14px;
}
.card-title{
  font-size:16px;
  font-weight:800;
  line-height:1.25;
}
.card-desc{
  font-size:13px;
  color:#6c655a;
  line-height:1.4;
  min-height:40px;
}
.price-row{
  display:flex;
  align-items:center;
  gap:8px;
  flex-wrap:wrap;
}
.price{
  font-size:19px;
  font-weight:900;
  color:#181818;
}
.old-price{
  font-size:13px;
  color:#968c7f;
  text-decoration:line-through;
}
.meta{
  font-size:12px;
  color:#6f685d;
  display:flex;
  flex-direction:column;
  gap:4px;
}
.sizes{
  display:flex;
  flex-wrap:wrap;
  gap:6px;
}
.size-btn{
  border:1px solid #d8c7aa;
  background:#fffaf0;
  color:#443725;
  border-radius:999px;
  font-size:12px;
  padding:7px 11px;
  cursor:pointer;
}
.size-btn.active{
  background:linear-gradient(180deg,var(--gold) 0%,var(--gold-dark) 100%);
  color:#fff;
  border-color:var(--gold-dark);
}
.qty-row{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
}
.qty-box{
  display:flex;
  align-items:center;
  border:1px solid #e2d3bb;
  border-radius:999px;
  overflow:hidden;
  background:#fffaf0;
}
.qty-btn{
  background:#fffaf0;
  border:none;
  width:36px;
  height:36px;
  font-size:18px;
  cursor:pointer;
  color:#2a2318;
}
.qty-value{
  min-width:36px;
  text-align:center;
  font-weight:800;
}
.action-row{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:8px;
}
.buy-btn,.quick-btn{
  width:100%;
  border:none;
  border-radius:14px;
  padding:12px 14px;
  font-size:14px;
  font-weight:800;
  cursor:pointer;
}
.buy-btn{
  background:#111;
  color:#fff;
}
.quick-btn{
  background:linear-gradient(180deg,var(--gold) 0%,var(--gold-dark) 100%);
  color:#fff;
}
.buy-btn:disabled,.quick-btn:disabled{
  opacity:.5;
  cursor:not-allowed;
}
.cart-wrap{
  padding:14px;
}
.cart-list{
  display:flex;
  flex-direction:column;
  gap:10px;
  margin-bottom:14px;
}
.cart-item{
  border:1px solid var(--line);
  border-radius:14px;
  padding:12px;
  display:flex;
  flex-direction:column;
  gap:6px;
  background:#fff;
}
.cart-item-top{
  display:flex;
  justify-content:space-between;
  gap:12px;
}
.cart-name{
  font-weight:800;
  font-size:14px;
}
.cart-meta{
  color:#6d665b;
  font-size:12px;
}
.cart-remove{
  background:#fff;
  border:1px solid #ddd2bd;
  color:#111;
  border-radius:10px;
  padding:8px 10px;
  cursor:pointer;
}
.cart-empty{
  color:#7d776d;
  font-size:14px;
  padding:10px 0 16px;
}
.summary{
  border-top:1px solid var(--line);
  padding-top:12px;
  display:flex;
  flex-direction:column;
  gap:8px;
}
.summary-row{
  display:flex;
  justify-content:space-between;
  gap:12px;
  font-size:14px;
}
.checkout-box{
  margin-top:14px;
  border-top:1px solid var(--line);
  padding-top:14px;
}
.checkout-box-title{
  font-size:16px;
  font-weight:900;
  margin-bottom:8px;
}
.checkout-box-text{
  color:#6b655b;
  font-size:13px;
  line-height:1.45;
  margin-bottom:12px;
}
.checkout-btn,.clear-btn{
  width:100%;
  border:none;
  border-radius:14px;
  padding:12px 14px;
  font-size:14px;
  font-weight:800;
  cursor:pointer;
}
.checkout-btn{
  background:#111;
  color:#fff;
}
.clear-btn{
  background:#f4ede0;
  color:#111;
  margin-top:10px;
}
.filters{
  display:flex;
  gap:8px;
  flex-wrap:wrap;
  padding:0 12px 12px;
}
.filter-btn{
  border:none;
  background:#fffaf1;
  border:1px solid #dfceb1;
  border-radius:999px;
  padding:8px 12px;
  cursor:pointer;
  font-size:12px;
  color:#3b3123;
}
.filter-btn.active{
  background:linear-gradient(180deg,var(--gold) 0%,var(--gold-dark) 100%);
  color:#fff;
  border-color:var(--gold-dark);
}
.notice{
  position:fixed;
  left:50%;
  bottom:18px;
  transform:translateX(-50%);
  background:#111;
  color:#fff;
  padding:12px 16px;
  border-radius:999px;
  font-size:13px;
  box-shadow:0 12px 30px rgba(0,0,0,.22);
  opacity:0;
  pointer-events:none;
  transition:.25s ease;
  z-index:1000;
}
.notice.show{
  opacity:1;
}
.reviews-box{
  margin-top:16px;
  background:var(--card);
  border-radius:var(--radius);
  box-shadow:var(--shadow);
  padding:16px;
  border:1px solid #efe3cf;
}
.reviews-title{
  font-size:20px;
  font-weight:900;
  margin-bottom:10px;
}
.reviews-list{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:12px;
}
.review-card{
  background:#fff;
  border:1px solid var(--line);
  border-radius:16px;
  padding:14px;
}
.review-stars{
  font-size:16px;
  margin-bottom:6px;
}
.review-name{
  font-size:14px;
  font-weight:800;
  margin-bottom:6px;
}
.review-text{
  font-size:13px;
  line-height:1.45;
  color:#625b50;
}
.social-box{
  margin-top:16px;
  background:var(--card);
  border-radius:var(--radius);
  box-shadow:var(--shadow);
  padding:16px;
  border:1px solid #efe3cf;
}
.social-title{
  font-size:18px;
  font-weight:900;
  margin-bottom:10px;
}
.social-links{
  display:flex;
  flex-wrap:wrap;
  gap:10px;
}
.social-links a{
  text-decoration:none;
  padding:10px 14px;
  border-radius:999px;
  background:linear-gradient(180deg,var(--gold) 0%,var(--gold-dark) 100%);
  color:#fff;
  font-weight:800;
  font-size:13px;
}
@media (max-width: 900px){
  .layout{
    grid-template-columns:1fr;
  }
}
@media (max-width: 640px){
  .grid,
  .reviews-list{
    grid-template-columns:1fr;
  }
  .brand{
    font-size:26px;
  }
  .action-row{
    grid-template-columns:1fr;
  }
}
</style>
</head>
<body>
<div class="hero">
  <div class="wrap">
    <div class="hero-top">
      <div class="brand-wrap">
        <div class="brand">ZARY & CO</div>
        <div class="brand-sub" id="heroSub">Premium Telegram shop</div>
      </div>
      <div class="badge" id="cartBadge">0</div>
    </div>
  </div>
</div>

<div class="wrap">
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
        <div class="cart-empty" id="cartEmpty">Корзина пуста</div>

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
          <div class="checkout-box-text" id="checkoutBoxText">
            Сначала добавьте товары в корзину. Затем нажмите кнопку ниже и бот продолжит оформление заказа.
          </div>
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
if (tg) {
  tg.ready();
  tg.expand();
}

const params = new URLSearchParams(window.location.search);
const lang = params.get("lang") || "ru";

const I18N = {
  ru: {
    heroSub: "Премиальный магазин внутри Telegram",
    catalogTitle: "Каталог",
    catalogSub: "Выберите размер, количество и добавьте товар в корзину или купите сразу.",
    cartTitle: "Корзина",
    cartEmpty: "Корзина пуста",
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
    startCheckoutMsg: "Откройте Telegram чат с ботом и нажмите «🛒 Корзина», затем «Оформить заказ».",
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
  },
  uz: {
    heroSub: "Telegram ichidagi premium do'kon",
    catalogTitle: "Katalog",
    catalogSub: "O‘lcham va sonni tanlab mahsulotni savatchaga qo‘shing yoki darhol sotib oling.",
    cartTitle: "Savatcha",
    cartEmpty: "Savatcha bo‘sh",
    summaryQtyLabel: "Jami mahsulot",
    summaryAmountLabel: "Summa",
    checkoutBoxTitle: "Buyurtma rasmiylashtirish",
    checkoutBoxText: "Avval mahsulotlarni savatchaga qo‘shing. Keyin tugmani bosing va bot buyurtmani davom ettiradi.",
    checkoutBtn: "Bot orqali rasmiylashtirish",
    clearBtn: "Savatchani tozalash",
    addToCart: "Savatchaga",
    buyNow: "Hozir sotib olish",
    sizes: "O‘lchamlar",
    stock: "Qoldiq",
    qty: "Soni",
    added: "Mahsulot savatchaga qo‘shildi",
    removed: "Pozitsiya o‘chirildi",
    cleared: "Savatcha tozalandi",
    chooseSize: "O‘lchamni tanlang",
    noProducts: "Hozircha mahsulotlar yo‘q",
    noPhoto: "Rasmsiz",
    startCheckoutMsg: "Telegram chatdagi botga qayting va «🛒 Savatcha», keyin buyurtma tugmasini bosing.",
    reviewsTitle: "Xaridorlar sharhlari",
    noReviews: "Hozircha sharhlar yo‘q",
    socialTitle: "Kanallarimiz",
    category_all: "Barchasi",
    category_new: "Yangi",
    category_hits: "Xitlar",
    category_sale: "Chegirma",
    category_limited: "Limit",
    category_school: "Maktab",
    category_casual: "Kundalik"
  }
};

const TXT = I18N[lang] || I18N.ru;

const state = {
  products: [],
  filteredProducts: [],
  cart: [],
  reviews: [],
  activeCategory: "all"
};

function fmtSum(value){
  const n = Number(value || 0);
  return n.toLocaleString("ru-RU") + " сум";
}

function showNotice(text){
  const n = document.getElementById("notice");
  n.textContent = text;
  n.classList.add("show");
  setTimeout(() => n.classList.remove("show"), 1800);
}

function applyTexts(){
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
}

function buildFilters(){
  const box = document.getElementById("filters");
  const categories = ["all", "new", "hits", "sale", "limited", "school", "casual"];
  box.innerHTML = "";

  categories.forEach(cat => {
    const btn = document.createElement("button");
    btn.className = "filter-btn" + (state.activeCategory === cat ? " active" : "");
    btn.textContent = TXT["category_" + cat] || cat;
    btn.onclick = () => {
      state.activeCategory = cat;
      buildFilters();
      applyFilter();
    };
    box.appendChild(btn);
  });
}

function applyFilter(){
  if (state.activeCategory === "all"){
    state.filteredProducts = [...state.products];
  } else {
    state.filteredProducts = state.products.filter(p => p.category_slug === state.activeCategory);
  }
  renderProducts();
}

async function loadProducts(){
  const res = await fetch(`/api/shop/products?lang=${encodeURIComponent(lang)}`);
  const data = await res.json();
  state.products = Array.isArray(data) ? data : [];
  applyFilter();
}

async function loadCart(){
  if (!tg || !tg.initDataUnsafe || !tg.initDataUnsafe.user){
    renderCart();
    return;
  }

  const userId = tg.initDataUnsafe.user.id;
  const res = await fetch(`/api/shop/cart?user_id=${encodeURIComponent(userId)}`);
  const data = await res.json();
  state.cart = data.items || [];
  renderCart();
}

async function loadReviews(){
  const res = await fetch(`/api/shop/reviews?lang=${encodeURIComponent(lang)}`);
  const data = await res.json();
  state.reviews = Array.isArray(data) ? data : [];
  renderReviews();
}

function renderProducts(){
  const grid = document.getElementById("productGrid");
  grid.innerHTML = "";

  if (!state.filteredProducts.length){
    const empty = document.createElement("div");
    empty.className = "card";
    empty.innerHTML = `<div class="card-desc">${TXT.noProducts}</div>`;
    grid.appendChild(empty);
    return;
  }

  state.filteredProducts.forEach(product => {
    const card = document.createElement("div");
    card.className = "card";

    const img = product.photo_url
      ? `<img src="${product.photo_url}" alt="">`
      : `<div class="photo-placeholder">${TXT.noPhoto}</div>`;

    const sizes = Array.isArray(product.sizes_list) ? product.sizes_list : [];
    const sizeHtml = sizes.length
      ? `<div class="sizes">${
          sizes.map((s) => `<button class="size-btn" data-size="${s}">${s}</button>`).join("")
        }</div>`
      : `<div class="meta">${TXT.sizes}: —</div>`;

    card.innerHTML = `
      <div class="photo">${img}</div>
      <div class="card-title">${product.title}</div>
      <div class="card-desc">${product.description || ""}</div>
      <div class="price-row">
        <div class="price current-total">${fmtSum(product.price)}</div>
        ${product.old_price && Number(product.old_price) > 0 ? `<div class="old-price">${fmtSum(product.old_price)}</div>` : ``}
      </div>
      <div class="meta">
        <div>${TXT.stock}: ${product.stock_qty}</div>
        <div>${TXT.sizes}: ${sizes.length ? sizes.join(", ") : "—"}</div>
      </div>
      ${sizeHtml}
      <div class="qty-row">
        <div class="qty-box">
          <button class="qty-btn minus">−</button>
          <div class="qty-value">1</div>
          <button class="qty-btn plus">+</button>
        </div>
        <div class="meta">${TXT.qty}</div>
      </div>
      <div class="action-row">
        <button class="buy-btn"${Number(product.stock_qty) <= 0 ? " disabled" : ""}>${TXT.addToCart}</button>
        <button class="quick-btn"${Number(product.stock_qty) <= 0 ? " disabled" : ""}>${TXT.buyNow}</button>
      </div>
    `;

    const qtyValue = card.querySelector(".qty-value");
    const currentTotal = card.querySelector(".current-total");
    let qty = 1;

    function recalcTotal(){
      currentTotal.textContent = fmtSum(Number(product.price || 0) * qty);
    }

    card.querySelector(".minus").onclick = () => {
      qty = Math.max(1, qty - 1);
      qtyValue.textContent = String(qty);
      recalcTotal();
    };

    card.querySelector(".plus").onclick = () => {
      qty = Math.min(99, qty + 1);
      qtyValue.textContent = String(qty);
      recalcTotal();
    };

    let activeSize = "";
    const sizeButtons = card.querySelectorAll(".size-btn");
    if (sizeButtons.length && sizeButtons[0].dataset.size) {
      sizeButtons[0].classList.add("active");
      activeSize = sizeButtons[0].dataset.size;
    }

    sizeButtons.forEach(btn => {
      btn.onclick = () => {
        sizeButtons.forEach(x => x.classList.remove("active"));
        btn.classList.add("active");
        activeSize = btn.dataset.size || "";
      };
    });

    card.querySelector(".buy-btn").onclick = () => {
      if (!tg || !tg.initDataUnsafe || !tg.initDataUnsafe.user){
        showNotice(TXT.startCheckoutMsg);
        return;
      }

      if (sizes.length && !activeSize){
        showNotice(TXT.chooseSize);
        return;
      }

      tg.sendData(JSON.stringify({
        action: "add_to_cart",
        product_id: product.id,
        qty: qty,
        size: activeSize
      }));

      showNotice(TXT.added);
      setTimeout(() => loadCart(), 500);
    };

    card.querySelector(".quick-btn").onclick = () => {
      if (!tg || !tg.initDataUnsafe || !tg.initDataUnsafe.user){
        showNotice(TXT.startCheckoutMsg);
        return;
      }

      if (sizes.length && !activeSize){
        showNotice(TXT.chooseSize);
        return;
      }

      tg.sendData(JSON.stringify({
        action: "buy_now",
        product_id: product.id,
        qty: qty,
        size: activeSize
      }));

      showNotice(TXT.added);
      setTimeout(() => loadCart(), 500);
    };

    grid.appendChild(card);
  });
}

function renderCart(){
  const list = document.getElementById("cartList");
  const empty = document.getElementById("cartEmpty");
  const badge = document.getElementById("cartBadge");
  const qtyEl = document.getElementById("summaryQty");
  const amountEl = document.getElementById("summaryAmount");

  list.innerHTML = "";

  let totalQty = 0;
  let totalAmount = 0;

  (state.cart || []).forEach(item => {
    totalQty += Number(item.qty || 0);
    totalAmount += Number(item.subtotal || 0);

    const div = document.createElement("div");
    div.className = "cart-item";
    div.innerHTML = `
      <div class="cart-item-top">
        <div>
          <div class="cart-name">${item.product_name}</div>
          <div class="cart-meta">
            ${item.size ? item.size + " | " : ""}${item.qty} × ${fmtSum(item.price)}
          </div>
        </div>
        <button class="cart-remove">×</button>
      </div>
      <div class="cart-meta">${fmtSum(item.subtotal)}</div>
    `;

    div.querySelector(".cart-remove").onclick = () => {
      if (!tg) return;
      tg.sendData(JSON.stringify({
        action: "remove_from_cart",
        cart_id: item.cart_id
      }));
      showNotice(TXT.removed);
      setTimeout(() => loadCart(), 500);
    };

    list.appendChild(div);
  });

  empty.style.display = state.cart.length ? "none" : "block";
  badge.textContent = String(totalQty);
  qtyEl.textContent = String(totalQty);
  amountEl.textContent = fmtSum(totalAmount);
}

function renderReviews(){
  const box = document.getElementById("reviewsList");
  box.innerHTML = "";

  if (!state.reviews.length){
    const div = document.createElement("div");
    div.className = "review-card";
    div.innerHTML = `<div class="review-text">${TXT.noReviews}</div>`;
    box.appendChild(div);
    return;
  }

  state.reviews.forEach(item => {
    const div = document.createElement("div");
    div.className = "review-card";
    div.innerHTML = `
      <div class="review-stars">${item.stars || ""}</div>
      <div class="review-name">${item.customer_name || "Клиент"}</div>
      <div class="review-text">${item.text || ""}</div>
    `;
    box.appendChild(div);
  });
}

function renderSocials(){
  const box = document.getElementById("socialLinks");
  box.innerHTML = "";

  const links = [
    {title:"Telegram", url:"__CHANNEL_LINK__"},
    {title:"Instagram", url:"__INSTAGRAM_LINK__"},
    {title:"YouTube", url:"__YOUTUBE_LINK__"},
  ].filter(x => x.url && x.url !== "__CHANNEL_LINK__" && x.url !== "__INSTAGRAM_LINK__" && x.url !== "__YOUTUBE_LINK__");

  links.forEach(item => {
    const a = document.createElement("a");
    a.href = item.url;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.textContent = item.title;
    box.appendChild(a);
  });
}

document.getElementById("clearBtn").onclick = () => {
  if (!tg) return;
  tg.sendData(JSON.stringify({action: "clear_cart"}));
  showNotice(TXT.cleared);
  setTimeout(() => loadCart(), 500);
};

document.getElementById("checkoutBtn").onclick = () => {
  showNotice(TXT.startCheckoutMsg);
};

applyTexts();
buildFilters();
renderSocials();
loadProducts();
loadCart();
loadReviews();
</script>
</body>
</html>
""".replace("__CHANNEL_LINK__", CHANNEL_LINK or "").replace("__INSTAGRAM_LINK__", INSTAGRAM_LINK or "").replace("__YOUTUBE_LINK__", YOUTUBE_LINK or "")

# ============================================================
# PART 8 / 10
# web routes, api, web admin pages
# ============================================================

# ============================================================
# WEB ADMIN HTML TEMPLATE
# ============================================================

def admin_access_ok(request: web.Request) -> bool:
    token = request.query.get("token", "").strip()
    return bool(ADMIN_PANEL_TOKEN) and token == ADMIN_PANEL_TOKEN


def admin_page_template(title: str, body: str) -> str:
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>{title}</title>
      <style>
        body {{
          margin:0;
          background:#f7f4ee;
          font-family:Arial,Helvetica,sans-serif;
          color:#161616;
        }}
        .wrap {{
          max-width:1240px;
          margin:0 auto;
          padding:20px;
        }}
        .top {{
          background:linear-gradient(180deg,#f8f3ea 0%,#efe6d7 100%);
          color:#111;
          border-radius:22px;
          padding:18px 20px;
          margin-bottom:18px;
          box-shadow:0 8px 24px rgba(0,0,0,.10);
          border:1px solid #e8dbc2;
        }}
        .brand {{
          font-size:30px;
          font-weight:900;
          letter-spacing:.07em;
          color:#c8a96b;
          text-shadow:
            0 1px 0 #fff3d6,
            0 2px 0 #ecd3a2,
            0 3px 8px rgba(0,0,0,.12);
        }}
        .nav {{
          display:flex;
          flex-wrap:wrap;
          gap:10px;
          margin-top:12px;
        }}
        .nav a {{
          color:#fff;
          text-decoration:none;
          padding:9px 14px;
          background:linear-gradient(180deg,#c8a96b 0%,#9e7b36 100%);
          border-radius:999px;
          font-weight:700;
          font-size:13px;
        }}
        .card {{
          background:#fffdfa;
          border-radius:20px;
          padding:18px;
          box-shadow:0 8px 24px rgba(0,0,0,.06);
          margin-bottom:16px;
          border:1px solid #efe3cf;
        }}
        .stats {{
          display:grid;
          grid-template-columns:repeat(5,minmax(0,1fr));
          gap:12px;
        }}
        .stat {{
          background:#fffdfa;
          border-radius:16px;
          padding:16px;
          box-shadow:0 8px 24px rgba(0,0,0,.05);
          border:1px solid #efe3cf;
        }}
        .stat-label {{
          color:#756d60;
          font-size:12px;
          margin-bottom:8px;
        }}
        .stat-value {{
          font-size:26px;
          font-weight:900;
        }}
        table {{
          width:100%;
          border-collapse:collapse;
        }}
        th, td {{
          border-bottom:1px solid #ece1cf;
          text-align:left;
          padding:10px 8px;
          font-size:14px;
          vertical-align:top;
        }}
        th {{
          background:#faf4ea;
        }}
        .muted {{
          color:#7b7468;
          font-size:13px;
        }}
        .filters {{
          display:flex;
          gap:8px;
          flex-wrap:wrap;
          margin-bottom:12px;
        }}
        .filters a {{
          text-decoration:none;
          color:#fff;
          background:linear-gradient(180deg,#c8a96b 0%,#9e7b36 100%);
          padding:8px 12px;
          border-radius:999px;
          font-size:12px;
          font-weight:700;
        }}
        .pill {{
          display:inline-block;
          padding:6px 10px;
          border-radius:999px;
          background:#f6ecdb;
          border:1px solid #e3d1af;
          font-size:12px;
          font-weight:700;
        }}
        .review-card {{
          background:#fff;
          border:1px solid #eee2cf;
          border-radius:16px;
          padding:14px;
          margin-bottom:12px;
        }}
        .review-stars {{
          margin-bottom:6px;
          font-size:16px;
        }}
        .review-name {{
          font-weight:800;
          margin-bottom:6px;
        }}
        .review-text {{
          color:#645c50;
          line-height:1.45;
        }}
        @media (max-width: 1080px) {{
          .stats {{
            grid-template-columns:repeat(2,minmax(0,1fr));
          }}
        }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="top">
          <div class="brand">ZARY & CO ADMIN</div>
          <div class="nav">
            <a href="/admin?token={ADMIN_PANEL_TOKEN}">Dashboard</a>
            <a href="/admin/orders?token={ADMIN_PANEL_TOKEN}">Orders</a>
            <a href="/admin/products?token={ADMIN_PANEL_TOKEN}">Products</a>
            <a href="/admin/reviews?token={ADMIN_PANEL_TOKEN}">Reviews</a>
          </div>
        </div>
        {body}
      </div>
    </body>
    </html>
    """


# ============================================================
# SHOP PAGE / API
# ============================================================

@web_router.get("/shop")
async def shop_page(request: web.Request) -> web.Response:
    return web.Response(text=SHOP_HTML, content_type="text/html")


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

    result = []
    for row in rows:
        result.append(
            {
                "id": row["id"],
                "customer_name": row["customer_name"] or "Клиент",
                "rating": safe_int(row["rating"], 5),
                "stars": stars_text(row["rating"]),
                "text": row["text"] or "",
                "created_at": row["created_at"],
            }
        )

    return web.json_response(result)


@web_router.post("/api/shop/order")
async def api_shop_order(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

    user_id = safe_int(payload.get("user_id"))
    if not user_id:
        return web.json_response({"ok": False, "error": "user_id_required"}, status=400)

    rows = get_cart_rows(user_id)
    if not rows:
        return web.json_response({"ok": False, "error": "cart_empty"}, status=400)

    checkout_data = {
        "customer_name": payload.get("customer_name", ""),
        "customer_phone": normalize_phone(payload.get("customer_phone", "")),
        "city": payload.get("city", ""),
        "delivery_service": payload.get("delivery_service", ""),
        "delivery_type": payload.get("delivery_type", ""),
        "delivery_address": payload.get("delivery_address", ""),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "pvz_code": payload.get("pvz_code", ""),
        "pvz_address": payload.get("pvz_address", ""),
        "payment_method": payload.get("payment_method", ""),
        "comment": payload.get("comment", ""),
    }

    if not is_valid_phone(checkout_data["customer_phone"]):
        return web.json_response({"ok": False, "error": "invalid_phone"}, status=400)

    order_id = create_order_from_checkout(
        user_id=user_id,
        username=str(payload.get("username", "")),
        checkout_data=checkout_data,
        source="webapp",
    )

    await notify_admins_about_order(order_id)

    order = get_order_by_id(order_id)
    return web.json_response(
        {
            "ok": True,
            "order_id": order_id,
            "payment_provider_url": order["payment_provider_url"] if order else "",
        }
    )


@web_router.get("/health")
async def health_route(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


@web_router.get("/media/{file_id}")
async def media_placeholder_route(request: web.Request) -> web.Response:
    file_id = request.match_info.get("file_id", "")
    return web.Response(text=f"media placeholder: {file_id}", content_type="text/plain")


# ============================================================
# PAYMENT PLACEHOLDERS
# ============================================================

@web_router.get("/pay/click/{order_id}")
async def pay_click_route(request: web.Request) -> web.Response:
    order_id = safe_int(request.match_info.get("order_id"))
    html = f"""
    <html>
    <head><title>Click payment</title></head>
    <body style="font-family:Arial;padding:30px;background:#f7f4ee">
      <div style="max-width:560px;margin:0 auto;background:#fffdfa;padding:24px;border-radius:18px;box-shadow:0 8px 24px rgba(0,0,0,.08);border:1px solid #efe3cf">
        <h2 style="margin-top:0">Click payment</h2>
        <p>Order ID: <b>{order_id}</b></p>
        <p>Интеграция Click будет подключена сюда.</p>
      </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")


@web_router.get("/pay/payme/{order_id}")
async def pay_payme_route(request: web.Request) -> web.Response:
    order_id = safe_int(request.match_info.get("order_id"))
    html = f"""
    <html>
    <head><title>Payme payment</title></head>
    <body style="font-family:Arial;padding:30px;background:#f7f4ee">
      <div style="max-width:560px;margin:0 auto;background:#fffdfa;padding:24px;border-radius:18px;box-shadow:0 8px 24px rgba(0,0,0,.08);border:1px solid #efe3cf">
        <h2 style="margin-top:0">Payme payment</h2>
        <p>Order ID: <b>{order_id}</b></p>
        <p>Интеграция Payme будет подключена сюда.</p>
      </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")


# ============================================================
# WEB ADMIN ROUTES
# ============================================================

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
      <div class="stat"><div class="stat-label">Отправленные</div><div class="stat-value">{stats['sent']}</div></div>
      <div class="stat"><div class="stat-label">Доставленные</div><div class="stat-value">{stats['delivered']}</div></div>
      <div class="stat"><div class="stat-label">Отменённые</div><div class="stat-value">{stats['cancelled']}</div></div>
      <div class="stat"><div class="stat-label">Уникальные пользователи</div><div class="stat-value">{stats['unique_users']}</div></div>
      <div class="stat"><div class="stat-label">Отзывы</div><div class="stat-value">{stats['reviews']}</div></div>
    </div>
    """
    return web.Response(
        text=admin_page_template("Admin dashboard", body),
        content_type="text/html",
    )


@web_router.get("/admin/orders")
async def admin_orders_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)

    status_filter = (request.query.get("status") or "").strip()

    conn = get_db()
    if status_filter and status_filter in ORDER_STATUSES:
        rows = conn.execute(
            """
            SELECT *
            FROM orders
            WHERE status = ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (status_filter,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM orders
            ORDER BY id DESC
            LIMIT 200
            """
        ).fetchall()
    conn.close()

    filters = ['<div class="filters">']
    filters.append(f'<a href="/admin/orders?token={ADMIN_PANEL_TOKEN}">Все</a>')
    for status in ORDER_STATUSES:
        filters.append(
            f'<a href="/admin/orders?token={ADMIN_PANEL_TOKEN}&status={status}">{status}</a>'
        )
    filters.append("</div>")

    table_rows = []
    for row in rows:
        username = mask_username(row["username"])
        table_rows.append(
            f"""
            <tr>
              <td>#{row['id']}</td>
              <td>{row['customer_name'] or '—'}<div class="muted">{row['customer_phone'] or '—'}</div></td>
              <td>{username}<div class="muted">{row['user_id']}</div></td>
              <td>{row['city'] or '—'}</td>
              <td>{delivery_label('ru', row['delivery_service'] or '')}</td>
              <td>{payment_method_label('ru', row['payment_method'] or '')}<div class="muted">{payment_status_label('ru', row['payment_status'])}</div></td>
              <td>{fmt_sum(row['total_amount'])}</td>
              <td><span class="pill">{status_label('ru', row['status'])}</span></td>
              <td>{row['created_at']}</td>
            </tr>
            """
        )

    body = (
        "".join(filters)
        + """
        <div class="card">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Клиент</th>
                <th>Telegram</th>
                <th>Город</th>
                <th>Доставка</th>
                <th>Оплата</th>
                <th>Сумма</th>
                <th>Статус</th>
                <th>Дата</th>
              </tr>
            </thead>
            <tbody>
        """
        + "".join(table_rows)
        + """
            </tbody>
          </table>
        </div>
        """
    )

    return web.Response(
        text=admin_page_template("Admin orders", body),
        content_type="text/html",
    )


@web_router.get("/admin/products")
async def admin_products_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)

    conn = get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM shop_products
        ORDER BY sort_order ASC, id DESC
        LIMIT 300
        """
    ).fetchall()
    conn.close()

    table_rows = []
    for row in rows:
        table_rows.append(
            f"""
            <tr>
              <td>#{row['id']}</td>
              <td>{row['title_ru']}</td>
              <td>{row['title_uz']}</td>
              <td>{row['category_slug']}</td>
              <td>{fmt_sum(row['price'])}</td>
              <td>{fmt_sum(row['old_price']) if safe_int(row['old_price']) > 0 else '—'}</td>
              <td>{row['sizes'] or '—'}</td>
              <td>{row['stock_qty']}</td>
              <td>{'Да' if row['is_published'] else 'Нет'}</td>
              <td>{row['sort_order']}</td>
            </tr>
            """
        )

    body = """
    <div class="card">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Title RU</th>
            <th>Title UZ</th>
            <th>Category</th>
            <th>Price</th>
            <th>Old price</th>
            <th>Sizes</th>
            <th>Stock</th>
            <th>Published</th>
            <th>Sort</th>
          </tr>
        </thead>
        <tbody>
    """ + "".join(table_rows) + """
        </tbody>
      </table>
    </div>
    """

    return web.Response(
        text=admin_page_template("Admin products", body),
        content_type="text/html",
    )


@web_router.get("/admin/reviews")
async def admin_reviews_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.Response(text="Access denied", status=403)

    rows = get_pending_reviews(limit=200)

    if not rows:
        body = '<div class="card">Новых отзывов нет.</div>'
        return web.Response(
            text=admin_page_template("Admin reviews", body),
            content_type="text/html",
        )

    blocks = []
    for row in rows:
        blocks.append(
            f"""
            <div class="review-card">
              <div class="review-stars">{stars_text(row['rating'])} ({row['rating']}/5)</div>
              <div class="review-name">{row['customer_name'] or '—'} | {mask_username(row['username'])}</div>
              <div class="review-text">{row['text'] or ''}</div>
              <div class="muted" style="margin-top:8px;">
                ID: #{row['id']} | Published: {'Да' if safe_int(row['is_published']) else 'Нет'} | Date: {row['created_at']}
              </div>
            </div>
            """
        )

    body = '<div class="card">' + "".join(blocks) + "</div>"
    return web.Response(
        text=admin_page_template("Admin reviews", body),
        content_type="text/html",
    )


@web_router.get("/api/admin/analytics")
async def api_admin_analytics_route(request: web.Request) -> web.Response:
    if not admin_access_ok(request):
        return web.json_response({"ok": False, "error": "forbidden"}, status=403)
    return web.json_response(get_basic_stats())


# ============================================================
# APP FACTORY
# ============================================================

def create_web_app() -> web.Application:
    app = web.Application()
    app.add_routes(web_router)
    return app

# ============================================================
# PART 9 / 10
# payment helpers skeleton, startup, router registration, main
# ============================================================

# ============================================================
# CLICK / PAYME SKELETON HELPERS
# ============================================================

def click_is_configured() -> bool:
    return bool(CLICK_SERVICE_ID and CLICK_MERCHANT_ID and CLICK_SECRET_KEY)


def payme_is_configured() -> bool:
    return bool(PAYME_MERCHANT_ID and PAYME_SECRET_KEY)


def make_click_signature(data: str) -> str:
    if not CLICK_SECRET_KEY:
        return ""
    return hmac.new(
        CLICK_SECRET_KEY.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def make_payme_basic_token() -> str:
    if not PAYME_MERCHANT_ID or not PAYME_SECRET_KEY:
        return ""
    raw = f"{PAYME_MERCHANT_ID}:{PAYME_SECRET_KEY}"
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


def build_click_payment_url(order_id: int) -> str:
    return f"{BASE_URL}/pay/click/{order_id}"


def build_payme_payment_url(order_id: int) -> str:
    return f"{BASE_URL}/pay/payme/{order_id}"


def update_order_payment_meta(order_id: int, invoice_id: str = "", payment_url: str = "") -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE orders
        SET payment_provider_invoice_id = ?,
            payment_provider_url = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (invoice_id or "", payment_url or "", utc_now_iso(), order_id),
    )
    conn.commit()
    conn.close()


# ============================================================
# EXTRA ADMIN COMMANDS
# ============================================================

@admin_router.message(Command("order"))
async def admin_open_order_by_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Используй: /order 15")
        return

    order_id = int(parts[1])
    order = get_order_by_id(order_id)
    if not order:
        await message.answer("Заказ не найден.")
        return

    await message.answer(
        admin_order_card_text(order, "ru"),
        reply_markup=admin_order_actions_keyboard(order["id"], order["user_id"]),
    )


@admin_router.message(Command("product"))
async def admin_open_product_by_command(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "not_admin"))
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Используй: /product 3")
        return

    product_id = int(parts[1])
    product = get_product_by_id(product_id)
    if not product:
        await message.answer("Товар не найден.")
        return

    text = product_card_text(product)
    kb = product_manage_keyboard(product["id"], safe_int(product["is_published"]))

    if product["photo_file_id"]:
        try:
            await bot.send_photo(
                chat_id=message.from_user.id,
                photo=product["photo_file_id"],
                caption=text,
                reply_markup=kb,
            )
            return
        except Exception:
            pass

    await message.answer(text, reply_markup=kb)


# ============================================================
# STARTUP VALIDATION
# ============================================================

def startup_validation() -> None:
    warnings: list[str] = []

    if not BASE_URL:
        warnings.append("BASE_URL is empty")
    if not ADMIN_IDS:
        warnings.append("ADMIN_IDS is empty")
    if not ADMIN_PANEL_TOKEN:
        warnings.append("ADMIN_PANEL_TOKEN is empty")
    if not CHANNEL_ID:
        warnings.append("CHANNEL_ID is empty or zero")

    if not click_is_configured():
        warnings.append("Click integration is not configured yet")

    if not payme_is_configured():
        warnings.append("Payme integration is not configured yet")

    for item in warnings:
        logger.warning("Startup warning: %s", item)


# ============================================================
# ROUTER REGISTRATION
# ============================================================

def register_routers() -> None:
    dp.include_router(user_router)
    dp.include_router(cart_router)
    dp.include_router(checkout_router)
    dp.include_router(orders_router)
    dp.include_router(reviews_router)
    dp.include_router(admin_router)


# ============================================================
# FINAL FALLBACK
# ============================================================

@dp.message()
async def fallback_handler(message: Message) -> None:
    await message.answer(
        t(message.from_user.id, "send_start_again"),
        reply_markup=user_main_menu(message.from_user.id),
    )


# ============================================================
# BACKGROUND TASKS STARTER
# ============================================================

async def final_background_tasks_start() -> None:
    asyncio.create_task(remind_admins_about_unseen_orders_loop())
    asyncio.create_task(scheduled_posts_loop())
    asyncio.create_task(sunday_admin_reminder_loop())
    asyncio.create_task(daily_channel_post_loop())
    asyncio.create_task(weekly_scheduled_posts_seed_reminder_loop())


# ============================================================
# MAIN
# ============================================================

async def main() -> None:
    startup_validation()
    init_db()
    patch_database_defaults()
    seed_demo_products_if_empty()
    register_routers()

    app = create_web_app()
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info("Web server started on port %s", PORT)

    await final_background_tasks_start()

    logger.info("Bot polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
