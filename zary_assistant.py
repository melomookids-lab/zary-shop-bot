# ==========================================================
# ZARY SHOP BOT
# Telegram + WebApp + Admin + API
# aiogram 3.x / aiohttp / SQLite
# ==========================================================

import os
import json
import asyncio
import logging
import sqlite3
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    KeyboardButton,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
    Location
)
from aiogram.filters import CommandStart

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from aiohttp import web
from openpyxl import Workbook

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(level=logging.INFO)

# ==========================================================
# ENV CONFIG (Render)
# ==========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")

ADMIN_IDS = [
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x
]

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))

BASE_URL = os.getenv("BASE_URL", "")

ADMIN_PANEL_TOKEN = os.getenv("ADMIN_PANEL_TOKEN", "")

CRON_SECRET = os.getenv("CRON_SECRET", "")

# ==========================================================
# BOT
# ==========================================================

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# ==========================================================
# DATABASE
# ==========================================================

DB_PATH = "bot.db"


def db():
    return sqlite3.connect(DB_PATH)


def init_db():

    conn = db()
    cur = conn.cursor()

    # USERS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        full_name TEXT,
        lang TEXT,
        created_at TEXT,
        updated_at TEXT
    )
    """)

    # CARTS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS carts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        product_id INTEGER,
        product_name TEXT,
        price INTEGER,
        qty INTEGER,
        size TEXT,
        photo_file_id TEXT,
        added_at TEXT
    )
    """)

    # ORDERS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        username TEXT,
        customer_name TEXT,
        customer_phone TEXT,
        city TEXT,
        items TEXT,
        total_qty INTEGER,
        total_amount INTEGER,
        delivery_service TEXT,
        delivery_type TEXT,
        delivery_address TEXT,
        latitude REAL,
        longitude REAL,
        pvz_code TEXT,
        pvz_address TEXT,
        payment_method TEXT,
        payment_status TEXT,
        payment_provider_invoice_id TEXT,
        payment_provider_url TEXT,
        comment TEXT,
        status TEXT,
        manager_seen INTEGER,
        manager_id INTEGER,
        source TEXT,
        created_at TEXT,
        updated_at TEXT,
        reminded_at TEXT
    )
    """)

    # PRODUCTS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shop_products(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        photo_file_id TEXT,
        title_ru TEXT,
        title_uz TEXT,
        description_ru TEXT,
        description_uz TEXT,
        sizes TEXT,
        category_slug TEXT,
        price INTEGER,
        old_price INTEGER,
        price_on_request INTEGER,
        stock_qty INTEGER,
        is_published INTEGER,
        sort_order INTEGER,
        created_at TEXT,
        updated_at TEXT
    )
    """)

    # EVENTS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        event_type TEXT,
        data TEXT,
        created_at TEXT
    )
    """)

    # SCHEDULED POSTS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_posts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text TEXT,
        media TEXT,
        post_time TEXT,
        created_at TEXT
    )
    """)

    # REPORTS

    cur.execute("""
    CREATE TABLE IF NOT EXISTS monthly_reports(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT,
        file_path TEXT,
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()

# ==========================================================
# I18N TRANSLATIONS
# ==========================================================

TEXTS = {

"ru": {

"menu_shop":"🛍 Магазин",
"menu_cart":"🛒 Корзина",
"menu_orders":"📦 Мои заказы",
"menu_size":"📏 Подбор размера",
"menu_contacts":"📞 Контакты",
"menu_lang":"🌐 Язык",
"menu_admin":"🛠 Админ",

"welcome":
"Добро пожаловать в ZARY & CO\n\n"
"Премиальная одежда для детей.",

"cart_empty":"Корзина пуста",

"checkout":"Оформить заказ",
"cart_clear":"Очистить корзину",

"enter_name":"Введите имя",
"enter_phone":"Введите телефон",

"choose_delivery":"Выберите способ доставки",

"delivery_yandex":"🚚 Яндекс курьер",
"delivery_post":"📦 B2B почта",
"delivery_pvz":"🏪 Яндекс ПВЗ",

"choose_address":"Как указать адрес",

"send_location":"📍 Отправить локацию",
"enter_address":"✍️ Ввести адрес",

"enter_city":"Введите город",

"enter_comment":"Комментарий к заказу",

"choose_payment":"Выберите оплату",

"payment_click":"Click",
"payment_payme":"Payme",

"confirm_order":"Подтвердить заказ",

"order_created":
"Спасибо за заказ!\n"
"Ваш номер заказа:",

"contacts":
"Контакты\n\n"
"Telegram менеджер\n"
"Instagram\n"
"YouTube",

},

"uz":{

"menu_shop":"🛍 Do‘kon",
"menu_cart":"🛒 Savatcha",
"menu_orders":"📦 Buyurtmalarim",
"menu_size":"📏 O‘lcham tanlash",
"menu_contacts":"📞 Kontaktlar",
"menu_lang":"🌐 Til",
"menu_admin":"🛠 Admin",

"welcome":
"ZARY & CO ga xush kelibsiz\n\n"
"Bolalar uchun premium kiyimlar.",

"cart_empty":"Savatcha bo‘sh",

"checkout":"Buyurtma berish",
"cart_clear":"Savatchani tozalash",

"enter_name":"Ismingizni kiriting",
"enter_phone":"Telefon kiriting",

"choose_delivery":"Yetkazib berish usuli",

"delivery_yandex":"🚚 Yandex kuryer",
"delivery_post":"📦 B2B pochta",
"delivery_pvz":"🏪 Yandex PVZ",

"choose_address":"Manzilni qanday kiritasiz",

"send_location":"📍 Lokatsiya yuborish",
"enter_address":"✍️ Manzil yozish",

"enter_city":"Shaharni kiriting",

"enter_comment":"Izoh",

"choose_payment":"To‘lov usuli",

"payment_click":"Click",
"payment_payme":"Payme",

"confirm_order":"Buyurtmani tasdiqlash",

"order_created":
"Buyurtma uchun rahmat!\n"
"Buyurtma raqami:",

"contacts":
"Kontaktlar\n\n"
"Telegram menejer\n"
"Instagram\n"
"YouTube"

}

}

# ==========================================================
# CATEGORIES
# ==========================================================

CATEGORIES = [

"new",
"hits",
"sale",
"limited",
"school",
"casual"

]

# ==========================================================
# LANGUAGE HELPERS
# ==========================================================

def get_lang(user_id):

    conn=db()
    cur=conn.cursor()

    cur.execute(
    "SELECT lang FROM users WHERE user_id=?",
    (user_id,)
    )

    row=cur.fetchone()

    conn.close()

    if row:
        return row[0]

    return "ru"


def t(user_id,key):

    lang=get_lang(user_id)

    return TEXTS.get(lang,{}).get(key,key)

# ==========================================================
# USER MAIN MENU
# ==========================================================

def user_menu(user_id):

    kb=[

    [KeyboardButton(
    text=t(user_id,"menu_shop"),
    web_app=WebAppInfo(
    url=f"{BASE_URL}/shop"
    )
    )],

    [KeyboardButton(text=t(user_id,"menu_cart"))],

    [KeyboardButton(text=t(user_id,"menu_orders"))],

    [KeyboardButton(text=t(user_id,"menu_size"))],

    [KeyboardButton(text=t(user_id,"menu_contacts"))],

    [KeyboardButton(text=t(user_id,"menu_lang"))]

    ]

    if user_id in ADMIN_IDS:

        kb.append(
        [KeyboardButton(text=t(user_id,"menu_admin"))]
        )

    return ReplyKeyboardMarkup(
    keyboard=kb,
    resize_keyboard=True
    )

# ==========================================================
# ADMIN MENU
# ==========================================================

def admin_menu():

    return ReplyKeyboardMarkup(

    keyboard=[

    [KeyboardButton(text="📦 Новые заказы")],

    [KeyboardButton(text="📋 Все заказы")],

    [KeyboardButton(text="➕ Добавить товар")],

    [KeyboardButton(text="📝 Редактировать товар")],

    [KeyboardButton(text="🗑 Удалить товар")],

    [KeyboardButton(text="📊 Статистика")]

    ],

    resize_keyboard=True

    )

# ==========================================================
# DELIVERY KEYBOARD
# ==========================================================

def delivery_keyboard(user_id):

    return ReplyKeyboardMarkup(

    keyboard=[

    [KeyboardButton(text=t(user_id,"delivery_yandex"))],

    [KeyboardButton(text=t(user_id,"delivery_post"))],

    [KeyboardButton(text=t(user_id,"delivery_pvz"))]

    ],

    resize_keyboard=True

    )

# ==========================================================
# ADDRESS KEYBOARD
# ==========================================================

def address_keyboard(user_id):

    return ReplyKeyboardMarkup(

    keyboard=[

    [KeyboardButton(
    text=t(user_id,"send_location"),
    request_location=True
    )],

    [KeyboardButton(text=t(user_id,"enter_address"))]

    ],

    resize_keyboard=True

    )

# ==========================================================
# PAYMENT KEYBOARD
# ==========================================================

def payment_keyboard(user_id):

    return ReplyKeyboardMarkup(

    keyboard=[

    [KeyboardButton(text=t(user_id,"payment_click"))],

    [KeyboardButton(text=t(user_id,"payment_payme"))]

    ],

    resize_keyboard=True

    )

# ==========================================================
# CART INLINE KEYBOARD
# ==========================================================

def cart_inline(user_id):

    return InlineKeyboardMarkup(

    inline_keyboard=[

    [

    InlineKeyboardButton(
    text=t(user_id,"checkout"),
    callback_data="checkout"
    )

    ],

    [

    InlineKeyboardButton(
    text=t(user_id,"cart_clear"),
    callback_data="cart_clear"
    )

    ]

    ]

    )

# ==========================================================
# FSM CHECKOUT STATES
# ==========================================================

class CheckoutStates(StatesGroup):

    name=State()

    phone=State()

    delivery=State()

    address_type=State()

    city=State()

    address=State()

    pvz=State()

    location=State()

    payment=State()

    comment=State()

    confirm=State()

# ==========================================================
# USER START
# ==========================================================

@dp.message(CommandStart())
async def start(message: Message):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR IGNORE INTO users
        (user_id, username, full_name, lang, created_at, updated_at)
        VALUES (?,?,?,?,?,?)
        """,
        (
            message.from_user.id,
            message.from_user.username,
            message.from_user.full_name,
            "ru",
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat()
        )
    )

    conn.commit()
    conn.close()

    await message.answer(
        TEXTS["ru"]["welcome"],
        reply_markup=user_menu(message.from_user.id)
    )


# ==========================================================
# LANGUAGE CHANGE
# ==========================================================

@dp.message(F.text.in_(["🌐 Язык", "🌐 Til"]))
async def change_language(message: Message):

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Русский",
                    callback_data="lang_ru"
                )
            ],
            [
                InlineKeyboardButton(
                    text="O‘zbekcha",
                    callback_data="lang_uz"
                )
            ]
        ]
    )

    await message.answer("Выберите язык", reply_markup=kb)


@dp.callback_query(F.data.startswith("lang_"))
async def set_language(callback: CallbackQuery):

    lang = callback.data.split("_")[1]

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "UPDATE users SET lang=? WHERE user_id=?",
        (lang, callback.from_user.id)
    )

    conn.commit()
    conn.close()

    await callback.message.answer(
        "OK",
        reply_markup=user_menu(callback.from_user.id)
    )

    await callback.answer()


# ==========================================================
# CONTACTS
# ==========================================================

@dp.message(F.text.in_(["📞 Контакты", "📞 Kontaktlar"]))
async def contacts(message: Message):

    text = t(message.from_user.id, "contacts")

    await message.answer(text)


# ==========================================================
# SIZE PICKER
# ==========================================================

SIZE_TABLE = {

"age":{

3:"98",
4:"104",
5:"110",
6:"116",
7:"122",
8:"128",
9:"134",
10:"140"

},

"height":{

98:"98",
104:"104",
110:"110",
116:"116",
122:"122",
128:"128",
134:"134",
140:"140"

}

}


@dp.message(F.text.in_(["📏 Подбор размера","📏 O‘lcham tanlash"]))
async def size_start(message:Message):

    text = (
    "Введите возраст ребёнка (3-10)\n"
    "или рост (98-140)"
    )

    await message.answer(text)


@dp.message()
async def size_answer(message:Message):

    try:

        v = int(message.text)

    except:
        return

    if v in SIZE_TABLE["age"]:

        size = SIZE_TABLE["age"][v]

        await message.answer(
        f"Рекомендуемый размер: {size}"
        )

        return

    if v in SIZE_TABLE["height"]:

        size = SIZE_TABLE["height"][v]

        await message.answer(
        f"Размер: {size}"
        )


# ==========================================================
# VIEW CART
# ==========================================================

@dp.message(F.text.in_(["🛒 Корзина","🛒 Savatcha"]))
async def view_cart(message: Message):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT product_name,price,qty
        FROM carts
        WHERE user_id=?
        """,
        (message.from_user.id,)
    )

    items = cur.fetchall()

    conn.close()

    if not items:

        await message.answer(
            t(message.from_user.id,"cart_empty")
        )

        return

    text = "🛒\n\n"

    total = 0

    for i in items:

        name,price,qty = i

        s = price * qty

        text += f"{name} x{qty} — {s}\n"

        total += s

    text += f"\nИтого: {total}"

    await message.answer(
        text,
        reply_markup=cart_inline(message.from_user.id)
    )


# ==========================================================
# CLEAR CART
# ==========================================================

@dp.callback_query(F.data=="cart_clear")
async def clear_cart(callback:CallbackQuery):

    conn=db()
    cur=conn.cursor()

    cur.execute(
    "DELETE FROM carts WHERE user_id=?",
    (callback.from_user.id,)
    )

    conn.commit()
    conn.close()

    await callback.message.answer(
    "Корзина очищена"
    )

    await callback.answer()


# ==========================================================
# MY ORDERS
# ==========================================================

@dp.message(F.text.in_(["📦 Мои заказы","📦 Buyurtmalarim"]))
async def my_orders(message:Message):

    conn=db()
    cur=conn.cursor()

    cur.execute(
    """
    SELECT id,total_amount,status,created_at
    FROM orders
    WHERE user_id=?
    ORDER BY id DESC
    """,
    (message.from_user.id,)
    )

    rows=cur.fetchall()

    conn.close()

    if not rows:

        await message.answer("Нет заказов")

        return

    text="📦\n\n"

    for r in rows:

        text+=(
        f"#{r[0]}\n"
        f"{r[1]}\n"
        f"{r[2]}\n"
        f"{r[3]}\n\n"
        )

    await message.answer(text)

# ==========================================================
# CHECKOUT START
# ==========================================================

@dp.callback_query(F.data == "checkout")
async def checkout_start(callback: CallbackQuery, state: FSMContext):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM carts WHERE user_id=?",
        (callback.from_user.id,)
    )

    items = cur.fetchall()
    conn.close()

    if not items:
        await callback.message.answer("Корзина пуста")
        return

    await state.set_state(CheckoutStates.name)

    await callback.message.answer(
        t(callback.from_user.id,"enter_name")
    )

    await callback.answer()


# ==========================================================
# NAME
# ==========================================================

@dp.message(CheckoutStates.name)
async def checkout_name(message: Message, state: FSMContext):

    await state.update_data(
        customer_name = message.text
    )

    await state.set_state(CheckoutStates.phone)

    await message.answer(
        t(message.from_user.id,"enter_phone")
    )


# ==========================================================
# PHONE
# ==========================================================

@dp.message(CheckoutStates.phone)
async def checkout_phone(message: Message, state: FSMContext):

    await state.update_data(
        customer_phone = message.text
    )

    await state.set_state(CheckoutStates.delivery)

    await message.answer(
        t(message.from_user.id,"choose_delivery"),
        reply_markup=delivery_keyboard(message.from_user.id)
    )


# ==========================================================
# DELIVERY
# ==========================================================

@dp.message(CheckoutStates.delivery)
async def checkout_delivery(message: Message, state: FSMContext):

    await state.update_data(
        delivery_service = message.text
    )

    await state.set_state(CheckoutStates.address_type)

    await message.answer(
        t(message.from_user.id,"choose_address"),
        reply_markup=address_keyboard(message.from_user.id)
    )


# ==========================================================
# ADDRESS TYPE
# ==========================================================

@dp.message(CheckoutStates.address_type)
async def checkout_address_type(message: Message, state: FSMContext):

    await state.update_data(
        address_type = message.text
    )

    await state.set_state(CheckoutStates.city)

    await message.answer(
        t(message.from_user.id,"enter_city")
    )


# ==========================================================
# CITY
# ==========================================================

@dp.message(CheckoutStates.city)
async def checkout_city(message: Message, state: FSMContext):

    await state.update_data(
        city = message.text
    )

    data = await state.get_data()

    if data["address_type"] == t(message.from_user.id,"send_location"):

        await state.set_state(CheckoutStates.location)

        await message.answer(
            "Отправьте локацию"
        )

    else:

        await state.set_state(CheckoutStates.address)

        await message.answer(
            "Введите адрес"
        )


# ==========================================================
# LOCATION
# ==========================================================

@dp.message(CheckoutStates.location)
async def checkout_location(message: Message, state: FSMContext):

    if not message.location:
        return

    await state.update_data(

        latitude = message.location.latitude,
        longitude = message.location.longitude

    )

    await state.set_state(CheckoutStates.payment)

    await message.answer(
        t(message.from_user.id,"choose_payment"),
        reply_markup=payment_keyboard(message.from_user.id)
    )


# ==========================================================
# ADDRESS TEXT
# ==========================================================

@dp.message(CheckoutStates.address)
async def checkout_address(message: Message, state: FSMContext):

    await state.update_data(
        delivery_address = message.text
    )

    await state.set_state(CheckoutStates.payment)

    await message.answer(
        t(message.from_user.id,"choose_payment"),
        reply_markup=payment_keyboard(message.from_user.id)
    )


# ==========================================================
# PAYMENT
# ==========================================================

@dp.message(CheckoutStates.payment)
async def checkout_payment(message: Message, state: FSMContext):

    await state.update_data(
        payment_method = message.text
    )

    await state.set_state(CheckoutStates.comment)

    await message.answer(
        t(message.from_user.id,"enter_comment")
    )


# ==========================================================
# COMMENT
# ==========================================================

@dp.message(CheckoutStates.comment)
async def checkout_comment(message: Message, state: FSMContext):

    await state.update_data(
        comment = message.text
    )

    await state.set_state(CheckoutStates.confirm)

    await message.answer(
        t(message.from_user.id,"confirm_order")
    )


# ==========================================================
# CONFIRM ORDER
# ==========================================================

@dp.message(CheckoutStates.confirm)
async def checkout_confirm(message: Message, state: FSMContext):

    data = await state.get_data()

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM carts WHERE user_id=?",
        (message.from_user.id,)
    )

    items = cur.fetchall()

    total_qty = 0
    total_amount = 0

    for i in items:

        price = i[4]
        qty = i[5]

        total_qty += qty
        total_amount += price * qty

    cur.execute(
        """
        INSERT INTO orders(
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
        payment_method,
        payment_status,
        comment,
        status,
        manager_seen,
        source,
        created_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (

            message.from_user.id,
            message.from_user.username,
            data.get("customer_name"),
            data.get("customer_phone"),
            data.get("city"),
            json.dumps(items),
            total_qty,
            total_amount,
            data.get("delivery_service"),
            data.get("address_type"),
            data.get("delivery_address"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("payment_method"),
            "pending",
            data.get("comment"),
            "new",
            0,
            "telegram",
            datetime.utcnow().isoformat()

        )
    )

    order_id = cur.lastrowid

    cur.execute(
        "DELETE FROM carts WHERE user_id=?",
        (message.from_user.id,)
    )

    conn.commit()
    conn.close()

    await state.clear()

    await message.answer(
        f"{t(message.from_user.id,'order_created')} #{order_id}"
    )

    # ======================================================
    # SEND ORDER TO ADMIN
    # ======================================================

    admin_text = f"""
🆕 Новый заказ #{order_id}

Имя: {data.get("customer_name")}
Телефон: {data.get("customer_phone")}

Username: @{message.from_user.username}
UserID: {message.from_user.id}

Город: {data.get("city")}

Доставка: {data.get("delivery_service")}
Адрес: {data.get("delivery_address")}

Оплата: {data.get("payment_method")}

Сумма: {total_amount}
"""

    kb = InlineKeyboardMarkup(

        inline_keyboard=[

        [

        InlineKeyboardButton(
        text="В работу",
        callback_data=f"order_work_{order_id}"
        )

        ],

        [

        InlineKeyboardButton(
        text="Подтвердить",
        callback_data=f"order_confirm_{order_id}"
        )

        ],

        [

        InlineKeyboardButton(
        text="Написать клиенту",
        url=f"tg://user?id={message.from_user.id}"
        )

        ]

        ]

    )

    for admin in ADMIN_IDS:

        try:

            await bot.send_message(
                admin,
                admin_text,
                reply_markup=kb
            )

        except:
            pass

# ==========================================================
# ADMIN OPEN
# ==========================================================

@dp.message(F.text.in_(["🛠 Админ","🛠 Admin"]))
async def admin_open(message:Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(
        "Админ панель",
        reply_markup=admin_menu()
    )


# ==========================================================
# ADMIN NEW ORDERS
# ==========================================================

@dp.message(F.text=="📦 Новые заказы")
async def admin_new_orders(message:Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn=db()
    cur=conn.cursor()

    cur.execute(
    """
    SELECT id,customer_name,total_amount,status
    FROM orders
    WHERE manager_seen=0
    ORDER BY id DESC
    """
    )

    rows=cur.fetchall()

    conn.close()

    if not rows:

        await message.answer("Нет новых заказов")

        return

    for r in rows:

        order_id=r[0]

        text=(
        f"🆕 Заказ #{order_id}\n"
        f"{r[1]}\n"
        f"{r[2]}\n"
        f"{r[3]}"
        )

        kb=InlineKeyboardMarkup(
        inline_keyboard=[

        [
        InlineKeyboardButton(
        text="Открыть",
        callback_data=f"admin_order_{order_id}"
        )
        ]

        ]
        )

        await message.answer(text,reply_markup=kb)


# ==========================================================
# ADMIN ORDER CARD
# ==========================================================

@dp.callback_query(F.data.startswith("admin_order_"))
async def admin_order_card(callback:CallbackQuery):

    order_id=int(callback.data.split("_")[2])

    conn=db()
    cur=conn.cursor()

    cur.execute(
    "SELECT * FROM orders WHERE id=?",
    (order_id,)
    )

    o=cur.fetchone()

    conn.close()

    if not o:
        return

    text=f"""
Заказ #{o[0]}

Имя: {o[3]}
Телефон: {o[4]}

Username: @{o[2]}
UserID: {o[1]}

Город: {o[5]}

Доставка: {o[9]}

Адрес: {o[10]}

Оплата: {o[14]}
Статус оплаты: {o[15]}

Комментарий: {o[17]}

Сумма: {o[8]}
"""

    kb=InlineKeyboardMarkup(
    inline_keyboard=[

    [
    InlineKeyboardButton(
    text="В работу",
    callback_data=f"order_work_{order_id}"
    )
    ],

    [
    InlineKeyboardButton(
    text="Подтвердить",
    callback_data=f"order_confirm_{order_id}"
    )
    ],

    [
    InlineKeyboardButton(
    text="Оплачен",
    callback_data=f"order_paid_{order_id}"
    )
    ],

    [
    InlineKeyboardButton(
    text="Отправлен",
    callback_data=f"order_sent_{order_id}"
    )
    ],

    [
    InlineKeyboardButton(
    text="Доставлен",
    callback_data=f"order_done_{order_id}"
    )
    ],

    [
    InlineKeyboardButton(
    text="Отменён",
    callback_data=f"order_cancel_{order_id}"
    )
    ],

    [
    InlineKeyboardButton(
    text="Написать клиенту",
    url=f"tg://user?id={o[1]}"
    )
    ]

    ]
    )

    await callback.message.answer(text,reply_markup=kb)

    await callback.answer()


# ==========================================================
# ORDER STATUS UPDATE
# ==========================================================

@dp.callback_query(F.data.startswith("order_"))
async def admin_update_order(callback:CallbackQuery):

    parts=callback.data.split("_")

    action=parts[1]
    order_id=int(parts[2])

    status_map={

    "work":"processing",
    "confirm":"confirmed",
    "paid":"paid",
    "sent":"sent",
    "done":"delivered",
    "cancel":"cancelled"

    }

    status=status_map.get(action)

    conn=db()
    cur=conn.cursor()

    cur.execute(
    "UPDATE orders SET status=?,manager_seen=1 WHERE id=?",
    (status,order_id)
    )

    conn.commit()
    conn.close()

    await callback.message.answer(
    f"Статус заказа {order_id}: {status}"
    )

    await callback.answer()


# ==========================================================
# ADMIN ALL ORDERS
# ==========================================================

@dp.message(F.text=="📋 Все заказы")
async def admin_all_orders(message:Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn=db()
    cur=conn.cursor()

    cur.execute(
    """
    SELECT id,total_amount,status
    FROM orders
    ORDER BY id DESC
    LIMIT 50
    """
    )

    rows=cur.fetchall()

    conn.close()

    text="Заказы\n\n"

    for r in rows:

        text+=(
        f"#{r[0]} | {r[1]} | {r[2]}\n"
        )

    await message.answer(text)


# ==========================================================
# ADMIN STATS
# ==========================================================

@dp.message(F.text=="📊 Статистика")
async def admin_stats(message:Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn=db()
    cur=conn.cursor()

    cur.execute("SELECT COUNT(*) FROM users")
    users=cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM shop_products")
    products=cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM orders")
    orders=cur.fetchone()[0]

    conn.close()

    text=(
    f"Пользователи: {users}\n"
    f"Товары: {products}\n"
    f"Заказы: {orders}"
    )

    await message.answer(text)


# ==========================================================
# ADD PRODUCT FSM
# ==========================================================

class AdminAddProduct(StatesGroup):

    photo=State()
    title_ru=State()
    title_uz=State()
    description_ru=State()
    description_uz=State()
    sizes=State()
    category=State()
    price=State()
    old_price=State()
    stock=State()


@dp.message(F.text=="➕ Добавить товар")
async def admin_add_product_start(message:Message,state:FSMContext):

    if message.from_user.id not in ADMIN_IDS:
        return

    await state.set_state(AdminAddProduct.photo)

    await message.answer("Фото товара")


@dp.message(AdminAddProduct.photo)
async def admin_add_product_photo(message:Message,state:FSMContext):

    file_id=message.photo[-1].file_id

    await state.update_data(photo=file_id)

    await state.set_state(AdminAddProduct.title_ru)

    await message.answer("Название RU")


@dp.message(AdminAddProduct.title_ru)
async def admin_add_title_ru(message:Message,state:FSMContext):

    await state.update_data(title_ru=message.text)

    await state.set_state(AdminAddProduct.title_uz)

    await message.answer("Название UZ")


@dp.message(AdminAddProduct.title_uz)
async def admin_add_title_uz(message:Message,state:FSMContext):

    await state.update_data(title_uz=message.text)

    await state.set_state(AdminAddProduct.description_ru)

    await message.answer("Описание RU")


@dp.message(AdminAddProduct.description_ru)
async def admin_add_desc_ru(message:Message,state:FSMContext):

    await state.update_data(description_ru=message.text)

    await state.set_state(AdminAddProduct.description_uz)

    await message.answer("Описание UZ")


@dp.message(AdminAddProduct.description_uz)
async def admin_add_desc_uz(message:Message,state:FSMContext):

    await state.update_data(description_uz=message.text)

    await state.set_state(AdminAddProduct.sizes)

    await message.answer("Размеры (через запятую)")


@dp.message(AdminAddProduct.sizes)
async def admin_add_sizes(message:Message,state:FSMContext):

    await state.update_data(sizes=message.text)

    await state.set_state(AdminAddProduct.category)

    await message.answer(
    "Категория\nnew hits sale limited school casual"
    )


@dp.message(AdminAddProduct.category)
async def admin_add_category(message:Message,state:FSMContext):

    await state.update_data(category=message.text)

    await state.set_state(AdminAddProduct.price)

    await message.answer("Цена")


@dp.message(AdminAddProduct.price)
async def admin_add_price(message:Message,state:FSMContext):

    await state.update_data(price=int(message.text))

    await state.set_state(AdminAddProduct.old_price)

    await message.answer("Старая цена")


@dp.message(AdminAddProduct.old_price)
async def admin_add_old_price(message:Message,state:FSMContext):

    await state.update_data(old_price=int(message.text))

    await state.set_state(AdminAddProduct.stock)

    await message.answer("Остаток")


@dp.message(AdminAddProduct.stock)
async def admin_add_stock(message:Message,state:FSMContext):

    data=await state.get_data()

    conn=db()
    cur=conn.cursor()

    cur.execute(
    """
    INSERT INTO shop_products(
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
    created_at
    )
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """,
    (

    data["photo"],
    data["title_ru"],
    data["title_uz"],
    data["description_ru"],
    data["description_uz"],
    data["sizes"],
    data["category"],
    data["price"],
    data["old_price"],
    int(message.text),
    1,
    datetime.utcnow().isoformat()

    )
    )

    conn.commit()
    conn.close()

    await message.answer("Товар добавлен")

    await state.clear()

# ==========================================================
# WEBAPP HTML
# ==========================================================

SHOP_HTML = """
<!DOCTYPE html>
<html>

<head>

<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">

<title>ZARY & CO</title>

<style>

body{
margin:0;
font-family:Arial;
background:#f4f4f4;
}

header{
background:black;
color:white;
padding:20px;
text-align:center;
font-size:22px;
font-weight:bold;
}

.products{
display:grid;
grid-template-columns:1fr 1fr;
gap:10px;
padding:10px;
}

.card{
background:white;
border-radius:10px;
padding:10px;
box-shadow:0 4px 10px rgba(0,0,0,0.1);
}

.card img{
width:100%;
border-radius:6px;
}

.title{
font-weight:bold;
margin-top:5px;
}

.price{
font-size:16px;
color:black;
}

.old{
font-size:13px;
color:gray;
text-decoration:line-through;
}

.size{
font-size:12px;
color:#555;
}

.stock{
font-size:12px;
color:#888;
}

button{
margin-top:5px;
width:100%;
padding:10px;
background:black;
color:white;
border:none;
border-radius:6px;
}

</style>

</head>

<body>

<header>ZARY & CO</header>

<div id="products" class="products"></div>

<script>

async function loadProducts(){

let r = await fetch("/api/shop/products")

let data = await r.json()

let container = document.getElementById("products")

data.forEach(p=>{

let card=document.createElement("div")

card.className="card"

card.innerHTML=`

<img src="/media/${p.photo_file_id}">

<div class="title">${p.title}</div>

<div class="price">${p.price}</div>

<div class="old">${p.old_price || ""}</div>

<div class="size">Размеры: ${p.sizes}</div>

<div class="stock">Остаток: ${p.stock}</div>

<button onclick="addToCart(${p.id})">
В корзину
</button>

`

container.appendChild(card)

})

}

function addToCart(id){

Telegram.WebApp.sendData(JSON.stringify({

action:"add_to_cart",
product_id:id

}))

}

loadProducts()

</script>

</body>

</html>
"""


# ==========================================================
# SHOP PAGE
# ==========================================================

async def shop_page(request):

    return web.Response(
        text=SHOP_HTML,
        content_type="text/html"
    )


# ==========================================================
# API PRODUCTS
# ==========================================================

async def api_products(request):

    conn=db()
    cur=conn.cursor()

    cur.execute(
    """
    SELECT
    id,
    photo_file_id,
    title_ru,
    price,
    old_price,
    sizes,
    stock_qty
    FROM shop_products
    WHERE is_published=1
    ORDER BY sort_order
    """
    )

    rows=cur.fetchall()

    conn.close()

    products=[]

    for r in rows:

        products.append({

        "id":r[0],
        "photo_file_id":r[1],
        "title":r[2],
        "price":r[3],
        "old_price":r[4],
        "sizes":r[5],
        "stock":r[6]

        })

    return web.json_response(products)


# ==========================================================
# API CREATE ORDER
# ==========================================================

async def api_create_order(request):

    data=await request.json()

    user_id=data.get("user_id")

    conn=db()
    cur=conn.cursor()

    cur.execute(
    "SELECT * FROM carts WHERE user_id=?",
    (user_id,)
    )

    items=cur.fetchall()

    total=0

    for i in items:

        price=i[4]
        qty=i[5]

        total+=price*qty

    cur.execute(
    """
    INSERT INTO orders(
    user_id,
    items,
    total_amount,
    status,
    payment_status,
    created_at
    )
    VALUES(?,?,?,?,?,?)
    """,
    (

    user_id,
    json.dumps(items),
    total,
    "new",
    "pending",
    datetime.utcnow().isoformat()

    )
    )

    order_id=cur.lastrowid

    cur.execute(
    "DELETE FROM carts WHERE user_id=?",
    (user_id,)
    )

    conn.commit()
    conn.close()

    return web.json_response({

    "order_id":order_id

    })


# ==========================================================
# MEDIA PLACEHOLDER
# ==========================================================

async def media_file(request):

    file_id=request.match_info["file_id"]

    return web.Response(
        text=f"media placeholder {file_id}"
    )


# ==========================================================
# PAYMENT PLACEHOLDERS
# ==========================================================

async def pay_click(request):

    order_id=request.match_info["order_id"]

    return web.Response(
        text=f"Click payment placeholder for order {order_id}"
    )


async def pay_payme(request):

    order_id=request.match_info["order_id"]

    return web.Response(
        text=f"Payme payment placeholder for order {order_id}"
    )


# ==========================================================
# HEALTH CHECK
# ==========================================================

async def health(request):

    return web.json_response({"status":"ok"})


# ==========================================================
# WEB APP SERVER
# ==========================================================

def create_web_app():

    app=web.Application()

    app.router.add_get("/shop",shop_page)

    app.router.add_get("/api/shop/products",api_products)

    app.router.add_post("/api/shop/order",api_create_order)

    app.router.add_get("/health",health)

    app.router.add_get("/media/{file_id}",media_file)

    app.router.add_get("/pay/click/{order_id}",pay_click)

    app.router.add_get("/pay/payme/{order_id}",pay_payme)

    return app

# ==========================================================
# WEB ADMIN MAIN PAGE
# ==========================================================

ADMIN_HTML = """
<!DOCTYPE html>
<html>

<head>

<title>ZARY ADMIN</title>

<style>

body{
font-family:Arial;
background:#f5f5f5;
padding:20px;
}

h1{
color:black;
}

.card{
background:white;
padding:20px;
border-radius:10px;
margin-bottom:20px;
box-shadow:0 4px 10px rgba(0,0,0,0.1);
}

a{
display:block;
margin:10px 0;
font-weight:bold;
}

</style>

</head>

<body>

<h1>ZARY & CO ADMIN</h1>

<div class="card">

<a href="/admin/orders">Orders</a>

<a href="/admin/products">Products</a>

</div>

</body>

</html>
"""


async def admin_page(request):

    token=request.query.get("token")

    if token!=ADMIN_PANEL_TOKEN:

        return web.Response(text="Access denied")

    return web.Response(
        text=ADMIN_HTML,
        content_type="text/html"
    )


# ==========================================================
# ADMIN ORDERS PAGE
# ==========================================================

async def admin_orders_page(request):

    token=request.query.get("token")

    if token!=ADMIN_PANEL_TOKEN:

        return web.Response(text="Access denied")

    status_filter=request.query.get("status")

    conn=db()
    cur=conn.cursor()

    query="""
    SELECT
    id,
    customer_name,
    customer_phone,
    total_amount,
    status,
    payment_status,
    created_at
    FROM orders
    """

    if status_filter:

        query+=" WHERE status=?"

        cur.execute(query,(status_filter,))

    else:

        cur.execute(query)

    rows=cur.fetchall()

    conn.close()

    html="""

<h2>Orders</h2>

<table border=1 cellpadding=10>

<tr>
<th>ID</th>
<th>Name</th>
<th>Phone</th>
<th>Amount</th>
<th>Status</th>
<th>Payment</th>
<th>Date</th>
</tr>

"""

    for r in rows:

        html+=f"""

<tr>

<td>{r[0]}</td>

<td>{r[1]}</td>

<td>{r[2]}</td>

<td>{r[3]}</td>

<td>{r[4]}</td>

<td>{r[5]}</td>

<td>{r[6]}</td>

</tr>

"""

    html+="</table>"

    return web.Response(
        text=html,
        content_type="text/html"
    )


# ==========================================================
# ADMIN PRODUCTS PAGE
# ==========================================================

async def admin_products_page(request):

    token=request.query.get("token")

    if token!=ADMIN_PANEL_TOKEN:

        return web.Response(text="Access denied")

    conn=db()
    cur=conn.cursor()

    cur.execute(
    """
    SELECT
    id,
    title_ru,
    price,
    old_price,
    stock_qty,
    is_published
    FROM shop_products
    ORDER BY id DESC
    """
    )

    rows=cur.fetchall()

    conn.close()

    html="""

<h2>Products</h2>

<table border=1 cellpadding=10>

<tr>
<th>ID</th>
<th>Name</th>
<th>Price</th>
<th>Old Price</th>
<th>Stock</th>
<th>Published</th>
</tr>

"""

    for r in rows:

        html+=f"""

<tr>

<td>{r[0]}</td>

<td>{r[1]}</td>

<td>{r[2]}</td>

<td>{r[3]}</td>

<td>{r[4]}</td>

<td>{r[5]}</td>

</tr>

"""

    html+="</table>"

    return web.Response(
        text=html,
        content_type="text/html"
    )


# ==========================================================
# ANALYTICS
# ==========================================================

async def analytics_summary():

    conn=db()
    cur=conn.cursor()

    cur.execute("SELECT COUNT(*) FROM orders")
    total_orders=cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users")
    total_users=cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM shop_products")
    total_products=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='new'"
    )
    new_orders=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='processing'"
    )
    processing_orders=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='confirmed'"
    )
    confirmed_orders=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='paid'"
    )
    paid_orders=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='sent'"
    )
    sent_orders=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='delivered'"
    )
    delivered_orders=cur.fetchone()[0]

    cur.execute(
    "SELECT COUNT(*) FROM orders WHERE status='cancelled'"
    )
    cancelled_orders=cur.fetchone()[0]

    conn.close()

    return {

    "total_orders":total_orders,
    "total_users":total_users,
    "total_products":total_products,

    "new":new_orders,
    "processing":processing_orders,
    "confirmed":confirmed_orders,
    "paid":paid_orders,
    "sent":sent_orders,
    "delivered":delivered_orders,
    "cancelled":cancelled_orders

    }


# ==========================================================
# ANALYTICS API
# ==========================================================

async def analytics_api(request):

    data=await analytics_summary()

    return web.json_response(data)


# ==========================================================
# ADD ROUTES TO WEB APP
# ==========================================================

def extend_web_admin(app):

    app.router.add_get("/admin",admin_page)

    app.router.add_get("/admin/orders",admin_orders_page)

    app.router.add_get("/admin/products",admin_products_page)

    app.router.add_get("/api/admin/analytics",analytics_api)

# ==========================================================
# EXCEL REPORT GENERATION
# ==========================================================

def generate_excel_report():

    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
        id,
        created_at,
        customer_name,
        customer_phone,
        city,
        items,
        total_amount,
        status,
        payment_method,
        payment_status,
        source
        FROM orders
        """
    )

    rows = cur.fetchall()

    conn.close()

    wb = Workbook()
    ws = wb.active

    ws.append([
        "ID",
        "date",
        "name",
        "phone",
        "city",
        "items",
        "amount",
        "status",
        "payment_method",
        "payment_status",
        "source"
    ])

    for r in rows:
        ws.append(r)

    filename = "orders_report.xlsx"

    wb.save(filename)

    return filename


# ==========================================================
# ADMIN REMINDER SYSTEM
# ==========================================================

async def remind_admin_loop():

    while True:

        await asyncio.sleep(3600)

        conn = db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM orders
            WHERE manager_seen = 0
            """
        )

        count = cur.fetchone()[0]

        conn.close()

        if count > 0:

            for admin in ADMIN_IDS:

                try:

                    await bot.send_message(
                        admin,
                        f"⚠️ {count} новых заказов без просмотра"
                    )

                except:
                    pass


# ==========================================================
# SCHEDULED POSTS LOOP
# ==========================================================

async def scheduled_posts_loop():

    while True:

        await asyncio.sleep(300)

        conn = db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id,text
            FROM scheduled_posts
            WHERE post_time <= datetime('now')
            """
        )

        posts = cur.fetchall()

        for p in posts:

            try:

                await bot.send_message(
                    CHANNEL_ID,
                    p[1]
                )

                cur.execute(
                    "DELETE FROM scheduled_posts WHERE id=?",
                    (p[0],)
                )

            except:
                pass

        conn.commit()
        conn.close()


# ==========================================================
# WEEKLY ADMIN REMINDER
# ==========================================================

async def sunday_reminder():

    while True:

        await asyncio.sleep(86400)

        now = datetime.utcnow()

        if now.weekday() == 6:

            for admin in ADMIN_IDS:

                try:

                    await bot.send_message(
                        admin,
                        "Напоминание: загрузите новые посты на неделю"
                    )

                except:
                    pass


# ==========================================================
# EXTEND WEB APP ROUTES
# ==========================================================

def extend_routes(app):

    extend_web_admin(app)


# ==========================================================
# MAIN SERVER START
# ==========================================================

async def main():

    init_db()

    app = create_web_app()

    extend_routes(app)

    runner = web.AppRunner(app)

    await runner.setup()

    port = int(os.getenv("PORT",8080))

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port
    )

    await site.start()

    asyncio.create_task(remind_admin_loop())

    asyncio.create_task(scheduled_posts_loop())

    asyncio.create_task(sunday_reminder())

    await dp.start_polling(bot)


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    asyncio.run(main())
