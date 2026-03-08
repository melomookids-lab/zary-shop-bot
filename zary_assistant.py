 # =========================================================
# ZARY & CO — SHOP BOT
# clean architecture version
# aiogram 3.x + aiohttp + SQLite
# =========================================================

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
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo
)
from aiogram.filters import CommandStart

from aiohttp import web
from openpyxl import Workbook

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(level=logging.INFO)

# =========================================================
# ENV CONFIG (Render compatible)
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")

ADMIN_IDS = [
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x
]

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))

BASE_URL = os.getenv("BASE_URL", "")

ADMIN_PANEL_TOKEN = os.getenv("ADMIN_PANEL_TOKEN", "")

CRON_SECRET = os.getenv("CRON_SECRET", "")

# =========================================================
# BOT INIT
# =========================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =========================================================
# DATABASE
# =========================================================

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

    # CART

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

    # POSTS

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


# =========================================================
# TRANSLATIONS
# =========================================================

TEXTS = {

    "ru": {

        "menu_shop": "🛍 Магазин",
        "menu_cart": "🛒 Корзина",
        "menu_orders": "📦 Мои заказы",
        "menu_size": "📏 Подбор размера",
        "menu_contacts": "📞 Контакты",
        "menu_lang": "🌐 Язык",
        "menu_admin": "🛠 Админ",

        "welcome":
            "Добро пожаловать в ZARY & CO\n\n"
            "Премиальная одежда для детей.",

        "choose_lang":
            "Выберите язык"

    },

    "uz": {

        "menu_shop": "🛍 Do‘kon",
        "menu_cart": "🛒 Savatcha",
        "menu_orders": "📦 Buyurtmalarim",
        "menu_size": "📏 O‘lcham tanlash",
        "menu_contacts": "📞 Kontaktlar",
        "menu_lang": "🌐 Til",
        "menu_admin": "🛠 Admin",

        "welcome":
            "ZARY & CO ga xush kelibsiz\n\n"
            "Bolalar uchun premium kiyimlar.",

        "choose_lang":
            "Tilni tanlang"

    }
}

# =========================================================
# HELPERS
# =========================================================


def get_lang(user_id):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT lang FROM users WHERE user_id=?",
        (user_id,)
    )

    row = cur.fetchone()
    conn.close()

    if row:
        return row[0]

    return "ru"


def t(user_id, key):

    lang = get_lang(user_id)

    return TEXTS.get(lang, {}).get(key, key)


# =========================================================
# MAIN MENU
# =========================================================


def main_menu(user_id):

    buttons = [

        [
            KeyboardButton(
                text=t(user_id, "menu_shop"),
                web_app=WebAppInfo(
                    url=f"{BASE_URL}/shop"
                )
            )
        ],

        [KeyboardButton(text=t(user_id, "menu_cart"))],
        [KeyboardButton(text=t(user_id, "menu_orders"))],
        [KeyboardButton(text=t(user_id, "menu_size"))],
        [KeyboardButton(text=t(user_id, "menu_contacts"))],
        [KeyboardButton(text=t(user_id, "menu_lang"))],
    ]

    if user_id in ADMIN_IDS:

        buttons.append(
            [KeyboardButton(text=t(user_id, "menu_admin"))]
        )

    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True
    )


# =========================================================
# START
# =========================================================


@dp.message(CommandStart())
async def start(message: Message):

    conn = db()
    cur = conn.cursor()

    cur.execute("""

    INSERT OR IGNORE INTO users(

        user_id,
        username,
        full_name,
        lang,
        created_at,
        updated_at

    )

    VALUES(?,?,?,?,?,?)

    """, (

        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name,
        "ru",
        datetime.utcnow().isoformat(),
        datetime.utcnow().isoformat()

    ))

    conn.commit()
    conn.close()

    await message.answer(

        TEXTS["ru"]["welcome"],

        reply_markup=main_menu(
            message.from_user.id
        )
    )


# =========================================================
# LANGUAGE
# =========================================================


@dp.message(F.text == "🌐 Язык")
@dp.message(F.text == "🌐 Til")
async def change_lang(message: Message):

    keyboard = InlineKeyboardMarkup(
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

    await message.answer(
        t(message.from_user.id, "choose_lang"),
        reply_markup=keyboard
    )


@dp.callback_query(F.data.startswith("lang_"))
async def set_lang(callback: CallbackQuery):

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
        reply_markup=main_menu(callback.from_user.id)
    )

    await callback.answer()

# =========================================================
# WEBAPP SHOP PAGE
# =========================================================

SHOP_HTML = """
<!DOCTYPE html>
<html>
<head>

<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">

<title>ZARY & CO</title>

<style>

body{
font-family:Arial;
background:#f5f5f5;
margin:0;
padding:0;
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
box-shadow:0 2px 10px rgba(0,0,0,0.1);
padding:10px;
}

.card img{
width:100%;
border-radius:8px;
}

.title{
font-weight:bold;
margin-top:6px;
}

.price{
color:black;
font-size:16px;
}

.old{
text-decoration:line-through;
color:gray;
font-size:13px;
}

button{
margin-top:6px;
padding:10px;
background:black;
color:white;
border:none;
border-radius:6px;
width:100%;
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

let card = document.createElement("div")
card.className="card"

card.innerHTML = `
<img src="/media/${p.photo_file_id}">
<div class="title">${p.title}</div>
<div class="price">${p.price}</div>
<div class="old">${p.old_price || ""}</div>
<button onclick="add(${p.id})">В корзину</button>
`

container.appendChild(card)

})

}

function add(id){

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


# =========================================================
# WEB ROUTE SHOP
# =========================================================

async def shop_page(request):

    return web.Response(
        text=SHOP_HTML,
        content_type="text/html"
    )


# =========================================================
# API PRODUCTS
# =========================================================

async def api_products(request):

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT
    id,
    photo_file_id,
    title_ru,
    price,
    old_price
    FROM shop_products
    WHERE is_published=1
    ORDER BY sort_order
    """)

    rows = cur.fetchall()

    conn.close()

    result = []

    for r in rows:

        result.append({

            "id": r[0],
            "photo_file_id": r[1],
            "title": r[2],
            "price": r[3],
            "old_price": r[4]

        })

    return web.json_response(result)


# =========================================================
# API ORDER
# =========================================================

async def api_create_order(request):

    data = await request.json()

    user_id = data.get("user_id")

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM carts WHERE user_id=?",
        (user_id,)
    )

    items = cur.fetchall()

    total = sum(i[4] * i[5] for i in items)

    cur.execute("""

    INSERT INTO orders(

    user_id,
    items,
    total_amount,
    status,
    payment_status,
    created_at

    )

    VALUES(?,?,?,?,?,?)

    """, (

        user_id,
        json.dumps(items),
        total,
        "new",
        "pending",
        datetime.utcnow().isoformat()

    ))

    order_id = cur.lastrowid

    cur.execute(
        "DELETE FROM carts WHERE user_id=?",
        (user_id,)
    )

    conn.commit()
    conn.close()

    return web.json_response({

        "order_id": order_id

    })


# =========================================================
# MEDIA FILE
# =========================================================

async def media_file(request):

    file_id = request.match_info["file_id"]

    return web.Response(
        text=f"media placeholder {file_id}"
    )


# =========================================================
# CART HANDLER FROM WEBAPP
# =========================================================

@dp.message(F.web_app_data)
async def webapp_data(message: Message):

    data = json.loads(message.web_app_data.data)

    if data["action"] == "add_to_cart":

        product_id = data["product_id"]

        conn = db()
        cur = conn.cursor()

        cur.execute(
            "SELECT title_ru, price, photo_file_id FROM shop_products WHERE id=?",
            (product_id,)
        )

        p = cur.fetchone()

        cur.execute("""

        INSERT INTO carts(

        user_id,
        product_id,
        product_name,
        price,
        qty,
        size,
        photo_file_id,
        added_at

        )

        VALUES(?,?,?,?,?,?,?,?)

        """, (

            message.from_user.id,
            product_id,
            p[0],
            p[1],
            1,
            "",
            p[2],
            datetime.utcnow().isoformat()

        ))

        conn.commit()
        conn.close()

        await message.answer("Товар добавлен в корзину")


# =========================================================
# CART VIEW
# =========================================================

@dp.message(F.text == "🛒 Корзина")
@dp.message(F.text == "🛒 Savatcha")
async def view_cart(message: Message):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT product_name, price, qty FROM carts WHERE user_id=?",
        (message.from_user.id,)
    )

    items = cur.fetchall()

    conn.close()

    if not items:

        await message.answer("Корзина пуста")
        return

    text = "🛒 Корзина\n\n"

    total = 0

    for i in items:

        line = f"{i[0]} x{i[2]} — {i[1]*i[2]}"

        text += line + "\n"

        total += i[1]*i[2]

    text += f"\nИтого: {total}"

    kb = InlineKeyboardMarkup(

        inline_keyboard=[

            [
                InlineKeyboardButton(
                    text="Оформить заказ",
                    callback_data="checkout"
                )
            ],

            [
                InlineKeyboardButton(
                    text="Очистить",
                    callback_data="cart_clear"
                )
            ]

        ]
    )

    await message.answer(text, reply_markup=kb)


# =========================================================
# CLEAR CART
# =========================================================

@dp.callback_query(F.data == "cart_clear")
async def clear_cart(callback: CallbackQuery):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM carts WHERE user_id=?",
        (callback.from_user.id,)
    )

    conn.commit()
    conn.close()

    await callback.message.answer("Корзина очищена")

    await callback.answer()


# =========================================================
# MY ORDERS
# =========================================================

@dp.message(F.text == "📦 Мои заказы")
@dp.message(F.text == "📦 Buyurtmalarim")
async def my_orders(message: Message):

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT id,total_amount,status,created_at FROM orders WHERE user_id=? ORDER BY id DESC",
        (message.from_user.id,)
    )

    rows = cur.fetchall()

    conn.close()

    if not rows:

        await message.answer("У вас нет заказов")
        return

    text = "📦 Ваши заказы\n\n"

    for r in rows:

        text += f"""
Заказ #{r[0]}
Сумма: {r[1]}
Статус: {r[2]}
Дата: {r[3]}

"""

    await message.answer(text)

# =========================================================
# ADMIN PANEL
# =========================================================

def admin_menu():

    kb = ReplyKeyboardMarkup(

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

    return kb


# =========================================================
# ADMIN OPEN
# =========================================================

@dp.message(F.text == "🛠 Админ")
async def open_admin(message: Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(
        "Админ панель",
        reply_markup=admin_menu()
    )


# =========================================================
# NEW ORDERS
# =========================================================

@dp.message(F.text == "📦 Новые заказы")
async def new_orders(message: Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn = db()
    cur = conn.cursor()

    cur.execute("""

    SELECT
    id,
    user_id,
    username,
    customer_name,
    customer_phone,
    city,
    total_amount,
    payment_method,
    payment_status,
    status

    FROM orders
    WHERE manager_seen=0
    ORDER BY id DESC

    """)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("Нет новых заказов")
        return

    for r in rows:

        text = f"""
🆕 Заказ #{r[0]}

Имя: {r[3]}
Телефон: {r[4]}
Username: @{r[2]}
User ID: {r[1]}

Город: {r[5]}

Сумма: {r[6]}

Оплата: {r[7]}
Статус оплаты: {r[8]}

Статус заказа: {r[9]}
"""

        kb = order_admin_keyboard(r[0], r[1])

        await message.answer(text, reply_markup=kb)


# =========================================================
# ALL ORDERS
# =========================================================

@dp.message(F.text == "📋 Все заказы")
async def all_orders(message: Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT id,total_amount,status
    FROM orders
    ORDER BY id DESC
    LIMIT 20
    """)

    rows = cur.fetchall()

    conn.close()

    if not rows:
        await message.answer("Заказов нет")
        return

    text = "📋 Последние заказы\n\n"

    for r in rows:

        text += f"""
#{r[0]} | {r[1]} | {r[2]}
"""

    await message.answer(text)


# =========================================================
# ORDER ADMIN KEYBOARD
# =========================================================

def order_admin_keyboard(order_id, user_id):

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
                    url=f"tg://user?id={user_id}"
                )
            ]

        ]
    )

    return kb


# =========================================================
# ORDER STATUS UPDATE
# =========================================================

@dp.callback_query(F.data.startswith("order_"))
async def update_order_status(callback: CallbackQuery):

    if callback.from_user.id not in ADMIN_IDS:
        return

    data = callback.data.split("_")

    action = data[1]
    order_id = int(data[2])

    status_map = {

        "work": "processing",
        "confirm": "confirmed",
        "paid": "paid",
        "sent": "sent",
        "done": "delivered",
        "cancel": "cancelled"

    }

    status = status_map.get(action)

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "UPDATE orders SET status=?,manager_seen=1 WHERE id=?",
        (status, order_id)
    )

    conn.commit()
    conn.close()

    await callback.message.answer(
        f"Статус заказа #{order_id} → {status}"
    )

    await callback.answer()


# =========================================================
# ADD PRODUCT
# =========================================================

from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext


class AddProduct(StatesGroup):

    photo = State()
    title_ru = State()
    title_uz = State()
    price = State()
    stock = State()


@dp.message(F.text == "➕ Добавить товар")
async def add_product_start(message: Message, state: FSMContext):

    if message.from_user.id not in ADMIN_IDS:
        return

    await state.set_state(AddProduct.photo)

    await message.answer("Отправьте фото товара")


@dp.message(AddProduct.photo)
async def add_product_photo(message: Message, state: FSMContext):

    file_id = message.photo[-1].file_id

    await state.update_data(photo=file_id)

    await state.set_state(AddProduct.title_ru)

    await message.answer("Название RU")


@dp.message(AddProduct.title_ru)
async def add_product_title_ru(message: Message, state: FSMContext):

    await state.update_data(title_ru=message.text)

    await state.set_state(AddProduct.title_uz)

    await message.answer("Название UZ")


@dp.message(AddProduct.title_uz)
async def add_product_title_uz(message: Message, state: FSMContext):

    await state.update_data(title_uz=message.text)

    await state.set_state(AddProduct.price)

    await message.answer("Цена")


@dp.message(AddProduct.price)
async def add_product_price(message: Message, state: FSMContext):

    await state.update_data(price=int(message.text))

    await state.set_state(AddProduct.stock)

    await message.answer("Остаток")


@dp.message(AddProduct.stock)
async def add_product_stock(message: Message, state: FSMContext):

    data = await state.get_data()

    conn = db()
    cur = conn.cursor()

    cur.execute("""

    INSERT INTO shop_products(

    photo_file_id,
    title_ru,
    title_uz,
    price,
    stock_qty,
    is_published,
    created_at

    )

    VALUES(?,?,?,?,?,?,?)

    """, (

        data["photo"],
        data["title_ru"],
        data["title_uz"],
        data["price"],
        int(message.text),
        1,
        datetime.utcnow().isoformat()

    ))

    conn.commit()
    conn.close()

    await message.answer("Товар добавлен")

    await state.clear()


# =========================================================
# DELETE PRODUCT
# =========================================================

@dp.message(F.text == "🗑 Удалить товар")
async def delete_product(message: Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT id,title_ru FROM shop_products")

    rows = cur.fetchall()

    conn.close()

    if not rows:
        await message.answer("Товаров нет")
        return

    text = "ID товара для удаления:\n\n"

    for r in rows:

        text += f"{r[0]} — {r[1]}\n"

    await message.answer(text)


# =========================================================
# ADMIN STATS
# =========================================================

@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):

    if message.from_user.id not in ADMIN_IDS:
        return

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM orders")
    orders = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users")
    users = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM shop_products")
    products = cur.fetchone()[0]

    conn.close()

    text = f"""
📊 Статистика

Пользователи: {users}
Товары: {products}
Заказы: {orders}
"""

    await message.answer(text)

# =========================================================
# HEALTH CHECK
# =========================================================

async def health(request):
    return web.json_response({"status": "ok"})


# =========================================================
# ADMIN WEB PANEL
# =========================================================

ADMIN_HTML = """
<html>
<head>
<title>ZARY Admin</title>
<style>
body{font-family:Arial;background:#f5f5f5;padding:20px}
h1{color:black}
.card{background:white;padding:20px;margin-bottom:10px;border-radius:10px}
a{display:block;margin:10px 0}
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

    token = request.query.get("token")

    if token != ADMIN_PANEL_TOKEN:
        return web.Response(text="Access denied")

    return web.Response(text=ADMIN_HTML, content_type="text/html")


# =========================================================
# ADMIN ORDERS PAGE
# =========================================================

async def admin_orders_page(request):

    token = request.query.get("token")

    if token != ADMIN_PANEL_TOKEN:
        return web.Response(text="Access denied")

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT id,customer_name,total_amount,status
    FROM orders
    ORDER BY id DESC
    LIMIT 50
    """)

    rows = cur.fetchall()
    conn.close()

    html = "<h2>Orders</h2>"

    for r in rows:

        html += f"""
<div>
#{r[0]} | {r[1]} | {r[2]} | {r[3]}
</div>
"""

    return web.Response(text=html, content_type="text/html")


# =========================================================
# ADMIN PRODUCTS PAGE
# =========================================================

async def admin_products_page(request):

    token = request.query.get("token")

    if token != ADMIN_PANEL_TOKEN:
        return web.Response(text="Access denied")

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT id,title_ru,price,stock_qty
    FROM shop_products
    """)

    rows = cur.fetchall()
    conn.close()

    html = "<h2>Products</h2>"

    for r in rows:

        html += f"""
<div>
#{r[0]} | {r[1]} | {r[2]} | stock {r[3]}
</div>
"""

    return web.Response(text=html, content_type="text/html")


# =========================================================
# PAYMENT PLACEHOLDERS
# =========================================================

async def pay_click(request):

    order_id = request.match_info["order_id"]

    return web.Response(
        text=f"Click payment placeholder for order {order_id}"
    )


async def pay_payme(request):

    order_id = request.match_info["order_id"]

    return web.Response(
        text=f"Payme payment placeholder for order {order_id}"
    )


# =========================================================
# EXCEL REPORT
# =========================================================

def generate_excel_report():

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT
    id,
    created_at,
    customer_name,
    customer_phone,
    city,
    total_amount,
    status,
    payment_method,
    payment_status,
    source
    FROM orders
    """)

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
        "amount",
        "status",
        "payment_method",
        "payment_status",
        "source"
    ])

    for r in rows:
        ws.append(r)

    file = "orders_report.xlsx"
    wb.save(file)

    return file


# =========================================================
# ADMIN REMINDER
# =========================================================

async def remind_admin():

    while True:

        await asyncio.sleep(3600)

        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT COUNT(*)
        FROM orders
        WHERE manager_seen=0
        """)

        count = cur.fetchone()[0]

        conn.close()

        if count > 0:

            for admin in ADMIN_IDS:

                try:

                    await bot.send_message(
                        admin,
                        f"⚠️ Есть {count} непросмотренных заказов"
                    )

                except:
                    pass


# =========================================================
# AUTO POSTS
# =========================================================

async def scheduled_posts_loop():

    while True:

        await asyncio.sleep(300)

        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT id,text
        FROM scheduled_posts
        WHERE post_time <= datetime('now')
        """)

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


# =========================================================
# WEB SERVER
# =========================================================

def create_web_app():

    app = web.Application()

    app.router.add_get("/shop", shop_page)

    app.router.add_get("/api/shop/products", api_products)
    app.router.add_post("/api/shop/order", api_create_order)

    app.router.add_get("/admin", admin_page)
    app.router.add_get("/admin/orders", admin_orders_page)
    app.router.add_get("/admin/products", admin_products_page)

    app.router.add_get("/health", health)

    app.router.add_get("/media/{file_id}", media_file)

    app.router.add_get("/pay/click/{order_id}", pay_click)
    app.router.add_get("/pay/payme/{order_id}", pay_payme)

    return app


# =========================================================
# MAIN
# =========================================================

async def main():

    init_db()

    app = create_web_app()

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", 8080))

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    asyncio.create_task(remind_admin())
    asyncio.create_task(scheduled_posts_loop())

    await dp.start_polling(bot)


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
