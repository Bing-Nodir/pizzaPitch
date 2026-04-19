import asyncio
import logging
import os
import re
from datetime import datetime
from html import escape
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, InputMediaPhoto
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from database import (
    create_order,
    get_user_profile,
    initialize_database,
    list_user_addresses,
    load_menu_snapshot,
    save_user_address,
    save_user_profile,
    touch_user_address,
    update_order_status,
)

# --- SOZLAMALAR ---
PROJECT_DIR = Path(__file__).resolve().parent
ENV_FILE = PROJECT_DIR / ".env"
ENV_EXAMPLE_FILE = PROJECT_DIR / ".env.example"
BOT_TOKEN_PLACEHOLDER = "PASTE_YOUR_TELEGRAM_BOT_TOKEN_HERE"


def load_local_env(env_path: Path) -> None:
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[7:].strip()

        key, separator, value = line.partition("=")
        if not separator:
            continue

        env_name = key.strip()
        if not env_name or env_name in os.environ:
            continue

        env_value = value.strip()
        if len(env_value) >= 2 and env_value[0] == env_value[-1] and env_value[0] in {"'", '"'}:
            env_value = env_value[1:-1]

        os.environ[env_name] = env_value


load_local_env(ENV_FILE)


def read_int_env(name: str, default: int) -> int:
    raw_value = (os.getenv(name) or "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        logging.warning("%s noto'g'ri berilgan: %r. Default qiymat ishlatildi: %s", name, raw_value, default)
        return default


TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
ADMIN_ID = read_int_env("ADMIN_ID", 0)
WORKING_HOURS = (9, 3)  # 09:00 dan 03:00 gacha
DELIVERY_FEE = read_int_env("DELIVERY_FEE", 10000)
BOT_TIMEZONE = os.getenv("BOT_TIMEZONE", "Asia/Tashkent")
CLICK_TERMINAL_CREDENTIAL = os.getenv(
    "CLICK_TERMINAL_CREDENTIAL",
    "",
)
CLICK_TERMINAL_CONNECTED_AT = (os.getenv("CLICK_TERMINAL_CONNECTED_AT") or "").strip()


def has_configured_bot_token() -> bool:
    return bool(TOKEN) and TOKEN != BOT_TOKEN_PLACEHOLDER


def build_missing_bot_token_message() -> str:
    env_exists = ENV_FILE.is_file()
    if not env_exists:
        setup_hint = (
            f"{ENV_FILE.name} fayli topilmadi. {ENV_EXAMPLE_FILE.name} dan nusxa olib "
            f"{ENV_FILE.name} yarating yoki PowerShell sessiyasida BOT_TOKEN ni sozlang."
        )
    else:
        setup_hint = (
            f"{ENV_FILE.name} ichidagi BOT_TOKEN bo'sh yoki namuna qiymatda qolgan. "
            "Uni haqiqiy Telegram bot tokeniga almashtiring."
        )

    return (
        "BOT_TOKEN muhit o'zgaruvchisi topilmadi.\n\n"
        f"{setup_hint}\n"
        f"Sozlama fayli joyi: {ENV_FILE}\n"
        "PowerShell uchun misol:\n"
        '$env:BOT_TOKEN="123456:ABCDEF..."\n'
        "python main.py"
    )

# --- DOIMIY MATNLAR ---
MAIN_MENU_BUTTON = "🍽 Menyu"
CART_BUTTON = "🛒 Savat"
WORKING_HOURS_BUTTON = "🕓 Ish vaqti"
CONTACT_BUTTON = "📞 Aloqa"
DELIVERY_BUTTON = f"🚖 Yetkazib berish (+{DELIVERY_FEE:,})"
PICKUP_BUTTON = "🏃 O'zi borib olish (0)"
SEND_LOCATION_BUTTON = "📍 Lokatsiya yuborish"
CONTACT_REQUEST_BUTTON = "📱 Kontaktni yuborish"
BACK_BUTTON = "⬅️ Orqaga"
SKIP_NOTE_BUTTON = "⏭ Izohsiz davom etish"
ADD_NEW_ADDRESS_TEXT = "➕ Yangi manzil qo'shish"

PAYMENT_OPTIONS = ["💵 Naqd pul", "💳 Click", "💳 Payme"]
PAYMENT_STATUS_LABELS = {
    "pending": "Kutilmoqda",
    "paid": "To'landi",
    "failed": "To'lanmadi",
}
ORDER_STATUS_LABELS = {
    "new": "Yangi",
    "accepted": "Qabul qilindi",
    "preparing": "Tayyorlanmoqda",
    "delivering": "Yo'lda",
    "completed": "Yakunlandi",
    "cancelled": "Bekor qilindi",
}
MENU_TEXT_ALIASES = {
    MAIN_MENU_BUTTON.casefold(),
    "menyu",
    "menu",
}
VALID_PAYMENT_STATUSES = set(PAYMENT_STATUS_LABELS)
VALID_ORDER_STATUSES = set(ORDER_STATUS_LABELS)

# --- FILIALLAR ---
BRANCHES = [
    {
        "code": "chilonzor",
        "name": "Bosh filial (Chilonzor)",
        "label": "📍 Chilonzor",
        "lat": 41.2827,
        "lon": 69.2041,
    },
    {
        "code": "yunusobod",
        "name": "Yunusobod filiali",
        "label": "📍 Yunusobod",
        "lat": 41.3645,
        "lon": 69.2867,
    },
]
BRANCH_BY_LABEL = {branch["label"]: branch for branch in BRANCHES}

initialize_database()
MENU_CATEGORIES, MENU = load_menu_snapshot()

USER_PROFILES = {}
NAME_RE = re.compile(r"^[A-Za-zА-Яа-яЁёʻ’' -]{2,30}$")
bot: Bot | None = None


class OrderProcess(StatesGroup):
    register_name = State()
    register_lastname = State()
    register_phone = State()
    choosing_dishes = State()
    delivery_type = State()
    get_location = State()
    manual_delivery_branch = State()
    pickup_branch = State()
    payment_type = State()
    order_note = State()


def format_price(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def require_user_id(user: types.User | None) -> int:
    if user is None:
        raise RuntimeError("Telegram foydalanuvchi ma'lumoti topilmadi")
    return user.id


def get_callback_message(call: types.CallbackQuery) -> types.Message | None:
    message = call.message
    return message if isinstance(message, types.Message) else None


def checkout_reset_data() -> dict:
    return {
        "delivery_mode": None,
        "branch": None,
        "address_text": None,
        "manual_address_text": None,
        "saved_address_id": None,
        "payment_type": None,
        "note": None,
        "delivery_fee": 0,
        "lat": None,
        "lon": None,
    }


def parse_click_terminal_credential(raw_value: str) -> dict:
    parts = (raw_value or "").split(":", 2)
    if len(parts) != 3:
        return {
            "connected": False,
            "terminal_id": "",
            "mode": "",
            "secret": "",
        }

    return {
        "connected": True,
        "terminal_id": parts[0],
        "mode": parts[1],
        "secret": parts[2],
    }


CLICK_TERMINAL = parse_click_terminal_credential(CLICK_TERMINAL_CREDENTIAL)


def now_local() -> datetime:
    try:
        return datetime.now(ZoneInfo(BOT_TIMEZONE))
    except ZoneInfoNotFoundError:
        return datetime.now()


def now_text() -> str:
    return now_local().strftime("%d.%m.%Y %H:%M")


def working_hours_text() -> str:
    return f"{WORKING_HOURS[0]:02d}:00 - {WORKING_HOURS[1]:02d}:00"


def is_working_time(current_hour: int | None = None) -> bool:
    hour = now_local().hour if current_hour is None else current_hour
    start_hour, end_hour = WORKING_HOURS
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def is_valid_name(value: str | None) -> bool:
    if not value:
        return False
    cleaned = value.strip()
    return bool(NAME_RE.fullmatch(cleaned)) and sum(ch.isalpha() for ch in cleaned) >= 2


def normalize_phone(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if digits.startswith("998") and len(digits) == 12:
        return f"+{digits}"
    if digits.startswith("0") and len(digits) == 10:
        return f"+998{digits[1:]}"
    if len(digits) == 9:
        return f"+998{digits}"
    return None


def get_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * radius * asin(sqrt(a))


def sanitize_cart(cart: dict | None) -> tuple[dict[str, int], bool]:
    if not isinstance(cart, dict):
        return {}, bool(cart)

    sanitized: dict[str, int] = {}
    changed = False
    for raw_key, raw_qty in cart.items():
        item_key = str(raw_key)
        try:
            quantity = int(raw_qty)
        except (TypeError, ValueError):
            changed = True
            continue

        if item_key not in MENU or quantity <= 0:
            changed = True
            continue

        sanitized[item_key] = quantity

    return sanitized, changed or len(sanitized) != len(cart)


async def get_clean_state_data(state: FSMContext) -> dict:
    data = await state.get_data()
    cart, cart_changed = sanitize_cart(data.get("cart"))
    updates = {}

    if cart_changed:
        updates["cart"] = cart

    if not cart:
        cleared_checkout = checkout_reset_data()
        for key, default_value in cleared_checkout.items():
            if data.get(key) != default_value:
                updates[key] = default_value

    if updates:
        await state.update_data(**updates)
        data = {**data, **updates}

    return data


def group_menu_items_by_category() -> list[tuple[dict, list[dict]]]:
    grouped = []
    for category in MENU_CATEGORIES:
        items = [item for item in MENU.values() if item["category_key"] == category["key"]]
        if items:
            grouped.append((category, items))
    return grouped


def build_menu_overview_text() -> str:
    lines = [
        "🔥 <b>Bugungi menyu</b>",
        "Mazali taomni tanlang yoki savatni oching.",
        "",
    ]
    for category, items in group_menu_items_by_category():
        lines.append(f"<b>{escape(category['name'])}</b>")
        lines.append(" | ".join(escape(item["short_name"]) for item in items))
        lines.append("")
    return "\n".join(lines).strip()


def load_cached_profile(user_id: int) -> dict | None:
    profile = USER_PROFILES.get(user_id)
    if profile:
        return profile

    profile = get_user_profile(user_id)
    if not profile:
        return None

    cached_profile = {
        "fname": profile["fname"],
        "lname": profile["lname"],
        "phone": profile["phone"],
    }
    USER_PROFILES[user_id] = cached_profile
    return cached_profile


def build_main_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text=MAIN_MENU_BUTTON), types.KeyboardButton(text=CART_BUTTON))
    builder.row(types.KeyboardButton(text=WORKING_HOURS_BUTTON), types.KeyboardButton(text=CONTACT_BUTTON))
    return builder.as_markup(resize_keyboard=True)


def build_contact_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text=CONTACT_REQUEST_BUTTON, request_contact=True))
    return builder.as_markup(resize_keyboard=True)


def build_delivery_type_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text=DELIVERY_BUTTON), types.KeyboardButton(text=PICKUP_BUTTON))
    builder.row(types.KeyboardButton(text=BACK_BUTTON))
    return builder.as_markup(resize_keyboard=True)


def build_delivery_location_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text=SEND_LOCATION_BUTTON, request_location=True))
    builder.row(types.KeyboardButton(text="✍️ Manzilni yozish"))
    builder.row(types.KeyboardButton(text=BACK_BUTTON))
    return builder.as_markup(resize_keyboard=True)


def build_pickup_branch_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    for branch in BRANCHES:
        builder.row(types.KeyboardButton(text=branch["label"]))
    builder.row(types.KeyboardButton(text=BACK_BUTTON))
    return builder.as_markup(resize_keyboard=True)


def build_payment_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text=PAYMENT_OPTIONS[0]), types.KeyboardButton(text=PAYMENT_OPTIONS[1]))
    builder.row(types.KeyboardButton(text=PAYMENT_OPTIONS[2]))
    builder.row(types.KeyboardButton(text=BACK_BUTTON))
    return builder.as_markup(resize_keyboard=True)


def build_note_keyboard() -> types.ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text=SKIP_NOTE_BUTTON))
    builder.row(types.KeyboardButton(text=BACK_BUTTON))
    return builder.as_markup(resize_keyboard=True)


def build_saved_addresses_markup(addresses: list[dict]) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for address in addresses:
        builder.button(
            text=f"📍 {address['title']}",
            callback_data=f"saved_address:use:{address['id']}",
        )
    builder.button(text=ADD_NEW_ADDRESS_TEXT, callback_data="saved_address:new")
    builder.adjust(*([1] * len(addresses)), 1)
    return builder.as_markup()


def build_saved_addresses_text(addresses: list[dict]) -> str:
    lines = [
        "📌 <b>Saqlangan manzillar</b>",
        "Pastdagilardan birini tanlang yoki yangi manzil qo'shing.",
        "",
    ]
    for address in addresses:
        lines.append(f"• {address['title']} — {escape(address['branch_name'])}")
    lines.append("")
    lines.append("Yangi manzil qo'shish uchun lokatsiya yuborish tugmasidan foydalaning yoki manzilni yozib yuboring.")
    return "\n".join(lines)


def build_menu_markup() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key, item in MENU.items():
        builder.button(
            text=f"{item['short_name']} • {format_price(item['price'])}",
            callback_data=f"item:{key}",
        )
    builder.button(text="🛒 Savatni ko'rish", callback_data="cart:view")
    builder.button(text="✅ Buyurtmani rasmiylashtirish", callback_data="checkout")
    builder.adjust(*([1] * len(MENU)), 1, 1)
    return builder.as_markup()


def build_item_markup(item_key: str) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Savatga qo'shish", callback_data=f"add:{item_key}")
    builder.button(text="🛒 Savat", callback_data="cart:view")
    builder.button(text="⬅️ Menyuga qaytish", callback_data="back_menu")
    builder.adjust(1, 1, 1)
    return builder.as_markup()


def build_cart_markup(cart: dict[str, int]) -> types.InlineKeyboardMarkup:
    cleaned_cart = sanitize_cart(cart)[0]
    builder = InlineKeyboardBuilder()
    for key, quantity in cleaned_cart.items():
        if quantity <= 0:
            continue
        builder.button(text=f"➖ {MENU[key]['short_name']}", callback_data=f"cart:dec:{key}")
        builder.button(text=f"➕ {MENU[key]['short_name']}", callback_data=f"cart:inc:{key}")
    builder.button(text="🍽 Menyu", callback_data="back_menu")
    if cleaned_cart:
        builder.button(text="🧹 Savatni tozalash", callback_data="cart:clear")
        builder.button(text="✅ Rasmiylashtirish", callback_data="checkout")
        builder.adjust(*([2] * len(cleaned_cart)), 1, 2)
    else:
        builder.adjust(1)
    return builder.as_markup()


def format_item_text(item_key: str, quantity: int = 0) -> str:
    item = MENU[item_key]
    lines = [
        f"<b>{escape(item['name'])}</b>",
        f"🏷 Kategoriya: {escape(item['category_name'])}",
        f"✨ {escape(item['creative_note'])}",
        f"🧾 Tarkibi: {escape(item['ingredients'])}",
        f"📌 {escape(item['measure_label'])}: {escape(item['measure_value'])}",
        f"💰 Narxi: {format_price(item['price'])} so'm",
    ]
    if quantity:
        lines.append(f"🛒 Savatda: {quantity} ta")
    return "\n".join(lines)


def resolve_item_image_path(item_key: str) -> Path | None:
    raw_path = (MENU[item_key].get("image_path") or "").strip()
    if not raw_path:
        return None

    image_path = Path(raw_path)
    if not image_path.is_absolute():
        image_path = PROJECT_DIR / image_path
    return image_path if image_path.is_file() else None


def format_cart_text(cart: dict[str, int], delivery_fee: int = 0) -> tuple[str, int]:
    cart = sanitize_cart(cart)[0]
    if not cart:
        return (
            "🛒 <b>Savat hozircha bo'sh.</b>\n"
            "Menyudan biror taom tanlang, keyin buyurtmani davom ettiramiz.",
            0,
        )

    lines = ["🛒 <b>Savat</b>", ""]
    total = 0
    for index, (key, qty) in enumerate(cart.items(), start=1):
        amount = MENU[key]["price"] * qty
        total += amount
        lines.append(f"{index}. {escape(MENU[key]['name'])} x{qty} — {format_price(amount)} so'm")

    if delivery_fee:
        total += delivery_fee
        lines.append("")
        lines.append(f"🚚 Yetkazib berish — {format_price(delivery_fee)} so'm")

    lines.append("")
    lines.append(f"💰 <b>Jami: {format_price(total)} so'm</b>")
    return "\n".join(lines), total


def payment_status_text(payment_type: str) -> str:
    parts = payment_type.split(" ", 1)
    if len(parts) == 2:
        return parts[1].strip()
    return payment_type.strip()


def is_click_payment(payment_type: str | None) -> bool:
    return payment_status_text(payment_type or "").casefold() == "click"


def payment_status_label(value: str | None) -> str:
    return PAYMENT_STATUS_LABELS.get(value or "pending", "Kutilmoqda")


def order_status_label(value: str | None) -> str:
    return ORDER_STATUS_LABELS.get(value or "new", "Yangi")


def serialize_cart_items(cart: dict[str, int]) -> tuple[list[dict], int]:
    items = []
    subtotal = 0
    for key, qty in sanitize_cart(cart)[0].items():
        menu_item = MENU[key]
        amount = menu_item["price"] * qty
        subtotal += amount
        items.append(
            {
                "item_id": key,
                "name": menu_item["name"],
                "short_name": menu_item["short_name"],
                "unit_price": menu_item["price"],
                "quantity": qty,
                "line_total": amount,
            }
        )
    return items, subtotal


def get_receipt_items(data: dict) -> tuple[list[dict], int]:
    if data.get("items"):
        items = []
        subtotal = 0
        for item in data["items"]:
            line_total = int(item.get("line_total", item.get("unit_price", 0) * item.get("quantity", 0)))
            subtotal += line_total
            items.append(
                {
                    "name": item["name"],
                    "quantity": int(item["quantity"]),
                    "line_total": line_total,
                }
            )
        return items, subtotal

    serialized_items, subtotal = serialize_cart_items(data.get("cart", {}))
    return [
        {"name": item["name"], "quantity": item["quantity"], "line_total": item["line_total"]}
        for item in serialized_items
    ], subtotal


def format_receipt_items(data: dict) -> tuple[list[str], int]:
    items, subtotal = get_receipt_items(data)
    lines = [
        f"{escape(item['name'])} ({item['quantity']}x) - {format_price(item['line_total'])} so'm"
        for item in items
    ]
    return lines, subtotal


def build_admin_order_actions(order_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ To'landi", callback_data=f"admin:payment:{order_id}:paid")
    builder.button(text="⏳ Kutilmoqda", callback_data=f"admin:payment:{order_id}:pending")
    builder.button(text="❌ To'lanmadi", callback_data=f"admin:payment:{order_id}:failed")
    builder.button(text="👨‍🍳 Qabul qilindi", callback_data=f"admin:order:{order_id}:accepted")
    builder.button(text="🔥 Tayyorlanmoqda", callback_data=f"admin:order:{order_id}:preparing")
    builder.button(text="🛵 Yo'lda", callback_data=f"admin:order:{order_id}:delivering")
    builder.button(text="🏁 Yakunlandi", callback_data=f"admin:order:{order_id}:completed")
    builder.button(text="🚫 Bekor qilindi", callback_data=f"admin:order:{order_id}:cancelled")
    builder.adjust(1, 2, 2, 2, 1)
    return builder.as_markup()


def build_order_receipt(data: dict, order_id: str | int | None = None) -> tuple[str, int]:
    item_lines, subtotal = format_receipt_items(data)
    delivery_fee = int(data.get("delivery_fee", 0))
    total = subtotal + delivery_fee
    customer_name = data.get("customer_name")
    if not customer_name:
        customer_name = " ".join(part for part in [data.get("fname"), data.get("lname")] if part).strip()
    customer_phone = data.get("customer_phone") or data.get("phone") or "Ko'rsatilmagan"
    payment_method = data.get("payment_method") or data.get("payment_type") or "Noma'lum"
    payment_status = payment_status_label(data.get("payment_status"))
    order_number = order_id or data.get("id")
    delivery_mode = data.get("delivery_mode")
    delivery_line = (
        f"🚚 Yetkazib berish - {format_price(delivery_fee)} so'm"
        if delivery_mode == "delivery"
        else "🏃 Olib ketish - 0 so'm"
    )

    lines = [
        f"📝 Buyurtma raqami #{order_number}",
        f"👤 Mijoz: {escape(customer_name or 'Nomaʼlum mijoz')}",
        f"📞 Tel: {escape(customer_phone)}",
        *item_lines,
        delivery_line,
        f"💰 Jami: {format_price(total)} so'm",
        f"🏁 Holat: {payment_status_text(payment_method)} ({payment_status})",
    ]
    return "\n".join(lines), total


def build_admin_receipt(data: dict, order_id: str | int | None = None, order_time: str | None = None, note: str | None = None) -> tuple[str, int]:
    receipt_text, total = build_order_receipt(data, order_id)
    delivery_mode_text = "Yetkazib berish" if data.get("delivery_mode") == "delivery" else "O'zi borib olish"
    note_value = note if note is not None else data.get("note", "")
    created_at = order_time or data.get("created_at", now_text())
    branch_name = data.get("branch_name") or data.get("branch") or "Ko'rsatilmagan"
    address_text = data.get("address_text") or "Ko'rsatilmagan"

    extra_lines = [
        f"🕒 Vaqt: {created_at}",
        f"🚖 Turi: {delivery_mode_text}",
        f"🏬 Filial: {escape(branch_name)}",
        f"🏠 Manzil: {escape(address_text)}",
        f"📦 Buyurtma statusi: {order_status_label(data.get('order_status'))}",
    ]
    payment_method = data.get("payment_method") or data.get("payment_type")
    if is_click_payment(payment_method) and CLICK_TERMINAL["connected"]:
        extra_lines.append(
            f"🔌 Click Terminal: {CLICK_TERMINAL['mode']} / {CLICK_TERMINAL['terminal_id']} / {CLICK_TERMINAL_CONNECTED_AT}"
        )
    if note_value:
        extra_lines.append(f"📝 Izoh: {escape(note_value)}")
    if data.get("lat") and data.get("lon"):
        extra_lines.append(
            f"🗺 <a href=\"https://maps.google.com/?q={data['lat']},{data['lon']}\">Lokatsiyani xaritada ochish</a>"
        )

    return "\n".join([receipt_text, "", *extra_lines]), total


def build_home_text(first_name: str) -> str:
    return (
        f"🍽 <b>Xush kelibsiz, {escape(first_name)}!</b>\n"
        "Super-tezkor buyurtma rejimi yoqildi.\n"
        "Pastdagi tugmalar bilan menyuni oching, savatni boshqaring va buyurtmani 1-2 daqiqada yakunlang."
    )


def build_help_text() -> str:
    if CLICK_TERMINAL["connected"]:
        payment_line = f"• Click terminal ulangan ({CLICK_TERMINAL['mode']})"
    else:
        payment_line = "• Click terminal ulanmagan"
    return (
        "ℹ️ <b>Yordam</b>\n"
        f"• /start - botni ishga tushirish\n"
        f"• /menu - menyuni ochish\n"
        f"• /cart - savatni ko'rish\n"
        f"• /cancel - joriy bosqichni bekor qilish\n\n"
        f"🕓 Ish vaqti: <b>{working_hours_text()}</b>\n"
        f"💳 To'lovlar:\n{payment_line}\n"
        "Buyurtma jarayonida tugmalar orqali harakat qilish eng qulay usul."
    )


def build_contact_text() -> str:
    return (
        "📞 <b>Aloqa</b>\n"
        "Operator: +998 90 000 00 00\n"
        "Telegram: @your_support\n"
        "Yetkazib berish va pickup buyurtmalari mavjud."
    )


def initial_payment_status(payment_method: str) -> str:
    return "pending"


def payment_method_notice(payment_method: str) -> str:
    if is_click_payment(payment_method) and CLICK_TERMINAL["connected"]:
        terminal_mode = CLICK_TERMINAL["mode"] or "UNKNOWN"
        return (
            f"🔌 Click Terminal {terminal_mode} rejimida ulangan.\n"
            f"Terminal: <b>{CLICK_TERMINAL['terminal_id']}</b>\n"
            "To'lov holati admin panel orqali kuzatiladi."
        )
    return ""


def build_customer_status_message(order: dict, changed_field: str) -> str:
    if changed_field == "payment":
        return (
            f"💳 Buyurtma #{order['id']} uchun to'lov holati yangilandi.\n"
            f"Holat: <b>{payment_status_label(order['payment_status'])}</b>"
        )

    return (
        f"📦 Buyurtma #{order['id']} statusi yangilandi.\n"
        f"Yangi holat: <b>{order_status_label(order['order_status'])}</b>"
    )


async def restore_registered_session(user_id: int, state: FSMContext, cart: dict[str, int] | None = None) -> bool:
    profile = load_cached_profile(user_id)
    if not profile:
        return False

    cleaned_cart, _ = sanitize_cart(cart)
    await state.clear()
    await state.set_state(OrderProcess.choosing_dishes)
    await state.update_data(**profile, cart=cleaned_cart)
    return True


def is_menu_text(message: types.Message) -> bool:
    return (message.text or "").strip().casefold() in MENU_TEXT_ALIASES


async def prompt_delivery_location_options(message: types.Message, user_id: int) -> None:
    saved_addresses = list_user_addresses(user_id)
    if saved_addresses:
        await message.answer(
            build_saved_addresses_text(saved_addresses),
            reply_markup=build_saved_addresses_markup(saved_addresses),
        )
        await message.answer(
            "Yoki yangi manzil uchun Telegram lokatsiyasini yuboring yoki manzilni matn ko'rinishida yozing.",
            reply_markup=build_delivery_location_keyboard(),
        )
        return

    await message.answer(
        "Saqlangan manzillar hozircha yo'q.\nYangi manzil qo'shish uchun lokatsiya yuboring yoki manzilni yozib yuboring.",
        reply_markup=build_delivery_location_keyboard(),
    )


async def open_menu(message: types.Message, state: FSMContext) -> None:
    data = await get_clean_state_data(state)
    cart = data.get("cart", {})
    user_id = require_user_id(message.from_user)
    has_session = data.get("phone") or await restore_registered_session(user_id, state, cart)

    if has_session:
        await state.set_state(OrderProcess.choosing_dishes)
        await message.answer("Menyu ochildi.", reply_markup=build_main_keyboard())
    else:
        await message.answer(
            "Menyuni ko'rishingiz mumkin.\nBuyurtma berish uchun /start yuboring."
        )

    await show_menu_message(message)


async def ensure_registered_message(message: types.Message, state: FSMContext) -> dict | None:
    data = await get_clean_state_data(state)
    if data.get("phone"):
        return data

    user_id = require_user_id(message.from_user)
    if await restore_registered_session(user_id, state, data.get("cart", {})):
        return await get_clean_state_data(state)

    await message.answer("Buyurtmani boshlash uchun avval /start yuboring.")
    return None


async def ensure_registered_callback(call: types.CallbackQuery, state: FSMContext) -> dict | None:
    data = await get_clean_state_data(state)
    if data.get("phone"):
        return data

    if await restore_registered_session(call.from_user.id, state, data.get("cart", {})):
        return await get_clean_state_data(state)

    await call.answer("Avval /start yuboring.", show_alert=True)
    return None


async def show_menu_message(message: types.Message) -> None:
    await message.answer(build_menu_overview_text(), reply_markup=build_menu_markup())


async def show_cart_message(message: types.Message, state: FSMContext) -> None:
    data = await get_clean_state_data(state)
    cart_text, _ = format_cart_text(data.get("cart", {}), data.get("delivery_fee", 0))
    await message.answer(cart_text, reply_markup=build_cart_markup(data.get("cart", {})))


async def render_callback_text(
    call: types.CallbackQuery,
    text: str,
    reply_markup: types.InlineKeyboardMarkup,
) -> None:
    callback_message = get_callback_message(call)
    if callback_message is None:
        await call.answer("Bu xabar endi mavjud emas.", show_alert=True)
        return

    if callback_message.photo:
        await callback_message.delete()
        await callback_message.answer(text, reply_markup=reply_markup)
        return
    try:
        await callback_message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        # Matn bir xil bo'lsa xatolik bermasligi uchun
        pass


async def render_item_detail_message(call: types.CallbackQuery, item_key: str, quantity: int) -> None:
    text = format_item_text(item_key, quantity)
    reply_markup = build_item_markup(item_key)
    image_path = resolve_item_image_path(item_key)
    callback_message = get_callback_message(call)

    if callback_message is None:
        await call.answer("Bu xabar endi mavjud emas.", show_alert=True)
        return

    if image_path:
        photo = FSInputFile(str(image_path))
        if callback_message.photo:
            await callback_message.edit_media(
                media=InputMediaPhoto(media=photo, caption=text, parse_mode="HTML"),
                reply_markup=reply_markup,
            )
            return
        try:
            await callback_message.delete()
            await callback_message.answer_photo(photo, caption=text, reply_markup=reply_markup)
        except Exception:
            await callback_message.answer_photo(photo, caption=text, reply_markup=reply_markup)
        return

    await render_callback_text(call, text, reply_markup)

dp = Dispatcher(storage=MemoryStorage())


def get_bot_instance() -> Bot:
    if bot is None:
        raise RuntimeError("Bot hali ishga tushirilmagan. BOT_TOKEN muhit o'zgaruvchisini tekshiring.")
    return bot


@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()

    if not is_working_time():
        await message.answer(
            "🌙 Hozir buyurtma qabul qilinmayapti.\n"
            f"Ish vaqti: <b>{working_hours_text()}</b>\n"
            "Ish vaqtida /start yuborsangiz, darrov davom ettiramiz.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    user_id = require_user_id(message.from_user)
    profile = load_cached_profile(user_id)
    if profile:
        await state.set_state(OrderProcess.choosing_dishes)
        await state.update_data(**profile, cart={})
        await message.answer(build_home_text(profile["fname"]), reply_markup=build_main_keyboard())
        await show_menu_message(message)
        return

    await message.answer(
        "Assalomu alaykum!\n"
        "Keling, botni super-tezkor rejimga tayyorlaymiz.\n"
        "<b>Ismingizni yuboring:</b>",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    await state.set_state(OrderProcess.register_name)


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(build_help_text())


@dp.message(Command("menu"))
async def cmd_menu(message: types.Message, state: FSMContext):
    await open_menu(message, state)


@dp.message(Command("cart"))
async def cmd_cart(message: types.Message, state: FSMContext):
    if not await ensure_registered_message(message, state):
        return
    await show_cart_message(message, state)


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_data = await state.get_data()
    user_id = require_user_id(message.from_user)
    if await restore_registered_session(user_id, state, current_data.get("cart", {})):
        await message.answer(
            "Joriy qadam bekor qilindi. Asosiy menyuga qaytdik.",
            reply_markup=build_main_keyboard(),
        )
        return

    await state.clear()
    await message.answer(
        "Joriy holat tozalandi. Yangidan boshlash uchun /start yuboring.",
        reply_markup=types.ReplyKeyboardRemove(),
    )


@dp.message(OrderProcess.register_name)
async def process_name(message: types.Message, state: FSMContext):
    if not is_valid_name(message.text):
        await message.answer("Ism noto'g'ri. 2-30 belgidan iborat, harf va oddiy belgilar bilan yuboring.")
        return

    cleaned_name = (message.text or "").strip().title()
    await state.update_data(fname=cleaned_name)
    await message.answer("<b>Familiyangizni yuboring:</b>")
    await state.set_state(OrderProcess.register_lastname)


@dp.message(OrderProcess.register_lastname)
async def process_lastname(message: types.Message, state: FSMContext):
    if not is_valid_name(message.text):
        await message.answer("Familiya noto'g'ri. Qayta yuborib ko'ring.")
        return

    cleaned_lastname = (message.text or "").strip().title()
    await state.update_data(lname=cleaned_lastname)
    await message.answer(
        "Ajoyib. Endi telefon raqamingizni yuboring.\n"
        "Kontakt tugmasini bossangiz ham bo'ladi, qo'lda yozsangiz ham qabul qilaman.",
        reply_markup=build_contact_keyboard(),
    )
    await state.set_state(OrderProcess.register_phone)


@dp.message(OrderProcess.register_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = None
    user_id = require_user_id(message.from_user)
    if message.contact:
        if message.contact.user_id and message.contact.user_id != user_id:
            await message.answer("Iltimos, o'zingizning kontaktingizni yuboring.")
            return
        phone = normalize_phone(message.contact.phone_number)
    else:
        phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "Telefon raqami formati noto'g'ri.\n"
            "Masalan: <b>+998901234567</b> yoki kontakt tugmasini bosing."
        )
        return

    data = await state.get_data()
    profile = {
        "fname": data["fname"],
        "lname": data["lname"],
        "phone": phone,
    }
    save_user_profile(user_id, profile["fname"], profile["lname"], profile["phone"])
    USER_PROFILES[user_id] = profile

    await state.set_state(OrderProcess.choosing_dishes)
    await state.update_data(**profile, cart={})
    await message.answer(
        "✅ Profil tayyor.\n"
        "Endi sizga qulay boshqaruv paneli ochildi.",
        reply_markup=build_main_keyboard(),
    )
    await message.answer(build_home_text(profile["fname"]))
    await show_menu_message(message)


@dp.message(is_menu_text)
async def menu_button(message: types.Message, state: FSMContext):
    await open_menu(message, state)


@dp.message(F.text == CART_BUTTON)
async def cart_button(message: types.Message, state: FSMContext):
    if not await ensure_registered_message(message, state):
        return
    await show_cart_message(message, state)


@dp.message(F.text == WORKING_HOURS_BUTTON)
async def working_hours_button(message: types.Message):
    status = "ochiq" if is_working_time() else "yopiq"
    await message.answer(
        f"🕓 Ish vaqti: <b>{working_hours_text()}</b>\n"
        f"Hozirgi holat: <b>{status}</b>."
    )


@dp.message(F.text == CONTACT_BUTTON)
async def contact_button(message: types.Message):
    await message.answer(build_contact_text())


@dp.callback_query(F.data.startswith("saved_address:"))
async def saved_address_callback(call: types.CallbackQuery, state: FSMContext):
    data = await ensure_registered_callback(call, state)
    if not data:
        return
    if not data.get("cart"):
        await call.answer("Avval savatni to'ldiring.", show_alert=True)
        return

    callback_message = get_callback_message(call)
    if callback_message is None:
        await call.answer("Bu xabar endi mavjud emas.", show_alert=True)
        return

    action = (call.data or "").split(":")
    if len(action) < 2:
        await call.answer()
        return

    if action[1] == "new":
        await state.update_data(
            **checkout_reset_data(),
            delivery_mode="delivery",
        )
        await state.set_state(OrderProcess.get_location)
        await callback_message.answer(
            "Yangi manzil qo'shish uchun Telegram lokatsiyasini yuboring yoki manzilni yozib yuboring.",
            reply_markup=build_delivery_location_keyboard(),
        )
        await call.answer()
        return

    if len(action) != 3 or action[1] != "use":
        await call.answer()
        return

    try:
        address_id = int(action[2])
    except ValueError:
        await call.answer("Manzil identifikatori noto'g'ri.", show_alert=True)
        return

    address = touch_user_address(call.from_user.id, address_id)
    if not address:
        await call.answer("Saqlangan manzil topilmadi.", show_alert=True)
        return

    await state.update_data(
        delivery_mode="delivery",
        branch=address["branch_name"],
        address_text=address["title"],
        delivery_fee=DELIVERY_FEE,
        lat=address["lat"],
        lon=address["lon"],
        saved_address_id=address["id"],
    )
    await callback_message.answer(
        f"📍 Tanlandi: <b>{escape(address['title'])}</b>\n"
        f"🏬 Eng yaqin filial: <b>{escape(address['branch_name'])}</b>\n"
        "Endi to'lov turini tanlang:",
        reply_markup=build_payment_keyboard(),
    )
    await state.set_state(OrderProcess.payment_type)
    await call.answer("Saqlangan manzil tanlandi")


@dp.callback_query(F.data.startswith("admin:"))
async def admin_order_callback(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Bu boshqaruv faqat admin uchun.", show_alert=True)
        return

    callback_message = get_callback_message(call)
    if callback_message is None:
        await call.answer("Bu xabar endi mavjud emas.", show_alert=True)
        return

    parts = (call.data or "").split(":")
    if len(parts) != 4:
        await call.answer()
        return

    _, action_type, raw_order_id, new_value = parts
    try:
        order_id = int(raw_order_id)
    except ValueError:
        await call.answer("Buyurtma identifikatori noto'g'ri.", show_alert=True)
        return

    order = None
    if action_type == "payment":
        if new_value not in VALID_PAYMENT_STATUSES:
            await call.answer("To'lov holati noto'g'ri.", show_alert=True)
            return
        order = update_order_status(order_id, now_text(), payment_status=new_value)
        changed_field = "payment"
    elif action_type == "order":
        if new_value not in VALID_ORDER_STATUSES:
            await call.answer("Buyurtma statusi noto'g'ri.", show_alert=True)
            return
        order = update_order_status(order_id, now_text(), order_status=new_value)
        changed_field = "order"
    else:
        await call.answer()
        return

    if not order:
        await call.answer("Buyurtma topilmadi.", show_alert=True)
        return

    admin_receipt, _ = build_admin_receipt(order)
    await callback_message.edit_text(
        admin_receipt,
        reply_markup=build_admin_order_actions(order["id"]),
    )
    try:
        await get_bot_instance().send_message(order["telegram_id"], build_customer_status_message(order, changed_field))
    except Exception:
        logging.exception("Mijozga status xabari yuborilmadi")

    label = payment_status_label(order["payment_status"]) if changed_field == "payment" else order_status_label(order["order_status"])
    await call.answer(f"Yangilandi: {label}")


@dp.callback_query(F.data == "back_menu")
async def back_to_menu(call: types.CallbackQuery, state: FSMContext):
    await render_callback_text(
        call,
        build_menu_overview_text(),
        reply_markup=build_menu_markup(),
    )
    await call.answer()


@dp.callback_query(F.data == "cart:view")
async def show_cart_callback(call: types.CallbackQuery, state: FSMContext):
    data = await get_clean_state_data(state)
    cart_text, _ = format_cart_text(data.get("cart", {}), data.get("delivery_fee", 0))
    await render_callback_text(
        call,
        cart_text,
        reply_markup=build_cart_markup(data.get("cart", {})),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("item:"))
async def item_detail(call: types.CallbackQuery, state: FSMContext):
    data = await get_clean_state_data(state)
    item_key = (call.data or "").split(":", 1)[1]
    if item_key not in MENU:
        await call.answer("Bu mahsulot endi mavjud emas.", show_alert=True)
        return
    quantity = data.get("cart", {}).get(item_key, 0)
    await render_item_detail_message(call, item_key, quantity)
    await call.answer()


@dp.callback_query(F.data.startswith("add:"))
async def add_to_cart(call: types.CallbackQuery, state: FSMContext):
    data = await ensure_registered_callback(call, state)
    if not data:
        return

    item_key = (call.data or "").split(":", 1)[1]
    if item_key not in MENU:
        await call.answer("Bu mahsulot endi mavjud emas.", show_alert=True)
        return

    cart = sanitize_cart(data.get("cart"))[0]
    cart[item_key] = cart.get(item_key, 0) + 1
    await state.update_data(cart=cart)

    await render_item_detail_message(call, item_key, cart[item_key])
    await call.answer(f"{MENU[item_key]['short_name']} savatga qo'shildi")


@dp.callback_query(F.data.startswith("cart:"))
async def manage_cart(call: types.CallbackQuery, state: FSMContext):
    data = await ensure_registered_callback(call, state)
    if not data:
        return

    callback_message = get_callback_message(call)
    if callback_message is None:
        await call.answer("Bu xabar endi mavjud emas.", show_alert=True)
        return

    parts = (call.data or "").split(":")
    if len(parts) < 2:
        await call.answer()
        return

    action = parts[1]
    if action not in {"inc", "dec", "clear"}:
        await call.answer()
        return

    cart = sanitize_cart(data.get("cart"))[0]

    if action == "clear":
        cart = {}
        await state.update_data(cart=cart, **checkout_reset_data())
        cart_text, _ = format_cart_text(cart)
        await callback_message.edit_text(cart_text, reply_markup=build_cart_markup(cart))
        await call.answer("Savat tozalandi")
        return

    if len(parts) != 3:
        await call.answer("Savat buyrug'i noto'g'ri.", show_alert=True)
        return

    item_key = parts[2]
    if item_key not in MENU:
        cart.pop(item_key, None)
        updates = {"cart": cart}
        if not cart:
            updates.update(checkout_reset_data())
        await state.update_data(**updates)
        await call.answer("Bu mahsulot endi menyuda yo'q.", show_alert=True)
        return

    if action == "inc":
        cart[item_key] = cart.get(item_key, 0) + 1
    elif action == "dec":
        if item_key in cart:
            cart[item_key] -= 1
            if cart[item_key] <= 0:
                cart.pop(item_key)

    updates = {"cart": cart}
    if not cart:
        updates.update(checkout_reset_data())

    await state.update_data(**updates)
    refreshed = await get_clean_state_data(state)
    cart_text, _ = format_cart_text(cart, refreshed.get("delivery_fee", 0))
    try:
        await callback_message.edit_text(cart_text, reply_markup=build_cart_markup(cart))
    except TelegramBadRequest:
        pass
    await call.answer("Savat yangilandi")


@dp.callback_query(F.data == "checkout")
async def start_checkout(call: types.CallbackQuery, state: FSMContext):
    data = await ensure_registered_callback(call, state)
    if not data:
        return

    callback_message = get_callback_message(call)
    if callback_message is None:
        await call.answer("Bu xabar endi mavjud emas.", show_alert=True)
        return

    if not is_working_time():
        await call.answer("Hozir buyurtma qabul qilinmayapti.", show_alert=True)
        return

    cart = sanitize_cart(data.get("cart"))[0]
    if not cart:
        await call.answer("Savat bo'sh. Avval taom tanlang.", show_alert=True)
        return

    await state.update_data(cart=cart, **checkout_reset_data())
    await callback_message.answer(
        "Yetkazib berish turini tanlang:",
        reply_markup=build_delivery_type_keyboard(),
    )
    await state.set_state(OrderProcess.delivery_type)
    await call.answer()


@dp.message(F.text == DELIVERY_BUTTON, OrderProcess.delivery_type)
async def choose_delivery(message: types.Message, state: FSMContext):
    user_id = require_user_id(message.from_user)
    await state.update_data(
        **checkout_reset_data(),
        delivery_mode="delivery",
    )
    await state.set_state(OrderProcess.get_location)
    await prompt_delivery_location_options(message, user_id)


@dp.message(F.text == PICKUP_BUTTON, OrderProcess.delivery_type)
async def choose_pickup(message: types.Message, state: FSMContext):
    await state.update_data(
        **checkout_reset_data(),
        delivery_mode="pickup",
    )
    await message.answer(
        "Qaysi filialdan olib ketasiz?",
        reply_markup=build_pickup_branch_keyboard(),
    )
    await state.set_state(OrderProcess.pickup_branch)


@dp.message(F.text == BACK_BUTTON, OrderProcess.delivery_type)
async def cancel_delivery_type(message: types.Message, state: FSMContext):
    user_id = require_user_id(message.from_user)
    await restore_registered_session(user_id, state, (await state.get_data()).get("cart", {}))
    await message.answer("Asosiy menyuga qaytdik.", reply_markup=build_main_keyboard())


@dp.message(OrderProcess.delivery_type)
async def invalid_delivery_type(message: types.Message):
    await message.answer("Iltimos, yetkazib berish yoki pickup tugmalaridan birini tanlang.")


@dp.message(F.location, OrderProcess.get_location)
async def handle_location(message: types.Message, state: FSMContext):
    location = message.location
    if location is None:
        await message.answer("Lokatsiya topilmadi. Iltimos, qayta yuboring.")
        return

    user_id = require_user_id(message.from_user)
    user_lat = location.latitude
    user_lon = location.longitude
    nearest_branch = min(
        BRANCHES,
        key=lambda branch: get_distance(user_lat, user_lon, branch["lat"], branch["lon"]),
    )
    saved_address, created_new, removed_title = save_user_address(
        user_id,
        nearest_branch["name"],
        user_lat,
        user_lon,
    )

    await state.update_data(
        branch=nearest_branch["name"],
        address_text=saved_address["title"],
        delivery_fee=DELIVERY_FEE,
        lat=user_lat,
        lon=user_lon,
        saved_address_id=saved_address["id"],
    )
    save_status = f"💾 Saqlandi: <b>{saved_address['title']}</b>" if created_new else f"📍 Tanlandi: <b>{saved_address['title']}</b>"
    if removed_title:
        save_status += f"\n♻️ 3 ta limit sabab <b>{removed_title}</b> o'rniga yangilandi."
    await message.answer(
        f"{save_status}\n"
        f"🏬 Eng yaqin filial: <b>{nearest_branch['name']}</b>\n"
        "Endi to'lov turini tanlang:",
        reply_markup=build_payment_keyboard(),
    )
    await state.set_state(OrderProcess.payment_type)


@dp.message(OrderProcess.get_location)
async def handle_location_fallback(message: types.Message, state: FSMContext):
    if message.text == BACK_BUTTON:
        await message.answer(
            "Buyurtma turini qayta tanlang.",
            reply_markup=build_delivery_type_keyboard(),
        )
        await state.set_state(OrderProcess.delivery_type)
        return

    message_text = (message.text or "").strip()
    if message_text == "✍️ Manzilni yozish":
        await message.answer(
            "Marhamat, to'liq manzilingizni yozib yuboring.\n"
            "Masalan: Chilonzor 19-kvartal, 12-uy, 45-xonadon.",
            reply_markup=build_delivery_location_keyboard(),
        )
        return

    if message_text == SEND_LOCATION_BUTTON:
        await message.answer(
            "Telegram lokatsiyani avtomatik yubormadi.\n"
            "Lokatsiya ruxsatini yoqing yoki manzilni matn ko'rinishida yuboring.",
            reply_markup=build_delivery_location_keyboard(),
        )
        return

    if not message_text:
        await message.answer(
            "Lokatsiya yuboring yoki manzilni yozib yuboring.",
            reply_markup=build_delivery_location_keyboard(),
        )
        return

    await state.update_data(manual_address_text=message_text, saved_address_id=None, lat=None, lon=None)
    await message.answer(
        f"📍 Manzil qabul qilindi: <b>{escape(message_text)}</b>\n"
        "Endi qaysi filialdan yetkazib berishni tanlang:",
        reply_markup=build_pickup_branch_keyboard(),
    )
    await state.set_state(OrderProcess.manual_delivery_branch)


@dp.message(OrderProcess.manual_delivery_branch)
async def manual_delivery_branch(message: types.Message, state: FSMContext):
    if message.text == BACK_BUTTON:
        await state.set_state(OrderProcess.get_location)
        await prompt_delivery_location_options(message, require_user_id(message.from_user))
        return

    branch = BRANCH_BY_LABEL.get(message.text)
    if not branch:
        await message.answer("Iltimos, filialni pastdagi tugmalardan tanlang.")
        return

    data = await state.get_data()
    manual_address_text = (data.get("manual_address_text") or "").strip()
    if not manual_address_text:
        await state.set_state(OrderProcess.get_location)
        await prompt_delivery_location_options(message, require_user_id(message.from_user))
        return

    await state.update_data(
        delivery_mode="delivery",
        branch=branch["name"],
        address_text=manual_address_text,
        delivery_fee=DELIVERY_FEE,
        lat=None,
        lon=None,
        saved_address_id=None,
    )
    await message.answer(
        f"🏬 Filial: <b>{branch['name']}</b>\n"
        f"📍 Manzil: <b>{escape(manual_address_text)}</b>\n"
        "Endi to'lov turini tanlang:",
        reply_markup=build_payment_keyboard(),
    )
    await state.set_state(OrderProcess.payment_type)


@dp.message(OrderProcess.pickup_branch)
async def pickup_branch(message: types.Message, state: FSMContext):
    if message.text == BACK_BUTTON:
        await message.answer(
            "Buyurtma turini qayta tanlang.",
            reply_markup=build_delivery_type_keyboard(),
        )
        await state.set_state(OrderProcess.delivery_type)
        return

    branch = BRANCH_BY_LABEL.get(message.text)
    if not branch:
        await message.answer("Iltimos, filialni pastdagi tugmalardan tanlang.")
        return

    await state.update_data(
        branch=branch["name"],
        address_text="Mijoz o'zi olib ketadi",
        delivery_fee=0,
        lat=None,
        lon=None,
    )
    await message.answer(
        f"Pickup filiali: <b>{branch['name']}</b>\n"
        "Endi to'lov turini tanlang:",
        reply_markup=build_payment_keyboard(),
    )
    await state.set_state(OrderProcess.payment_type)


@dp.message(OrderProcess.payment_type)
async def process_payment(message: types.Message, state: FSMContext):
    if message.text == BACK_BUTTON:
        data = await state.get_data()
        if data.get("delivery_mode") == "pickup":
            await message.answer(
                "Filialni qayta tanlang.",
                reply_markup=build_pickup_branch_keyboard(),
            )
            await state.set_state(OrderProcess.pickup_branch)
            return

        if data.get("manual_address_text") and not data.get("lat") and not data.get("lon"):
            await message.answer(
                "Filialni qayta tanlang.",
                reply_markup=build_pickup_branch_keyboard(),
            )
            await state.set_state(OrderProcess.manual_delivery_branch)
            return

        await state.set_state(OrderProcess.get_location)
        await prompt_delivery_location_options(message, require_user_id(message.from_user))
        return

    if message.text not in PAYMENT_OPTIONS:
        await message.answer("Iltimos, to'lov turini tugmalardan birini tanlab yuboring.")
        return

    await state.update_data(payment_type=message.text)
    notice = payment_method_notice(message.text)
    prompt_text = (
        "Qo'shimcha izoh qoldirasizmi?\n"
        "Masalan: achchiqroq bo'lsin, qo'ng'iroq qilmasin, tezroq olib keling."
    )
    if notice:
        prompt_text = f"{notice}\n\n{prompt_text}"

    await message.answer(prompt_text, reply_markup=build_note_keyboard())
    await state.set_state(OrderProcess.order_note)


@dp.message(OrderProcess.order_note)
async def finish_order(message: types.Message, state: FSMContext):
    user_id = require_user_id(message.from_user)
    if message.text == BACK_BUTTON:
        await message.answer("To'lov turini qayta tanlang.", reply_markup=build_payment_keyboard())
        await state.set_state(OrderProcess.payment_type)
        return

    note_text = (message.text or "").strip()
    if not note_text:
        await message.answer("Izoh yozing yoki `Izohsiz davom etish` tugmasini bosing.")
        return

    note = "" if note_text == SKIP_NOTE_BUTTON else note_text
    data = await get_clean_state_data(state)
    cart = data.get("cart", {})

    if not cart:
        await restore_registered_session(user_id, state)
        await message.answer("Savat bo'shab qoldi. Menyudan yana tanlab ko'ring.", reply_markup=build_main_keyboard())
        return

    required_fields = ("delivery_mode", "branch", "address_text", "payment_type")
    missing_fields = [field for field in required_fields if not data.get(field)]
    if missing_fields:
        logging.warning("Buyurtma yaratishda state to'liq emas: %s", ", ".join(missing_fields))
        await restore_registered_session(user_id, state, cart)
        await message.answer(
            "Buyurtma ma'lumotlari to'liq saqlanmagan. Jarayon qayta ochildi, iltimos yana bir bor rasmiylashtiring.",
            reply_markup=build_main_keyboard(),
        )
        return

    serialized_items, subtotal = serialize_cart_items(cart)
    delivery_fee = int(data.get("delivery_fee", 0))
    total_amount = subtotal + delivery_fee
    order_time = now_text()
    try:
        order = create_order(
            telegram_id=user_id,
            customer_name=f"{data['fname']} {data['lname']}",
            customer_phone=data["phone"],
            delivery_mode=data["delivery_mode"],
            branch_name=data["branch"],
            address_text=data["address_text"],
            payment_method=data["payment_type"],
            payment_status=initial_payment_status(data["payment_type"]),
            order_status="new",
            delivery_fee=delivery_fee,
            subtotal=subtotal,
            total_amount=total_amount,
            items=serialized_items,
            timestamp=order_time,
            note=note or None,
            lat=data.get("lat"),
            lon=data.get("lon"),
        )
    except ValueError:
        logging.exception("Buyurtma validatsiyadan o'tmadi")
        await restore_registered_session(user_id, state, cart)
        await message.answer(
            "Buyurtmani saqlashda xatolik yuz berdi. Ma'lumotlar tekshirildi, iltimos yana bir marta urinib ko'ring.",
            reply_markup=build_main_keyboard(),
        )
        return

    receipt_text, total = build_order_receipt(order)
    admin_receipt, _ = build_admin_receipt(order)

    eta = "25-35 daqiqa" if data.get("delivery_mode") == "delivery" else "12-20 daqiqa"
    if ADMIN_ID > 0:
        try:
            await get_bot_instance().send_message(
                ADMIN_ID,
                admin_receipt,
                reply_markup=build_admin_order_actions(order["id"]),
            )
        except Exception:
            logging.exception("Admin ga buyurtma yuborilmadi")
    else:
        logging.warning("ADMIN_ID sozlanmagan. Buyurtma admin ga yuborilmadi.")

    await message.answer(
        "✅ Buyurtmangiz qabul qilindi.\n\n"
        f"{receipt_text}\n"
        f"⏱ Tayyor bo'lish vaqti: {eta}",
        reply_markup=build_main_keyboard(),
    )

    profile = load_cached_profile(user_id)
    await state.clear()
    if profile:
        await state.set_state(OrderProcess.choosing_dishes)
        await state.update_data(**profile, cart={})


@dp.message(OrderProcess.choosing_dishes)
async def choosing_dishes_fallback(message: types.Message):
    await message.answer(
        "Pastdagi tugmalar orqali menyuni oching yoki savatni ko'ring.",
        reply_markup=build_main_keyboard(),
    )


@dp.message()
async def fallback(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if data.get("phone") or load_cached_profile(require_user_id(message.from_user)):
        await message.answer(
            "Hozircha shu buyruqni tushunmadim.\n"
            "Menyu, savat yoki /help dan foydalaning.",
            reply_markup=build_main_keyboard(),
        )
        return

    await message.answer("Botni ishga tushirish uchun /start yuboring.")


async def main():
    if not has_configured_bot_token():
        raise RuntimeError(build_missing_bot_token_message())

    global bot
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
