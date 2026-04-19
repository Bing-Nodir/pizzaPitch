import os
import sqlite3
import json
from pathlib import Path


DB_PATH = Path(os.getenv("BOT_DB_PATH", Path(__file__).with_name("bot_menu.sqlite3")))
VALID_DELIVERY_MODES = {"delivery", "pickup"}
VALID_PAYMENT_STATUSES = {"pending", "paid", "failed"}
VALID_ORDER_STATUSES = {"new", "accepted", "preparing", "delivering", "completed", "cancelled"}


CATEGORY_SEED = [
    {"key": "national", "name": "🇺🇿 Milliy Taomlar", "sort_order": 1},
    {"key": "fast_food", "name": "🍔 Fast Food", "sort_order": 2},
    {"key": "drinks", "name": "🥤 Ichimliklar", "sort_order": 3},
    {"key": "sets", "name": "🎁 Maxsus Setlar", "sort_order": 4},
]


MENU_SEED = [
    {
        "id": "palov",
        "category_key": "national",
        "name": "👑 Xon Saroy Maxsus Palovi",
        "short_name": "Palov",
        "image_path": "assets/menu/palov.png",
        "measure_label": "Vazni",
        "measure_value": "450 gr",
        "price": 55000,
        "ingredients": (
            "Saralab olingan Devzira guruchi, 12 soat davomida marinadlangan mol go'shti, "
            "sershira sariq va qizil sabzilar, bedana tuxumi, kishmish va Kings maxsus ziravorlari."
        ),
        "creative_note": (
            "Haqiqiy shohlar munosib ko'rgan ta'm. Bir qoshiq va siz o‘tmishdagi saroylar "
            "hashamatini his qilasiz."
        ),
        "sort_order": 1,
    },
    {
        "id": "mastava",
        "category_key": "national",
        "name": "🍲 Vazir Mastavasi",
        "short_name": "Mastava",
        "image_path": "assets/menu/mastava.png",
        "measure_label": "Hajmi",
        "measure_value": "400 ml",
        "price": 28000,
        "ingredients": (
            "Mayda to'ralgan tender go'sht, dumaloq guruch, xushbo'y rayhon, yangi kartoshka "
            "va maxsus suzma bilan tortiladi."
        ),
        "creative_note": "Sizni ich-ichingizdan isituvchi va quvvatga to'ldiruvchi sehrli sho'rva.",
        "sort_order": 2,
    },
    {
        "id": "somsa",
        "category_key": "national",
        "name": "🥟 Shaxrixon Somsasi (3 dona)",
        "short_name": "Somsa",
        "image_path": "assets/menu/somsa.png",
        "measure_label": "Vazni",
        "measure_value": "300 gr",
        "price": 27000,
        "ingredients": "Varaqi xamir, pichoqda to'ralgan go'sht, dumba yog'i, ko'p piyoz va zira.",
        "creative_note": "Tandirdan uzilgan, qirsillashi qo'shni mahallaga eshitiladigan afsonaviy somsa.",
        "sort_order": 3,
    },
    {
        "id": "lavash",
        "category_key": "fast_food",
        "name": "🌯 Kings Maxsus Lavashi",
        "short_name": "Lavash",
        "image_path": "assets/menu/lavash.png",
        "measure_label": "Vazni",
        "measure_value": "350 gr",
        "price": 30000,
        "ingredients": (
            "Grilda pishgan shirali go'sht, tilla rang chips, yangi pomidor, "
            "tuzlangan bodring va maxfiy oq sous."
        ),
        "creative_note": "Oddiy lavashlardan charchadingizmi? Qirollik darajasidagi ta'mni sinab ko'ring.",
        "sort_order": 1,
    },
    {
        "id": "klab",
        "category_key": "fast_food",
        "name": "🥪 Mega Klab Sendvich",
        "short_name": "Klab",
        "image_path": "assets/menu/klab.png",
        "measure_label": "Vazni",
        "measure_value": "420 gr",
        "price": 30000,
        "ingredients": (
            "Uch qavatli sariyog'li tost noni, dudlangan kurka go'shti, Chedder pishlog'i, "
            "Aysberg salat bargi va qarsillovchi fri."
        ),
        "creative_note": "To'ylilik darajasi maksimum. Katta ishtahalar uchun eng to'g'ri tanlov.",
        "sort_order": 2,
    },
    {
        "id": "hotdog",
        "category_key": "fast_food",
        "name": "🌶 Chilli Hot-Dog",
        "short_name": "Hot-Dog",
        "image_path": "assets/menu/hotdog.png",
        "measure_label": "Vazni",
        "measure_value": "250 gr",
        "price": 24000,
        "ingredients": (
            "Sutli sosiska, Jalapeno qalampiri, achchiq ketchu-mayonez, "
            "koreyscha sabzi va qovurilgan piyoz chiplari."
        ),
        "creative_note": "O'tkir hislarni yaxshi ko'radiganlar uchun haqiqiy portlash.",
        "sort_order": 3,
    },
    {
        "id": "olcha_choy",
        "category_key": "drinks",
        "name": "🍒 Olcha Tarovati Yaxna Choyi",
        "short_name": "Olcha Choyi",
        "image_path": "assets/menu/olcha_choy.png",
        "measure_label": "Hajmi",
        "measure_value": "400 ml",
        "price": 16000,
        "ingredients": "Tabiiy olcha sharbati, qora choy damlamasi, yalpiz novdasi va muz bo'laklari.",
        "creative_note": "Issiq kunda charchoqni bir zumda arituvchi vitaminli salqinlik.",
        "sort_order": 1,
    },
    {
        "id": "smuzi",
        "category_key": "drinks",
        "name": "🥭 Tropicana Smuzi",
        "short_name": "Smuzi",
        "image_path": "assets/menu/smuzi.png",
        "measure_label": "Hajmi",
        "measure_value": "350 ml",
        "price": 12000,
        "ingredients": "Mango pyuresi, banan, chia urug'lari va kokos suti.",
        "creative_note": "Bir qultum bilan o'zingizni tropik orollarda his eting.",
        "sort_order": 2,
    },
    {
        "id": "talaba_power",
        "category_key": "sets",
        "name": "⚡ Talaba Power Seti",
        "short_name": "Talaba Set",
        "image_path": "assets/menu/talaba_power.png",
        "measure_label": "Format",
        "measure_value": "Duo-kombo",
        "price": 45000,
        "ingredients": "1 ta Klab sendvich + 1 ta Pepsi (0.5L) + kichik porsiya fri.",
        "creative_note": "Imtihon oldidan yoki darsdan keyin miyangizni zaryadlash uchun eng yaxshi energiya.",
        "sort_order": 1,
    },
    {
        "id": "yengil_vazn",
        "category_key": "sets",
        "name": "👸 Yengil-Vazn Seti",
        "short_name": "Yengil Set",
        "image_path": "assets/menu/yengil_vazn.png",
        "measure_label": "Format",
        "measure_value": "Duo-kombo",
        "price": 35000,
        "ingredients": "Smuzi + olchali choy + mevali asorti.",
        "creative_note": "Qomatiga e'tiborli bo'lgan, lekin shirinlikdan voz kecha olmaydiganlar uchun.",
        "sort_order": 2,
    },
]


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    columns = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def initialize_database() -> None:
    connection = get_connection()
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS menu_categories (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                sort_order INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS menu_items (
                id TEXT PRIMARY KEY,
                category_key TEXT NOT NULL,
                name TEXT NOT NULL,
                short_name TEXT NOT NULL,
                measure_label TEXT NOT NULL,
                measure_value TEXT NOT NULL,
                price INTEGER NOT NULL,
                ingredients TEXT NOT NULL,
                creative_note TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (category_key) REFERENCES menu_categories(key)
            )
            """
        )
        ensure_column(connection, "menu_items", "image_path", "TEXT NOT NULL DEFAULT ''")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                fname TEXT NOT NULL,
                lname TEXT NOT NULL,
                phone TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS user_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                branch_name TEXT NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_used_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                customer_phone TEXT NOT NULL,
                delivery_mode TEXT NOT NULL,
                branch_name TEXT NOT NULL,
                address_text TEXT NOT NULL,
                payment_method TEXT NOT NULL,
                payment_status TEXT NOT NULL,
                order_status TEXT NOT NULL,
                delivery_fee INTEGER NOT NULL,
                subtotal INTEGER NOT NULL,
                total_amount INTEGER NOT NULL,
                items_json TEXT NOT NULL,
                note TEXT,
                lat REAL,
                lon REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_addresses_telegram_last_used
            ON user_addresses (telegram_id, last_used_at DESC, id DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_telegram_created_at
            ON orders (telegram_id, created_at DESC, id DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_statuses
            ON orders (order_status, payment_status, created_at DESC, id DESC)
            """
        )

        connection.executemany(
            """
            INSERT INTO menu_categories (key, name, sort_order)
            VALUES (:key, :name, :sort_order)
            ON CONFLICT(key) DO UPDATE SET
                name = excluded.name,
                sort_order = excluded.sort_order
            """,
            CATEGORY_SEED,
        )
        connection.executemany(
            """
            INSERT INTO menu_items (
                id, category_key, name, short_name, measure_label, measure_value,
                price, ingredients, creative_note, image_path, sort_order, is_active
            )
            VALUES (
                :id, :category_key, :name, :short_name, :measure_label, :measure_value,
                :price, :ingredients, :creative_note, :image_path, :sort_order, 1
            )
            ON CONFLICT(id) DO UPDATE SET
                category_key = excluded.category_key,
                name = excluded.name,
                short_name = excluded.short_name,
                measure_label = excluded.measure_label,
                measure_value = excluded.measure_value,
                price = excluded.price,
                ingredients = excluded.ingredients,
                creative_note = excluded.creative_note,
                image_path = excluded.image_path,
                sort_order = excluded.sort_order,
                is_active = 1
            """,
            [{**item, "image_path": item.get("image_path", "")} for item in MENU_SEED],
        )

        connection.execute(
            """
            UPDATE menu_items
            SET is_active = 0
            WHERE id NOT IN ({placeholders})
            """.format(placeholders=", ".join("?" for _ in MENU_SEED)),
            [item["id"] for item in MENU_SEED],
        )
        connection.commit()
    finally:
        connection.close()


def load_menu_snapshot() -> tuple[list[dict], dict[str, dict]]:
    connection = get_connection()
    try:
        categories = [dict(row) for row in connection.execute(
            "SELECT key, name, sort_order FROM menu_categories ORDER BY sort_order, name"
        ).fetchall()]
        
        items = [dict(row) for row in connection.execute(
            """
            SELECT menu_items.id, menu_items.category_key, menu_categories.name AS category_name,
                   menu_items.name, menu_items.short_name, menu_items.measure_label,
                   menu_items.measure_value, menu_items.price, menu_items.ingredients,
                   menu_items.creative_note, menu_items.image_path, menu_items.sort_order
            FROM menu_items
            JOIN menu_categories ON menu_categories.key = menu_items.category_key
            WHERE menu_items.is_active = 1
            ORDER BY menu_categories.sort_order, menu_items.sort_order, menu_items.name
            """
        ).fetchall()]
        return categories, {item["id"]: item for item in items}
    finally:
        connection.close()


def save_user_profile(telegram_id: int, fname: str, lname: str, phone: str) -> dict:
    connection = get_connection()
    try:
        connection.execute(
            """
            INSERT INTO users (telegram_id, fname, lname, phone)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                fname = excluded.fname,
                lname = excluded.lname,
                phone = excluded.phone,
                updated_at = CURRENT_TIMESTAMP
            """,
            (telegram_id, fname, lname, phone),
        )
        connection.commit()
    finally:
        connection.close()

    profile = get_user_profile(telegram_id)
    if profile is None:
        raise RuntimeError(f"Foydalanuvchi profili saqlanmadi: {telegram_id}")
    return profile


def get_user_profile(telegram_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT telegram_id, fname, lname, phone
            FROM users
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def list_user_addresses(telegram_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT id, telegram_id, title, branch_name, lat, lon, created_at, last_used_at
            FROM user_addresses
            WHERE telegram_id = ?
            ORDER BY last_used_at DESC, id DESC
            LIMIT 3
            """,
            (telegram_id,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def get_user_address(telegram_id: int, address_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT id, telegram_id, title, branch_name, lat, lon, created_at, last_used_at
            FROM user_addresses
            WHERE telegram_id = ? AND id = ?
            """,
            (telegram_id, address_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def touch_user_address(telegram_id: int, address_id: int) -> dict | None:
    connection = get_connection()
    try:
        connection.execute(
            """
            UPDATE user_addresses
            SET last_used_at = CURRENT_TIMESTAMP
            WHERE telegram_id = ? AND id = ?
            """,
            (telegram_id, address_id),
        )
        connection.commit()
    finally:
        connection.close()

    return get_user_address(telegram_id, address_id)


def save_user_address(telegram_id: int, branch_name: str, lat: float, lon: float) -> tuple[dict, bool, str | None]:
    addresses = list_user_addresses(telegram_id)
    for address in addresses:
        if abs(address["lat"] - lat) <= 0.0003 and abs(address["lon"] - lon) <= 0.0003:
            connection = get_connection()
            try:
                connection.execute(
                    """
                    UPDATE user_addresses
                    SET branch_name = ?, lat = ?, lon = ?, last_used_at = CURRENT_TIMESTAMP
                    WHERE telegram_id = ? AND id = ?
                    """,
                    (branch_name, lat, lon, telegram_id, address["id"]),
                )
                connection.commit()
            finally:
                connection.close()
            updated_address = get_user_address(telegram_id, address["id"])
            if updated_address is None:
                raise RuntimeError(f"Manzil yangilanmadi: {address['id']}")
            return updated_address, False, None

    removed_title = None
    if len(addresses) >= 3:
        oldest_address = addresses[-1]
        removed_title = oldest_address["title"]
        connection = get_connection()
        try:
            connection.execute(
                "DELETE FROM user_addresses WHERE telegram_id = ? AND id = ?",
                (telegram_id, oldest_address["id"]),
            )
            connection.commit()
        finally:
            connection.close()
        addresses = [address for address in addresses if address["id"] != oldest_address["id"]]

    used_titles = {address["title"] for address in addresses}
    title = next(
        (f"Manzil {index}" for index in range(1, 4) if f"Manzil {index}" not in used_titles),
        f"Manzil {len(addresses) + 1}",
    )

    connection = get_connection()
    try:
        cursor = connection.execute(
            """
            INSERT INTO user_addresses (telegram_id, title, branch_name, lat, lon)
            VALUES (?, ?, ?, ?, ?)
            """,
            (telegram_id, title, branch_name, lat, lon),
        )
        address_id = cursor.lastrowid
        connection.commit()
    finally:
        connection.close()

    if address_id is None:
        raise RuntimeError("Yangi manzil ID qaytmadi")

    created_address = get_user_address(telegram_id, address_id)
    if created_address is None:
        raise RuntimeError(f"Manzil saqlanmadi: {address_id}")
    return created_address, True, removed_title


def create_order(
    telegram_id: int,
    customer_name: str,
    customer_phone: str,
    delivery_mode: str,
    branch_name: str,
    address_text: str,
    payment_method: str,
    payment_status: str,
    order_status: str,
    delivery_fee: int,
    subtotal: int,
    total_amount: int,
    items: list[dict],
    timestamp: str,
    note: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
) -> dict:
    if delivery_mode not in VALID_DELIVERY_MODES:
        raise ValueError(f"Noto'g'ri delivery_mode: {delivery_mode!r}")
    if payment_status not in VALID_PAYMENT_STATUSES:
        raise ValueError(f"Noto'g'ri payment_status: {payment_status!r}")
    if order_status not in VALID_ORDER_STATUSES:
        raise ValueError(f"Noto'g'ri order_status: {order_status!r}")
    if not items:
        raise ValueError("Buyurtma items bo'sh bo'lishi mumkin emas")
    if delivery_fee < 0 or subtotal < 0 or total_amount < 0:
        raise ValueError("Buyurtma summalari manfiy bo'lishi mumkin emas")
    if total_amount != subtotal + delivery_fee:
        raise ValueError("Jami summa subtotal va delivery_fee yig'indisiga teng emas")

    connection = get_connection()
    try:
        cursor = connection.execute(
            """
            INSERT INTO orders (
                telegram_id, customer_name, customer_phone, delivery_mode, branch_name,
                address_text, payment_method, payment_status, order_status, delivery_fee,
                subtotal, total_amount, items_json, note, lat, lon, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                customer_name,
                customer_phone,
                delivery_mode,
                branch_name,
                address_text,
                payment_method,
                payment_status,
                order_status,
                delivery_fee,
                subtotal,
                total_amount,
                json.dumps(items, ensure_ascii=False),
                note,
                lat,
                lon,
                timestamp,
                timestamp,
            ),
        )
        order_id = cursor.lastrowid
        connection.commit()
    finally:
        connection.close()

    if order_id is None:
        raise RuntimeError("Buyurtma ID qaytmadi")

    order = get_order(order_id)
    if order is None:
        raise RuntimeError(f"Buyurtma saqlanmadi: {order_id}")
    return order


def get_order(order_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id, telegram_id, customer_name, customer_phone, delivery_mode, branch_name,
                address_text, payment_method, payment_status, order_status, delivery_fee,
                subtotal, total_amount, items_json, note, lat, lon, created_at, updated_at
            FROM orders
            WHERE id = ?
            """,
            (order_id,),
        ).fetchone()
        if not row:
            return None
        order = dict(row)
        order["items"] = json.loads(order.pop("items_json"))
        return order
    finally:
        connection.close()


def update_order_status(
    order_id: int,
    timestamp: str,
    payment_status: str | None = None,
    order_status: str | None = None,
) -> dict | None:
    if payment_status is None and order_status is None:
        return get_order(order_id)
    if payment_status is not None and payment_status not in VALID_PAYMENT_STATUSES:
        raise ValueError(f"Noto'g'ri payment_status: {payment_status!r}")
    if order_status is not None and order_status not in VALID_ORDER_STATUSES:
        raise ValueError(f"Noto'g'ri order_status: {order_status!r}")

    updates = ["updated_at = ?"]
    params: list[object] = [timestamp]
    if payment_status is not None:
        updates.append("payment_status = ?")
        params.append(payment_status)
    if order_status is not None:
        updates.append("order_status = ?")
        params.append(order_status)
    params.append(order_id)

    connection = get_connection()
    try:
        connection.execute(
            f"""
            UPDATE orders
            SET {", ".join(updates)}
            WHERE id = ?
            """,
            params,
        )
        connection.commit()
    finally:
        connection.close()

    return get_order(order_id)
