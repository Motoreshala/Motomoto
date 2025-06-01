import logging
from telegram import ReplyKeyboardMarkup, KeyboardButton
from telegram import Update
from telegram.ext import CallbackContext
from io import BytesIO
from openpyxl import Workbook
import config
import logic
import gdrive

# Определения состояний (константы должны соответствовать logic/handlers определениям)
MAIN_MENU, PROCESSING_WB, PROCESSING_OZ, PROCESSING_CONFIRM, STOCK_MENU = range(1, 6)
WAITING_SUBTRACT_ITEM, WAITING_SUBTRACT_QTY = 6, 7
WAITING_ADD_ITEM, WAITING_ADD_QTY = 8, 9
WAITING_RECEIVE_FILE = 10
WAITING_REVISION_FILE = 11
CLEAR_CONFIRM = 12
WAITING_SEARCH_QUERY = 13

# Разметка основных клавиатур
main_menu_buttons = [
    ["Обработка", "Остатки"],
    ["Поиск", "Помощь"],
    ["Очистить заказы"]
]
main_menu_kb = ReplyKeyboardMarkup(main_menu_buttons, resize_keyboard=True)

inventory_buttons = [
    ["Вычесть", "Добавить", "Приемка"],
    ["Ревизия", "Откат", "Назад"]
]
inventory_kb = ReplyKeyboardMarkup(inventory_buttons, resize_keyboard=True)

cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("Отмена")]], resize_keyboard=True, one_time_keyboard=True)

def start(update: Update, context: CallbackContext):
    user = update.effective_user
    logging.info(f"Пользователь {user.first_name} ({user.id}) запустил /start.")
    update.message.reply_text(
        "Добро пожаловать! Выберите действие:",
        reply_markup=main_menu_kb
    )
    return MAIN_MENU

# Обработчики основного меню
def handle_processing(update: Update, context: CallbackContext):
    update.message.reply_text(
        "Пришлите файл заказов Wildberries (Excel/CSV). Если заказов WB нет, отправьте 'нет'.",
        reply_markup=cancel_kb
    )
    context.user_data.clear()  # очистка старых данных заказов
    return PROCESSING_WB

def handle_inventory(update: Update, context: CallbackContext):
    update.message.reply_text(
        "Управление остатками:\n- Вычесть: списать товар со склада\n- Добавить: вернуть товар\n- Приемка: оприходовать новый товар\n- Ревизия: инвентаризация\n- Откат: отмена последней операции",
        reply_markup=inventory_kb
    )
    return STOCK_MENU

def handle_search(update: Update, context: CallbackContext):
    update.message.reply_text("Введите название или код товара для поиска:", reply_markup=cancel_kb)
    return WAITING_SEARCH_QUERY

def handle_help(update: Update, context: CallbackContext):
    help_text = (
        "ℹ️ *Помощь по командам:*\n"
        "• *Обработка* – обработка новых заказов с маркетплейсов (Wildberries, Ozon). Бот спишет товары со склада, сформирует список перемещений и заказ поставщику.\n"
        "• *Остатки* – управление складскими остатками: вычитание/добавление вручную, приемка нового товара по файлу, ревизия (инвентаризация), откат последнего изменения.\n"
        "• *Поиск* – поиск товара по названию или коду.\n"
        "• *Очистить заказы* – очистка списка заказов (лист 'Заказы' в Google Таблице) после завершения обработки.\n"
        "• *Отмена* – отмена текущей операции."
    )
    update.message.reply_text(help_text, reply_markup=main_menu_kb, parse_mode='Markdown')
    return MAIN_MENU

def handle_clear_orders(update: Update, context: CallbackContext):
    update.message.reply_text(
        "❗ Очистить список заказов в таблице? Отправьте 'Да' для подтверждения или 'Нет' для отмены.",
        reply_markup=ReplyKeyboardMarkup([["Да", "Нет"]], resize_keyboard=True, one_time_keyboard=True)
    )
    return CLEAR_CONFIRM

# Обработчики этапов обработки заказов
def handle_wb_file(update: Update, context: CallbackContext):
    document = update.message.document
    if not document:
        return PROCESSING_WB
    file_name = document.file_name or "wb_orders"
    ext = file_name.split(".")[-1] if "." in file_name else "xlsx"
    local_path = f"wb_orders.{ext}"
    try:
        file_obj = document.get_file()
        file_obj.download(custom_path=local_path)
        orders_wb = __import__('utils').parse_orders_file(local_path, "WB")
        context.user_data["orders_wb"] = orders_wb
    except Exception as e:
        logging.error(f"Ошибка обработки файла WB: {e}")
        update.message.reply_text("❌ Не удалось обработать файл Wildberries. Проверьте формат.", reply_markup=cancel_kb)
        return PROCESSING_WB
    update.message.reply_text(f"📥 Файл Wildberries получен. Заказов: {len(context.user_data.get('orders_wb', []))}.", reply_markup=cancel_kb)
    update.message.reply_text("Пришлите файл заказов Ozon (или 'нет', если нет заказов Ozon).", reply_markup=cancel_kb)
    return PROCESSING_OZ

def skip_wb_file(update: Update, context: CallbackContext):
    context.user_data["orders_wb"] = []
    update.message.reply_text("Пришлите файл заказов Ozon (или 'нет', если нет заказов Ozon).", reply_markup=cancel_kb)
    return PROCESSING_OZ

def handle_oz_file(update: Update, context: CallbackContext):
    document = update.message.document
    if document:
        file_name = document.file_name or "ozon_orders"
        ext = file_name.split(".")[-1] if "." in file_name else "xlsx"
        local_path = f"ozon_orders.{ext}"
        try:
            file_obj = document.get_file()
            file_obj.download(custom_path=local_path)
            orders_oz = __import__('utils').parse_orders_file(local_path, "Ozon")
            context.user_data["orders_oz"] = orders_oz
        except Exception as e:
            logging.error(f"Ошибка обработки файла Ozon: {e}")
            update.message.reply_text("❌ Не удалось обработать файл Ozon. Проверьте формат.", reply_markup=cancel_kb)
            return PROCESSING_OZ
        update.message.reply_text(f"📥 Файл Ozon получен. Заказов: {len(context.user_data.get('orders_oz', []))}.", reply_markup=cancel_kb)
    # Если получен файл Ozon или 'нет' (обрабатывается skip_oz_file), выполняем анализ и подтверждение
    orders_wb = context.user_data.get("orders_wb", [])
    orders_oz = context.user_data.get("orders_oz", [])
    all_orders = orders_wb + orders_oz
    if not all_orders:
        update.message.reply_text("❕ Заказы отсутствуют.", reply_markup=main_menu_kb)
        return MAIN_MENU
    analysis = logic.analyze_orders(all_orders)
    context.user_data["analysis"] = analysis
    missing_tm = analysis.get("missing_tm", {})
    if missing_tm:
        lines = [f"{gdrive.base_by_code.get(code, {}).get('brand', '')} {gdrive.base_by_code.get(code, {}).get('name', code)} – {qty} шт" for code, qty in missing_tm.items()]
        text = "⚠️ Недостаточно товаров для выполнения всех заказов:\n" + "\n".join(lines) + "\nЗаказать недостающие товары у поставщика? (Да/Нет)"
        update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup([["Да", "Нет"]], resize_keyboard=True, one_time_keyboard=True))
        return PROCESSING_CONFIRM
    else:
        # Все товары в наличии, завершаем сразу
        logic.commit_order_processing(analysis, order_tm_confirmed=True)
        send_movement_and_order_tm(update, context, analysis, order_tm_confirmed=True)
        context.user_data.clear()
        return MAIN_MENU

def skip_oz_file(update: Update, context: CallbackContext):
    """Обрабатывает ответ 'нет' на запрос файла Ozon, выполняет обработку только с заказами WB."""
    context.user_data["orders_oz"] = []
    orders_wb = context.user_data.get("orders_wb", [])
    all_orders = orders_wb
    if not all_orders:
        update.message.reply_text("❕ Заказы отсутствуют.", reply_markup=main_menu_kb)
        return MAIN_MENU
    analysis = logic.analyze_orders(all_orders)
    context.user_data["analysis"] = analysis
    missing_tm = analysis.get("missing_tm", {})
    if missing_tm:
        lines = [f"{gdrive.base_by_code.get(code, {}).get('brand', '')} {gdrive.base_by_code.get(code, {}).get('name', code)} – {qty} шт" for code, qty in missing_tm.items()]
        text = "⚠️ Недостаточно товаров для выполнения всех заказов:\n" + "\n".join(lines) + "\nЗаказать недостающие товары у поставщика? (Да/Нет)"
        update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup([["Да", "Нет"]], resize_keyboard=True, one_time_keyboard=True))
        return PROCESSING_CONFIRM
    else:
        logic.commit_order_processing(analysis, order_tm_confirmed=True)
        send_movement_and_order_tm(update, context, analysis, order_tm_confirmed=True)
        context.user_data.clear()
        return MAIN_MENU

def handle_process_confirm(update: Update, context: CallbackContext):
    user_reply = update.message.text.strip().lower()
    analysis = context.user_data.get("analysis")
    if not analysis:
        update.message.reply_text("Данные заказов не найдены. Возврат в меню.", reply_markup=main_menu_kb)
        return MAIN_MENU
    if user_reply in ["да", "yes", "y"]:
        logic.commit_order_processing(analysis, order_tm_confirmed=True)
        send_movement_and_order_tm(update, context, analysis, order_tm_confirmed=True)
    else:
        logic.commit_order_processing(analysis, order_tm_confirmed=False)
        send_movement_and_order_tm(update, context, analysis, order_tm_confirmed=False)
    context.user_data.clear()
    return MAIN_MENU

def send_movement_and_order_tm(update: Update, context: CallbackContext, analysis, order_tm_confirmed):
    """Отправляет пользователю файлы 'Перемещение' и 'Заказ ТМ' и текстовый отчет."""
    chat_id = update.effective_chat.id
    # Получаем данные из таблицы для файлов
    move_values = []
    ordertm_values = []
    try:
        move_values = gdrive.movement_sheet.get_all_values()
    except Exception as e:
        logging.error(f"Ошибка чтения 'Перемещение': {e}")
    try:
        if order_tm_confirmed:
            ordertm_values = gdrive.order_tm_sheet.get_all_values()
    except Exception as e:
        logging.error(f"Ошибка чтения 'Заказ ТМ': {e}")
    # Отправка файла 'Перемещение'
    if move_values and len(move_values) > 1:
        wb = Workbook()
        ws = wb.active
        for row in move_values:
            ws.append(row)
        bio = BytesIO()
        wb.save(bio); bio.seek(0)
        context.bot.send_document(chat_id, bio, filename="Перемещение.xlsx")
    # Отправка файла 'Заказ ТМ'
    if order_tm_confirmed and ordertm_values and len(ordertm_values) > 1:
        wb2 = Workbook()
        ws2 = wb2.active
        for row in ordertm_values:
            ws2.append(row)
        bio2 = BytesIO()
        wb2.save(bio2); bio2.seek(0)
        context.bot.send_document(chat_id, bio2, filename="Заказ_ТМ.xlsx")
    # Формирование текстового отчета
    msg_lines = []
    if move_values and len(move_values) > 1:
        msg_lines.append("✅ Файл 'Перемещение' сформирован.")
    else:
        msg_lines.append("✅ Перемещение со склада не требуется.")
    if order_tm_confirmed:
        if ordertm_values and len(ordertm_values) > 1:
            msg_lines.append("✅ Файл 'Заказ ТМ' сформирован.")
        else:
            msg_lines.append("✅ Заказ у поставщика не требуется.")
    else:
        msg_lines.append("⚠️ Товары поставщика не заказаны (пропущены).")
    # Сообщаем о недостатках товаров других брендов, если были
    shortages = analysis.get("shortages", {})
    if shortages:
        shortage_lines = []
        for code, qty in shortages.items():
            name = gdrive.base_by_code.get(code, {}).get("name", code)
            brand = gdrive.base_by_code.get(code, {}).get("brand", "")
            shortage_lines.append(f"{name} ({brand}) – {qty} шт")
        msg_lines.append("⚠️ Дефицит на складе:\n" + "\n".join(shortage_lines))
    report_text = "\n".join(msg_lines)
    context.bot.send_message(chat_id, report_text, reply_markup=main_menu_kb)

# Обработчики меню Остатки
def start_subtract(update: Update, context: CallbackContext):
    update.message.reply_text("Введите код или название товара для списания:", reply_markup=cancel_kb)
    context.user_data.pop("search_results", None)
    context.user_data.pop("target_code", None)
    return WAITING_SUBTRACT_ITEM

def handle_subtract_item(update: Update, context: CallbackContext):
    query = update.message.text.strip()
    if query.isdigit() and "search_results" in context.user_data:
        results = context.user_data["search_results"]
        idx = int(query)
        if 1 <= idx <= len(results):
            code, name, brand, stock = results[idx-1]
            context.user_data["target_code"] = code
            context.user_data.pop("search_results", None)
            update.message.reply_text(f"Товар: {name} ({code}). На складе {stock} шт. Сколько списать?", reply_markup=cancel_kb)
            return WAITING_SUBTRACT_QTY
        else:
            query = ""  # неправильный индекс, повторим поиск
    results, total = logic.search_products(query)
    if total == 0:
        update.message.reply_text("❌ Товар не найден. Уточните запрос:", reply_markup=cancel_kb)
        return WAITING_SUBTRACT_ITEM
    elif total == 1:
        code, name, brand, stock = results[0]
        context.user_data["target_code"] = code
        context.user_data.pop("search_results", None)
        update.message.reply_text(f"Товар: {name} ({code}). На складе {stock} шт. Введите количество для списания:", reply_markup=cancel_kb)
        return WAITING_SUBTRACT_QTY
    else:
        context.user_data["search_results"] = results
        list_text = "🔎 Найдено несколько товаров:\n"
        for i, (code, name, brand, stock) in enumerate(results, start=1):
            list_text += f"{i}. {name} ({brand}) – {stock} шт (код {code})\n"
        list_text += "Введите номер нужного товара или уточните запрос:"
        update.message.reply_text(list_text, reply_markup=cancel_kb)
        return WAITING_SUBTRACT_ITEM

def handle_subtract_qty(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    if not text.isdigit():
        update.message.reply_text("🔢 Введите числом количество для списания:", reply_markup=cancel_kb)
        return WAITING_SUBTRACT_QTY
    qty = int(text)
    if qty <= 0:
        update.message.reply_text("Количество должно быть больше 0.", reply_markup=cancel_kb)
        return WAITING_SUBTRACT_QTY
    code = context.user_data.get("target_code")
    name = gdrive.base_by_code.get(code, {}).get("name", code)
    current_stock = gdrive.warehouse_data.get(code, 0)
    try:
        logic.adjust_stock(code, -qty, "Ручное вычитание")
    except Exception as e:
        logging.error(f"Ошибка вычитания остатков: {e}")
        update.message.reply_text("❌ Не удалось обновить остаток.", reply_markup=inventory_kb)
        return STOCK_MENU
    new_stock = gdrive.warehouse_data.get(code, 0)
    if new_stock < 0:
        update.message.reply_text(f"✅ Списано {qty} шт товара \"{name}\". Новый остаток: {new_stock} шт (дефицит).", reply_markup=inventory_kb)
    else:
        update.message.reply_text(f"✅ Списано {qty} шт товара \"{name}\". Текущий остаток: {new_stock} шт.", reply_markup=inventory_kb)
    context.user_data.pop("target_code", None)
    context.user_data.pop("search_results", None)
    return STOCK_MENU

def start_add(update: Update, context: CallbackContext):
    update.message.reply_text("Введите код или название товара для добавления:", reply_markup=cancel_kb)
    context.user_data.pop("search_results", None)
    context.user_data.pop("target_code", None)
    return WAITING_ADD_ITEM

def handle_add_item(update: Update, context: CallbackContext):
    query = update.message.text.strip()
    if query.isdigit() and "search_results" in context.user_data:
        results = context.user_data["search_results"]
        idx = int(query)
        if 1 <= idx <= len(results):
            code, name, brand, stock = results[idx-1]
            context.user_data["target_code"] = code
            context.user_data.pop("search_results", None)
            update.message.reply_text(f"Товар: {name} ({code}). На складе {stock} шт. Сколько добавить?", reply_markup=cancel_kb)
            return WAITING_ADD_QTY
        else:
            query = ""
    results, total = logic.search_products(query)
    if total == 0:
        update.message.reply_text("❌ Товар не найден. Уточните запрос:", reply_markup=cancel_kb)
        return WAITING_ADD_ITEM
    elif total == 1:
        code, name, brand, stock = results[0]
        context.user_data["target_code"] = code
        context.user_data.pop("search_results", None)
        update.message.reply_text(f"Товар: {name} ({code}). На складе {stock} шт. Введите количество для добавления:", reply_markup=cancel_kb)
        return WAITING_ADD_QTY
    else:
        context.user_data["search_results"] = results
        list_text = "🔎 Найдено несколько товаров:\n"
        for i, (code, name, brand, stock) in enumerate(results, start=1):
            list_text += f"{i}. {name} ({brand}) – {stock} шт (код {code})\n"
        list_text += "Введите номер нужного товара или уточните запрос:"
        update.message.reply_text(list_text, reply_markup=cancel_kb)
        return WAITING_ADD_ITEM

def handle_add_qty(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    if not text.isdigit():
        update.message.reply_text("🔢 Введите числом количество для добавления:", reply_markup=cancel_kb)
        return WAITING_ADD_QTY
    qty = int(text)
    if qty <= 0:
        update.message.reply_text("Количество должно быть больше 0.", reply_markup=cancel_kb)
        return WAITING_ADD_QTY
    code = context.user_data.get("target_code")
    name = gdrive.base_by_code.get(code, {}).get("name", code)
    try:
        logic.adjust_stock(code, qty, "Ручное добавление")
    except Exception as e:
        logging.error(f"Ошибка добавления остатков: {e}")
        update.message.reply_text("❌ Не удалось обновить остаток.", reply_markup=inventory_kb)
        return STOCK_MENU
    new_stock = gdrive.warehouse_data.get(code, 0)
    update.message.reply_text(f"✅ Добавлено {qty} шт товара \"{name}\". Текущий остаток: {new_stock} шт.", reply_markup=inventory_kb)
    context.user_data.pop("target_code", None)
    context.user_data.pop("search_results", None)
    return STOCK_MENU

def start_receiving(update: Update, context: CallbackContext):
    update.message.reply_text("Пришлите файл приемки товара (Excel/CSV) с колонками [код, количество]:", reply_markup=cancel_kb)
    return WAITING_RECEIVE_FILE

def handle_receive_file(update: Update, context: CallbackContext):
    document = update.message.document
    if not document:
        return WAITING_RECEIVE_FILE
    file_name = document.file_name or "receiving"
    ext = file_name.split(".")[-1] if "." in file_name else "xlsx"
    local_path = f"receiving.{ext}"
    try:
        file_obj = document.get_file()
        file_obj.download(custom_path=local_path)
        deliveries = __import__('utils').parse_stock_file(local_path)
        changes = logic.process_receiving(deliveries)
    except Exception as e:
        logging.error(f"Ошибка обработки файла приемки: {e}")
        update.message.reply_text("❌ Не удалось обработать файл приемки.", reply_markup=cancel_kb)
        return WAITING_RECEIVE_FILE
    if not changes:
        update.message.reply_text("✅ Приемка выполнена. Изменений остатков нет.", reply_markup=inventory_kb)
    else:
        lines = [f"{name}: +{qty}" for name, qty in changes]
        update.message.reply_text("✅ Остатки обновлены (приемка):\n" + "\n".join(lines), reply_markup=inventory_kb)
    return STOCK_MENU

def start_revision(update: Update, context: CallbackContext):
    update.message.reply_text("Пришлите файл ревизии (Excel/CSV) с колонками [код, фактическое количество]:", reply_markup=cancel_kb)
    return WAITING_REVISION_FILE

def handle_revision_file(update: Update, context: CallbackContext):
    document = update.message.document
    if not document:
        return WAITING_REVISION_FILE
    file_name = document.file_name or "revision"
    ext = file_name.split(".")[-1] if "." in file_name else "xlsx"
    local_path = f"revision.{ext}"
    try:
        file_obj = document.get_file()
        file_obj.download(custom_path=local_path)
        revision_list = __import__('utils').parse_stock_file(local_path)
        changes = logic.process_revision(revision_list)
    except Exception as e:
        logging.error(f"Ошибка обработки файла ревизии: {e}")
        update.message.reply_text("❌ Не удалось обработать файл ревизии.", reply_markup=cancel_kb)
        return WAITING_REVISION_FILE
    if not changes:
        update.message.reply_text("✅ Ревизия проведена: расхождений нет.", reply_markup=inventory_kb)
    else:
        lines = [f"{name}: {'+' if diff>0 else ''}{diff}" for name, diff in changes]
        update.message.reply_text("✅ Ревизия завершена. Изменения:\n" + "\n".join(lines), reply_markup=inventory_kb)
    return STOCK_MENU

def handle_rollback(update: Update, context: CallbackContext):
    result = logic.rollback_last_action()
    if not result:
        update.message.reply_text("🔄 Нет действий для отката.", reply_markup=inventory_kb)
    else:
        name, opposite = result
        update.message.reply_text(f"🔄 Откат выполнен: {name} {opposite:+} шт.", reply_markup=inventory_kb)
    return STOCK_MENU

def handle_back_to_main(update: Update, context: CallbackContext):
    update.message.reply_text("Возврат в главное меню.", reply_markup=main_menu_kb)
    return MAIN_MENU

# Обработчик поиска товара
def handle_search_query(update: Update, context: CallbackContext):
    query = update.message.text.strip()
    results, total = logic.search_products(query)
    if total == 0:
        update.message.reply_text("❌ Ничего не найдено по вашему запросу.", reply_markup=main_menu_kb)
    else:
        msg = "🔎 *Результаты поиска:*\n"
        for code, name, brand, stock in results:
            msg += f"- {name} ({brand}) – {stock} шт (код {code})\n"
        if total > len(results):
            msg += f"_...и еще {total - len(results)} результатов._\n"
        update.message.reply_text(msg, reply_markup=main_menu_kb, parse_mode='Markdown')
    return MAIN_MENU

# Обработчик отмены /cancel или 'Отмена'
def cancel(update: Update, context: CallbackContext):
    update.message.reply_text("🚫 Действие отменено.", reply_markup=main_menu_kb)
    return MAIN_MENU

# Обработчик подтверждения очистки заказов
def handle_clear_confirm(update: Update, context: CallbackContext):
    text = update.message.text.strip().lower()
    if text in ["да", "yes"]:
        try:
            gdrive.clear_orders()
            update.message.reply_text("✅ Лист 'Заказы' очищен.", reply_markup=main_menu_kb)
        except Exception as e:
            update.message.reply_text("❌ Ошибка при очистке листа 'Заказы'.", reply_markup=main_menu_kb)
    else:
        update.message.reply_text("Отмена очистки заказов.", reply_markup=main_menu_kb)
    return MAIN_MENU

# Ежедневный отчет (функция для JobQueue)
def daily_report(context: CallbackContext):
    chat_id = config.ADMIN_CHAT_ID
    if not chat_id:
        return
    try:
        values = gdrive.history_sheet.get_all_values()
    except Exception as e:
        logging.error(f"Ошибка чтения истории для отчета: {e}")
        return
    if not values or len(values) < 2:
        context.bot.send_message(chat_id, "За последние сутки изменений не было.")
        return
    now = datetime.now()
    day_ago = now.timestamp() - 24*3600
    report_lines = []
    for row in values[1:]:
        if not row or len(row) < 4:
            continue
        date_str, item_str, change_str, reason_str = row[0], row[1], row[2], row[3]
        try:
            entry_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except:
            continue
        if entry_time.timestamp() >= day_ago:
            report_lines.append(f"{entry_time.strftime('%d.%m %H:%M')} – {item_str}: {change_str} ({reason_str})")
    if not report_lines:
        context.bot.send_message(chat_id, "За последние сутки изменений не было.")
    else:
        report_text = "📊 *Отчет за последние 24 часа:*\n" + "\n".join(report_lines)
        context.bot.send_message(chat_id, report_text, parse_mode='Markdown')
