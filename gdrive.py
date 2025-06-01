import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import config
from datetime import datetime

# Глобальные объекты Google Sheets
client = None
workbook = None
base_sheet = None
warehouse_sheet = None
order_tm_sheet = None
movement_sheet = None
orders_sheet = None
history_sheet = None
kits_sheet = None

# Локальные данные для быстрого доступа
base_by_code = {}      # словарь: код товара -> {name, brand}
warehouse_data = {}    # словарь: код товара -> текущее количество
warehouse_rows = {}    # словарь: код товара -> номер строки на листе склада
kits_data = {}         # словарь: код комплекта -> список компонентов (код, количество)

def connect():
    """Подключается к Google Sheets и загружает данные из всех необходимых листов."""
    global client, workbook, base_sheet, warehouse_sheet, order_tm_sheet
    global movement_sheet, orders_sheet, history_sheet, kits_sheet
    global base_by_code, warehouse_data, warehouse_rows, kits_data
    logging.info("Подключение к Google Sheets...")
    # Авторизация через файл учетных данных сервисного аккаунта
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(config.CREDENTIALS_FILE, scope)
    client = gspread.authorize(credentials)
    # Открытие таблицы по ID или имени
    workbook = client.open_by_key(config.GSHEET_ID) if config.GSHEET_ID else client.open(config.GSHEET_NAME)
    logging.info(f"Таблица Google '{workbook.title}' успешно открыта.")
    # Получение объектов рабочих листов по названиям
    base_sheet = workbook.worksheet(config.BASE_SHEET_NAME)
    warehouse_sheet = workbook.worksheet(config.WAREHOUSE_SHEET_NAME)
    order_tm_sheet = workbook.worksheet(config.ORDER_TM_SHEET_NAME)
    movement_sheet = workbook.worksheet(config.MOVEMENT_SHEET_NAME)
    orders_sheet = workbook.worksheet(config.ORDERS_SHEET_NAME)
    history_sheet = workbook.worksheet(config.HISTORY_SHEET_NAME)
    kits_sheet = workbook.worksheet(config.KITS_SHEET_NAME)
    logging.info("Все вкладки Google Sheets успешно доступны.")
    # Загрузка данных листа "База"
    try:
        records = base_sheet.get_all_records()
        base_by_code = {}
        for rec in records:
            code = rec.get("Артикул") or rec.get("Артикул поставщика") or rec.get("Код") or rec.get("SKU")
            name = rec.get("Наименование") or rec.get("Название") or rec.get("Товар")
            brand = rec.get("Бренд") or rec.get("Производитель") or rec.get("Бренд товара")
            if code:
                base_by_code[str(code).strip()] = {
                    "name": str(name).strip() if name else "",
                    "brand": str(brand).strip() if brand else ""
                }
        logging.info(f"Загружено записей из 'База': {len(base_by_code)}")
    except Exception as e:
        logging.error(f"Не удалось загрузить 'База': {e}")
        base_by_code = {}
    # Загрузка данных листа "Наш склад"
    try:
        records = warehouse_sheet.get_all_records()
        warehouse_data = {}
        warehouse_rows = {}
        row_num = 2  # предполагается, что строка 1 - заголовки
        for rec in records:
            code = rec.get("Артикул") or rec.get("Код") or rec.get("Артикул поставщика") or rec.get("Товар")
            qty = rec.get("Количество") or rec.get("Остаток") or rec.get("Кол-во")
            if not code:
                continue
            code = str(code).strip()
            try:
                qty_val = int(qty)
            except:
                try:
                    qty_val = int(float(qty))
                except:
                    qty_val = 0
            warehouse_data[code] = qty_val
            warehouse_rows[code] = row_num
            row_num += 1
        logging.info(f"Загружено позиций склада: {len(warehouse_data)}")
    except Exception as e:
        logging.error(f"Не удалось загрузить 'Наш склад': {e}")
        warehouse_data = {}
        warehouse_rows = {}
    # Загрузка данных листа "Комплекты"
    try:
        values = kits_sheet.get_all_values()
        kits_data = {}
        if values:
            start_idx = 1  # пропуск заголовка, если есть
            if values[0] and any("комплект" in str(x).lower() for x in values[0]):
                start_idx = 1
            else:
                start_idx = 0
            for row in values[start_idx:]:
                if not row or not row[0]:
                    continue
                kit_code = str(row[0]).strip()
                components = []
                j = 2
                while j < len(row):
                    comp_code = str(row[j]).strip() if j < len(row) and row[j] else ""
                    comp_qty = str(row[j+1]).strip() if j+1 < len(row) and row[j+1] else ""
                    if comp_code == "":
                        break
                    try:
                        comp_qty_val = int(float(comp_qty)) if comp_qty != "" else 0
                    except:
                        comp_qty_val = 0
                    components.append((comp_code, comp_qty_val if comp_qty_val != 0 else 1))
                    j += 2
                kits_data[kit_code] = components
        logging.info(f"Загружено комплектов: {len(kits_data)}")
    except Exception as e:
        logging.error(f"Не удалось загрузить 'Комплекты': {e}")
        kits_data = {}

def update_stock(code, new_qty):
    """Обновляет остаток товара (по коду) в листе 'Наш склад' до значения new_qty."""
    code = str(code).strip()
    if code in warehouse_rows:
        row = warehouse_rows[code]
        try:
            warehouse_sheet.update_cell(row, config.WAREHOUSE_QTY_COL, new_qty)
        except Exception as e:
            logging.error(f"Ошибка обновления остатка для {code}: {e}")
            raise
    else:
        try:
            name = base_by_code.get(code, {}).get("name", "")
            new_row = [code, name, new_qty]
            warehouse_sheet.append_row(new_row, value_input_option="USER_ENTERED")
            # Обновляем локальные данные
            row_count = len(warehouse_sheet.get_all_values())
            warehouse_rows[code] = row_count
        except Exception as e:
            logging.error(f"Ошибка добавления нового товара {code} в склад: {e}")
            raise
    warehouse_data[code] = new_qty
    logging.info(f"Обновлен остаток товара {code}: {new_qty}")

def append_history(item_code, item_name, change, reason):
    """Добавляет запись о изменении остатка в лист 'История остатков'."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item_display = f"{item_name} ({item_code})" if item_name else str(item_code)
        change_str = f"{change:+}"
        history_sheet.append_row([timestamp, item_display, change_str, reason], value_input_option="USER_ENTERED")
        logging.info(f"История обновлена: {item_display} {change_str} ({reason})")
    except Exception as e:
        logging.error(f"Ошибка записи в историю: {e}")

def clear_orders():
    """Очищает лист 'Заказы', сохраняя строку заголовков."""
    try:
        values = orders_sheet.get_all_values()
        header = values[0] if values else []
        orders_sheet.clear()
        if header:
            orders_sheet.append_row(header, value_input_option="USER_ENTERED")
        logging.info("Лист 'Заказы' очищен пользователем.")
    except Exception as e:
        logging.error(f"Ошибка очистки листа 'Заказы': {e}")
        raise

def update_movement(data_rows):
    """Очищает лист 'Перемещение' и записывает новые данные."""
    try:
        values = movement_sheet.get_all_values()
        header = values[0] if values else []
        movement_sheet.clear()
        if header:
            movement_sheet.append_row(header, value_input_option="USER_ENTERED")
        else:
            header = ["Товар", "Количество к перемещению"]
            movement_sheet.append_row(header, value_input_option="USER_ENTERED")
        for row in data_rows:
            movement_sheet.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"Обновлен лист 'Перемещение' ({len(data_rows)} строк).")
    except Exception as e:
        logging.error(f"Ошибка обновления 'Перемещение': {e}")
        raise

def update_order_tm(data_rows):
    """Очищает лист 'Заказ ТМ' и записывает новые данные."""
    try:
        values = order_tm_sheet.get_all_values()
        header = values[0] if values else []
        order_tm_sheet.clear()
        if header:
            order_tm_sheet.append_row(header, value_input_option="USER_ENTERED")
        else:
            header = ["Бренд", "Товар", "Количество к заказу"]
            order_tm_sheet.append_row(header, value_input_option="USER_ENTERED")
        for row in data_rows:
            order_tm_sheet.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"Обновлен лист 'Заказ ТМ' ({len(data_rows)} строк).")
    except Exception as e:
        logging.error(f"Ошибка обновления 'Заказ ТМ': {e}")
        raise

def get_last_history_entry():
    """Возвращает последнюю запись из листа 'История остатков' (код, название, изменение, причина) или None."""
    try:
        values = history_sheet.get_all_values()
        if not values or len(values) < 2:
            return None
        last = values[-1]
        if len(last) < 4:
            return None
        item_field = last[1]
        change_field = last[2]
        reason_field = last[3]
        code = item_field
        name = item_field
        if "(" in item_field and item_field.endswith(")"):
            try:
                code = item_field[item_field.rfind("(")+1:-1]
                name = item_field[:item_field.rfind("(")].strip()
            except:
                code = item_field
                name = item_field
        try:
            change = int(change_field.replace("+", ""))
        except:
            change = 0
        reason = reason_field
        return code.strip(), name.strip(), change, reason.strip()
    except Exception as e:
        logging.error(f"Ошибка чтения последней записи истории: {e}")
        return None
