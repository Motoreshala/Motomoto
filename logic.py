import logging
import gdrive
from datetime import datetime

def analyze_orders(orders):
    """Анализирует список заказов и рассчитывает изменения остатков, недостающие товары и т.д."""
    local_stock = gdrive.warehouse_data.copy()
    changes = {}
    missing_tm = {}
    shortages = {}
    for order in orders:
        # Определение кода товара через базу по названию, если код не указан
        if not order.get("code") and order.get("name"):
            name = order["name"]
            found_code = None
            for code, info in gdrive.base_by_code.items():
                if info.get("name") and info["name"].lower() == name.lower():
                    found_code = code; break
            if not found_code:
                for code, info in gdrive.base_by_code.items():
                    if info.get("name") and name.lower() in info["name"].lower():
                        found_code = code; break
            if found_code:
                order["code"] = found_code
            else:
                logging.warning(f"Товар '{name}' не найден в базе, пропускаем.")
                order["shipped_qty"] = 0
                continue
        code = str(order.get("code")).strip() if order.get("code") else None
        qty = order.get("qty", 0)
        if not code or qty <= 0:
            order["shipped_qty"] = 0
            continue
        # Проверка: комплект или одиночный товар
        if code in gdrive.kits_data:
            kit_qty = qty
            kit_fulfillable = True
            for comp_code, comp_need_each in gdrive.kits_data[code]:
                comp_brand = gdrive.base_by_code.get(comp_code, {}).get("brand", "")
                total_needed = comp_need_each * kit_qty
                available = local_stock.get(comp_code, 0)
                if available >= total_needed:
                    local_stock[comp_code] = available - total_needed
                    changes[comp_code] = changes.get(comp_code, 0) - total_needed
                else:
                    total_short = total_needed
                    if available > 0:
                        changes[comp_code] = changes.get(comp_code, 0) - available
                        total_short = total_needed - available
                        local_stock[comp_code] = 0
                    if comp_brand.lower().startswith("техномарин") or comp_brand.lower().startswith("tm") or comp_brand.lower().startswith("тм"):
                        missing_tm[comp_code] = missing_tm.get(comp_code, 0) + total_short
                    else:
                        shortages[comp_code] = shortages.get(comp_code, 0) + total_short
                    kit_fulfillable = False
            order["shipped_qty"] = kit_qty if kit_fulfillable else 0
        else:
            brand = gdrive.base_by_code.get(code, {}).get("brand", "")
            available = local_stock.get(code, 0)
            if available >= qty:
                local_stock[code] = available - qty
                changes[code] = changes.get(code, 0) - qty
                order["shipped_qty"] = qty
            else:
                if available > 0:
                    changes[code] = changes.get(code, 0) - available
                    short = qty - available
                    local_stock[code] = 0
                    order["shipped_qty"] = available
                    if brand.lower().startswith("техномарин") or brand.lower().startswith("tm") or brand.lower().startswith("тм"):
                        missing_tm[code] = missing_tm.get(code, 0) + short
                    else:
                        shortages[code] = shortages.get(code, 0) + short
                else:
                    order["shipped_qty"] = 0
                    short = qty
                    if brand.lower().startswith("техномарин") or brand.lower().startswith("tm") or brand.lower().startswith("тм"):
                        missing_tm[code] = missing_tm.get(code, 0) + short
                    else:
                        shortages[code] = shortages.get(code, 0) + short
    return {"changes": changes, "missing_tm": missing_tm, "shortages": shortages, "orders": orders}

def commit_order_processing(analysis_result, order_tm_confirmed=True):
    """Применяет результаты обработки заказов: обновляет остатки, историю и листы, формирует файлы."""
    changes = analysis_result.get("changes", {})
    orders = analysis_result.get("orders", [])
    missing_tm = analysis_result.get("missing_tm", {})
    # Обновление остатков и истории
    for code, change in changes.items():
        if change == 0:
            continue
        new_qty = gdrive.warehouse_data.get(code, 0) + change
        try:
            gdrive.update_stock(code, new_qty)
        except Exception as e:
            logging.error(f"Ошибка обновления остатков для {code}: {e}")
        name = gdrive.base_by_code.get(code, {}).get("name", "")
        gdrive.append_history(code, name, change, "Отгрузка")
    # Обновление листа "Перемещение"
    move_rows = []
    for code, change in changes.items():
        if change < 0:
            name = gdrive.base_by_code.get(code, {}).get("name", code)
            move_rows.append([name, abs(change)])
    try:
        gdrive.update_movement(move_rows)
    except Exception as e:
        logging.error(f"Ошибка записи в 'Перемещение': {e}")
    # Обновление листа "Заказ ТМ"
    if order_tm_confirmed and missing_tm:
        order_tm_rows = []
        for code, qty in missing_tm.items():
            name = gdrive.base_by_code.get(code, {}).get("name", code)
            brand = gdrive.base_by_code.get(code, {}).get("brand", "")
            order_tm_rows.append([brand, name, qty])
        try:
            gdrive.update_order_tm(order_tm_rows)
        except Exception as e:
            logging.error(f"Ошибка записи в 'Заказ ТМ': {e}")
    else:
        try:
            gdrive.update_order_tm([])  # очистка (оставляем только заголовок)
        except Exception as e:
            logging.error(f"Ошибка очистки 'Заказ ТМ': {e}")
    # Обновление листа "Заказы"
    try:
        for order in orders:
            shipped = order.get("shipped_qty", 0)
            if shipped and shipped > 0:
                code = str(order.get("code")) if order.get("code") else ""
                name = gdrive.base_by_code.get(code, {}).get("name", "") if code else order.get("name", "")
                qty = shipped
                source = order.get("source", "")
                order_id = order.get("order_id", "")
                gdrive.orders_sheet.append_row([source, order_id, name, qty], value_input_option="USER_ENTERED")
        logging.info("Лист 'Заказы' обновлен записями о обработанных заказах.")
    except Exception as e:
        logging.error(f"Ошибка обновления листа 'Заказы': {e}")

def adjust_stock(code, change, reason):
    """Корректирует остаток товара на указанную величину и логирует изменение."""
    code = str(code).strip()
    current = gdrive.warehouse_data.get(code, 0)
    new_qty = current + change
    gdrive.update_stock(code, new_qty)
    name = gdrive.base_by_code.get(code, {}).get("name", "")
    gdrive.append_history(code, name, change, reason)
    logging.info(f"Остаток {code} изменен на {change} ({reason}), новый остаток: {new_qty}")

def process_receiving(deliveries):
    """Обрабатывает приемку товара (список пар код, количество) и возвращает список изменений."""
    changes = []
    for code, qty in deliveries:
        if qty and qty != 0:
            try:
                adjust_stock(code, qty, "Приемка товара")
                name = gdrive.base_by_code.get(str(code).strip(), {}).get("name", str(code))
                changes.append((name, qty))
            except Exception as e:
                logging.error(f"Ошибка приемки для {code}: {e}")
    return changes

def process_revision(revision_list):
    """Обрабатывает результаты ревизии склада и возвращает список изменений."""
    changes = []
    for code, actual in revision_list:
        current = gdrive.warehouse_data.get(str(code).strip(), 0)
        diff = actual - current
        if diff != 0:
            try:
                adjust_stock(code, diff, "Ревизия")
                name = gdrive.base_by_code.get(str(code).strip(), {}).get("name", str(code))
                changes.append((name, diff))
            except Exception as e:
                logging.error(f"Ошибка ревизии для {code}: {e}")
    return changes

def rollback_last_action():
    """Отменяет последнее изменение остатков, возвращает (название, обратное изменение) или None."""
    entry = gdrive.get_last_history_entry()
    if not entry:
        return None
    code, name, change, reason = entry
    if change == 0 or not code:
        return None
    opposite = -change
    try:
        adjust_stock(code, opposite, "Откат")
        logging.info(f"Откат выполнен для последней операции: {code} {opposite:+}")
        return (name, opposite)
    except Exception as e:
        logging.error(f"Ошибка при откате: {e}")
        return None

def search_products(query):
    """Ищет товары по коду, названию или бренду. Возвращает список результатов и общее их число."""
    query_lower = str(query).strip().lower()
    results = []
    for code, info in gdrive.base_by_code.items():
        name = info.get("name", "")
        brand = info.get("brand", "")
        stock = gdrive.warehouse_data.get(code, 0)
        if query_lower in code.lower() or (name and query_lower in name.lower()) or (brand and query_lower in brand.lower()):
            results.append((code, name, brand, stock))
    total = len(results)
    results.sort(key=lambda x: x[1] or "")
    if total > 10:
        results = results[:10]
    return results, total
