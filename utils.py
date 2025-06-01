import logging
import pandas as pd
from pandas.errors import EmptyDataError

def read_table_file(file_path):
    """Считывает таблицу из Excel/CSV файла в DataFrame pandas."""
    try:
        if file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path, engine='openpyxl')
        elif file_path.lower().endswith(('.csv', '.txt')):
            try:
                df = pd.read_csv(file_path, sep=';', engine='python')
                if df.shape[1] <= 1:
                    df = pd.read_csv(file_path, sep=',', engine='python')
            except EmptyDataError:
                df = pd.DataFrame()
        else:
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
            except Exception:
                try:
                    df = pd.read_csv(file_path, sep=';', engine='python')
                except Exception:
                    df = pd.read_csv(file_path, sep=',', engine='python')
        if not df.empty:
            df.dropna(how='all', inplace=True)
        return df
    except Exception as e:
        logging.error(f"Ошибка чтения файла {file_path}: {e}")
        raise

def parse_orders_file(file_path, source_name=""):
    """Парсит файл заказов (Wildberries или Ozon) и возвращает список словарей заказов."""
    df = read_table_file(file_path)
    if df is None or df.empty:
        logging.warning(f"Файл заказов {file_path} пустой или не содержит данных.")
        return []
    cols = [str(c) for c in df.columns]
    code_col = None
    name_col = None
    qty_col = None
    orderid_col = None
    for col in cols:
        cl = col.lower()
        if 'артикул' in cl:
            code_col = col; break
    if code_col is None:
        for col in cols:
            cl = col.lower()
            if 'код' in cl and 'заказ' not in cl:
                code_col = col; break
    for col in cols:
        cl = col.lower()
        if 'кол' in cl or 'quantity' in cl or 'qty' in cl:
            qty_col = col; break
    for col in cols:
        cl = col.lower()
        if 'наимен' in cl or 'назв' in cl or 'товар' in cl:
            if code_col and col == code_col:
                continue
            name_col = col; break
    for col in cols:
        cl = col.lower()
        if 'заказ' in cl or 'order' in cl:
            if 'дата' in cl:
                continue
            orderid_col = col; break
    if not qty_col:
        logging.error(f"В файле {file_path} не найдена колонка количества.")
        raise Exception("Не удалось определить колонку количества.")
    orders = []
    for _, row in df.iterrows():
        code_val = None
        name_val = None
        qty_val = None
        order_id_val = None
        if code_col:
            code_val = str(row[code_col]).strip() if not pd.isna(row[code_col]) else None
        if name_col:
            name_val = str(row[name_col]).strip() if not pd.isna(row[name_col]) else None
        else:
            name_val = None
        try:
            qty_val = int(row[qty_col]) if not pd.isna(row[qty_col]) else 0
        except Exception:
            try:
                qty_val = int(float(row[qty_col])) if not pd.isna(row[qty_col]) else 0
            except Exception:
                qty_val = 0
        if orderid_col:
            order_id_val = str(row[orderid_col]).strip() if not pd.isna(row[orderid_col]) else ""
        if qty_val is None or qty_val == 0:
            continue
        order = {
            "code": code_val if code_val else None,
            "name": name_val if name_val else None,
            "qty": int(qty_val),
            "source": source_name,
            "order_id": order_id_val if order_id_val else ""
        }
        orders.append(order)
    logging.info(f"Из файла {file_path} ({source_name}) получено заказов: {len(orders)}")
    return orders

def parse_stock_file(file_path):
    """Парсит файл (приемка или ревизия) с колонками код и количество. Возвращает список пар (код, количество)."""
    df = read_table_file(file_path)
    if df is None or df.empty:
        logging.warning(f"Файл {file_path} пустой или не содержит данных.")
        return []
    cols = [str(c) for c in df.columns]
    code_col = None
    qty_col = None
    for col in cols:
        cl = col.lower()
        if 'артикул' in cl or 'код' in cl or 'sku' in cl or 'товар' in cl:
            code_col = col; break
    for col in cols:
        cl = col.lower()
        if 'кол' in cl or 'qty' in cl or 'колич' in cl or 'count' in cl:
            qty_col = col; break
    if not code_col or not qty_col:
        logging.error(f"Не удалось найти код или количество в файле {file_path}")
        raise Exception("Неизвестный формат файла остатков.")
    data = {}
    for _, row in df.iterrows():
        code_val = None
        if not pd.isna(row[code_col]):
            code_val = str(row[code_col]).strip()
        if not code_val:
            continue
        try:
            qty_val = int(row[qty_col]) if not pd.isna(row[qty_col]) else 0
        except:
            try:
                qty_val = int(float(row[qty_col])) if not pd.isna(row[qty_col]) else 0
            except:
                qty_val = 0
        data[code_val] = data.get(code_val, 0) + (qty_val or 0)
    result = [(code, qty) for code, qty in data.items()]
    logging.info(f"Из файла {file_path} получено записей: {len(result)}")
    return result
