import logging
import os

# Настройки и конфигурационные параметры бота

# Токен Telegram-бота (необходимо заменить на реальный токен)
BOT_TOKEN = os.getenv("BOT_TOKEN", "<TELEGRAM_BOT_TOKEN_HERE>")

# Учетные данные Google API (JSON-файл сервисного аккаунта)
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS", "credentials.json")

# Имя или ID таблицы Google Sheets, используемой ботом
GSHEET_NAME = os.getenv("GSHEET_NAME", "WarehouseData")  # Название таблицы
GSHEET_ID = os.getenv("GSHEET_ID", "")                   # ID таблицы (если указан, имеет приоритет над именем)

# Названия рабочих листов (вкладок) в Google Sheets
BASE_SHEET_NAME = "База"
WAREHOUSE_SHEET_NAME = "Наш склад"
ORDER_TM_SHEET_NAME = "Заказ ТМ"
MOVEMENT_SHEET_NAME = "Перемещение"
ORDERS_SHEET_NAME = "Заказы"
HISTORY_SHEET_NAME = "История остатков"
KITS_SHEET_NAME = "Комплекты"

# Индексы колонок для листа "Наш склад" (1-индексированные для gspread)
# Предполагается структура: Код товара, Название, Количество
WAREHOUSE_CODE_COL = 1
WAREHOUSE_NAME_COL = 2
WAREHOUSE_QTY_COL = 3

# ID чата для ежедневного отчета (например, ID администратора или группы)
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# Настройка логирования: в файл bot.log и вывод в консоль
LOG_FILE = "bot.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logging.info("Конфигурация загружена. Логирование успешно настроено.")
