from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler, PicklePersistence
import logging
import config
import handlers
import gdrive
from datetime import time as dtime

def main():
    # Подключение к Google Sheets и загрузка данных
    try:
        gdrive.connect()
    except Exception as e:
        logging.error(f"Ошибка при подключении к Google Sheets: {e}")
    # Настройка persistence (сохранение состояния бота на диск)
    persistence = PicklePersistence(filename="bot_state.pkl")
    updater = Updater(token=config.BOT_TOKEN, persistence=persistence, use_context=True)
    dp = updater.dispatcher
    # Определение ConversationHandler с состояниями и обработчиками
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", handlers.start)],
        states={
            handlers.MAIN_MENU: [
                MessageHandler(Filters.regex("^Обработка$"), handlers.handle_processing),
                MessageHandler(Filters.regex("^Остатки$"), handlers.handle_inventory),
                MessageHandler(Filters.regex("^Поиск$"), handlers.handle_search),
                MessageHandler(Filters.regex("^Помощь$"), handlers.handle_help),
                MessageHandler(Filters.regex("^Очистить заказы$"), handlers.handle_clear_orders)
            ],
            handlers.PROCESSING_WB: [
                MessageHandler(Filters.document, handlers.handle_wb_file),
                MessageHandler(Filters.regex("^(нет|Нет)$"), handlers.skip_wb_file)
            ],
            handlers.PROCESSING_OZ: [
                MessageHandler(Filters.document, handlers.handle_oz_file),
                MessageHandler(Filters.regex("^(нет|Нет)$"), handlers.skip_oz_file)
            ],
            handlers.PROCESSING_CONFIRM: [
                MessageHandler(Filters.regex("^(Да|да|Нет|нет)$"), handlers.handle_process_confirm)
            ],
            handlers.STOCK_MENU: [
                MessageHandler(Filters.regex("^Вычесть$"), handlers.start_subtract),
                MessageHandler(Filters.regex("^Добавить$"), handlers.start_add),
                MessageHandler(Filters.regex("^Приемка$"), handlers.start_receiving),
                MessageHandler(Filters.regex("^Ревизия$"), handlers.start_revision),
                MessageHandler(Filters.regex("^Откат$"), handlers.handle_rollback),
                MessageHandler(Filters.regex("^Назад$"), handlers.handle_back_to_main)
            ],
            handlers.WAITING_SUBTRACT_ITEM: [
                MessageHandler(Filters.text & ~Filters.command, handlers.handle_subtract_item)
            ],
            handlers.WAITING_SUBTRACT_QTY: [
                MessageHandler(Filters.text & ~Filters.command, handlers.handle_subtract_qty)
            ],
            handlers.WAITING_ADD_ITEM: [
                MessageHandler(Filters.text & ~Filters.command, handlers.handle_add_item)
            ],
            handlers.WAITING_ADD_QTY: [
                MessageHandler(Filters.text & ~Filters.command, handlers.handle_add_qty)
            ],
            handlers.WAITING_RECEIVE_FILE: [
                MessageHandler(Filters.document, handlers.handle_receive_file)
            ],
            handlers.WAITING_REVISION_FILE: [
                MessageHandler(Filters.document, handlers.handle_revision_file)
            ],
            handlers.CLEAR_CONFIRM: [
                MessageHandler(Filters.regex("^(Да|да|Yes|yes)$"), handlers.handle_clear_confirm),
                MessageHandler(Filters.regex("^(Нет|нет|No|no)$"), handlers.handle_clear_confirm)
            ],
            handlers.WAITING_SEARCH_QUERY: [
                MessageHandler(Filters.text & ~Filters.command, handlers.handle_search_query)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", handlers.cancel),
            MessageHandler(Filters.regex("^Отмена$"), handlers.cancel)
        ],
        allow_reentry=True,
        persistent=True,
        name="conversation"
    )
    dp.add_handler(conv_handler)
    # Планирование ежедневного отчета в 9:00 (если задан ADMIN_CHAT_ID)
    if config.ADMIN_CHAT_ID:
        updater.job_queue.run_daily(handlers.daily_report, time=dtime(hour=9, minute=0), context=config.ADMIN_CHAT_ID)
    # Запуск бота
    updater.start_polling()
    logging.info("Бот запущен и готов к работе.")
    updater.idle()

if __name__ == "__main__":
    main()
