#!/usr/bin/env python
import logging.handlers

import asyncio
import builtins
import locale
import os

from typing import Optional

# setup builtins used by pylib init
from . import APP_NAME
builtins.SENTRY_EXTRAS = []
influx_creds_section = 'local'

class CredsConfig:
    sentry_dsn: f'opitem:"Sentry" opfield:{APP_NAME}.dsn' = None  # type: ignore
    cronitor_token: f'opitem:"cronitor" opfield:.password' = None  # type: ignore
    telegram_bot_api_token: f'opitem:"Telegram" opfield:{APP_NAME}.token' = None # type: ignore
    aes_sym_key: f'opitem:"AES.{APP_NAME}" opfield:.password' = None # type: ignore
    influxdb_org: f'opitem:"InfluxDB" opfield:{influx_creds_section}.org' = None # type: ignore
    influxdb_token: f'opitem:"InfluxDB" opfield:{APP_NAME}.token' = None # type: ignore
    influxdb_url: f'opitem:"InfluxDB" opfield:{influx_creds_section}.url' = None # type: ignore
    mongodb_user: f'opitem:"MongoDB" opfield:{APP_NAME}.user' = None # type: ignore
    mongodb_password: f'opitem:"MongoDB" opfield:{APP_NAME}.pwd' = None # type: ignore
    aws_akid: f'opitem:"AWS.{APP_NAME}" opfield:.username' = None # type: ignore
    aws_sak: f'opitem:"AWS.{APP_NAME}" opfield:.password' = None # type: ignore
    investec_client_id: f'opitem:"Investec" opfield:.client_id' = None # type: ignore
    investec_secret: f'opitem:"Investec" opfield:.secret' = None # type: ignore
    investec_apikey: f'opitem:"Investec" opfield:.api_key' = None # type: ignore

# instantiate class
builtins.creds_config = CredsConfig()

from sentry_sdk.integrations.logging import ignore_logger

from tailucas_pylib import (
    app_config,
    creds,
    log
)

from tailucas_pylib.threads import bye, die
from tailucas_pylib.zmq import zmq_term

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.cursor import Cursor

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ConversationHandler,
    CommandHandler,
    MessageHandler,
    TypeHandler,
    filters
)

from .influx import influxdb

from .database import (
    db_startup,
)

from .bot import (
    accounts,
    cards,
    forget,
    history,
    account_history,
    card_report,
    card_report_interval,
    start,
    settings,
    update_settings,
    askpayday,
    askbillcycleday,
    show_profile,
    refresh,
    registration,
    help_command,
    cancel,
    echo,
    telegram_error_handler,
    transaction_update,
    ACTION_AUTHORIZE,
    ACTION_REFRESH_PROFILE,
    ACTION_SHOW_PROFILE,
    ACTION_FORGET,
    ACTION_NONE,
    ACTION_SETTINGS,
    ACTION_CARD_REPORT,
    ACTION_ACCOUNT_HISTORY,
    ACTION_SETTINGS_ACCOUNT_DATE,
    ACTION_SETTINGS_CARD_DATE,
    ACTION_SETTINGS_PAY_DAY,
    ACTION_SETTINGS_BILL_CYCLE_DAY,
    ACTION_SETTINGS_BILL_CYCLE_DAY_ASK,
    ACTION_SETTINGS_UPDATE,
    ACTION_CARD_REPORT_INTERVAL,
)

from .currency import CurrencyConverter
from .event import TransactionUpdate, SQSEvent
from .transaction import TransactionHistory

# Reduce Sentry noise
ignore_logger('telegram.ext.Updater')
ignore_logger('telegram.ext._updater')


def main():
    log.setLevel(logging.DEBUG)
    if app_config.getboolean('app', 'demo_mode'):
        log.warning('Demo mode enabled! Most responses will be garbage.')
    locale.setlocale(locale.LC_ALL, os.environ['LC_ALL'])
    conv=locale.localeconv()
    int_curr_symbol = str(conv['int_curr_symbol']).rstrip()
    currency_symbol = str(conv['currency_symbol'])
    if int_curr_symbol is None or len(int_curr_symbol) == 0:
        raise AssertionError('Locale settings are incomplete. Local currency configuration is not available.')
    log.info(f'Locale is {locale.getlocale()} using currency symbols [{int_curr_symbol}] => [{currency_symbol}]')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    md_conn: Optional[MongoClient] = None
    try:
        # Application threads
        currency_converter = CurrencyConverter(
            int_curr_symbol=int_curr_symbol,
            currency_symbol=currency_symbol)
        currency_converter.start()
        log.info('Starting local SQLite database...')
        loop.run_until_complete(db_startup())
        # MongoDB cluster
        mongodb_db_name = app_config.get('mongodb', 'db_name')
        log.info(f'Opening MongoDB connection {creds.mongodb_user}@{mongodb_db_name}...')
        mongodb_connection_string = app_config.get('mongodb', 'conn_string')
        db_url = mongodb_connection_string.replace('__USER__', creds.mongodb_user).replace('__PASSWORD__', creds.mongodb_password)
        md_conn = MongoClient(db_url)
        md_db: Database = md_conn[mongodb_db_name]
        mongodb_card_collection_name = app_config.get('mongodb', 'card_collection_name')
        log.info(f'Opening MongoDB connection {mongodb_card_collection_name}...')
        md_card_collection: Collection = md_db[mongodb_card_collection_name]
        # account history thread
        mongodb_account_collection_name = app_config.get('mongodb', 'account_collection_name')
        log.info(f'Opening MongoDB connection {mongodb_account_collection_name}...')
        md_account_collection: Collection = md_db[mongodb_account_collection_name]
        log.info('Starting transaction history synchronizer...')
        transaction_history = TransactionHistory(
            mongodb_collection=md_account_collection,
            sync_interval=app_config.getint('app', 'transaction_history_refresh_interval_secs'))
        transaction_history.start()
        log.info('Starting Telegram Bot...')
        """Start the bot."""
        # Create the Application and pass it your bot's token.
        application = Application.builder().token(creds.telegram_bot_api_token).build()
        application.bot_data['mongodb_card_collection'] = md_card_collection
        application.bot_data['mongodb_account_collection'] = md_account_collection
        #application.bot_data["custom"] = None
        # bot commands
        command_handlers = [
            CommandHandler("accounts", accounts),
            CommandHandler("cards", cards),
            CommandHandler("history", history),
            CommandHandler("help", help_command),
            CommandHandler("start", start),
            CallbackQueryHandler(callback=forget, pattern="^" + str(ACTION_FORGET) + "$"),
            CallbackQueryHandler(callback=show_profile, pattern="^" + str(ACTION_SHOW_PROFILE) + "$"),
            CallbackQueryHandler(callback=account_history, pattern=f'^{ACTION_ACCOUNT_HISTORY}.*$'),
            CallbackQueryHandler(callback=card_report, pattern=f'^{ACTION_CARD_REPORT}.*$'),
            CallbackQueryHandler(callback=card_report_interval, pattern=f'^{ACTION_CARD_REPORT_INTERVAL}.*$'),
            CallbackQueryHandler(callback=refresh, pattern="^" + str(ACTION_REFRESH_PROFILE) + "$"),
            CallbackQueryHandler(callback=registration, pattern="^" + str(ACTION_AUTHORIZE) + "$"),
            CallbackQueryHandler(callback=askpayday, pattern="^" + str(ACTION_SETTINGS_PAY_DAY) + "$"),
            CallbackQueryHandler(callback=askbillcycleday, pattern="^" + str(ACTION_SETTINGS_BILL_CYCLE_DAY) + "$"),
            CallbackQueryHandler(callback=cancel, pattern=f'^{ACTION_NONE}.*$'),
        ]
        settings_handler = ConversationHandler(
            allow_reentry=True,
            entry_points=[CommandHandler("settings", settings)],
            states={
                ACTION_SETTINGS_UPDATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, update_settings)],
            },
            fallbacks=command_handlers
        )
        application.add_handler(settings_handler)
        for handler in command_handlers:
            application.add_handler(handler)
        # on non command i.e message - echo the message on Telegram
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
        # custom event triggers
        application.add_handler(TypeHandler(type=TransactionUpdate, callback=transaction_update))
        # error handling
        application.add_error_handler(callback=telegram_error_handler)
        # transaction events
        sqs_events = SQSEvent(application=application)
        sqs_events.start()
        influxdb.write('app', 'startup', 1)
        log.info('Starting Telegram Bot...')
        application.run_polling()
        log.info('Shutting down...')
    finally:
        die()
        if md_conn:
            md_conn.close()
        zmq_term()
        loop.close()
    bye()


if __name__ == "__main__":
    main()