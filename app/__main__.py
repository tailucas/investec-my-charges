#!/usr/bin/env python
import logging.handlers

import asyncio
import boto3
import builtins

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
    mongodb_user: f'opitem:"MongoDB" opfield:{APP_NAME}.username' = None # type: ignore
    mongodb_password: f'opitem:"MongoDB" opfield:{APP_NAME}.password' = None # type: ignore


# instantiate class
builtins.creds_config = CredsConfig()

from pylib import (
    app_config,
    creds,
    log
)

from pylib.threads import bye, die
from pylib.zmq import zmq_term

from pymongo import MongoClient

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters
)

from .influx import influxdb

from .database import (
    db_startup,
)

from .bot import (
    echo,
    telegram_error_handler
)


ACTION_NONE = 0


def main():
    log.setLevel(logging.INFO)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    md_conn = None
    try:
        # Connecting to MongoDB cluster
        mongodb_db_name = app_config.get('mongodb', 'db_name')
        mongodb_collection_name = app_config.get('mongodb', 'cluster_name')
        log.debug(f'Opening MongoDB connection {creds.mongodb_user}@{mongodb_db_name}::{mongodb_collection_name}...')
        mongodb_connection_string = app_config.get('mongodb', 'conn_string')
        db_url = mongodb_connection_string.replace('__USER__', creds.mongodb_user).replace('__PASSWORD__', creds.mongodb_password)
        md_conn = MongoClient(db_url)
        md_db = md_conn[mongodb_db_name]
        md_collection = md_db[mongodb_collection_name]
        query = {}
        projection = {}
        sort = []
        cursor = md_collection.find(query, projection=projection, sort=sort)
        for doc in cursor:
            log.info(f'{doc!s}')

        sqs_queue_name = app_config.get('aws', 'sqs_queue_name')
        log.info(f'Creating SQS client for queue {sqs_queue_name}')
        sqs = boto3.resource('sqs')
        sqs_queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
        for sqs_message in sqs_queue.receive_messages(WaitTimeSeconds=20):
            # forward for further processing
            message_body = sqs_message.body
            log.info(f'SQS message says {message_body}')

        log.info('Starting local SQLite database...')
        loop.run_until_complete(db_startup())
        log.info('Starting Telegram Bot...')
        """Start the bot."""
        # Create the Application and pass it your bot's token.
        application = Application.builder().token(creds.telegram_bot_api_token).build()
        #application.bot_data["custom"] = None
        # bot commands
        command_handlers = [
        ]
        for handler in command_handlers:
            application.add_handler(handler)
        # on non command i.e message - echo the message on Telegram
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
        # error handling
        application.add_error_handler(callback=telegram_error_handler)
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