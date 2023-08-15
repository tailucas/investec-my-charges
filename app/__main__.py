#!/usr/bin/env python
import logging.handlers

import asyncio
import builtins

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

from pylib import (
    app_config,
    creds,
    log
)

from pylib.aws import boto3_session
from pylib.threads import bye, die
from pylib.zmq import zmq_term

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.cursor import Cursor

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
    accounts,
    cards,
    forget,
    report,
    history,
    start,
    show_profile,
    refresh,
    registration,
    help_command,
    cancel,
    echo,
    telegram_error_handler,
    ACTION_AUTHORIZE,
    ACTION_REFRESH_PROFILE,
    ACTION_SHOW_PROFILE,
    ACTION_FORGET,
    ACTION_NONE,
    ACTION_HISTORY
)


def main():
    log.setLevel(logging.DEBUG)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    md_conn: Optional[MongoClient] = None
    try:
        log.info('Starting local SQLite database...')
        loop.run_until_complete(db_startup())
        # MongoDB cluster
        mongodb_db_name = app_config.get('mongodb', 'db_name')
        mongodb_collection_name = app_config.get('mongodb', 'collection_name')
        log.info(f'Opening MongoDB connection {creds.mongodb_user}@{mongodb_db_name}::{mongodb_collection_name}...')
        mongodb_connection_string = app_config.get('mongodb', 'conn_string')
        db_url = mongodb_connection_string.replace('__USER__', creds.mongodb_user).replace('__PASSWORD__', creds.mongodb_password)
        md_conn = MongoClient(db_url)
        md_db: Database = md_conn[mongodb_db_name]
        md_collection: Collection = md_db[mongodb_collection_name]
        # SQS client
        sqs_queue_name = app_config.get('aws', 'sqs_queue_name')
        log.info(f'Creating SQS client for queue {sqs_queue_name}')
        sqs = boto3_session.client('sqs')
        sqs_queue_url = app_config.get('aws', 'sqs_queue_url')
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            MessageAttributeNames=['All'],
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )
        # Print out the received messages
        if 'Messages' in response.keys():
            for message in response['Messages']:
                message_body = message['Body']
                log.info(f'SQS message says {message_body}')
        log.info('Starting Telegram Bot...')
        """Start the bot."""
        # Create the Application and pass it your bot's token.
        application = Application.builder().token(creds.telegram_bot_api_token).build()
        application.bot_data['mongodb_collection'] = md_collection
        #application.bot_data["custom"] = None
        # bot commands
        command_handlers = [
            CommandHandler("accounts", accounts),
            CommandHandler("cards", cards),
            CommandHandler("report", report),
            CommandHandler("start", start),
            CommandHandler("help", help_command),
            CallbackQueryHandler(callback=forget, pattern="^" + str(ACTION_FORGET) + "$"),
            CallbackQueryHandler(callback=show_profile, pattern="^" + str(ACTION_SHOW_PROFILE) + "$"),
            CallbackQueryHandler(callback=history, pattern=f'^{ACTION_HISTORY}.*$'),
            CallbackQueryHandler(callback=refresh, pattern="^" + str(ACTION_REFRESH_PROFILE) + "$"),
            CallbackQueryHandler(callback=registration, pattern="^" + str(ACTION_AUTHORIZE) + "$"),
            CallbackQueryHandler(callback=cancel, pattern="^" + str(ACTION_NONE) + "$"),
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