#!/usr/bin/env python
import logging.handlers

import asyncio
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


# instantiate class
builtins.creds_config = CredsConfig()

from pylib import (
    app_config,
    creds,
    log
)

from pylib.threads import bye, die
from pylib.zmq import zmq_term

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
    try:
        loop.run_until_complete(db_startup())
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
        zmq_term()
        loop.close()
    bye()


if __name__ == "__main__":
    main()