import emoji
import string
import urllib

from sentry_sdk.integrations.logging import ignore_logger

from pylib import (
    app_config,
    creds,
    log,
    threads
)

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    User as TelegramUser,
    ChatMember as TelegramChatMember,
)
from telegram.constants import (
    ParseMode,
    ChatAction,
    ChatType
)
from telegram.ext import (
    ContextTypes,
    ConversationHandler
)

# Reduce Sentry noise
ignore_logger('telegram.ext._updater')

ACTION_NONE = 0

from .influx import influxdb

from .database import (
    User,
    get_user_registration
)


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.info(f'Incoming message from Telegram user ID {update.effective_user.id}.')
    await update.message.reply_text(update.message.text)
    return ConversationHandler.END


async def telegram_error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.warning(msg="Bot error:", exc_info=context.error)
    return ConversationHandler.END
