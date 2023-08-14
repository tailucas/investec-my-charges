import emoji
import string
import urllib

import simplejson as json

from typing import Optional, Tuple, Sequence

from pymongo.collection import Collection
from pymongo.cursor import Cursor

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

from investec_api_python import InvestecOpenApiClient

# Reduce Sentry noise
ignore_logger('telegram.ext._updater')

ACTION_SETTINGS_PREFIX = "settings"

ACTION_NONE = 0
ACTION_AUTHORIZE = 2
ACTION_REFRESH_PROFILE = 3
ACTION_SHOW_PROFILE = 4

DEFAULT_TAG_UNTAGGED = '_untagged_'

from .influx import influxdb

from .database import (
    Account,
    Card,
    User,
    get_access_token,
    update_access_token,
    get_user,
    add_user,
    get_accounts,
    add_accounts,
    get_cards,
    add_cards
)


async def validate(command_name: str, update: Update, validate_registration=True) -> Optional[User]:
    user: TelegramUser = update.effective_user
    if user.is_bot:
        log.warning(f'{command_name}: ignoring bot user {user.id}.')
        return None
    allowed_users = app_config.get('telegram', 'enabled_users_csv').split(',')
    if str(user.id) not in allowed_users:
        log.warning(f'{command_name}: ignoring user {user.id} not in allowlist.')
        return None
    else:
        log.debug(f'Telegram user {user.id} is in the allow-list: {allowed_users}')
    log.info(f'{command_name}: Telegram user ID {user.id} (language {user.language_code}).')
    influxdb.write('command', f'{command_name}', 1)
    db_user: Optional[User] = None
    if validate_registration:
        db_user = await get_user(telegram_user_id=user.id)
        if db_user is None:
            log.info(f'No database registration found for Telegram user ID {user.id}.')
            if update.message is None:
                log.warning(f'Cannot update null message from Telegram user ID {user.id} with no update message context.')
                return None
            user_response = rf'{emoji.emojize(":passport_control:")} {user.first_name}, authorization with your bank is needed.'
            user_keyboard = [
                [
                    InlineKeyboardButton("Authorize", callback_data=str(ACTION_AUTHORIZE)),
                    InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
                ]
            ]
            reply_markup = InlineKeyboardMarkup(user_keyboard)
            await update.message.reply_html(
                text=user_response,
                reply_markup=reply_markup
            )
            return None
    return db_user


async def accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_user: User = await validate(command_name='accounts', update=update)
    if db_user is None:
        return
    user: TelegramUser = update.effective_user
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)

    telegram_user = app_config.get('telegram', 'enabled_users_csv')
    if str(user.id) == telegram_user:
        access_token: Optional[Tuple] = await get_access_token(telegram_user_id=user.id, user_id=db_user.id)
        creds = json.loads(db_user.investec_credentials)
        client = InvestecOpenApiClient(
            client_id=db_user.investec_client_id,
            secret=creds['secret'],
            api_key=creds['api_key'],
            additional_headers={'Accept-Encoding': 'gzip, deflate, br'},
            access_token=access_token)
        if access_token is None or client.access_token != access_token[0]:
            log.debug(f'Persisting access token...')
            await update_access_token(
                telegram_user_id=user.id,
                user_id=db_user.id,
                access_token=client.access_token,
                access_token_expiry=client.access_token_expiry)

        log.debug(f'Fetching Investec accounts...')
        response = client.get_accounts()
        await add_accounts(
            telegram_user_id=user.id,
            user_id=db_user.id,
            account_info=response)
        log.debug(f'Accounts response: {response!s}')
        message = ''
        for account in response:
            account_number = account['accountNumber']
            account_name = account['accountName']
            message += f'{account_name}: {account_number}'
        await update.message.reply_text(
            text=message,
            parse_mode=ParseMode.HTML
        )
    return ConversationHandler.END


async def cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_user: User = await validate(command_name='cards', update=update)
    if db_user is None:
        return
    user: TelegramUser = update.effective_user
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)



    await update.message.reply_text(
        text='hi',
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END


async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_user: User = await validate(command_name='report', update=update)
    if db_user is None:
        return
    user: TelegramUser = update.effective_user
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)

    telegram_user = app_config.get('telegram', 'enabled_users_csv')
    if str(user.id) == telegram_user:
        query = {}
        projection = {}
        sort = []
        md_collection: Collection = context.bot_data['mongodb_collection']
        log.debug(f'Fetching data from MongoDB collection...')
        cursor = md_collection.find(query, projection=projection, sort=sort)

        account_map = {}
        for doc in cursor:
            account_number = doc['accountNumber']
            amount = doc['centsAmount']
            currency = doc['currencyCode']
            if account_number not in account_map.keys():
                account_map[account_number] = amount
            else:
                account_map[account_number] += amount

        message = f'{len(account_map)} charges:'
        for account_number in account_map:
            message += f' Account {account_number} has {account_map[account_number]}c charges.'

        await update.message.reply_text(
            text=message,
            parse_mode=ParseMode.HTML
        )
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    response_message = None
    reply_markup = None
    if update.message.chat.type == ChatType.PRIVATE:
        user: TelegramUser = update.effective_user
        db_user: User = await validate(command_name='start', update=update)
        if db_user is None:
            return
        elif db_user.telegram_user_id != user.id:
            raise AssertionError(f'Telegram user ID mismatch for {user.id}, got {db_user.telegram_user_id} instead.')
        access_token: Optional[Tuple] = await get_access_token(
            telegram_user_id=db_user.telegram_user_id,
            user_id=db_user.id)
        if access_token is not None:
            log.info(f'Access token has an expiry of {access_token[1]}.')
        else:
            log.info(f'No access token stored for user.')
        response_message = rf'{emoji.emojize(":check_box_with_check:")} {user.first_name}, you are authorized.'
        user_keyboard = [
            [
                InlineKeyboardButton("Reauthorize", callback_data=str(ACTION_AUTHORIZE)),
                InlineKeyboardButton("Refresh Profile", callback_data=str(ACTION_REFRESH_PROFILE)),
            ],
            [
                InlineKeyboardButton("Show Profile", callback_data=str(ACTION_SHOW_PROFILE)),
                InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
            ]
        ]
        reply_markup = InlineKeyboardMarkup(user_keyboard)
    else:
        response_message = rf'<tg-emoji emoji-id="1">{emoji.emojize(":gear:")}</tg-emoji> This does not work in group chats, only in private chat.'
    await update.message.reply_html(
        text=response_message,
        reply_markup=reply_markup
    )
    return ConversationHandler.END


async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='refresh', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(
        text=f'{emoji.emojize(":hourglass_not_done:")}',
        parse_mode=ParseMode.MARKDOWN)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    access_token: Optional[Tuple] = await get_access_token(telegram_user_id=user.id, user_id=db_user.id)
    creds = json.loads(db_user.investec_credentials)
    client = InvestecOpenApiClient(
        client_id=db_user.investec_client_id,
        secret=creds['secret'],
        api_key=creds['api_key'],
        additional_headers={'Accept-Encoding': 'gzip, deflate, br'},
        access_token=access_token)
    log.debug(f'Fetching Investec accounts for Telegram user {user.id}...')
    response = client.get_accounts()
    log.debug(f'Accounts: {response!s}')
    await add_accounts(
        telegram_user_id=user.id,
        user_id=db_user.id,
        account_info=response)
    account_count = len(response)
    if client.access_token and client.access_token_expiry:
        if access_token is None or client.access_token != access_token[0]:
            log.debug(f'Persisting access token...')
            await update_access_token(
                telegram_user_id=user.id,
                user_id=db_user.id,
                access_token=client.access_token,
                access_token_expiry=client.access_token_expiry)
    log.debug(f'Fetching Investec cards for Telegram user {user.id}...')
    response = client.get_cards()
    log.debug(f'Cards: {response!s}')
    await add_cards(
        telegram_user_id=user.id,
        user_id=db_user.id,
        card_info=response)
    card_count = len(response)
    influxdb.write('bot', 'refresh', 1)
    await query.edit_message_text(
        text=f'Profile refresh complete. {account_count} account(s) and {card_count} card(s).',
        parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='show_profile', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(
        text=f'{emoji.emojize(":hourglass_not_done:")}',
        parse_mode=ParseMode.MARKDOWN)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    accounts: Optional[Sequence[Account]] = await get_accounts(
        telegram_user_id=user.id,
        user_id=db_user.id)
    account_summary = f'No account metadata saved.\n'
    if accounts:
        account_summary = f''
        for account in accounts:
            info: dict = json.loads(account.account_info)
            account_summary += f'{emoji.emojize(":ledger:")} {info["productName"]} ({info["accountNumber"]})\n'
    cards: Optional[Sequence[Card]] = await get_cards(
        telegram_user_id=user.id,
        user_id=db_user.id
    )
    card_summary = f'No card metadata saved.\n'
    if cards:
        card_summary = f''
        for card in cards:
            info: dict = json.loads(card.card_info)
            card_summary += f'{emoji.emojize(":credit_card:")} {info["EmbossedName"]} ({info["CardNumber"]})\n'
    influxdb.write('bot', 'show_profile', 1)
    if accounts is None and cards is None:
        response_message = f'{account_summary}{card_summary}\nTry a profile refresh.'
    else:
        response_message = f'{account_summary}{card_summary}'
    await query.edit_message_text(
        text=response_message,
        parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    await validate(command_name='registration', update=update, validate_registration=False)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    # fetch request token
    telegram_user = app_config.get('telegram', 'enabled_users_csv')
    if str(user.id) == telegram_user:
        investec_client_id = creds.investec_client_id
        investec_credentials = {
            "secret": creds.investec_secret,
            "api_key": creds.investec_apikey
        }
        await add_user(
            telegram_user_id=user.id,
            investec_client_id=investec_client_id,
            investec_credentials=json.dumps(investec_credentials))
        #influxdb.write('bot', 'registration_oauth', 1)
        await query.edit_message_text(
            text=f'User registration completed.',
            parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='help', update=update)
    if db_user is None:
        return
    help_url = app_config.get('telegram', 'help_url')
    message = rf'{emoji.emojize(":light_bulb:")} {user.first_name}, the documentation is [here]({help_url}).'
    await update.message.reply_text(
        text=message,
        # do not render the summary
        disable_web_page_preview=True,
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.info(f'Incoming message from Telegram user ID {update.effective_user.id}.')
    await update.message.reply_text(update.message.text)
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(text=f"No changes made.")
    return ConversationHandler.END


async def telegram_error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.warning(msg="Bot error:", exc_info=context.error)
    return ConversationHandler.END