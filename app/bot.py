import emoji
import html
import re
import requests
import string
import urllib

import pandas as pd
import plotly.express as px
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
from pylib.zmq import zmq_socket

from telegram import (
    Update,
    Message,
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
from .event import TransactionUpdate, CustomContext
import zmq
from zmq.asyncio import Socket
from .currency import URL_WORKER_CURRENCY_CONVERTER


# Reduce Sentry noise
ignore_logger('telegram.ext._updater')

ACTION_SETTINGS_PREFIX = "settings"

ACTION_NONE = 0
ACTION_AUTHORIZE = 2
ACTION_REFRESH_PROFILE = 3
ACTION_SHOW_PROFILE = 4
ACTION_FORGET = 5
ACTION_ACCOUNT_REPORT = 6
ACTION_CARD_REPORT = 7
ACTION_ACCOUNT_HISTORY = 8

ACTION_ACCOUNT_DEBITS = 9
ACTION_ACCOUNT_CREDITS = 10

DEFAULT_TAG_UNTAGGED = '_untagged_'
DEFAULT_ALL = '_all_'

from .influx import influxdb

from .database import (
    Account,
    Card,
    User,
    get_access_token,
    update_access_token,
    get_user,
    add_user,
    get_account,
    get_accounts,
    add_accounts,
    get_card,
    get_cards,
    add_cards
)


def split_camel_case(s: str) -> str:
    return re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', s))


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


async def accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)
    db_user: User = await validate(command_name='accounts', update=update)
    if db_user is None:
        return ConversationHandler.END
    user: TelegramUser = update.effective_user
    accounts: Optional[Sequence[Account]] = await get_accounts(telegram_user_id=user.id, user_id=db_user.id)
    if accounts is None:
        return ConversationHandler.END
    for account in accounts:
        log.debug(f'Telegram user {user.id} selects account ID {account.account_id}')
        # fetch associated transaction data
        mongo_query = {
            "accountId": {
                "$eq": account.account_id
            },
            "reference": {
                "$ne": "simulation"
            }
        }
        projection = {}
        sort = []
        md_collection: Collection = context.bot_data['mongodb_account_collection']
        log.debug(f'Fetching data from MongoDB collection...')
        cursor = md_collection.find(mongo_query, projection=projection, sort=sort)
        costs = {}
        i=0
        total_debit: float = 0
        for doc in cursor:
            tran_type = doc['type']
            if tran_type == 'CREDIT':
                continue
            i+=1
            description = html.unescape(doc['description'])
            charge_home_currency = float(doc['amount'])
            total_debit += charge_home_currency
            if description not in costs.keys():
                costs[description] = charge_home_currency
            else:
                costs[description] += charge_home_currency
        to_plot = {'Merchant': [], 'Total': []}
        for merchant, amount_mind in costs.items():
            to_plot['Merchant'].append(merchant)
            to_plot['Total'].append(amount_mind)
        # output totals
        account_info = json.loads(account.account_info)
        account_name = account_info['productName']
        account_number = account_info['accountNumber']
        log.debug(f'Generating graphic of account activity...')
        df = pd.DataFrame(to_plot)
        fig = px.pie(df, values='Total', names='Merchant', title=f'{account_name} debits.')
        img_bytes = fig.to_image(format="png")
        # FIXME currency symbol
        caption = f'{account_name} ({account_number}) has {i} debits coming to a total of R{total_debit:.2f}.'
        await update.message.reply_photo(photo=img_bytes, caption=caption)
    return ConversationHandler.END


async def history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_user: User = await validate(command_name='history', update=update)
    if db_user is None:
        return
    user: TelegramUser = update.effective_user
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)
    accounts: Optional[Sequence[Account]] = await get_accounts(telegram_user_id=user.id, user_id=db_user.id)
    if accounts:
        response_message = rf'{emoji.emojize(":ledger:")} Pick an account:'
        user_keyboard = []
        for account in accounts:
            info = json.loads(account.account_info)
            account_label = info['productName']
            user_keyboard.append([InlineKeyboardButton(account_label, callback_data=f'{ACTION_ACCOUNT_HISTORY}:{account.account_id}')])
        user_keyboard.append(
            [
                InlineKeyboardButton("All", callback_data=str(ACTION_ACCOUNT_HISTORY)),
                InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
            ]
        )
        reply_markup = InlineKeyboardMarkup(user_keyboard)
        await update.message.reply_html(
            text=response_message,
            reply_markup=reply_markup
        )
    return ConversationHandler.END


async def account_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='account_history', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(
        text=f'{emoji.emojize(":hourglass_not_done:")}',
        parse_mode=ParseMode.MARKDOWN)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

    account_id = query.data.split(':')[1]
    log.debug(f'Telegram user {user.id} selects account ID {account_id}')
    # fetch associated transaction data
    mongo_query = {
        "accountId": {
            "$eq": account_id
        },
        "reference": {
            "$ne": "simulation"
        }
    }
    projection = {}
    sort = []
    md_collection: Collection = context.bot_data['mongodb_account_collection']
    log.debug(f'Fetching data from MongoDB collection...')
    cursor = md_collection.find(mongo_query, projection=projection, sort=sort)

    costs = {}
    for tran in cursor:
        if tran['type'] == 'CREDIT':
            continue
        tran_amnt = float(tran['amount'])
        tran_detail = tran['transactionType']
        if tran_detail is None:
            tran_detail: str = tran['description']
            tran_detail = tran_detail.title()
        else:
            tran_detail = split_camel_case(s=tran_detail)
        if tran_detail not in costs:
            costs[tran_detail] = tran_amnt
        else:
            costs[tran_detail] += tran_amnt
    messages = []
    for tran_detail, tran_amnt in costs.items():
        messages.append(f'`R{float(tran_amnt):.2f}` *{tran_detail}*')
    await query.edit_message_text(
        text='\n'.join(messages),
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END


async def cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_user: User = await validate(command_name='cards', update=update)
    if db_user is None:
        return
    user: TelegramUser = update.effective_user
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)
    cards: Optional[Sequence[Card]] = await get_cards(telegram_user_id=user.id, user_id=db_user.id)
    if cards:
        response_message = rf'{emoji.emojize(":credit_card:")} Pick a card:'
        user_keyboard = []
        for card in cards:
            info = json.loads(card.card_info)
            card_label = info['EmbossedName']
            user_keyboard.append([InlineKeyboardButton(card_label, callback_data=f'{ACTION_CARD_REPORT}:{card.card_id}')])
        user_keyboard.append(
            [
                InlineKeyboardButton("All", callback_data=str(DEFAULT_ALL)),
                InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
            ]
        )
        reply_markup = InlineKeyboardMarkup(user_keyboard)
        await update.message.reply_html(
            text=response_message,
            reply_markup=reply_markup
        )
    return ConversationHandler.END


async def home_currency(charge_cents: int, charge_currency: str):
    home_currency_code = app_config.get('app', 'home_currency_code')
    currency_converter: Socket = zmq_socket(zmq.REQ, is_async=True)
    currency_converter.connect(addr=URL_WORKER_CURRENCY_CONVERTER)
    currency_query = {
        'function_path': 'latest',
        'params': {
            'base': charge_currency,
            'symbols': home_currency_code,
            'amount': 1
        }
    }
    await currency_converter.send_pyobj(currency_query)
    response = await currency_converter.recv_pyobj()
    currency_converter.close()
    rate = response['rate']
    charge_cents_home_currency = rate * charge_cents
    log.debug(f'Converted {charge_cents}c {charge_currency} to {charge_cents_home_currency}c {home_currency_code} ({rate=})')
    return charge_cents_home_currency


async def card_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='card_report', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(
        text=f'{emoji.emojize(":hourglass_not_done:")}',
        parse_mode=ParseMode.MARKDOWN)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

    card_id = query.data.split(':')[1]
    log.debug(f'Telegram user {user.id} selects card ID {card_id}')

    account_numbers = []
    card_ids = []
    if card_id == DEFAULT_ALL:
        cards: Optional[Sequence[Card]] = await get_cards(telegram_user_id=user.id, user_id=db_user.id)
        if cards:
            for card in cards:
                info = json.loads(card.card_info)
                account_numbers.append(info['AccountNumber'])
                card_ids.append(str(card.card_id))
    else:
        card: Optional[Card] = await get_card(telegram_user_id=user.id, user_id=db_user.id, card_id=int(card_id))
        if card:
            info = json.loads(card.card_info)
            account_numbers.append(info['AccountNumber'])
            card_ids.append(str(card.card_id))
    log.debug(f'Running MongoDB query: {account_numbers=}, {card_ids=}')
    # fetch associated transaction data
    mongo_query = {
        "accountNumber": {
            "$in": account_numbers
        },
        "card.id" : {
            "$in": card_ids
        },
        "type": "card",
        "reference": {
                "$ne": "simulation"
            }
        }
    projection = {}
    sort = []
    md_collection: Collection = context.bot_data['mongodb_card_collection']
    log.debug(f'Fetching data from MongoDB collection...')
    cursor = md_collection.find(mongo_query, projection=projection, sort=sort)
    costs = {}
    i=0
    for doc in cursor:
        i+=1
        charge_cents_home_currency = await home_currency(
            charge_cents=int(doc['centsAmount']),
            charge_currency=str(doc['currencyCode']).upper())
        merchant = doc['merchant']['name']
        if merchant not in costs.keys():
            costs[merchant] = charge_cents_home_currency
        else:
            costs[merchant] += charge_cents_home_currency

    log.debug(f'{i} transactions fetched.')
    to_plot = {'Merchant': [], 'Total': []}
    for merchant, amount_mind in costs.items():
        to_plot['Merchant'].append(merchant)
        #amount_majd = amount_mind / 100.0
        #amount_label = f'R{amount_majd:.2f}'
        to_plot['Total'].append(amount_mind)
    await query.edit_message_text(
        text=f'{i} charges.',
        parse_mode=ParseMode.MARKDOWN
    )
    df = pd.DataFrame(to_plot)
    fig = px.pie(df, values='Total', names='Merchant', title='Proportion of charges')
    img_bytes = fig.to_image(format="png")
    await context.bot.send_photo(chat_id=update.effective_chat.id, photo=img_bytes)
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
                InlineKeyboardButton("Forget Me", callback_data=str(ACTION_FORGET))
            ],
            [
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


async def forget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='forget', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(
        text=f'{emoji.emojize(":hourglass_not_done:")}',
        parse_mode=ParseMode.MARKDOWN)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)


    influxdb.write('bot', 'forget', 1)
    await query.edit_message_text(
        text='Forgotten.',
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
    await query.edit_message_text(text=f"OK")
    return ConversationHandler.END


async def telegram_error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.warning(msg="Bot error:", exc_info=context.error)
    return ConversationHandler.END


async def transaction_update(update: TransactionUpdate, context: CustomContext) -> None:
    influxdb.write('bot', 'transaction_update', 1)
    chat_member: TelegramChatMember = await context.bot.get_chat_member(chat_id=update.user_id, user_id=update.user_id)
    telegram_user: TelegramUser = chat_member.user
    log.debug(f'Transaction for Telegram user ID {update.user_id}.')
    log.debug(f'Transaction details {update.payload!s}.')
    await context.bot.send_message(
        chat_id=update.user_id,
        text=f"{telegram_user.first_name}, {html.unescape(update.payload['merchant']['name'])}",
        parse_mode=ParseMode.HTML)