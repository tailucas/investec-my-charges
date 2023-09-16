import emoji
import html
import locale
import random
import re
import requests
import string
import urllib

import pandas as pd
import plotly.express as px
import simplejson as json

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from typing import Optional, Tuple, Sequence

from pymongo.collection import Collection
from pymongo.cursor import Cursor

from sentry_sdk.integrations.logging import ignore_logger

from tailucas_pylib import (
    app_config,
    creds,
    log,
    threads
)

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
from .currency import local_currency


# Reduce Sentry noise
ignore_logger('telegram.ext._updater')

ACTION_SETTINGS_PREFIX = "settings"

ACTION_NONE = 0
ACTION_SETTINGS = 1
ACTION_AUTHORIZE = 2
ACTION_REFRESH_PROFILE = 3
ACTION_SHOW_PROFILE = 4
ACTION_FORGET = 5
ACTION_CARD_REPORT = 7
ACTION_CARD_REPORT_INTERVAL = 8
ACTION_ACCOUNT_HISTORY = 9
ACTION_DEFAULT_DAY = 10

ACTION_SETTINGS_UPDATE = 20
ACTION_SETTINGS_PAY_DAY = 21
ACTION_SETTINGS_BILL_CYCLE_DAY = 22
ACTION_SETTINGS_RESET_DEFAULT_DAY = 23

REPORT_INTERVAL_TYPE_MONTH = 0
REPORT_INTERVAL_TYPE_DATE = 1

DEFAULT_TAG_UNTAGGED = '_untagged_'
DEFAULT_ALL = '_all_'
DEFAULT_INTERVAL = '_month_'

USER_DATA_KEY_PAY_DAY = 'save_pay_day'
USER_DATA_KEY_BILL_CYCLE_DAY = 'save_bill_cycle_day'
USER_DATA_KEY_DEFAULT_DAY = 'save_default_day'

from .influx import influxdb

from .database import (
    Account,
    Card,
    User,
    UserSetting,
    IntervalSetting,
    get_access_token,
    update_access_token,
    get_user,
    add_user,
    get_account,
    get_accounts,
    add_accounts,
    get_user_setting,
    add_user_setting,
    get_card,
    get_cards,
    add_cards,
    add_interval_setting,
    get_interval_setting,
    delete_interval_setting
)


def get_datetime_a_month_ago() -> datetime:
    now = datetime.now()
    a_month_ago = now - relativedelta(months=1)
    return a_month_ago


def get_last_of_day(day: int) -> datetime:
    now = datetime.now()
    if now.day >= day:
        this_month = now.replace(day=day)
        return this_month
    else:
        previous_month = (now - relativedelta(months=1)).replace(day=day)
        return previous_month


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
        help_url = app_config.get('telegram', 'help_url')
        message = rf'{emoji.emojize(":construction:")} {user.first_name}, I am not ready for general use. You can express interest [here]({help_url}).'
        await update.message.reply_text(
            text=message,
            # do not render the summary
            disable_web_page_preview=True,
            parse_mode=ParseMode.MARKDOWN)
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
    random_words = []
    with open('random_words.txt', 'r') as file:
        random_words = file.readlines()
    date = get_datetime_a_month_ago()
    start_date = date.strftime('%Y-%m-%d')
    for account in accounts:
        log.debug(f'Telegram user {user.id} selects account ID {account.account_id}')
        # fetch associated transaction data
        mongodb_query = {
            "accountId": {
                "$eq": account.account_id
            },
        }
        if not app_config.getboolean('app', 'demo_mode'):
            mongodb_query["dateTime"] = {
                "$gte": start_date
            }
        projection = {}
        sort = []
        md_collection: Collection = context.bot_data['mongodb_account_collection']
        log.debug(f'Fetching data from MongoDB collection...')
        cursor = md_collection.find(mongodb_query, projection=projection, sort=sort)
        costs = {}
        i=0
        total_debit: float = 0
        for doc in cursor:
            tran_type = doc['type']
            if tran_type == 'CREDIT':
                continue
            i+=1
            description = html.unescape(doc['description'])
            charge_local_currency = float(doc['amount'])
            if app_config.getboolean('app', 'demo_mode'):
                log.warning(f'Demo mode enabled! Generating fake description and amount for Telegram user {user.id}.')
                description = random.choice(random_words).strip().title()
                charge_local_currency = random.uniform(1.0, charge_local_currency)
            total_debit += charge_local_currency
            if description not in costs.keys():
                costs[description] = charge_local_currency
            else:
                costs[description] += charge_local_currency
        to_plot = {'Merchant': [], 'Total': []}
        for merchant, amount_mind in costs.items():
            to_plot['Merchant'].append(merchant)
            to_plot['Total'].append(amount_mind)
        # output totals
        account_info = json.loads(account.account_info)
        account_name = account_info['productName']
        account_number = account_info['accountNumber']
        if app_config.getboolean('app', 'demo_mode'):
            log.warning(f'Demo mode enabled! Generating fake account number and amount for Telegram user {user.id}.')
            account_number = random.randint(10010000000, 10020000000)
        log.debug(f'Generating graphic of account activity...')
        df = pd.DataFrame(to_plot)
        # plot the top n
        top_n = 15
        fig = px.pie(df.nlargest(top_n, 'Total'), values='Total', names='Merchant', title=f'Top {top_n} {account_name} debits since {date.strftime("%d %B %Y")}')
        img_bytes = fig.to_image(format="png")
        caption = f'{account_name} ({account_number}) has {i} debits coming to a total of {locale.currency(total_debit)}.'
        await update.message.reply_photo(photo=img_bytes, caption=caption)
    return ConversationHandler.END


async def history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    db_user: User = await validate(command_name='history', update=update)
    if db_user is None:
        return ConversationHandler.END
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
                InlineKeyboardButton("All", callback_data=f'{ACTION_ACCOUNT_HISTORY}:{DEFAULT_ALL}'),
                InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
            ]
        )
        reply_markup = InlineKeyboardMarkup(user_keyboard)
        await update.message.reply_html(
            text=response_message,
            reply_markup=reply_markup
        )
    return ConversationHandler.END


async def account_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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

    mongodb_query: dict = {}
    log.debug(f'{query.data=}')
    account_id = query.data.split(':')[1]
    log.debug(f'Telegram user {user.id} selects account ID {account_id}')
    # fetch associated transaction data
    if account_id != DEFAULT_ALL:
        mongodb_query['accountId'] = {
            "$eq": account_id
        }
    date = get_datetime_a_month_ago()
    start_date = date.strftime('%Y-%m-%d')
    mongodb_query['transactionDate'] = {
        "$gte": start_date
    }
    projection = {}
    sort = []
    md_collection: Collection = context.bot_data['mongodb_account_collection']
    log.debug(f'Fetching data from MongoDB collection: {mongodb_query=}...')
    cursor = md_collection.find(mongodb_query, projection=projection, sort=sort)

    costs = {}
    for tran in cursor:
        log.debug(f'{tran=}')
        if tran['type'] == 'CREDIT':
            continue
        tran_amnt = float(tran['amount'])
        if app_config.getboolean('app', 'demo_mode'):
            log.warning(f'Demo mode enabled! Generating fake amount for Telegram user {user.id}.')
            tran_amnt = random.uniform(1.0, tran_amnt)
        tran_detail = tran['transactionType']
        if tran_detail is None:
            tran_detail: str = tran['description']
            tran_detail = tran_detail.title().replace('*', ' ')
        else:
            tran_detail = split_camel_case(s=tran_detail)
        if app_config.getboolean('app', 'demo_mode'):
            log.warning(f'Demo mode enabled! Generating fake transaction type {user.id}.')
            if tran_detail.startswith('Cross-Border Card Fee'):
                tran_detail = 'Cross-Border Card Fee'
        if tran_detail not in costs:
            costs[tran_detail] = tran_amnt
        else:
            costs[tran_detail] += tran_amnt
    messages = [f'Since `{start_date}`:']
    for tran_detail, tran_amnt in costs.items():
        messages.append(f'`{locale.currency(tran_amnt)}` *{tran_detail}*')
    await query.edit_message_text(
        text='\n'.join(messages),
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END


async def cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    db_user: User = await validate(command_name='cards', update=update)
    if db_user is None:
        return ConversationHandler.END
    user: TelegramUser = update.effective_user
    await context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)
    cards: Optional[Sequence[Card]] = await get_cards(telegram_user_id=user.id, user_id=db_user.id)
    if cards:
        response_message = rf'{emoji.emojize(":credit_card:")} Pick a card:'
        user_keyboard = []
        for card in cards:
            callback_action = ACTION_CARD_REPORT_INTERVAL
            action_interval = DEFAULT_INTERVAL
            report_interval: IntervalSetting = await get_interval_setting(user_id=db_user.id, card_id=card.card_id)
            log.debug(f'Telegram user {user.id} card {card.card_id} report interval: {report_interval!r}')
            if report_interval:
                callback_action = ACTION_CARD_REPORT
                if report_interval.report_interval_type == REPORT_INTERVAL_TYPE_DATE:
                    billing_cycle_day: int = app_config.getint('app', 'default_bill_cycle_day_of_month')
                    db_user_setting: UserSetting = await get_user_setting(user_id=db_user.id)
                    if db_user_setting is not None and db_user_setting.bill_cycle_day_of_month is not None:
                        billing_cycle_day: int = db_user_setting.bill_cycle_day_of_month
                        action_interval = billing_cycle_day
            log.debug(f'Telegram user {user.id} card {card.card_id} report: {callback_action=}, {action_interval=}')
            info = json.loads(card.card_info)
            card_label = info['EmbossedName']
            user_keyboard.append([InlineKeyboardButton(card_label, callback_data=f'{callback_action}:{card.card_id}:{action_interval}')])
        user_keyboard.append(
            [
                InlineKeyboardButton("All", callback_data=f'{callback_action}:{DEFAULT_ALL}:{action_interval}'),
                InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
            ]
        )
        reply_markup = InlineKeyboardMarkup(user_keyboard)
        await update.message.reply_html(
            text=response_message,
            reply_markup=reply_markup
        )
    return ConversationHandler.END


async def card_report_interval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='card_report', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    card_id = query.data.split(':')[1]
    log.debug(f'Telegram user {user.id} selects card ID {card_id}')
    billing_cycle_day: int = app_config.getint('app', 'default_bill_cycle_day_of_month')
    db_user_setting: UserSetting = await get_user_setting(user_id=db_user.id)
    if db_user_setting is not None and db_user_setting.bill_cycle_day_of_month is not None:
        billing_cycle_day: int = db_user_setting.bill_cycle_day_of_month
    response_message = rf'{emoji.emojize(":spiral_calendar:")} Pick a report interval:'
    user_keyboard = [
        [
            InlineKeyboardButton("Billing Cycle", callback_data=f'{ACTION_CARD_REPORT}:{card_id}:{billing_cycle_day}'),
            InlineKeyboardButton("Calendar Month", callback_data=f'{ACTION_CARD_REPORT}:{card_id}:{DEFAULT_INTERVAL}'),
        ],
        [
            InlineKeyboardButton("Cancel", callback_data=str(ACTION_NONE))
        ]
    ]
    reply_markup = InlineKeyboardMarkup(user_keyboard)
    await query.edit_message_text(
        text=response_message,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )
    return ConversationHandler.END


async def card_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='card_report', update=update)
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    await query.edit_message_text(text=f'{emoji.emojize(":hourglass_not_done:")}', parse_mode=ParseMode.MARKDOWN)

    callback_data = query.data.split(':')
    card_id = callback_data[1]
    interval = callback_data[2]
    date = None
    if interval == DEFAULT_INTERVAL:
        date = get_datetime_a_month_ago()
        await add_interval_setting(user_id=db_user.id, report_interval_type=REPORT_INTERVAL_TYPE_MONTH, report_interval_days=31, card_id=int(card_id))
    else:
        date = get_last_of_day(day=int(interval))
        await add_interval_setting(user_id=db_user.id, report_interval_type=REPORT_INTERVAL_TYPE_DATE, report_interval_days=int(interval), card_id=int(card_id))
    start_date = date.strftime('%Y-%m-%d')
    log.debug(f'Telegram user {user.id} selects card ID {card_id} with interval {interval} ({start_date})')

    account_numbers = []
    card_ids = []
    card_names = []
    if card_id == DEFAULT_ALL:
        cards: Optional[Sequence[Card]] = await get_cards(telegram_user_id=user.id, user_id=db_user.id)
        if cards:
            for card in cards:
                info = json.loads(card.card_info)
                account_numbers.append(info['AccountNumber'])
                card_ids.append(str(card.card_id))
                card_names.append(str(info['EmbossedName']).title())
    else:
        card: Optional[Card] = await get_card(telegram_user_id=user.id, user_id=db_user.id, card_id=int(card_id))
        if card:
            info = json.loads(card.card_info)
            account_numbers.append(info['AccountNumber'])
            card_ids.append(str(card.card_id))
            card_names.append(str(info['EmbossedName']).title())
    log.debug(f'Running MongoDB query: {account_numbers=}, {card_ids=}')
    # fetch associated transaction data
    reference = {
        "$ne": "simulation"
    }
    if app_config.getboolean('app', 'demo_mode'):
        log.warning(f'Demo mode enabled! Using simulation references only for Telegram user {user.id}.')
        reference = {
            "$eq": "simulation"
        }
    mongo_query = {
        "accountNumber": {
            "$in": account_numbers
        },
        "card.id" : {
            "$in": card_ids
        },
        "type": "card",
        "dateTime": {
            "$gte": start_date
        },
        "reference": reference
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
        charge_cents_local_currency = await local_currency(
            charge_cents=int(doc['centsAmount']),
            charge_currency=str(doc['currencyCode']).upper(),
            charge_date=str(doc['dateTime']).split('T')[0])
        merchant = doc['merchant']['name']
        if merchant not in costs.keys():
            costs[merchant] = charge_cents_local_currency
        else:
            costs[merchant] += charge_cents_local_currency

    log.debug(f'{i} transactions fetched.')
    to_plot = {'Merchant': [], 'Total': []}
    total_charges: float = 0.0
    for merchant, amount_mind in costs.items():
        to_plot['Merchant'].append(merchant)
        to_plot['Total'].append(amount_mind)
        total_charges += amount_mind
    # switch to major denomination
    total_charges = total_charges / 100.0
    top_n = 15
    df = pd.DataFrame(to_plot)
    card_labels = ','.join(sorted(card_names))
    fig = px.pie(df.nlargest(top_n, 'Total'), values='Total', names='Merchant', title=f'Top {top_n} charges on {card_labels} since {date.strftime("%d %B %Y")}')
    img_bytes = fig.to_image(format="png")
    caption = f'{i} charges coming to a total of {locale.currency(total_charges)}.'
    # remove the emoji
    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.effective_message.id)
    await context.bot.send_photo(chat_id=update.effective_chat.id, photo=img_bytes, caption=caption)
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    response_message = None
    reply_markup = None
    if update.message.chat.type == ChatType.PRIVATE:
        user: TelegramUser = update.effective_user
        db_user: User = await validate(command_name='start', update=update)
        if db_user is None:
            return ConversationHandler.END
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


async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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


async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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
            account_number = info["accountNumber"]
            if app_config.getboolean('app', 'demo_mode'):
                log.warning(f'Demo mode enabled! Generating fake account number and amount for Telegram user {user.id}.')
                account_number = random.randint(10010000000, 10020000000)
            account_summary += f'{emoji.emojize(":ledger:")} {info["productName"]} ({account_number})\n'
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


async def forget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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

    # TODO

    influxdb.write('bot', 'forget', 1)
    await query.edit_message_text(
        text='Not implemented.',
        parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    response_message = None
    reply_markup = None
    if update.message.chat.type == ChatType.PRIVATE:
        user: TelegramUser = update.effective_user
        db_user: User = await validate(command_name='settings', update=update)
        if db_user is None:
            return ConversationHandler.END
        db_user_setting: UserSetting = await get_user_setting(user_id=db_user.id)
        note_default = ''
        if db_user_setting is None:
            pay_day = app_config.getint('app', 'default_pay_day_of_month')
            billing_cycle_day = app_config.getint('app', 'default_bill_cycle_day_of_month')
            note_default = ' (default settings)'
        else:
            pay_day = db_user_setting.pay_day_of_month
            billing_cycle_day = db_user_setting.bill_cycle_day_of_month
        response_message = rf'<tg-emoji emoji-id="1">{emoji.emojize(":gear:")}</tg-emoji> ' \
            f'{user.first_name}, pay day is on day {pay_day} of the month and billing cycle ends on day {billing_cycle_day}{note_default}.'
        user_keyboard = [
            [
                InlineKeyboardButton("Set Pay Day", callback_data=ACTION_SETTINGS_PAY_DAY),
                InlineKeyboardButton("Set Billing Cycle Day", callback_data=ACTION_SETTINGS_BILL_CYCLE_DAY),
            ],
            [
                InlineKeyboardButton("Reset Default Day", callback_data=ACTION_SETTINGS_RESET_DEFAULT_DAY),
            ],
            [
                InlineKeyboardButton("Cancel", callback_data=f'{ACTION_NONE}:No changes made.')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(user_keyboard)
    else:
        response_message = rf'<tg-emoji emoji-id="1">{emoji.emojize(":gear:")}</tg-emoji> This does not work in group conversations, only in private chat.'
    await update.message.reply_html(
        text=response_message,
        reply_markup=reply_markup
    )
    return ACTION_SETTINGS_UPDATE


async def askpayday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user: TelegramUser = update.effective_user
    log.info(f'Asking Telegram user {user.id} for pay day preference...')
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    valid_min = 1
    # FIXME: last day of month
    valid_max = 28
    valid_response = f'a number between {valid_min} and {valid_max} (inclusive)'
    context.user_data[USER_DATA_KEY_PAY_DAY] = {'min': valid_min, 'max': valid_max, 'response': valid_response}
    if USER_DATA_KEY_BILL_CYCLE_DAY in context.user_data.keys():
        del context.user_data[USER_DATA_KEY_BILL_CYCLE_DAY]
    await query.edit_message_text(
        text=f'{emoji.emojize(":calendar:")} Enter {valid_response} to represent the day of the month for *pay day*.',
        parse_mode=ParseMode.MARKDOWN)
    return ACTION_SETTINGS_UPDATE


async def askbillcycleday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user: TelegramUser = update.effective_user
    log.info(f'Asking Telegram user {user.id} for billing cycle day preference...')
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    valid_min = 1
    valid_max = 28
    valid_response = f'a number between {valid_min} and {valid_max} (inclusive)'
    context.user_data[USER_DATA_KEY_BILL_CYCLE_DAY] = {'min': valid_min, 'max': valid_max, 'response': valid_response}
    if USER_DATA_KEY_PAY_DAY in context.user_data.keys():
        del context.user_data[USER_DATA_KEY_PAY_DAY]
    await query.edit_message_text(
        text=f'{emoji.emojize(":calendar:")} Enter {valid_response} to represent the day of the month when the *billing cycle* is complete.',
        parse_mode=ParseMode.MARKDOWN)
    return ACTION_SETTINGS_UPDATE


async def resetdefaultday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user: TelegramUser = update.effective_user
    log.info(f'Resetting default day for Telegram user {user.id}...')
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    db_user: User = await validate(command_name='update_settings', update=update)
    if db_user is None:
        return ConversationHandler.END
    await delete_interval_setting(user_id=db_user.id)
    await query.edit_message_text(
        text=f'Reporting interval is reset for all cards. Query each one to set the default.',
        parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def update_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='update_settings', update=update)
    if db_user is None:
        return ConversationHandler.END
    log.debug(f'{update.callback_query=} with {context.user_data=}')
    save_pay_day = None
    if USER_DATA_KEY_PAY_DAY in context.user_data.keys():
        save_pay_day = context.user_data[USER_DATA_KEY_PAY_DAY]
    save_billing_cycle_day = None
    if USER_DATA_KEY_BILL_CYCLE_DAY in context.user_data.keys():
        save_billing_cycle_day = context.user_data[USER_DATA_KEY_BILL_CYCLE_DAY]
    valid_response = None
    valid_min: int = None
    valid_max: int = None
    if save_pay_day:
        valid_min = save_pay_day['min']
        valid_max = save_pay_day['max']
        valid_response = save_pay_day['response']
    elif save_billing_cycle_day:
        valid_min = save_billing_cycle_day['min']
        valid_max = save_billing_cycle_day['max']
        valid_response = save_billing_cycle_day['response']
    feedback: str = update.message.text
    valid_feedback = True
    day: int = None
    try:
        day = int(feedback)
    except ValueError:
        valid_feedback = False
    if not valid_feedback or day < valid_min or day > valid_max:
        await update.message.reply_markdown(
            text=f'Sorry, send *{valid_response}*.',
            reply_to_message_id=update.message.id)
        return ACTION_SETTINGS_UPDATE
    else:
        if save_pay_day:
            await add_user_setting(user_id=db_user.id, pay_day_of_month=day)
            del context.user_data[USER_DATA_KEY_PAY_DAY]
        elif save_billing_cycle_day:
            await add_user_setting(user_id=db_user.id, bill_cycle_day_of_month=day)
            del context.user_data[USER_DATA_KEY_BILL_CYCLE_DAY]
    await update.message.reply_text(f'Settings updated.')
    return ConversationHandler.END


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user: TelegramUser = update.effective_user
    db_user: User = await validate(command_name='help', update=update)
    if db_user is None:
        return ConversationHandler.END
    help_url = app_config.get('telegram', 'help_url')
    message = rf'{emoji.emojize(":light_bulb:")} {user.first_name}, the documentation is [here]({help_url}).'
    await update.message.reply_text(
        text=message,
        # do not render the summary
        disable_web_page_preview=True,
        parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    log.info(f'Incoming message from Telegram user ID {update.effective_user.id}.')
    await update.message.reply_text(update.message.text)
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()
    backchat = None
    feedback = query.data.split(':')
    if len(feedback) > 1:
        backchat = feedback[1]
    if backchat is None:
        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.effective_message.id)
    else:
        await context.bot.edit_message_text(text=backchat, chat_id=update.effective_chat.id, message_id=update.effective_message.id)
    return ConversationHandler.END


async def telegram_error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    log.warning(msg="Bot error:", exc_info=context.error)
    return ConversationHandler.END


async def transaction_update(update: TransactionUpdate, context: CustomContext) -> None:
    influxdb.write('bot', 'transaction_update', 1)
    chat_member: TelegramChatMember = await context.bot.get_chat_member(chat_id=update.user_id, user_id=update.user_id)
    user: TelegramUser = chat_member.user
    log.debug(f'Transaction for Telegram user ID {user.id}.')
    db_user: Optional[User] = await get_user(telegram_user_id=user.id)
    if db_user is None:
        return
    tran_event: dict = update.payload
    log.debug(f'Transaction details {tran_event!s}.')
    card_id = tran_event['card']['id']
    # user settings for interval type
    billing_cycle_day: int = app_config.getint('app', 'default_bill_cycle_day_of_month')
    db_user_setting: UserSetting = await get_user_setting(user_id=db_user.id)
    if db_user_setting is not None and db_user_setting.bill_cycle_day_of_month is not None:
        billing_cycle_day: int = db_user_setting.bill_cycle_day_of_month
    date = get_last_of_day(day=int(billing_cycle_day))
    # interval configuration
    db_interval_setting: IntervalSetting = await get_interval_setting(user_id=db_user.id, card_id=int(card_id))
    if db_interval_setting:
        if db_interval_setting.report_interval_type == REPORT_INTERVAL_TYPE_MONTH:
            date = get_datetime_a_month_ago()
    start_date = date.strftime('%Y-%m-%d')
    log.debug(f'Telegram user {user.id} has a card event for card ID {card_id}, searching others from {start_date}.')
    card: Optional[Card] = await get_card(telegram_user_id=user.id, user_id=db_user.id, card_id=int(card_id))
    if card is None:
        log.debug(f'No card for Telegram user ID {user.id}.')
        return
    card_info = json.loads(card.card_info)
    account_number: str = card_info['AccountNumber']
    card_name: str = str(card_info['EmbossedName']).title()
    merchant_name: str = tran_event['merchant']['name']
    log.debug(f'Running MongoDB query: {account_number=}, {card_id=}, {start_date=}, {merchant_name=}')
    # fetch associated transaction data
    reference = {
        "$ne": "simulation"
    }
    if app_config.getboolean('app', 'demo_mode'):
        log.warning(f'Demo mode enabled! Using simulation references only for Telegram user {user.id}.')
        reference = {
            "$eq": "simulation"
        }
    mongo_query = {
        "accountNumber": {
            "$eq": account_number
        },
        "card.id" : {
            "$eq": card_id
        },
        "type": "card",
        "dateTime": {
            "$gte": start_date
        },
        "merchant.name": {
            "$eq": merchant_name
        },
        "reference": reference
    }
    projection = {}
    sort = []
    md_collection: Collection = context.application.bot_data['mongodb_card_collection']
    log.debug(f'Fetching data from MongoDB collection {md_collection!r}...')
    cursor = md_collection.find(mongo_query, projection=projection, sort=sort)
    total_charges: float = await local_currency(
        charge_cents=int(tran_event['centsAmount']),
        charge_currency=str(tran_event['currencyCode']).upper(),
        charge_date=str(tran_event['dateTime']).split('T')[0])
    i=1
    for doc in cursor:
        log.debug(f'MongoDB result: {doc!s}')
        doc_id = str(doc['_id'])
        if doc_id == tran_event['_id']:
            log.debug(f'Skipping database item {doc_id} already present in notification.')
            continue
        i+=1
        charge_cents_local_currency = await local_currency(
            charge_cents=int(doc['centsAmount']),
            charge_currency=str(doc['currencyCode']).upper(),
            charge_date=str(doc['dateTime']).split('T')[0])
        total_charges += charge_cents_local_currency
    # switch to major denomination
    total_charges = total_charges / 100.0
    await context.bot.send_message(
        chat_id=update.user_id,
        text=f"{user.first_name}, your card <b>{card_name}</b> has <b>{i} charge(s)</b> since {start_date} from <i>{html.unescape(merchant_name)}</i> coming to a total of <b>{locale.currency(total_charges)}</b>.",
        parse_mode=ParseMode.HTML)
