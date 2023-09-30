import asyncio
import html
import simplejson as json
import time
import zmq

from requests.exceptions import HTTPError
from typing import Dict, Optional, Tuple, Sequence, List
from zmq.error import ZMQError, ContextTerminated, Again

from tailucas_pylib import log, threads
from tailucas_pylib.app import AppThread, Closable
from tailucas_pylib.handler import exception_handler

from pymongo import MongoClient, InsertOne, DESCENDING
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.cursor import Cursor

from .database import (
    Account,
    Card,
    User,
    get_access_token,
    update_access_token,
    get_users,
    get_user,
    add_user,
    get_accounts,
    add_accounts,
    get_card,
    get_cards,
    add_cards
)

from investec_api_python import InvestecOpenApiClient


URL_WORKER_TRANSACTION_HISTORY = 'inproc://transaction-history'


class TransactionHistory(AppThread):

        def __init__(self, mongodb_collection: Collection, sync_interval: int):
            AppThread.__init__(self, name=self.__class__.__name__)
            self._mongodb_collection: Collection = mongodb_collection
            self._last_sync: Optional[int] = None
            self._sync_interval_secs: int = sync_interval

        def run(self):
            with exception_handler(
                connect_url=URL_WORKER_TRANSACTION_HISTORY,
                socket_type=zmq.REP,
                and_raise=False,
                shutdown_on_error=True) as zmq_socket:
                while not threads.shutting_down:
                    trigger = None
                    try:
                        trigger = zmq_socket.recv_pyobj(flags=zmq.NOBLOCK)
                    except Again:
                        # ignore, no data
                        pass
                    now = int(time.time())
                    sync_needed: bool = trigger or self._last_sync is None or now - self._last_sync >= self._sync_interval_secs
                    if not sync_needed:
                        # never spin
                        threads.interruptable_sleep.wait(30)
                        continue
                    else:
                        self._last_sync = now
                    # fetch registered users
                    users: Optional[Sequence[User]] = asyncio.run(get_users())
                    if users is None:
                        log.info('No users registered.')
                        continue
                    else:
                        log.info(f'Loaded {len(users)} users.')
                    for user in users:
                        access_token: Optional[Tuple] = asyncio.run(get_access_token(telegram_user_id=user.telegram_user_id, user_id=user.id))
                        creds = json.loads(user.investec_credentials)
                        client = InvestecOpenApiClient(
                            client_id=user.investec_client_id,
                            secret=creds['secret'],
                            api_key=creds['api_key'],
                            additional_headers={'Accept-Encoding': 'gzip, deflate, br'},
                            access_token=access_token)
                        log.info(f'Fetching accounts for Telegram user {user.telegram_user_id}...')
                        accounts: Optional[Sequence[Account]] = asyncio.run(get_accounts(telegram_user_id=user.telegram_user_id, user_id=user.id))
                        if accounts is None:
                            log.info(f'No accounts for Telegram user {user.telegram_user_id}')
                            continue
                        else:
                            log.info(f'Loaded {len(accounts)} accounts for Telegram user {user.telegram_user_id}.')
                        for account in accounts:
                            # fetch latest persisted
                            mongo_query = {
                                "accountId": {
                                    "$eq": account.account_id
                                }
                            }
                            projection = {}
                            sort = [
                                ("postedOrder", DESCENDING)
                            ]
                            log.debug(f'Fetching data from MongoDB collection...')
                            last_post: Optional[int] = None
                            last_date: Optional[str] = None
                            doc = self._mongodb_collection.find_one(mongo_query, projection=projection, sort=sort)
                            if doc:
                                account_id = doc['accountId']
                                if account_id != account.account_id:
                                    raise AssertionError(f'Expected account {account.account_id} from MongoDB but got {account_id}.')
                                last_post = int(doc['postedOrder'])
                                last_date = doc['postingDate']
                            log.info(f'Last transaction for account ID {account.account_id} for Telegram user {user.telegram_user_id} is {last_date} (posted order {last_post}). Fetching since...')
                            response = None
                            try:
                                response = client.get_account_transactions(account_id=account.account_id, from_date=last_date)
                            except HTTPError as e:
                                raise ResourceWarning(f'Cannot fetch transactions for account {account.account_id}') from e
                            log.debug(f'Accounts response: {response!s}')
                            if access_token is None or client.access_token != access_token[0]:
                                log.debug(f'Persisting access token...')
                                asyncio.run(update_access_token(
                                    telegram_user_id=user.telegram_user_id,
                                    user_id=user.id,
                                    access_token=client.access_token,
                                    access_token_expiry=client.access_token_expiry))
                            log.info(f'Fetched {len(response)} results from API. Collecting keys...')
                            updates: List = []
                            for tran in response:
                                posted_order: int = int(tran['postedOrder'])
                                if last_post and posted_order <= last_post:
                                    log.debug(f'Skipping known transaction for account ID {account.account_id} with post order {posted_order}')
                                    continue
                                updates.append(tran)
                            if len(updates) > 0:
                                log.info(f'Inserting {len(updates)} into MongoDB collection...')
                                self._mongodb_collection.insert_many(updates)
                            else:
                                log.info(f'No new records to insert.')
                    # respond to the trigger
                    if trigger:
                        response = None
                        zmq_socket.send_pyobj(response)
                    done_now = int(time.time())
                    log.info(f'Sync completed in {done_now-now}s. Next account history sync is in {self._sync_interval_secs}s.')
