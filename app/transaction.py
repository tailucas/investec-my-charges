import asyncio
import html
import simplejson as json
import time
import zmq

from typing import Dict, Optional, Tuple, Sequence, List
from zmq.error import ZMQError, ContextTerminated, Again

from pylib import log
from pylib.app import AppThread, Closable
from pylib.handler import exception_handler
from pylib.threads import shutting_down, interruptable_sleep

from pymongo import MongoClient, InsertOne
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


class TransactionHistory(AppThread, Closable):

        def __init__(self, mongodb_collection: Collection, sync_interval: int):
            AppThread.__init__(self, name=self.__class__.__name__)
            Closable.__init__(self, connect_url=URL_WORKER_TRANSACTION_HISTORY, socket_type=zmq.REP, do_connect=False)
            self._mongodb_collection: Collection = mongodb_collection
            self._last_sync: Optional[int] = None
            self._sync_interval_secs: int = sync_interval

        def run(self):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            with exception_handler(closable=self, and_raise=False, close_on_exit=True) as zmq_socket:
                while not shutting_down:
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
                        interruptable_sleep.wait(1)
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
                            log.info(f'Processing account ID {account.account_id} for Telegram user {user.telegram_user_id}...')
                            response = client.get_account_transactions(account_id=account.account_id)
                            log.debug(f'Accounts response: {response!s}')
                            if access_token is None or client.access_token != access_token[0]:
                                log.debug(f'Persisting access token...')
                                asyncio.run(update_access_token(
                                    telegram_user_id=user.telegram_user_id,
                                    user_id=user.id,
                                    access_token=client.access_token,
                                    access_token_expiry=client.access_token_expiry))
                            log.info(f'Fetched {len(response)} results from API. Collecting keys...')
                            post_list = []
                            for tran in response:
                                account_id = tran['accountId']
                                if account_id != account.account_id:
                                    raise AssertionError(f'Expected account {account.account_id} from API but got {account_id}.')
                                post_list.append(int(tran['postedOrder']))
                            log.info(f'Account ID {account.account_id} has {len(post_list)} posted transactions.')
                            # fetch associated transaction data
                            mongo_query = {
                                "accountId": {
                                    "$eq": account.account_id
                                },
                                "postedOrder": {
                                    "$in": post_list
                                }
                            }
                            projection = {}
                            sort = []
                            log.debug(f'Fetching data from MongoDB collection...')
                            cursor = self._mongodb_collection.find(mongo_query, projection=projection, sort=sort)
                            post_list = []
                            for doc in cursor:
                                account_id = doc['accountId']
                                if account_id != account.account_id:
                                    raise AssertionError(f'Expected account {account.account_id} from MongoDB but got {account_id}.')
                                post_list.append(int(doc['postedOrder']))
                            log.info(f'MongoDB contains {len(post_list)} posted transactions for account ID {account.account_id}.')
                            updates: List = []
                            post_set = set(post_list)
                            for tran in response:
                                if int(tran['postedOrder']) not in post_set:
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
            loop.close()