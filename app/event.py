import asyncio
import simplejson as json

from asyncio.events import AbstractEventLoop
from botocore.exceptions import (
    ConnectTimeoutError as bccte,
    EndpointConnectionError as bcece
)
from dataclasses import dataclass

from tailucas_pylib import (
    app_config,
    creds,
    log,
    threads
)

from tailucas_pylib.app import AppThread
from tailucas_pylib.aws import boto3_session
from tailucas_pylib.handler import exception_handler
from tailucas_pylib.zmq import zmq_term, zmq_socket

from pymongo import MongoClient, InsertOne, DESCENDING
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError, OperationFailure

from telegram.ext import (
    Application,
    CallbackContext,
    ExtBot
)

from .database import (
    get_user_from_card,
    User
)

from typing import Tuple

@dataclass
class TransactionUpdate:
    user_id: int
    payload: dict


class CustomContext(CallbackContext[ExtBot, dict, dict, dict]):
    """
    Custom CallbackContext class that makes `user_data` available for updates of type
    `TransactionUpdate`.
    """
    @classmethod
    def from_update(
        cls,
        update: object,
        application: "Application",
    ) -> "CustomContext":
        if isinstance(update, TransactionUpdate):
            return cls(application=application, user_id=update.user_id)
        return super().from_update(update, application)


class SQSEvent(AppThread):

    def __init__(self, application: Application, mongodb_collection: Collection, queue_url: str, do_db_mutations: bool, remove_queued_messages: bool):
        super().__init__(name=self.__class__.__name__)
        self._application: Application = application
        self._mongodb_collection: Collection = mongodb_collection
        self._queue_url = queue_url
        self._do_db_mutations = do_db_mutations
        self._remove_queued_messages = remove_queued_messages

    async def create_event(self, telegram_user_id: int, payload: dict):
        log.debug(f'Generating bot event for Telegram user {telegram_user_id}...')
        await self._application.update_queue.put(TransactionUpdate(user_id=telegram_user_id, payload=payload))

    def unwrap_db_message(self, m: dict) -> Tuple[bool, dict]:
        if 'detail' in m.keys():
            log.info(f"{m['id']} {m['detail-type']} from {m['source']}")
            m_detail = m['detail']
            op_type = m_detail['operationType']
            if op_type != 'delete':
                if 'fullDocument' in m_detail:
                    return (True, m_detail['fullDocument'])
            else:
                log.warning(f'Ignoring event based on operation type {op_type}.')
            # nothing to unwrap
            return (True, None)
        return (False, m)

    def run(self):
        sqs_queue_name = self._queue_url.split('/')[-1]
        log.info(f'Creating SQS client for queue {sqs_queue_name}')
        sqs = boto3_session.client('sqs')
        while not threads.shutting_down:
            try:
                # Take the messages off the queue
                response = sqs.receive_message(
                    QueueUrl=self._queue_url,
                    AttributeNames=['All'],
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    VisibilityTimeout=30,
                    WaitTimeSeconds=10
                )
                if 'Messages' in response.keys():
                    for message in response['Messages']:
                        m = json.loads(message['Body'])
                        db_origin, doc = self.unwrap_db_message(m=m)
                        if doc and 'accountNumber' in doc and 'card' in doc:
                            account_number = doc['accountNumber']
                            card_id = int(doc['card']['id'])
                            log.debug(f'Transaction on card {card_id} to account {account_number}.')
                            db: User = asyncio.run(get_user_from_card(card_id=card_id))
                            # ensure that the event is on the application queue
                            if db:
                                log.debug(f'Card {card_id} belongs to Telegram user {db.telegram_user_id}')
                                duplicate_event = False
                                # but first, if the message is not of DB origin, then write it to the DB
                                if not db_origin:
                                    if self._do_db_mutations:
                                        log.info(f'Inserting transaction into MongoDB collection...')
                                        try:
                                            self._mongodb_collection.insert_one(m)
                                        except DuplicateKeyError as e:
                                            log.warning(f'Not inserting duplicate transaction into MongoDB collection due to {e.details}', exc_info=True)
                                            duplicate_event = True
                                    else:
                                        log.warning(f'Not inserting transaction into MongoDB collection due to feature flag or config.')
                                if not duplicate_event:
                                    log.info(f'Creating notification event for Telegram user {db.telegram_user_id}')
                                    asyncio.run(self.create_event(telegram_user_id=db.telegram_user_id, payload=doc))
                            else:
                                log.warning(f'Ignoring event for card ID {card_id} (account {account_number}) without an associated user.')
                        else:
                            log.warning(f'Ignoring event without transaction detail: {doc!s}.')
                        # de-queue the processed message
                        message_handle = message['ReceiptHandle']
                        if self._remove_queued_messages:
                            log.debug(f'Removing message {message_handle} from queue.')
                            # remove the message from the queue
                            sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message_handle)
                        else:
                            log.warning(f'Not removing message {message_handle} from queue due to feature flag or config.')
            except (bcece, bccte, OperationFailure):
                log.warning(f'SQS', exc_info=True)
                threads.interruptable_sleep.wait(10)
