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


from telegram.ext import (
    Application,
    CallbackContext,
    ExtBot
)

from .database import (
    get_user_from_card,
    User
)

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

    def __init__(self, application: Application):
        super().__init__(name=self.__class__.__name__)
        self._application: Application = application

    async def create_event(self, telegram_user_id: int, payload: dict):
        log.debug(f'Generating bot event for Telegram user {telegram_user_id}...')
        await self._application.update_queue.put(TransactionUpdate(user_id=telegram_user_id, payload=payload))

    def run(self):
        sqs_queue_name = app_config.get('aws', 'sqs_queue_name')
        log.info(f'Creating SQS client for queue {sqs_queue_name}')
        sqs = boto3_session.client('sqs')
        sqs_queue_url = app_config.get('aws', 'sqs_queue_url')
        while not threads.shutting_down:
            try:
                # Take the messages off the queue
                response = sqs.receive_message(
                    QueueUrl=sqs_queue_url,
                    AttributeNames=['All'],
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    VisibilityTimeout=30,
                    WaitTimeSeconds=10
                )
                if 'Messages' in response.keys():
                    for message in response['Messages']:
                        m = json.loads(message['Body'])
                        log.info(f"{m['id']} {m['detail-type']} from {m['source']}")
                        m_detail = m['detail']
                        op_type = m_detail['operationType']
                        if op_type != 'delete':
                            if 'fullDocument' in m_detail:
                                doc = m_detail['fullDocument']
                                account_number = doc['accountNumber']
                                card_id = int(doc['card']['id'])
                                log.debug(f'Transaction on card {card_id} to account {account_number}.')
                                db: User = asyncio.run(get_user_from_card(card_id=card_id))
                                log.debug(f'Card {card_id} belongs to Telegram user {db.telegram_user_id}')
                                # ensure that the event is on the application queue
                                if db:
                                    asyncio.run(self.create_event(telegram_user_id=db.telegram_user_id, payload=doc))
                            else:
                                log.warning(f'Ignoring event without document detail.')
                        else:
                            log.warning(f'Ignoring event based on operation type {op_type}.')
                        # de-queue the processed message
                        message_handle = message['ReceiptHandle']
                        log.debug(f'Removing message {message_handle} from queue.')
                        # remove the message from the queue
                        sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=message_handle)
            except (bcece, bccte):
                log.warning(f'SQS', exc_info=True)
                threads.interruptable_sleep.wait(10)
