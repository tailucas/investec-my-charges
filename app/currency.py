import requests

from datetime import datetime
from requests.exceptions import RequestException
from typing import Dict, Optional, Tuple

from tailucas_pylib import (
    creds,
    log
)
from tailucas_pylib.app import ZmqWorker
from tailucas_pylib.zmq import zmq_socket

import zmq
from zmq.asyncio import Socket


URL_WORKER_CURRENCY_CONVERTER = 'inproc://currency-converter'
URL_WEB_CONVERTER_PREFIX = 'http://api.exchangerate.host/'


class CurrencyConverter(ZmqWorker):

    def __init__(self, int_curr_symbol: str, currency_symbol: str):
        super().__init__(name=self.__class__.__name__, worker_zmq_url=URL_WORKER_CURRENCY_CONVERTER)
        self._currency_map: dict = {}
        self._int_curr_symbol: str = int_curr_symbol
        self._currency_symbol: str = currency_symbol

    def process_message(self, message: Dict) -> Dict:
        log.debug(f'Processing {message=}')
        params = message['params']
        params['currencies'] = self._int_curr_symbol
        currency = params['source']
        rate = 1
        data = None
        if currency != self._int_curr_symbol:
            if 'date' in params:
                currency_map_key = f'{currency}:{params["date"]}'
            else:
                today = datetime.today()
                currency_map_key = f'{currency}:{today.strftime("%Y-%m-%d")}'
            function_path = message['function_path']
            if currency_map_key not in self._currency_map.keys():
                log.debug(f'Making request to convert {self._int_curr_symbol} to {currency} for {function_path}.')
                error_message = f'Issue with request to {URL_WEB_CONVERTER_PREFIX}'
                try:
                    params['access_key'] = creds.exchangerate_host
                    response = requests.get(
                        url=URL_WEB_CONVERTER_PREFIX+function_path,
                        params=params)
                    response.raise_for_status()
                except RequestException as e:
                    raise AssertionError(error_message) from e
                data = response.json()
                request_success = False
                if 'success' in data:
                    request_success = data['success']
                if not request_success:
                    if 'error' in data and 'info' in data['error']:
                        error_message = data['error']['info']
                    raise AssertionError(error_message)
                log.debug(f'Currency response is {data}')
                rate = float(data['quotes'][f'{currency}{self._int_curr_symbol}'])
                self._currency_map[currency_map_key] = rate
            else:
                rate = self._currency_map[currency_map_key]
                log.debug(f'Returning previously fetched {function_path} rate {rate} for {currency} to {self._int_curr_symbol}.')
        return {
            'int_curr_symbol': self._int_curr_symbol,
            'currency_symbol': self._currency_symbol,
            'rate': rate,
            'data': data,
        }


async def local_currency(charge_cents: int, charge_currency: str, charge_date: Optional[str] = None) -> float:
    currency_converter: Socket = zmq_socket(zmq.REQ, is_async=True)
    currency_converter.connect(addr=URL_WORKER_CURRENCY_CONVERTER)
    currency_query = {
        'params': {
            'amount': 1,
            'source': charge_currency
        }
    }
    function_path = 'live'
    if charge_date:
        function_path = 'historical'
        currency_query['params']['date'] = charge_date
    currency_query['function_path'] = function_path
    await currency_converter.send_pyobj(currency_query)
    response = await currency_converter.recv_pyobj()
    currency_converter.close()
    rate: float = response['rate']
    charge_cents_local_currency = rate * charge_cents
    int_curr_symbol = response['int_curr_symbol']
    currency_symbol = response['currency_symbol']
    log.debug(f'Converted {charge_cents}c {charge_currency} to {charge_cents_local_currency}c {int_curr_symbol} ({currency_symbol}) ({rate=})')
    return charge_cents_local_currency