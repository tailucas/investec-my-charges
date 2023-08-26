import requests

from typing import Dict, Optional, Tuple

from pylib import app_config, log
from pylib.app import ZmqWorker
from pylib.zmq import zmq_socket

import zmq
from zmq.asyncio import Socket


URL_WORKER_CURRENCY_CONVERTER = 'inproc://currency-converter'
URL_WEB_CONVERTER_PREFIX = 'https://api.exchangerate.host/'


class CurrencyConverter(ZmqWorker):

    def __init__(self, int_curr_symbol: str, currency_symbol: str):
        super().__init__(name=self.__class__.__name__, worker_zmq_url=URL_WORKER_CURRENCY_CONVERTER)
        self._currency_map: dict = {}
        self._int_curr_symbol: str = int_curr_symbol
        self._currency_symbol: str = currency_symbol

    def process_message(self, message: Dict) -> Dict:
        log.debug(f'Processing {message=}')
        params = message['params']
        params['symbols'] = self._int_curr_symbol
        currency = params['base']
        rate = 1
        data = None
        function_path = message['function_path']
        if currency != self._int_curr_symbol:
            currency_map_key = f'{currency}:{function_path}'
            if currency_map_key not in self._currency_map.keys():
                log.debug(f'Making request to convert {self._int_curr_symbol} to {currency} for date {function_path}.')
                response = requests.get(
                    url=URL_WEB_CONVERTER_PREFIX+function_path,
                    params=params)
                data = response.json()
                log.debug(f'Currency response is {data}')
                rate = float(data['rates'][self._int_curr_symbol])
                self._currency_map[currency_map_key] = rate
            else:
                rate = self._currency_map[currency_map_key]
        return {
            'int_curr_symbol': self._int_curr_symbol,
            'currency_symbol': self._currency_symbol,
            'rate': rate,
            'data': data,
        }


async def local_currency(charge_cents: int, charge_currency: str, charge_date: Optional[str] = None) -> float:
    currency_converter: Socket = zmq_socket(zmq.REQ, is_async=True)
    currency_converter.connect(addr=URL_WORKER_CURRENCY_CONVERTER)
    function_path = 'latest'
    if charge_date:
        function_path = charge_date
    currency_query = {
        'function_path': function_path,
        'params': {
            'base': charge_currency,
            'amount': 1
        }
    }
    await currency_converter.send_pyobj(currency_query)
    response = await currency_converter.recv_pyobj()
    currency_converter.close()
    rate: float = response['rate']
    charge_cents_local_currency = rate * charge_cents
    int_curr_symbol = response['int_curr_symbol']
    currency_symbol = response['currency_symbol']
    log.debug(f'Converted {charge_cents}c {charge_currency} to {charge_cents_local_currency}c {int_curr_symbol} ({currency_symbol}) ({rate=})')
    return charge_cents_local_currency