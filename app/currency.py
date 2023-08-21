import requests

from typing import Dict

from pylib import log
from pylib.app import ZmqWorker


URL_WORKER_CURRENCY_CONVERTER = 'inproc://currency-converter'
URL_WEB_CONVERTER_PREFIX = 'https://api.exchangerate.host/'


class CurrencyConverter(ZmqWorker):

    def __init__(self, home_currency):
        super().__init__(name=self.__class__.__name__, worker_zmq_url=URL_WORKER_CURRENCY_CONVERTER)
        self._currency_map = {}
        self._home_currency = home_currency

    def process_message(self, message: Dict) -> Dict:
        log.debug(f'Processing {message=}')
        params = message['params']
        currency = params['base']
        rate = 1
        data = None
        if currency != self._home_currency:
            if currency not in self._currency_map.keys():
                log.debug(f'Making request to convert {self._home_currency} to {currency}.')
                response = requests.get(
                    url=URL_WEB_CONVERTER_PREFIX+message['function_path'],
                    params=params)
                data = response.json()
                log.debug(f'Currency response is {data}')
                rate = float(data['rates'][self._home_currency])
                self._currency_map[currency] = rate
            else:
                rate = self._currency_map[currency]
        return {
            'rate': rate,
            'data': data
        }
