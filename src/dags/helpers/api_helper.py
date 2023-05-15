from dataclasses import dataclass, asdict
import logging
import requests
import json
from urllib.parse import urljoin
from airflow.hooks.base_hook import BaseHook


@dataclass
class Params:
    sort_field: str = '_id'
    sort_direction: str = 'asc'
    limit: str = '50'
    offset: str = '0'

    def as_string(self):
        return json.dumps(self.__dict__)

    def as_dict(self):
        return self.__dict__

    def offset_inc(self, val: int):
        self.offset = str(int(self.offset) + val)


@dataclass
class CourierParams(Params):
    sort_field: str = '_id'
    sort_direction: str = 'asc'
    limit: str = '50'
    offset: str = '0'


@dataclass
class DeliveryParams(Params):
    sort_field: str = 'from_date'
    sort_direction: str = 'asc'
    limit: str = '50'
    offset: str = '0'
    restaurant_id: str = ''
    from_date: str = '2022-01-01 00:00:00'
    to_date: str = ''


class APIHelper:
    def __init__(self, connection_id):
        self.connection_id = connection_id
        self.url = self._get_url()
        self.headers = self._get_headers()

    def _get_url(self):
        connection = BaseHook.get_connection(self.connection_id)
        return f"{connection.schema}://{connection.host}"

    def _get_headers(self):
        connection = BaseHook.get_connection(self.connection_id)
        extra = connection.extra or '{}'
        try:
            extra_data = json.loads(extra)
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid extra data for connection_id {self.connection_id}. "
                "Expected JSON format.")

        headers = {
            'X-Nickname': extra_data.get('nickname'),
            'X-Cohort': str(extra_data.get('cohort')),
            'X-API-KEY': extra_data.get('api_key')
        }

        if not all(headers.values()):
            raise ValueError(
                f"Missing required headers in extra data for connection_id {self.connection_id}. "
                "Expected fields: nickname, cohort, api_key")

        return headers

    def _send_request(self, endpoint, params=None):
        full_url = urljoin(self.url, endpoint)
        response = requests.get(full_url, headers=self.headers,
                                params=asdict(params))
        response.raise_for_status()
        data = response.json()

        logging.info(f"Запрос с параметрами  {params} вернул {len(data)} строк")

        return [{"json_value": json.dumps(row)} for row in data]

    @staticmethod
    def get_params(method_name, **kwargs):
        if method_name == 'couriers':
            return CourierParams(**kwargs)
        elif method_name == 'deliveries':
            return DeliveryParams(**kwargs)
        else:
            raise ValueError(f"Invalid method name: {method_name}")

    def get_data(self, method_name, params):
        if method_name == 'restaurants':
            return self.get_restaurants(params)
        elif method_name == 'couriers':
            return self.get_couriers(params)
        elif method_name == 'deliveries':
            return self.get_deliveries(params)
        else:
            raise ValueError(f"Invalid method name: {method_name}")

    def get_restaurants(self, params):
        return self._send_request('restaurants', params)

    def get_couriers(self, params):
        return self._send_request('couriers', params)

    def get_deliveries(self, params):
        return self._send_request('deliveries', params)
