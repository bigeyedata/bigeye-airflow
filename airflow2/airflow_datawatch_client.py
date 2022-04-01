import logging
from typing import List

from bigeye_sdk.datawatch_client import DatawatchClient, Method
from bigeye_airflow.bigeye_requests.http_hook import get_hook


headers = {"Content-Type": "application/json", "Accept": "application/json"}


class AirflowDatawatchClient(DatawatchClient):
    def __init__(self, connection_id: str):
        self.conn_id = connection_id
        pass

    def _call_datawatch(self, method: Method, url, body: str = None):
        bigeye_request_hook = get_hook(self.conn_id, method.name)
        try:
            response = bigeye_request_hook.run(
                endpoint=url,
                headers=headers,
                data=body)

        except Exception as e:
            logging.error(f'Exception calling airflow datawatch: {str(e)}')
            raise e

        if response.status_code != 204:
            return response.json()

    def get_tables_for_schema(self, warehouse_id: int, schema_name: str) -> List[dict]:

        url = f"dataset/tables/{warehouse_id}/{schema_name}"
        return self._call_datawatch(Method.GET, url)
