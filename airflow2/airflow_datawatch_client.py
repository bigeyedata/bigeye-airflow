import logging
from typing import List


from bigeye_sdk.functions.table_functions import transform_table_list_to_dict
from bigeye_sdk.datawatch_client import DatawatchClient, Method
from bigeye_airflow.bigeye_requests.http_hook import get_hook
from airflow2.bigeye_airflow.models.configurations import CreateMetricConfiguration

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

    def get_asset_ix(connection_id: str, warehouse_id: int, conf: List[CreateMetricConfiguration]) -> dict:
        """
        Builds a case-insensitive, keyable index of assets needed by the CreateMetricConfiguration
        :param connection_id: name of connection in airflow with bigeye login info
        :param warehouse_id: int id of Bigeye warehouse
        :param conf: the CreateMetricConfiguration object
        :return: { <schema_name.lower>: { <table_name.lower>: <transformed_table_entry> }}
        """

        url = f"dataset/tables/{warehouse_id}/{schema_name}"

