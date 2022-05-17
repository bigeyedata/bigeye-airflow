from abc import abstractmethod

from airflow.models import BaseOperator
from bigeye_sdk.client.datawatch_client import DatawatchClient


class ClientExtensibleOperator(BaseOperator):
    @abstractmethod
    def get_client(self) -> DatawatchClient:
        pass
