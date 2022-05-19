from typing import Any, Optional, List

from airflow.models import BaseOperator


class CreateSlaOperator(BaseOperator):
    """
        The CreateSlaOperator will create a collection in Bigeye. Collections are labeled as SLAs in Bigeye's UI.
    """

    def __init__(self,
                 connection_id: str,
                 sla_name: str,
                 description: Optional[str] = None,
                 notification_channels: Optional[List[dict]] = None,
                 muted_until: Optional[int] = None,
                 metric_ids: Optional[List[int]] = None,
                 *args,
                 **kwargs):
        """
            param connection_id: str referencing a defined connection in the Airflow deployment.
            param sla_name: str the name of  the SLA to display in Bigeye
            param description: Optional[str] description of the SLA
            param notification_channels: Optional[List[dict]] notification channels to set for the SLA
            param muted_until: int unix timestamp to keep notifications muted
            param metric_ids: Optional[List[int]] list of metric IDs to add to the SLA.
            param args: not currently supported
            param kwargs: not currently supported
        """
        super(CreateSlaOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.sla_name = sla_name
        self.description = description
        self.notification_channels = notification_channels
        self.muted_until = muted_until
        self.metric_ids = metric_ids

    def execute(self, context: Any):
        pass
