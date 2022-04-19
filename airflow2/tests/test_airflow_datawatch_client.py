from unittest import TestCase
from unittest.mock import patch, Mock

from bigeye_sdk.datawatch_client import Method
from bigeye_airflow.airflow_datawatch_client import AirflowDatawatchClient


class TestAirflowDatawatchClient(TestCase):

    def setUp(self):
        self.client = AirflowDatawatchClient("test")

    @patch.object(AirflowDatawatchClient, '_call_datawatch', autospec=True)
    def test_datawatch_run(self, mock_get_run):
        mock_get_run.return_value = {'metric': 'config'}
        response = self.client._call_datawatch(method=Method.GET, url="/api/v1/metrics")
        self.assertEqual(response, {'metric': 'config'})
        mock_get_run.assert_called()

    @patch.object(AirflowDatawatchClient, '_call_datawatch', autospec=True, side_effect=Exception('Test'))
    def test_datawatch_fail(self, mock_run_fail):
        with self.assertRaises(Exception):
            response = self.client._call_datawatch(method=Method.GET, url="/api/v1/metrics")
            self.assertFalse(response)
            mock_run_fail.assert_called()
