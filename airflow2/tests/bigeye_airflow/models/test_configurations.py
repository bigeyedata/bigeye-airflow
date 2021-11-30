from unittest import TestCase

from models.configurations import CreateMetricConfiguration


class TestCreateMetricConfiguration(TestCase):
    def test_create_metric_conf_defaults(self):
        c = CreateMetricConfiguration(
            table_name='test_table',
            schema_name='test_schema',
            column_name='test_column',
            metric_name='test_metric_name'
        )

        self.assertEqual(c.notifications, [])
        self.assertEqual(c.thresholds, [])
        self.assertEqual(c.filters, [])
        self.assertEqual(c.group_by, [])


