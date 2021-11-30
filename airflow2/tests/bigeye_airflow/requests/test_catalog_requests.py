from unittest import TestCase
import os
import json

from bigeye_requests.catalog_requests import _transform_table_list_to_dict

DATASET_TABLES_RESPONSE_FILE = f"{os.getcwd()}/resources/dataset-tables-sample-response.json"
DATASET_TABLES_TRANSFORMED_OUTPUT_FILE = f"{os.getcwd()}/resources/dataset-tables-transformed-output.json"


def get_sample_response():
    with open(DATASET_TABLES_RESPONSE_FILE, 'r') as f:
        return json.load(f)


def get_sample_response_expected_transformed_output():
    with open(DATASET_TABLES_TRANSFORMED_OUTPUT_FILE, 'r') as f:
        return json.load(f)


class CatalogRequestUnitTests(TestCase):
    def test__transform_table_list_to_dict(self):
        sr = get_sample_response()
        eto = get_sample_response_expected_transformed_output()
        result = _transform_table_list_to_dict(sr)
        self.assertEqual(result, eto)
