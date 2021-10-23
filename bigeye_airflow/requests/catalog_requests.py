from typing import List, Dict

from bigeye_airflow.models.configurations import CreateMetricConfiguration
from bigeye_airflow.requests import get_hook


def _transform_table_field_list_to_dict(table: dict) -> dict:
    """
    Converts the table['fields'] list to a dictionary of { <tableName.lower>: <field_entry> } for quick, easy, case
    insensitive keying.
    :param table: a dictionary representing a dataset in Bigeye derived from the dataset/tables endpoint.
    :return: the modified table entriy
    """
    table['fields'] = {f['fieldName'].lower(): f for f in table['fields']}
    return table


def _get_schema_tables_json(connection_id: str, warehouse_id: int, schema_name: str) -> Dict[str, dict]:
    """
    Calls the dataset/tables/{warehouse_id}/{schema_name} API endpoint then transforms the result to include lower case
    table and column names for quick, easy, case insensitive keying.
    :param connection_id: url base address???
    :param warehouse_id: int id of Bigeye warehouse
    :param schema_name: name of the schema for which to query tables.
    :return: { <table_name.lower>: <transformed_table_entry>
    """
    hook = get_hook(connection_id, 'GET')
    tables = hook.run("dataset/tables/{warehouse_id}/{schema_name}"
                      .format(warehouse_id=warehouse_id,
                              schema_name=schema_name),
                      headers={"Accept": "application/json"}).json()

    return {t['datasetName'].lower(): _transform_table_field_list_to_dict(t) for t in tables}


def get_asset_ix(connection_id: str, warehouse_id: int, conf: List[CreateMetricConfiguration]) -> dict:
    """
    Builds a case insensitive, keyable index of assets needed by the create metric configuration.
    :param connection_id: url base address???
    :param warehouse_id: int id of Bigeye warehouse
    :param conf: the create metric configuration
    :return: { <schmea_name.lower>: { <table_name.lower>: <transformed_table_entry> }}
    """
    return {sn.lower(): _get_schema_tables_json(connection_id, warehouse_id, sn)
            for sn in {c.schema_name for c in conf}}