from unittest import TestCase

from airflow1.bigeye_airflow.operators.create_metric_operator import get_case_sensitive_field_name

sample_table_metadata = {
        "id": 1225460,
        "schemaName": "DEMO_DB.PUBLIC",
        "datasetName": "SGA_EU_P2_EBS_INPUT_STAGING_P9",
        "fields": [
            {
                "id": 11577745,
                "fieldName": "co_object_Name",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577745,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577746,
                "fieldName": "co_code",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577746,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577747,
                "fieldName": "cost_ctr",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577747,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577748,
                "fieldName": "cost_elem",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577748,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577749,
                "fieldName": "cost_element_descr",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577749,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577750,
                "fieldName": "cost_element_name",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577750,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577751,
                "fieldName": "doc_date",
                "type": "DATE_LIKE",
                "nullable": True,
                "entityId": 11577751,
                "partitionField": False,
                "canBeLoadedDate": True,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577752,
                "fieldName": "doc_header_text",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577752,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577753,
                "fieldName": "document_no",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577753,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577754,
                "fieldName": "year",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577754,
                "partitionField": False,
                "canBeLoadedDate": True,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577755,
                "fieldName": "name_off_acc",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577755,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577756,
                "fieldName": "func_area",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577756,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577757,
                "fieldName": "name",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577757,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577758,
                "fieldName": "object_currency",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577758,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577759,
                "fieldName": "off_acc_type",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577759,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577760,
                "fieldName": "off_acct_no",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577760,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577761,
                "fieldName": "per",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577761,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577762,
                "fieldName": "postg_date",
                "type": "DATE_LIKE",
                "nullable": True,
                "entityId": 11577762,
                "partitionField": False,
                "canBeLoadedDate": True,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577763,
                "fieldName": "purch_doc",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577763,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577764,
                "fieldName": "ref_tran",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577764,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577765,
                "fieldName": "user",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577765,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577766,
                "fieldName": "value_tcur",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577766,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577767,
                "fieldName": "ref_doc_no",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577767,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577768,
                "fieldName": "created_dttm",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577768,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577769,
                "fieldName": "primary_key_md5_value",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577769,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577770,
                "fieldName": "region",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577770,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577771,
                "fieldName": "source_system_name",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577771,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577772,
                "fieldName": "country",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577772,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577773,
                "fieldName": "subject_area",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577773,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577774,
                "fieldName": "company_cd",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577774,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577775,
                "fieldName": "file_name",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577775,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            },
            {
                "id": 11577776,
                "fieldName": "file_created_dttm",
                "type": "STRING",
                "nullable": True,
                "entityId": 11577776,
                "partitionField": False,
                "canBeLoadedDate": False,
                "loadedDateField": False,
                "entityType": "DatasetField"
            }
        ],
        "schema": {
            "id": 10572,
            "name": "DEMO_DB.PUBLIC",
            "warehouse": {
                "id": 498,
                "username": "SVC-MAP-BIGEYE-POV@ECOLAB.COM",
                "databaseName": "ENT_UKIE_COM_PRD_SVC_ANALYTICS_WH",
                "hostname": "ecolab.east-us-2.azure.snowflakecomputing.com",
                "name": "Snowflake",
                "warehouseVendor": "Snowflake",
                "company": {
                    "id": 324,
                    "name": "Ecolab",
                    "entityId": 324,
                    "warehouseIds": [
                        498
                    ],
                    "entityType": "Company"
                },
                "queryTimeoutSeconds": 600,
                "allSchemaNames": [
                    "DEMO_DB.APAC_IL",
                    "DEMO_DB.APAC_RAWDB",
                    "DEMO_DB.AZ_STAGE",
                    "DEMO_DB.COM_AUDIT",
                    "DEMO_DB.EG_COM_UKIE_PREP",
                    "DEMO_DB.EG_COM_UKIE_RAW",
                    "DEMO_DB.EZ_STAGE",
                    "DEMO_DB.PUBLIC",
                    "DEMO_DB.base_model",
                    "EG_PRD_LOG_DB.APP",
                    "EG_PRD_MD_DB.CATALOG",
                    "EG_PRD_PREP_DB.BASE_MODEL",
                    "EG_PRD_PREP_DB.INTEGRATION",
                    "ENT_UKIE_COM_PRD_ANALYTICS_DB.INSIGHT_REPORT",
                    "ENT_UKIE_COM_PRD_ANALYTICS_DB.PUBLIC",
                    "ENT_UKIE_COM_PRD_ANALYTICS_DB.SHARED_SPECS",
                    "ENT_UKIE_COM_PRD_COMMON_DB.ADMIN",
                    "ENT_UKIE_COM_PRD_COMMON_DB.PUBLIC",
                    "ENT_UKIE_COM_PRD_COMMON_DB.UTIL",
                    "ENT_UKIE_COM_PRD_LOG_DB.ACCT_USAGE",
                    "ENT_UKIE_COM_PRD_LOG_DB.APP",
                    "ENT_UKIE_COM_PRD_LOG_DB.PUBLIC",
                    "ENT_UKIE_COM_PRD_MD_DB.CATALOG",
                    "ENT_UKIE_COM_PRD_MD_DB.PUBLIC",
                    "ENT_UKIE_COM_PRD_MD_DB.SC_ENT_UKIE_COM",
                    "ENT_UKIE_COM_PRD_PREP_DB.BASE_MODEL",
                    "ENT_UKIE_COM_PRD_PREP_DB.EXT_MODEL",
                    "ENT_UKIE_COM_PRD_PREP_DB.INTEGRATION",
                    "UTIL_DB.PUBLIC"
                ],
                "warehouseType": "snowflake",
                "columnar": True,
                "redshiftWarehouse": False,
                "entityId": 498,
                "entityType": "Warehouse"
            },
            "entityId": 10572,
            "entityType": "Schema"
        },
        "datasetFieldCount": 32,
        "fqtn": "DEMO_DB.PUBLIC.SGA_EU_P2_EBS_INPUT_STAGING_P9",
        "entityId": 1225460,
        "partitioned": False,
        "columnNames": [
            "co_object_name",
            "co_code",
            "cost_ctr",
            "cost_elem",
            "cost_element_descr",
            "cost_element_name",
            "doc_date",
            "doc_header_text",
            "document_no",
            "year",
            "name_off_acc",
            "func_area",
            "name",
            "object_currency",
            "off_acc_type",
            "off_acct_no",
            "per",
            "postg_date",
            "purch_doc",
            "ref_tran",
            "user",
            "value_tcur",
            "ref_doc_no",
            "created_dttm",
            "primary_key_md5_value",
            "region",
            "source_system_name",
            "country",
            "subject_area",
            "company_cd",
            "file_name",
            "file_created_dttm"
        ],
        "entityType": "Dataset"
    }


class TestMetadataFunctions(TestCase):
    def test_case_sensitive_field_name(self):
        result = get_case_sensitive_field_name(table=sample_table_metadata, inbound_field_name='co_object_name')
        self.assertEqual("co_object_Name", result)

        result = get_case_sensitive_field_name(table=sample_table_metadata, inbound_field_name='Co_Code')
        self.assertEqual("co_code", result)
