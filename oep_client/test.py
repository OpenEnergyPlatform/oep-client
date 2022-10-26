# coding: utf-8
import logging
import os
import random
import unittest

from . import OepClient

TOKEN_ENV_VAR = "OEP_API_TOKEN"
SCHEMA = "sandbox"
MAX_TRIES = 10
N_RECORDS = 1000


TEST_TABLE_DEFINITION = {
    "columns": [
        {
            "name": "id",
            "data_type": "bigserial",
            "is_nullable": False,
            "primary_key": True,
        },
        {"name": "field1", "data_type": "varchar(128)", "is_nullable": False},
        {"name": "field2", "data_type": "integer", "is_nullable": True},
    ]
}
TEST_TABLE_DATA = [
    {"field1": "test öäü 😀", "field2": None},  # some unicode data
]


def roundtrip(client, schema=SCHEMA, n_records=N_RECORDS):
    """
    * create table
    * upload data
    * download data
    * delete table
    """
    # create a random test table name that does not exist
    tries_left = MAX_TRIES
    while tries_left:
        table_name = "test_table_%s" % random.randint(0, 1000000000)
        if not client.table_exists(table_name, schema=schema):
            break
        if not tries_left:
            raise Exception(
                "Could not create a random test table name after %d tries" % MAX_TRIES
            )
    test_data = TEST_TABLE_DATA * n_records
    client.create_table(table_name, TEST_TABLE_DEFINITION, schema=schema)
    client.insert_into_table(table_name, test_data, schema=schema)
    data = client.select_from_table(table_name, schema=schema)
    client.drop_table(table_name, schema=schema)
    return data


class TestRoundtrip(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        token = os.environ.get(TOKEN_ENV_VAR)
        if not token:
            raise Exception(
                "In order to run the test, you must set the environment variable %s"
                % TOKEN_ENV_VAR
            )

        logging.basicConfig(
            format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
        )

        cls.client = OepClient(token=token)

    def test_roundtrip(self):
        data = roundtrip(self.client)
        logging.info(data)
        self.assertEqual(data, TEST_TABLE_DATA)
