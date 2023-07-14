# coding: utf-8
import json
import logging
import os
import random
import unittest

from . import TOKEN_ENV_VAR, OepClient

SCHEMA = "sandbox"
MAX_TRIES = 10
N_RECORDS = 1000


TEST_TABLE_DEFINITION = {
    "columns": [
        {
            "name": "id",
            "type": "integer",
            "data_type": "bigserial",
            "is_nullable": False,
            "primary_key": True,
        },
        {
            "name": "field1",
            "type": "string",
            "data_type": "varchar(128)",
            "is_nullable": False,
        },
        {"name": "field2", "type": "integer", "is_nullable": True},
    ]
}


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

    def test_roundtrip(self, client=None, schema=None):
        """
        * create table
        * upload data
        * download data
        * delete table
        """

        client = client or self.client
        schema = schema or SCHEMA

        # generate test data
        test_data = [{"field1": "test öäü 😀", "field2": i} for i in range(N_RECORDS)]

        # create a random test table name that does not exist
        tries_left = MAX_TRIES
        while tries_left:
            table_name = "test_table_%s" % random.randint(0, 1000000000)
            if not client.table_exists(table_name, schema=schema):
                break
            if not tries_left:
                raise Exception(
                    "Could not create a random test table name after %d tries"
                    % MAX_TRIES
                )
        client.create_table(table_name, TEST_TABLE_DEFINITION, schema=schema)
        client.insert_into_table(table_name, test_data, schema=schema)
        data = client.select_from_table(table_name, schema=schema)

        # also test where
        data_partial = client.select_from_table(
            table_name, schema=schema, where=["field2>0", "field2<2"]
        )
        self.assertEqual(len(data_partial), 1)
        self.assertEqual(data_partial[0]["field2"], 1)

        client.drop_table(table_name, schema=schema)

        # remove generated id columns
        for row in data:
            del row["id"]

        # test equality
        self.assertEqual(
            json.dumps(test_data, sort_keys=True), json.dumps(data, sort_keys=True)
        )
