# coding: utf-8
import json
import logging
import os
import random
import unittest

from . import TOKEN_ENV_VAR, OepClient
from .exceptions import OepClientSideException

MAX_TRIES_FIND_RANDOM_TEST_TABLE = 10


TEST_TABLE_DEFINITION = {
    "columns": [
        # {
        #     "name": "id",
        #     "type": "integer",
        #     "data_type": "bigserial",
        #     "is_nullable": False,
        #     "primary_key": True,
        # },
        {
            "name": "field1",
            "type": "string",
            "data_type": "varchar(128)",
            "is_nullable": False,
        },
        {"name": "field2", "type": "integer", "is_nullable": True},
    ],
    "constraints": [{"constraint_type": "UNIQUE", "columns": ["field1"]}],
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

        cls.client = OepClient(token=token)

    def test_roundtrip(self, client=None):
        """
        * create table
        * upload data
        * download data
        * delete table
        """

        client = client or self.client

        # generate test data
        test_data = [
            {"field1": "test unicode 😀"},
            {"field1": "k1", "field2": 1},
            {"field1": "k2", "field2": 2},
            {"field1": "k3", "field2": 3},
        ]

        # create a random test table name that does not exist
        tries_left = MAX_TRIES_FIND_RANDOM_TEST_TABLE
        while tries_left:
            table_name = "test_table_%s" % random.randint(0, 1000000000)
            if not client.table_exists(table_name):
                break
            if not tries_left:
                raise Exception(
                    "Could not create a random test table name after %d tries"
                    % MAX_TRIES_FIND_RANDOM_TEST_TABLE
                )

        tdef = client.create_table(table_name, TEST_TABLE_DEFINITION, is_sandbox=True)
        logging.info(tdef)

        rcount = client.insert_into_table(table_name, test_data)

        # insert second time should fail because unique constraint
        self.assertRaises(
            OepClientSideException,
            client.insert_into_table,
            table_name,
            test_data,
        )

        self.assertEqual(rcount, len(test_data))
        data = client.select_from_table(table_name)
        # remove generated id columns
        for row in data:
            del row["id"]

        # test equality
        self.assertEqual(
            json.dumps(test_data, sort_keys=True), json.dumps(data, sort_keys=True)
        )

        # also test where
        data_partial = client.select_from_table(
            table_name, where=["field2>1", "field2<3"]
        )
        self.assertEqual(len(data_partial), 1)
        self.assertEqual(data_partial[0]["field2"], 2)

        # test count_rows
        self.assertEqual(client.count_rows(table_name), len(test_data))

        # test publish: must have license
        metadata = {
            "resources": [{"licenses": [{"name": "CC0"}]}],
            "metaMetadata": {"metadataVersion": "OEMetadata-2.0.0"},
        }
        client.set_metadata(table_name, metadata)
        client.publish_table(table_name, "scenario")
        client.unpublish_table(table_name)

        # delete data
        client.delete_from_table(table_name)
        self.assertEqual(client.count_rows(table_name), 0)

        # drop table
        client.drop_table(table_name)


class TestUtils(unittest.TestCase):
    """functions without token auth"""

    @classmethod
    def setUpClass(cls):
        cls.client = OepClient()

    def test_iter_tables(self):
        self.assertTrue(
            all(set(["table"]) == set(x) for x in self.client.iter_tables())
        )
