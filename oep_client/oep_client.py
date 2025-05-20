"""Small client implementation to use the oep API.

Example usage: create a table, insert data, retrieve data, delete table

cli = OepClient(token='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

table = 'my_awesome_test_table'
definition = {
    "columns": [
        {"name": "id", "data_type": "bigint", "is_nullable": False, "primary_key": True},
        {"name": "field1", "data_type": "varchar(128)", "is_nullable": False},
        {"name": "field2", "data_type": "integer", "is_nullable": True}
    ]
}
data =  [
    {'id': 1, 'field1': 'test', 'field2': 100},
    {'id': 2, 'field1': 'test2', 'field2': None}
]

cli.create_table(table, definition)
cli.insert_into_table(table, data)
return_data = cli.select_from_table(table)
cli.drop_table(table)

"""  # noqa

import functools
import json
import logging
import math
import re

import click
import pandas as pd
import requests

from .advanced_api import AdvancedApiSession

# from .dialect import get_sqlalchemy_table
from .exceptions import (
    OepAuthenticationException,
    OepClientSideException,
    OepServerSideException,
    OepTableAlreadyExistsException,
    OepTableNotFoundException,
)
from .utils import dataframe_to_records, fix_table_definition

DEFAULT_HOST = "openenergyplatform.org"
DEFAULT_PROTOCOL = "https"
DEFAULT_API_VERSION = "v0"
DEFAULT_SCHEMA = "model_draft"
DEFAULT_BATCH_SIZE = 5000
DEFAULT_INSERT_RETRIES = 10
TOKEN_ENV_VAR = "OEP_API_TOKEN"


def check_exception(pattern, exception):
    """create decorator for custom Exceptions."""

    def decorator(fun):
        @functools.wraps(fun)
        def _fun(*args, **kwargs):
            try:
                return fun(*args, **kwargs)
            except Exception as exc:
                msg = str(exc)
                if re.match(".*" + pattern, msg, re.IGNORECASE):
                    raise exception(msg)
                raise

        return _fun

    return decorator


class OepClient:
    """Small client implementation to use the oep API."""

    def __init__(
        self,
        token=None,
        protocol=DEFAULT_PROTOCOL,
        host=DEFAULT_HOST,
        api_version=DEFAULT_API_VERSION,
        default_schema=DEFAULT_SCHEMA,
        batch_size=DEFAULT_BATCH_SIZE,
        insert_retries=DEFAULT_INSERT_RETRIES,
    ):
        """
        Args:
            token(str): your API token
            host(str, optional): default is "https". "http" may be used for
              local installations
            host(str, optional): host of the oep platform.
              default is "openenergyplatform.org"
            api_version(str, optional): currently only "v0"
            default_schema(str, optional): the default schema for the tables,
              usually "model_draft"
            batch_size(int, optional): number of records that will be uploaded
              per batch.
               if 0 or None: do not use batches
            insert_retries(int, optional): number of insert_retries for insert
               on OepServerSideExceptions
        """
        self.headers = {"Authorization": "Token %s" % token} if token else {}
        self.api_url = "%s://%s/api/%s/" % (protocol, host, api_version)
        self.web_url = "%s://%s/dataedit/view/" % (protocol, host)
        self.protocol = protocol
        self.host = host
        self.token = token
        self.default_schema = default_schema
        self.batch_size = batch_size
        self.insert_retries = insert_retries

    def _get_table_api_url(self, table, schema=None):
        """Return base api url for table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        schema = schema or self.default_schema
        url = self.api_url + "schema/%s/tables/%s/" % (schema, table)
        logging.debug("URL: %s", url)
        return url

    def get_web_url(self, table, schema=None):
        """Return web url for data edit/view

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        schema = schema or self.default_schema
        url = self.web_url + "%s/%s" % (schema, table)
        logging.debug("URL: %s", url)
        return url

    @check_exception("invalid token", OepAuthenticationException)
    def _request(self, method, url, expected_status, jsondata=None):
        """Send a request and perform basic check for results

        Args:
            method(str): http method, that will be passed on to `requests.request`
            url(str): request url
            expected_status(int): expected http status code.
                if result has a different code, an error will be raised
            jsondata(object, optional): payload that will be send as json
                in the request.
        Returns:
            result object from returned json data
        """
        res = requests.request(
            url=url, method=method, json=jsondata, headers=self.headers
        )
        logging.debug("%d %s %s", res.status_code, method, url)
        try:
            res_json = res.json()
        except Exception:
            # api should return json, but some actions don't,
            # and 500 errors obviously also don't
            res_json = {}
        if res.status_code >= 500:
            raise OepServerSideException(res_json)
        elif res.status_code >= 400:
            raise OepClientSideException(res_json)
        if res.status_code != expected_status:
            if "reason" in res_json:
                res_json = res_json["reason"]
            raise OepClientSideException(res_json)
        return res_json

    @check_exception("exists", OepTableAlreadyExistsException)
    def create_table(self, table, definition, schema=None):
        """Create table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            definition(object): column and constraint definitions
                according to the oep specifications

                Notes:
                * data_type should be understood by postgresql database
                * is_nullable: True or False
                * the first column should be the primary key
                  and a numeric column with the name `id` for full functionality om the platform

                Example:
                {
                    "columns": [
                        {"name": "id", "data_type": "bigint", "is_nullable": False, "primary_key": True},
                        {"name": "field1", "data_type": "varchar(128)", "is_nullable": False},
                        {"name": "field2", "data_type": "integer", "is_nullable": True}
                    ]
                }

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """  # noqa
        url = self._get_table_api_url(table=table, schema=schema)
        definition = fix_table_definition(definition)
        logging.debug(definition)
        self._request("PUT", url, 201, {"query": definition})
        # to check: return schema of newlycreated table
        definition_final = self.get_table_definition(table=table, schema=schema)
        logging.debug(definition_final)

    # inconsistent message from server:
    # "do not have permission" when table does not exist
    @check_exception("do not have permission", OepTableNotFoundException)
    def drop_table(self, table, schema=None):
        """Drop table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        url = self._get_table_api_url(table=table, schema=schema)
        return self._request("DELETE", url, 200)

    @check_exception("not found", OepTableNotFoundException)
    def select_from_table(self, table, schema=None, where=None):
        """Select all rows from table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
            where(list, optional): filter criteria in form of field/operator/value,
                e.g. ["id>10"]

        Returns:
            list of records(dict: column_name -> value)
        """
        url = self._get_table_api_url(table=table, schema=schema) + "rows/"

        if where:
            # convert dict into url
            where = "&".join(f"where={w}" for w in where)
            url = f"{url}?{where}"

        res = self._request("GET", url, 200)
        return res

    # inconsistent message from server:
    # "do not have permission" when table does not exist
    @check_exception("do not have permission", OepTableNotFoundException)
    def insert_into_table(
        self, table, data, schema=None, batch_size=None, method="api"
    ):
        """Insert records into table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            data(list): list of records(dict: column_name -> value)
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
            batch_size(int, optional): defaults to client's default batch size
            method(list, optional):
                * 'api': sent records via regular API
                * 'advanced' (default): sent records via advanced API
        """

        table_def = self.get_table_definition(table, schema=schema)
        column_names = [c["name"] for c in table_def["columns"]]

        if isinstance(data, pd.DataFrame):
            used_column_names = set(data.columns)
            data = dataframe_to_records(data)
        else:
            used_column_names = set()
            for row in data:
                used_column_names = used_column_names | set(row.keys())

        # FIXME: on oep server: columns are determined by keys in first row!
        # for now, we have to fix at least the first row
        if data and set(data[0]) < used_column_names:
            for c in used_column_names - set(data[0]):
                data[0][c] = None

        unknown_column_names = used_column_names - set(column_names)
        if unknown_column_names:
            raise OepClientSideException(
                "Columns not in table: %s", unknown_column_names
            )

        batch_size = batch_size or self.batch_size
        n_batches = math.ceil(len(data) / batch_size)
        data_batches = []
        for i in range(n_batches):
            i_from = i * batch_size
            i_to = (i + 1) * batch_size
            data_batches.append(data[i_from:i_to])

        n_items = 0
        with click.progressbar(data_batches) as data_parts:
            for i_item, data_part in enumerate(data_parts):
                logging.debug(
                    "Starting upload batch %d/%d (%d/%d)...",
                    i_item + 1,
                    n_batches,
                    n_items,
                    len(data),
                )
                try_number = 0
                success = False
                while try_number <= self.insert_retries:
                    try_number += 1
                    try:
                        if method == "api":
                            self._insert_into_table_api(table, data_part, schema)
                        elif method == "advanced":
                            with AdvancedApiSession(self) as ses:
                                ses.insert_into_table(table, data_part, schema)
                        else:
                            raise NotImplementedError(method)
                        # batch upload ok
                        success = True
                        break
                    except OepServerSideException:
                        if try_number <= self.insert_retries:
                            # FIXME ???
                            logging.debug("A server side error occurred. retrying...")

                if not success:
                    raise OepServerSideException()

                n_items += len(data_part)

        return self.count_rows(table=table, schema=schema)

    def _insert_into_table_api(self, table, data, schema):
        """Insert records into table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            data(list): list of records(dict: column_name -> value)
            schema(str): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        if not data:
            logging.warning("no data")
            return {}

        url = self._get_table_api_url(table=table, schema=schema) + "rows/new"
        res = self._request("POST", url, 201, {"query": data})
        return res

    @check_exception("not found", OepTableNotFoundException)
    def get_table_definition(self, table, schema=None):
        """Returns table info

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        url = self._get_table_api_url(table=table, schema=schema)
        res = self._request("GET", url, 200)
        definition = {
            "columns": [],
            "constraints": [],
        }

        # res["columns"] is dict
        # first: add constrains to field definitions / constraints
        for const in res["constraints"].values():
            const_type = const["constraint_type"]
            const_def = const["definition"]
            if const_type == "PRIMARY KEY":
                args = re.match(
                    r"^PRIMARY KEY \((?P<field>[^)]+)\)$", const_def
                ).groupdict()
                # NOTE currently only single field PK allowed
                res["columns"][args["field"]]["primary_key"] = True
            elif const_type == "FOREIGN KEY":
                args = re.match(
                    r"^FOREIGN KEY \((?P<field>[^)]+)\) REFERENCES (?P<ref_schema>[^.]+)\.(?P<ref_table>[^()]+)\((?P<ref_field>[^)]+)\)$",  # noqa
                    const_def,
                ).groupdict()
                # NOTE currently only single field PK allowed
                res["columns"][args["field"]]["foreign_key"] = [
                    {
                        "schema": args["ref_schema"],
                        "table": args["ref_table"],
                        "column": args["ref_field"],
                    }
                ]
            elif const_type == "UNIQUE":
                args = re.match(
                    r"^UNIQUE \((?P<fields>[^)]+)\)$", const_def
                ).groupdict()
                columns = [f.strip() for f in args["fields"].split(",")]
                definition["constraints"].append(
                    {"constraint_type": "UNIQUE", "columns": columns}
                )

        def get_datatype(coldef):
            dt = coldef["data_type"].upper()
            if dt == "CHARACTER":
                dt = "CHAR(%d)" % coldef["character_maximum_length"]
            elif dt == "CHARACTER VARYING":
                dt = "VARCHAR(%d)" % coldef["character_maximum_length"]
            elif dt == "DOUBLE PRECISION":
                dt = "FLOAT"

            if (coldef["column_default"] or "").startswith("nextval"):
                if "INT" not in dt:
                    raise NotImplementedError(dt)
                else:
                    dt = dt.replace("INT", "SERIAL")

            return dt

        # fix columns: list instead of dict
        for name, coldef in sorted(
            res["columns"].items(), key=lambda c: c[1]["ordinal_position"]
        ):
            col = {
                "name": name,
                "data_type": get_datatype(coldef),
                "is_nullable": coldef["is_nullable"],
            }
            if coldef.get("primary_key", False):
                col["primary_key"] = True
            if "foreign_key" in coldef:
                col["foreign_key"] = coldef["foreign_key"]
            definition["columns"].append(col)

        return definition

    def table_exists(self, table, schema=None):
        """True or False

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """

        try:
            return self._table_exists(table, schema)
        except OepTableNotFoundException:
            return False

    @check_exception("not found", OepTableNotFoundException)
    def _table_exists(self, table, schema=None):
        """True or False

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        url = self._get_table_api_url(table=table, schema=schema)
        self._request("GET", url, 200)
        return True

    def get_metadata(self, table, schema=None):
        """Returns metadata json

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        if not self.table_exists(table=table, schema=schema):
            raise OepTableNotFoundException
        url = self._get_table_api_url(table=table, schema=schema) + "meta/"
        res = self._request("GET", url, 200)
        return res

    @check_exception("not found", OepTableNotFoundException)
    def set_metadata(self, table, metadata, schema=None):
        """write  metadata json, return accepted data from server

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"

            metadata(object): json serializable object that follows the meta data specs
        """
        if not self.table_exists(table=table, schema=schema):
            raise OepTableNotFoundException
        url = self._get_table_api_url(table=table, schema=schema) + "meta/"
        metadata = self.validate_metadata(table, metadata)
        self._request("POST", url, 200, metadata)
        return self.get_metadata(table=table, schema=schema)

    def validate_metadata(self, table, data):
        if "id" not in data:
            data["id"] = table
        return data

    def advanced_session(self):
        return AdvancedApiSession(self)

    def count_rows(self, table, schema=None):
        schema = schema or self.default_schema
        query = {
            "type": "select",
            "from": [{"type": "table", "schema": schema, "table": table}],
            "fields": [
                {
                    "type": "label",
                    "element": {
                        "type": "function",
                        "function": "count",
                        "operands": {
                            "type": "grouping",
                            "grouping": [
                                {"type": "column", "column": "*", "is_literal": True}
                            ],
                        },
                    },
                    "label": "rowcount",
                }
            ],
        }
        with self.advanced_session() as sas:
            # not data yet
            res = sas._command("search", query)
            content = res["content"]
            description = content["description"]
            fieldnames = [f[0] for f in description]
            res = sas._command("cursor/fetch_one")
            content = res["content"]
            rec = dict(zip(fieldnames, content))
            rowcount = rec["rowcount"]
        return rowcount

    def move_table(self, table, target_schema, schema=None):
        """Move table into new target schema"""
        url = (
            self._get_table_api_url(table=table, schema=schema)
            + "move/%s/" % target_schema
        )
        return self._request("POST", url, 200)

    # def get_sqlalchemy_table(self, table, schema=None):
    #    return get_sqlalchemy_table(self, table, schema=schema)

    def delete_from_table(self, table, schema=None):
        """Delete all rows from table (without dropping it).

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"

        """
        with self.advanced_session() as sas:
            sas.delete_from_table(table, schema=schema)

    def iter_tables(self):
        adv = AdvancedApiSession(self)  # no need to enter context
        url = adv.api_url + "get_schema_names"
        schemas = self._request("post", url, expected_status=200)["content"]
        url = adv.api_url + "get_table_names"

        for schema in schemas:
            if schema.startswith("_") or schema in [
                "topology",
                "test",
                "sandbox",
                "information_schema",
            ]:
                continue
            tables = self._request(
                "post", url, jsondata={"query": {"schema": schema}}, expected_status=200
            )["content"]
            for table in tables:
                yield {"schema": schema, "table": table}
