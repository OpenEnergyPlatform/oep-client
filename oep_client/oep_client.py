"""Small client implementation to use the oep API.

Version: 0.1

Example usage: create a table, insert data, retrieve data, delete table

cli = OEPClient(token='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

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

"""

import logging
import json
import math
import re
import functools

import click
import requests


DEFAULT_HOST = "https://openenergy-platform.org"
DEFAULT_API_VERSION = "v0"
DEFAULT_SCHEMA = "model_draft"
DEFAULT_BATCH_SIZE = None
DEFAULT_INSERT_RETRIES = 0


class OepApiException(Exception):
    pass


class OepServerSideException(OepApiException):
    pass


class OepClientSideException(OepApiException):
    pass


class OepAuthenticationException(OepClientSideException):
    pass


class OepTableNotFoundException(OepClientSideException):
    def __init__(self, _msg):
        # the API falsely returns message: {'detail': 'You do not have permission to perform this action}
        # but this is only because table  does not exist
        # TODO: create better error message on server side!
        super().__init__("Table does not exist OR you don't have permission")


class OepTableAlreadyExistsException(OepClientSideException):
    pass


def fix_table_definition(definition):
    if "fields" in definition:
        definition["columns"] = definition.pop("fields")
    definition["columns"] = [fix_column_definition(c) for c in definition["columns"]]
    return definition


def fix_column_definition(definition):
    if "type" in definition:
        definition["data_type"] = definition.pop("type")
    return definition


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
        host=DEFAULT_HOST,
        api_version=DEFAULT_API_VERSION,
        default_schema=DEFAULT_SCHEMA,
        batch_size=DEFAULT_BATCH_SIZE,
        insert_retries=DEFAULT_INSERT_RETRIES,
    ):
        """
        Args:
            token(str): your API token
            host(str, optional): host of the oep platform. default is "https://openenergy-platform.org"
            api_version(str, optional): currently only "v0"
            default_schema(str, optional): the default schema for the tables, usually "model_draft"
            batch_size(int, optional): number of records that will be uploaded per batch.
               if 0 or None: do not use batches
            insert_retries(int, optional): number of insert_retries for insert on OepServerSideExceptions
        """
        self.headers = {"Authorization": "Token %s" % token} if token else {}
        self.api_url = "%s/api/%s/" % (host, api_version)
        self.default_schema = default_schema
        self.batch_size = batch_size
        self.insert_retries = insert_retries

    def _get_table_url(self, table, schema=None):
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

    @check_exception("invalid token", OepAuthenticationException)
    def _request(self, method, url, expected_status, jsondata=None):
        """Send a request and perform basic check for results

        Args:
            method(str): http method, that will be passed on to `requests.request`
            url(str): request url
            expected_status(int): expected http status code.
                if result has a different code, an error will be raised
            jsondata(object, optional): payload that will be send as json in the request.
        Returns:
            result object from returned json data
        """
        res = requests.request(
            url=url, method=method, json=jsondata, headers=self.headers
        )
        logging.debug("%d %s %s", res.status_code, method, url)
        try:
            res_json = res.json()
        except json.decoder.JSONDecodeError:
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
        """
        url = self._get_table_url(table=table, schema=schema)
        definition = fix_table_definition(definition)
        logging.debug(definition)
        return self._request("PUT", url, 201, {"query": definition})

    # inconsistent message from server: "do not have permission" when table does not exist
    @check_exception("do not have permission", OepTableNotFoundException)
    def drop_table(self, table, schema=None):
        """Drop table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        url = self._get_table_url(table=table, schema=schema)
        return self._request("DELETE", url, 200)

    @check_exception("not found", OepTableNotFoundException)
    def select_from_table(self, table, schema=None):
        """Select all rows from table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"

        Returns:
            list of records(dict: column_name -> value)
        """
        url = self._get_table_url(table=table, schema=schema) + "rows/"
        res = self._request("GET", url, 200)
        return res

    # inconsistent message from server: "do not have permission" when table does not exist
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

        table_def = self._get_table_definition(table, schema=schema)
        column_names = [
            c["name"]
            for c in sorted(table_def["columns"], key=lambda c: c["ordinal_position"])
        ]
        used_column_names = set()
        for row in data:
            used_column_names = used_column_names | set(row.keys())
        unknown_column_names = used_column_names - set(column_names)
        if unknown_column_names:
            raise OepClientSideException(
                "Columns not in table: %s", unknown_column_names
            )

        batch_size = (batch_size or self.batch_size) or 1
        n_batches = math.ceil(len(data) / batch_size)
        data_batches = [
            data[i * batch_size : (i + 1) * batch_size] for i in range(n_batches)
        ]

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
                            logging.warning("A server side error occurred. retrying...")

                if not success:
                    raise OepServerSideException()

                n_items += len(data_part)

        # todo: return info to user?

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

        url = self._get_table_url(table=table, schema=schema) + "rows/new"
        res = self._request("POST", url, 201, {"query": data})
        return res

    @check_exception("not found", OepTableNotFoundException)
    def _get_table_definition(self, table, schema=None):
        """Returns table info

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        url = self._get_table_url(table=table, schema=schema)
        res = self._request("GET", url, 200)
        # fix columns: list instead of dict
        for name, col in res["columns"].items():
            col["name"] = name
        res["columns"] = sorted(
            res["columns"].values(), key=lambda c: c["ordinal_position"]
        )
        return res

    def table_exists(self, table, schema=None):
        """True or False

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore

            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """
        try:
            self._get_table_definition(table, schema)
            return True
        except OepTableNotFoundException:
            return False

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
        url = self._get_table_url(table=table, schema=schema) + "meta/"
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
        url = self._get_table_url(table=table, schema=schema) + "meta/"
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
            res = sas._command("search", {"query": query})
            content = res["content"]
            description = content["description"]
            fieldnames = [f[0] for f in description]
            _rowcount = content["rowcount"]  # should be 1
            res = sas._command("cursor/fetch_one")
            content = res["content"]
            rec = dict(zip(fieldnames, content))
            rowcount = rec["rowcount"]
        return rowcount

    def move_table(self, table, target_schema, schema=None):
        """Move table into new target schema"""
        url = (
            self._get_table_url(table=table, schema=schema) + "move/%s/" % target_schema
        )
        return self._request("POST", url, 200)


class AdvancedApiSession:
    """Context for advanced api session (close connection on exit)"""

    def __init__(self, oepclient):
        """
        Args:

            oepclient(OEPClient)
        """
        self.oepclient = oepclient
        self.api_url = self.oepclient.api_url + "advanced/"
        self.connection_id = None
        self.cursor_id = None

    def _command(self, command, jsondata=None):
        url = self.api_url + command
        jsondata = jsondata or {}
        if self.connection_id:
            jsondata["connection_id"] = self.connection_id
        if self.cursor_id:
            jsondata["cursor_id"] = self.cursor_id
        return self.oepclient._request("POST", url, 200, jsondata)

    def __enter__(self):
        self.connection_id = self._command("connection/open")["content"][
            "connection_id"
        ]
        self.cursor_id = self._command("cursor/open")["content"]["cursor_id"]
        logging.debug("Started connection: %s", self.connection_id)
        return self

    def __exit__(self, *args):
        if self.cursor_id:
            self._command("cursor/close")
            logging.debug("Closed cursor: %s", self.cursor_id)
            self.cursor_id = None
        if self.connection_id:
            self._command("connection/close")
            logging.debug("Closed connection: %s", self.connection_id)
            self.connection_id = None

    def insert_into_table(self, table, data, schema):
        """Insert records into table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            data(list): list of records(dict: column_name -> value)
            schema(str): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """

        if not isinstance(data, (list, tuple)) or (
            data and not isinstance(data[0], dict)
        ):
            raise OepClientSideException(
                "data must be list or tuple of record dictionaries"
            )

        def _get_query(values):
            query = {
                "schema": schema or self.oepclient.default_schema,
                "table": table,
                "values": values,
            }
            return {"query": query}

        try:
            res = self._command("insert", _get_query(data))
            self._command("connection/commit")
            return res
        except Exception as exc:
            logging.error(exc)
            self._command("connection/rollback")
            raise
