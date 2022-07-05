import logging

from .exceptions import OepClientSideException


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

    def _command(self, command, query=None):
        url = self.api_url + command
        jsondata = {}
        if self.connection_id:
            jsondata["connection_id"] = self.connection_id
        if self.cursor_id:
            jsondata["cursor_id"] = self.cursor_id
        if query:
            jsondata["query"] = query
        logging.debug(jsondata)
        return self.oepclient._request("POST", url, 200, jsondata)

    def __enter__(self):
        self.connection_id = self._command("connection/open")["content"][
            "connection_id"
        ]
        self.cursor_id = self._command("cursor/open")["content"]["cursor_id"]
        logging.debug("Started connection: %s", self.connection_id)
        return self

    def __exit__(self, _exc_type, exc_val, _exc_tb):
        # rollback on error, otherwise commit
        if exc_val:
            logging.error(exc_val)
            self._command("connection/rollback")
        else:
            self._command("connection/commit")
        if self.cursor_id:
            self._command("cursor/close")
            logging.debug("Closed cursor: %s", self.cursor_id)
            self.cursor_id = None
        if self.connection_id:
            self._command("connection/close")
            logging.debug("Closed connection: %s", self.connection_id)
            self.connection_id = None

    def _get_query(self, table, schema=None, **kwargs):
        query = {"schema": schema or self.oepclient.default_schema, "table": table}
        query.update(kwargs)
        return query

    def insert_into_table(self, table, data, schema=None):
        """Insert records into table.

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            data(list): list of records(dict: column_name -> value)
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"
        """

        if not isinstance(data, (list, tuple)) or (
            data and not isinstance(data[0], dict)
        ):
            raise OepClientSideException(
                "data must be list or tuple of record dictionaries"
            )

        query = self._get_query(table, schema=schema, values=data)
        return self._command("insert", query)

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

        query = self._get_query(table, schema=schema)
        return self._command("search", query)

    def delete_from_table(self, table, schema=None):
        """Delete all rows from table (without dropping it).

        Args:
            table(str): table name. Must be valid postgres table name,
                all lowercase, only letters, numbers and underscore
            schema(str, optional): table schema name.
                defaults to self.default_schema which is usually "model_draft"

        """

        query = self._get_query(table, schema=schema)
        return self._command("delete", query)
