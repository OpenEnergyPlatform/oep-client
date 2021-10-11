from .exceptions import OepClientSideException
import logging


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
