# coding: utf-8

import re
import json
import requests
import logging
import pandas as pd
from numpy import nan
import omi.dialects.oep.parser

logger = logging.getLogger()


def fix_name(name):
    name_new = re.sub("[^a-z0-9_]+", "_", name.lower())
    if name_new != name:
        logger.warning('Changed name "%s" to "%s"' % (name, name_new))
    return name_new


def get_fields_values(records):
    fields = set()
    values = []
    for rec in records:
        fields = fields | set(rec.keys())
    fields = list(fields)
    for rec in records:
        values.append([rec.get(f) for f in fields])
    return fields, values


class OepClient:
    def __init__(self, **settings):
        self.settings = {
            "host": "openenergy-platform.org",
            "api_version": "v0",
            "protocol": "https",
            "schema_draft": "model_draft",
            "schema": "model_draft",
            "batch_size": 1000,
            "encoding": "utf-8",
            "delimiter": ",",
            "ssl_verify": True,
            "metadata_version": "1.4",
        }
        self.settings.update(settings)
        for k, v in self.settings.items():
            logger.debug("%s = %s" % (k, v))
        self.session = requests.session()
        self.session.verify = self.settings["ssl_verify"]
        if "token" in self.settings:
            self.session.headers = {"Authorization": "Token %(token)s" % self.settings}

    def request(self, method, url, jsondata=None):
        logger.info("%s %s" % (method, url))
        res = self.session.request(url=url, method=method, json=jsondata)
        try:
            res.raise_for_status()
        except Exception as e:
            try:
                err = res.json()
            except:
                err = e
            raise Exception(err)
        return res

    def validate(self, metadata):
        """
        """
        logger.info("VALIDATE")
        validator = getattr(
            omi.dialects.oep.parser,
            "JSONParser_" + self.settings["metadata_version"].replace(".", "_"),
        )()
        validator.parse(metadata)
        logger.info("   ok.")

    def delete(self, metadata=None):
        """
        """
        logger.info("DELETE")
        url = self.get_url(is_draft=True, metadata=metadata)
        self.request("DELETE", url)
        logger.info("   ok.")

    def create(self, metadata):
        """
        """
        logger.info("CREATE")
        url = self.get_url(is_draft=True, metadata=metadata)
        columns = self.get_column_defs_from_meta(metadata)
        constraints = self.get_constraints_from_meta(metadata)
        jsondata = {"query": {"columns": columns, "constraints": constraints}}
        logger.debug(jsondata)
        self.request("PUT", url, jsondata=jsondata)
        logger.info("   ok.")

    def upload_data_single(self, data, metadata=None, batch_size=None):
        """records one by one: very slow
        """
        url = (
            self.get_url(is_draft=True, metadata=metadata) + "rows/"
        )  # or /new works too
        for rec in data:
            jsondata = {"query": rec}
            self.request("POST", url, jsondata=jsondata)

    def upload_data_batch_with_id(self, data, metadata=None, batch_size=None):
        """records as batch, but only works if we have id column
        """
        url = self.get_url(is_draft=True, metadata=metadata) + "rows/new"
        while data:
            batch, data = data[:batch_size], data[batch_size:]
            jsondata = {"query": batch}
            logger.info("   sending batch (n=%d) ..." % len(batch))
            self.request("POST", url, jsondata=jsondata)

    def upload_data_advanced(self, data, metadata=None, batch_size=None):
        """use advanced api
        """
        jsondata = None
        url = self.get_api_url() + "advanced/connection/open"
        connection_id = self.request("POST", url, jsondata=jsondata).json()["content"][
            "connection_id"
        ]

        try:
            jsondata = {"connection_id": connection_id}
            url = self.get_api_url() + "advanced/cursor/open"
            cursor_id = self.request("POST", url, jsondata=jsondata).json()["content"][
                "cursor_id"
            ]

            schema, table = self.get_schema_table(is_draft=True, metadata=metadata)
            url = self.get_api_url() + "advanced/insert"
            while data:
                batch, data = data[:batch_size], data[batch_size:]
                fields, values = get_fields_values(batch)
                query = {
                    "schema": schema,
                    "table": table,
                    "fields": fields,
                    "values": values,
                }
                jsondata = {
                    "connection_id": connection_id,
                    "cursor_id": cursor_id,
                    "query": query,
                }
                logger.info("   sending batch (n=%d) ..." % len(batch))
                self.request("POST", url, jsondata=jsondata)

            jsondata = {"connection_id": connection_id, "cursor_id": cursor_id}
            url = self.get_api_url() + "advanced/connection/commit"
            self.request("POST", url, jsondata=jsondata)

            jsondata = {"connection_id": connection_id, "cursor_id": cursor_id}
            url = self.get_api_url() + "advanced/connection/close"
            self.request("POST", url, jsondata=jsondata)

        except Exception as e:
            try:
                jsondata = {"connection_id": connection_id}
                url = self.get_api_url() + "advanced/connection/rollback"
                self.request("POST", url, jsondata=jsondata)
            except:
                pass

            try:
                jsondata = {"connection_id": connection_id, "cursor_id": cursor_id}
                url = self.get_api_url() + "advanced/connection/close"
                self.request("POST", url, jsondata=jsondata)
            except:
                pass

            raise e

    def upload_data(self, dataframe, metadata=None, batch_size=None):
        """
        """
        logger.info("UPLOAD_DATA")
        data = self.convert_dataframe(dataframe)
        n_records = len(data)
        if batch_size is None:  # use default
            batch_size = self.settings["batch_size"]
        elif batch_size == 0:
            batch_size = n_records

        # self.upload_data_single(data=data, metadata=metadata, batch_size=batch_size)
        # self.upload_data_batch(data=data, metadata=metadata, batch_size=batch_size)
        self.upload_data_advanced(data=data, metadata=metadata, batch_size=batch_size)

        logger.info("   ok. (n=%d)" % n_records)

    def download_data(self, metadata=None):
        """
        """
        logger.info("DOWNLOAD_DATA")
        url = self.get_url(is_draft=False, metadata=metadata) + "rows/"
        res = self.request("GET", url)
        try:
            # TODO: file bugreport: empty table returns invalid json (content = b']')
            res = res.json()
        except Exception as e:
            logger.warning("Empty table")
            res = []
        df = pd.DataFrame(res)
        # df.set_index(['id'], inplace=True)  # make id column to index
        logger.info("   ok. (n=%d)" % len(res))
        return df

    def update_metadata(self, metadata):
        """
        """
        logger.info("UPDATE_METADATA")
        url = self.get_url(is_draft=True, metadata=metadata) + "meta/"
        self.request("POST", url, jsondata=metadata)
        logger.info("   ok.")

    def download_metadata(self, metadata=None):
        """
        """
        logger.info("DOWNLOAD_METADATA")
        url = self.get_url(is_draft=False, metadata=metadata) + "meta/"
        res = self.request("GET", url)
        res = res.json()
        logger.info("   ok.")
        return res

    @classmethod
    def save_dataframe(
        cls, df, filepath, sheet=None, delimiter=",", encoding="utf-8", index=False
    ):
        """
        """
        if filepath.endswith(".xlsx"):
            if not sheet:
                raise Exception("Must specify sheet when using xlsx")
            df.to_excel(filepath, sheet_name=sheet, index=index)
        elif filepath.endswith(".csv"):
            df.to_csv(
                filepath, encoding=encoding, sep=delimiter, na_rep="", index=index
            )
        elif filepath.endswith(".json"):
            data = cls.convert_dataframe(df)
            cls.save_json(data, filepath)
        else:
            raise Exception("Unsupported data type: %s" % filepath)
        return df

    @classmethod
    def convert_dataframe(cls, df):
        columns = [fix_name(n) for n in df.columns]
        # replace nan
        df = df.replace({nan: None})
        data = df.values.tolist()
        data = [dict(zip(columns, row)) for row in data]
        return data

    @classmethod
    def load_dataframe(cls, filepath, sheet=None, delimiter=",", encoding="utf-8"):
        """
        """
        if filepath.endswith(".xlsx"):
            if not sheet:
                raise Exception("Must specify sheet when using xlsx")
            df = pd.read_excel(filepath, sheet_name=sheet)
        elif filepath.endswith(".csv"):
            df = pd.read_csv(
                filepath,
                encoding=encoding,
                sep=delimiter,
                na_values=[""],
                keep_default_na=False,
            )
        elif filepath.endswith(".json"):
            df = pd.read_json(filepath, orient="records")
        else:
            raise Exception("Unsupported data type: %s" % filepath)
        return df

    @staticmethod
    def load_json(filepath):
        logger.info("reading %s" % filepath)
        with open(filepath, "rb") as f:
            return json.load(f)

    @staticmethod
    def save_json(data, filepath, encoding="utf-8"):
        logger.info("saving %s" % filepath)
        with open(filepath, "w", encoding=encoding) as f:
            return json.dump(data, f, sort_keys=True, indent=2)

    @staticmethod
    def get_constraints_from_meta(metadata):
        constraints = []
        schema = metadata["resources"][0]["schema"]
        pk_fields = schema.get("primaryKey")
        if pk_fields:
            if not isinstance(pk_fields, str):
                raise Exception(
                    "platform currently only supports single field primary key"
                )
            constraints.append(
                {"constraint_type": "PRIMARY KEY", "constraint_parameter": pk_fields}
            )

        # TODO: foreignKeys
        return constraints

    @classmethod
    def get_is_nullable(cls, val):
        if val in (None, "YES", "yes", "true", True, 1):
            return True
        elif val in ("NO", "no", "false", False, 0):
            return False
        else:
            raise Exception("Invalid value for is_nullable: %s" % val)

    @classmethod
    def get_datatype(cls, val):
        return val

    @classmethod
    def get_column_defs_from_meta(cls, metadata):
        """Return column definitions as list of {
            name: STR,
            type: STR,
            [is_nullable:YES|NO],
            [unit],
            [description]
        }"""
        res = []
        for c in metadata["resources"][0]["schema"]["fields"]:
            f = {
                "name": fix_name(c["name"]),
                "data_type": cls.get_datatype(c["type"]),
                "is_nullable": cls.get_is_nullable(c.get("is_nullable")),
                "description": c.get("description", ""),
                "unit": c.get("unit", ""),
            }
            res.append(f)
        return res

    def get_tablename_from_meta(self, metadata):
        try:
            name = metadata["resources"][0]["name"]
        except:
            raise Exception("table name not found in metadata (name in resource[0])")
        return name

    def get_api_url(self):
        url = "%(protocol)s://%(host)s/api/%(api_version)s/" % self.settings
        return url

    def get_schema_table(self, is_draft=False, metadata=None):
        tablename = self.settings.get("tablename") or self.get_tablename_from_meta(
            metadata
        )
        if "." in tablename:
            schema, tablename = tablename.split(".")
        else:
            schema = self.settings["schema"]
        if is_draft:
            schema = self.settings["schema_draft"]
        return schema, tablename

    def get_url(self, is_draft=False, metadata=None):
        schema, tablename = self.get_schema_table(is_draft=is_draft, metadata=metadata)
        url = self.get_api_url() + "schema/%s/tables/%s/" % (schema, tablename)
        return url
