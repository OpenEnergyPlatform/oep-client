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
        jsondata = {
            "query": {
                "columns": columns,
                # TODO: additional constraints from metadata
                "constraints": [
                    {"constraint_type": "PRIMARY KEY", "constraint_parameter": "id"}
                ],
            }
        }
        self.request("PUT", url, jsondata=jsondata)
        logger.info("   ok.")

    def upload_data(self, dataframe, metadata=None):
        """
        """
        logger.info("UPLOAD_DATA")
        url = self.get_url(is_draft=True, metadata=metadata) + "rows/new"
        data = self.convert_dataframe(dataframe)
        n_records = len(data)
        bs = self.settings["batch_size"]
        while data:
            batch, data = data[:bs], data[bs:]
            jsondata = {"query": batch}
            logger.info("   sending batch (n=%d) ..." % len(batch))
            self.request("POST", url, jsondata=jsondata)
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
    def get_column_defs_from_meta(metadata):
        """Return column definitions as list of {
            name: STR,
            type: STR,
            [character_maximum_length: INT],
            [is_nullable:YES|NO],
            [unit],
            [description]
        }"""
        res = []
        for c in metadata["resources"][0]["schema"]["fields"]:
            f = {
                "name": fix_name(c["name"]),
                "data_type": fix_name(c["type"]),
                "is_nullable": c.get("is_nullable", "YES"),
                "description": c.get("description", ""),
                "unit": c.get("unit", ""),
            }
            if "character_maximum_length" in c:
                f["character_maximum_length"] = int(c["character_maximum_length"])
            # fix type names
            if f["data_type"] == "double precision":
                f["data_type"] = "float"
            elif f["data_type"] == "serial":
                f["data_type"] = "integer"
            elif f["data_type"] == "string":
                f["data_type"] = "varchar"
            res.append(f)
        # add id, if not exist
        if not any(c["name"].lower() == "id" for c in res):
            res = [{"name": "id", "data_type": "bigserial", "is_nullable": "NO"}] + res
        return res

    def get_tablename_from_meta(self, metadata):
        try:
            name = metadata["resources"][0]["name"]
        except:
            raise Exception("table name not found in metadata (name in resource[0])")
        return name

    def get_url(self, is_draft=False, metadata=None):
        tablename = self.settings.get("tablename") or self.get_tablename_from_meta(
            metadata
        )
        if "." in tablename:
            schema, tablename = tablename.split(".")
        else:
            schema = self.settings["schema"]
        if is_draft:
            schema = self.settings["schema_draft"]
        url = "%(protocol)s://%(host)s/api/%(api_version)s/schema/" % self.settings
        url += "%s/tables/%s/" % (schema, tablename)
        return url
