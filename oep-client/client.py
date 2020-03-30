# coding: utf-8

import re
import json
import requests
import pandas as pd
import omi.dialects.oep.parser
import logging

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
        self.session = requests.session()
        self.session.verify = self.settings["ssl_verify"]
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
        return res.json()

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
        res = self.request("DELETE", url)
        return res

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
        res = self.request("PUT", url, jsondata=jsondata)
        return res

    def upload_data(self, dataframe, metadata=None):
        """
        """
        logger.info("UPLOAD_DATA")
        url = self.get_url(is_draft=True, metadata=metadata) + "rows/new"
        data = self.convert_dataframe(dataframe)
        bs = self.settings["batch_size"]
        while data:
            batch, data = data[:bs], data[bs:]
            data = {"query": batch}
            logger.info("   sending batch (n=%d) ..." % bs)
            res = self.request("POST", url, jsondata=batch)
        return None

    def download_data(self, metadata=None):
        """
        """
        logger.info("DOWNLOAD_DATA")
        url = self.get_url(is_draft=False, metadata=metadata) + "rows/"
        res = self.request("GET", url)
        df = pd.DataFrame(res)
        return df

    def update_metadata(self, metadata):
        """
        """
        logger.info("UPDATE_METADATA")
        url = self.get_url(is_draft=True, metadata=metadata) + "meta/"
        res = self.request("POST", url, jsondata=metadata)
        return res

    def download_metadata(self, metadata=None):
        """
        """
        logger.info("DOWNLOAD_METADATA")
        url = self.get_url(is_draft=False, metadata=metadata) + "meta/"
        res = self.request("GET", url)
        return res

    def save_dataframe(self, df, filepath):
        """
        """
        if filepath.endswith(".xlsx"):
            sheet = self.settings.get("sheet")
            if not sheet:
                raise Exception("Must specify sheet when using xlsx")
            df.to_excel(filepath, sheet)
        elif filepath.endswith(".csv"):
            df = pd.to_csv(
                filepath,
                encoding=self.settings["encoding"],
                delimiter=self.settings["delimiter"],
                na_values=[""],
                keep_default_na=False,
            )
        elif filepath.endswith(".json"):
            data = self.convert_dataframe(df)
            self.save_json(data, filepath)
        else:
            raise Exception("Unsupported data type: %s" % filepath)
        return df

    def convert_dataframe(self, df):
        columns = [fix_name(n) for n in df.columns]
        data = df.fillna().values.tolist()
        data = [dict(zip(columns, row)) for row in data]
        return data

    def load_dataframe(self, filepath):
        """
        """
        if filepath.endswith(".xlsx"):
            sheet = self.settings.get("sheet")
            if not sheet:
                raise Exception("Must specify sheet when using xlsx")
            df = pd.read_excel(filepath, sheet=sheet)
        elif filepath.endswith(".csv"):
            df = pd.read_csv(
                filepath,
                encoding=self.settings["encoding"],
                delimiter=self.settings["delimiter"],
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

    def save_json(self, data, filepath):
        logger.info("saving %s" % filepath)
        with open(filepath, "w", encoding=self.settings["encoding"]) as f:
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

    def get_tablename_from_meta(metadata):
        return metadata["resources"][0]["name"]

    def get_url(self, is_draft=False, metadata=None):
        tablename = self.settings.get("tablename") or self.get_tablename_from_meta(
            metadata
        )
        if "." in tablename:
            schema, tablename = tablename.split(".")
        else:
            schema = self.setting["schema"]
        if is_draft:
            schema = self.setting["schema_draft"]
        url = "%(protocol)s://%(host)s/api/%(api_version)s/schema/" % self.settings
        url += "%s/tables/%s/" % (schema, tablename)
        return url
