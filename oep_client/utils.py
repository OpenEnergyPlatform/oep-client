"""Command line script for OepClient
"""
__version__ = "0.15.0"

import json
import logging
import re
from copy import deepcopy
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit

import pandas as pd
import requests
from numpy import nan

from oep_client.exceptions import OepClientSideException

DEFAULT_INDENT = 2


def read_json(filepath, encoding):

    filepath = make_local(filepath)

    with open(filepath, encoding=encoding) as file:
        return json.load(file)


def write_json(data, filepath, encoding):
    with open(filepath, "w", encoding=encoding) as file:
        json.dump(data, file, indent=DEFAULT_INDENT, ensure_ascii=False)


def read_metadata_json(filepath, encoding):
    # TODO: valdiation?
    return read_json(filepath, encoding)


def get_schema_definition_from_metadata(meta):
    definition = meta["resources"][0]["schema"]
    definition = fix_table_definition(definition)
    return definition


def fix_name(name):
    name_new = re.sub("[^a-z0-9_]+", "_", name.lower())
    if name_new != name:
        logging.warning('Changed name "%s" to "%s"', name, name_new)
    return name_new


def dataframe_to_records(df):
    columns = [fix_name(n) for n in df.columns]
    # replace nan
    df = df.replace({nan: None})
    data = df.values.tolist()
    data = [dict(zip(columns, row)) for row in data]
    return data


def records_to_dataframe(records):
    df = pd.DataFrame(records)
    return df


def make_local(filepath_or_url: str) -> str:
    # if filepath is url: download to tempfile
    if not re.match("^http[s]?://", filepath_or_url):
        return filepath_or_url
    suffix = urlsplit(filepath_or_url).path.split("/")[-1]
    suffix = re.sub("[^a-z0-9.]", "_", suffix.lower())  # replace non word chars
    with NamedTemporaryFile(mode="wb", delete=False, suffix="_" + suffix) as file:
        logging.debug(f"Downloading {filepath_or_url} => {file.name}")
        resp = requests.get(filepath_or_url)
        resp.raise_for_status()
        file.write(resp.content)
        return file.name


def read_dataframe(filepath, **kwargs):

    filepath = make_local(filepath)

    if filepath.endswith(".json"):
        df = pd.read_json(filepath)
    elif filepath.endswith(".csv"):
        df = pd.read_csv(
            filepath, encoding=kwargs.get("encoding"), sep=kwargs.get("delimiter")
        )
    elif filepath.endswith(".xlsx"):
        # pd.read_excel default for sheet_name = 0 (first sheet)
        sheet = kwargs.get("sheet", 0)
        df = pd.read_excel(filepath, sheet)
    else:
        raise OepClientSideException("Unsupported filetype: %s" % filepath)
    return df


def write_dataframe(df, filepath, **kwargs):
    if filepath.endswith(".json"):
        df.to_json(filepath, orient="records", indent=DEFAULT_INDENT, force_ascii=False)
    elif filepath.endswith(".csv"):
        df.to_csv(
            filepath,
            encoding=kwargs.get("encoding"),
            sep=kwargs.get("delimiter"),
            index=False,
        )
    elif filepath.endswith(".xlsx"):
        sheet = kwargs.get("sheet")
        if not sheet:
            raise OepClientSideException("Must specify sheet when reading excel files")
        df.to_excel(filepath, sheet, index=False)
    elif filepath == "-":
        # stdout
        s = df.to_string(index=False)
        print(s)
    else:
        raise OepClientSideException("Unsupported filetype: %s" % filepath)
    return df


def fix_table_definition(definition):
    definition = deepcopy(definition)
    if "fields" in definition:
        definition["columns"] = definition.pop("fields")
    definition["columns"] = [fix_column_definition(c) for c in definition["columns"]]
    return definition


def fix_column_definition(definition):
    definition = deepcopy(definition)
    definition["data_type"] = definition.get("data_type") or definition.pop("type")
    return definition
