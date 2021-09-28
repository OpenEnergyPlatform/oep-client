"""Command line script for OepClient
"""
__version__ = "0.9.1"

import sys
import logging
import json
import re

import click
import pandas as pd
from numpy import nan

import os

sys.path.insert(0, os.path.join(os.path.basename(__file__), ".."))

from oep_client.oep_client import (
    OepClient,
    DEFAULT_HOST,
    DEFAULT_API_VERSION,
    DEFAULT_SCHEMA,
    DEFAULT_INSERT_RETRIES,
    DEFAULT_BATCH_SIZE,
    OepClientSideException,
    OepApiException,
    fix_table_definition,
)
from oep_client.test import roundtrip

PROG_NAME = "oep-client"
LOGGING_DATE_FMT = "%Y-%m-%d %H:%M:%S"
LOGGING_FMT = "[%(asctime)s %(levelname)7s] %(message)s"
DEFAULT_ENCODING = "utf-8"
DEFAULT_INDENT = 2


def read_json(filepath, encoding):
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


def read_dataframe(filepath, **kwargs):
    if filepath.endswith(".json"):
        df = pd.read_json(filepath)
    elif filepath.endswith(".csv"):
        df = pd.read_csv(
            filepath, encoding=kwargs.get("encoding"), sep=kwargs.get("delimiter")
        )
    elif filepath.endswith(".xlsx"):
        sheet = kwargs.get("sheet")
        if not sheet:
            raise OepClientSideException("Must specify sheet when reading excel files")
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
    else:
        raise OepClientSideException("Unsupported filetype: %s" % filepath)
    return df


@click.group()
@click.pass_context
@click.version_option(__version__, prog_name=PROG_NAME)
@click.option(
    "--loglevel",
    "-l",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error"]),
)
@click.option("--token", "-t")
@click.option("--host", default=DEFAULT_HOST)
@click.option("--api-version", default=DEFAULT_API_VERSION)
@click.option("--schema", "-s", default=DEFAULT_SCHEMA)
@click.option("--batch-size", "-b", default=DEFAULT_BATCH_SIZE)
@click.option("--insert-retries", default=DEFAULT_INSERT_RETRIES)
def main(
    ctx,
    loglevel,
    token,
    host,
    api_version,
    schema,
    batch_size,
    insert_retries,
):
    loglevel = getattr(logging, loglevel.upper())
    logging.basicConfig(format=LOGGING_FMT, datefmt=LOGGING_DATE_FMT, level=loglevel)
    ctx.ensure_object(dict)
    ctx.obj["client"] = OepClient(
        token=token,
        host=host,
        api_version=api_version,
        default_schema=schema,
        batch_size=batch_size,
        insert_retries=insert_retries,
    )


@main.command("create")
@click.pass_context
@click.argument("table")
@click.argument("metadata_file", type=click.Path(exists=True))
@click.option("--encoding", "-e", default=DEFAULT_ENCODING)
def create_table(ctx, table, metadata_file, encoding):
    metadata = read_metadata_json(metadata_file, encoding)
    definition = get_schema_definition_from_metadata(metadata)
    client = ctx.obj["client"]
    client.create_table(table, definition)
    # automatically upload metadata?
    # client.set_metadata(table, metadata)

    # logging.info("OK")


@main.command("drop")
@click.pass_context
@click.argument("table")
def drop_table(ctx, table):
    client = ctx.obj["client"]
    client.drop_table(table)
    logging.info("OK")


@main.command("insert")
@click.pass_context
@click.argument("table")
@click.argument("data_file", type=click.Path(exists=True))
@click.option("--encoding", "-e", default=DEFAULT_ENCODING)
@click.option("--sheet", "-s", default=None)
@click.option("--delimiter", "-d", default=",")
def insert_into_table(ctx, table, data_file, encoding, sheet, delimiter):
    df = read_dataframe(data_file, encoding=encoding, sheet=sheet, delimiter=delimiter)
    data = dataframe_to_records(df)
    client = ctx.obj["client"]
    client.insert_into_table(table, data)
    logging.info("OK")


@main.command("select")
@click.pass_context
@click.argument("table")
@click.argument("data_file", type=click.Path(exists=False))
@click.option("--sheet", "-s", default=None)
@click.option("--delimiter", "-d", default=",")
def select_from_table(ctx, table, data_file, sheet, delimiter):
    client = ctx.obj["client"]
    data = client.select_from_table(table)
    df = records_to_dataframe(data)
    write_dataframe(df, data_file, sheet=sheet, delimiter=delimiter)
    logging.info("OK")


@main.group("metadata")
def metadata():
    pass


@metadata.command("get")
@click.pass_context
@click.argument("table")
@click.argument("metadata_file", type=click.Path(exists=False))
@click.option("--encoding", "-e", default=DEFAULT_ENCODING)
def get_metadata(ctx, table, metadata_file, encoding):
    client = ctx.obj["client"]
    metadata = client.get_metadata(table)
    write_json(metadata, metadata_file, encoding=encoding)
    logging.info("OK")


@metadata.command("set")
@click.pass_context
@click.argument("table")
@click.argument("metadata_file", type=click.Path(exists=False))
@click.option("--encoding", "-e", default=DEFAULT_ENCODING)
def set_metadata(ctx, table, metadata_file, encoding):
    metadata = read_metadata_json(metadata_file, encoding)
    client = ctx.obj["client"]
    client.set_metadata(table, metadata)
    logging.info("OK")


@main.command("test")
@click.pass_context
def test_roundtrip(ctx):
    client = ctx.obj["client"]
    roundtrip(client)
    logging.info("OK")


@main.command("move")
@click.pass_context
@click.argument("table")
@click.argument("target_schema")
def move_table(ctx, table, target_schema):
    client = ctx.obj["client"]
    client.move_table(table, target_schema)
    logging.info("OK")


if __name__ == "__main__":
    try:
        main(prog_name=PROG_NAME)
    except OepApiException as exc:
        logging.error("%s: %s", exc.__class__.__name__, str(exc))
        sys.exit(1)
