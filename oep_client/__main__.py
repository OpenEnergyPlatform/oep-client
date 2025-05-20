"""Command line script for OepClient
"""

__version__ = "0.17.1"

import json
import logging
import os
import sys

import click

from oep_client.exceptions import OepApiException
from oep_client.oep_client import (
    DEFAULT_API_VERSION,
    DEFAULT_BATCH_SIZE,
    DEFAULT_HOST,
    DEFAULT_INSERT_RETRIES,
    DEFAULT_PROTOCOL,
    DEFAULT_SCHEMA,
    TOKEN_ENV_VAR,
    OepClient,
)
from oep_client.test import TestRoundtrip
from oep_client.utils import (
    dataframe_to_records,
    get_schema_definition_from_metadata,
    read_dataframe,
    read_metadata_json,
    records_to_dataframe,
    write_dataframe,
    write_json,
)

PROG_NAME = "oep-client"
LOGGING_DATE_FMT = "%Y-%m-%d %H:%M:%S"
LOGGING_FMT = "[%(asctime)s.%(msecs)03d %(levelname)7s] %(message)s"
DEFAULT_ENCODING = "utf-8"


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
@click.option("--protocol", default=DEFAULT_PROTOCOL)
@click.option("--host", default=DEFAULT_HOST)
@click.option("--api-version", default=DEFAULT_API_VERSION)
@click.option("--schema", "-s", default=DEFAULT_SCHEMA)
@click.option("--batch-size", "-b", default=DEFAULT_BATCH_SIZE)
@click.option("--insert-retries", default=DEFAULT_INSERT_RETRIES)
def main(
    ctx,
    loglevel,
    token,
    protocol,
    host,
    api_version,
    schema,
    batch_size,
    insert_retries,
):
    loglevel = getattr(logging, loglevel.upper())
    logging.basicConfig(format=LOGGING_FMT, datefmt=LOGGING_DATE_FMT, level=loglevel)
    ctx.ensure_object(dict)

    token = token or os.environ.get(TOKEN_ENV_VAR)

    ctx.obj["client"] = OepClient(
        token=token,
        protocol=protocol,
        host=host,
        api_version=api_version,
        default_schema=schema,
        batch_size=batch_size,
        insert_retries=insert_retries,
    )


@main.command("create")
@click.pass_context
@click.argument("table")
@click.argument("metadata_file", type=click.Path())
@click.option("--encoding", "-e", default=DEFAULT_ENCODING)
@click.option("--upload-metadata", "-m", is_flag=True)
def create_table(ctx, table, metadata_file, encoding, upload_metadata):
    metadata = read_metadata_json(metadata_file, encoding)
    definition = get_schema_definition_from_metadata(metadata)
    client = ctx.obj["client"]
    client.create_table(table, definition)
    # automatically upload metadata
    if upload_metadata:
        client.set_metadata(table, metadata)
    logging.info("OK")


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
@click.argument("data_file", type=click.Path())
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
@click.argument("data_file", type=click.Path(exists=False), required=False)
@click.option("--where", "-w", multiple=True)
@click.option("--sheet", "-s", default=None)
@click.option("--delimiter", "-d", default=",")
def select_from_table(ctx, table, data_file, where, sheet, delimiter):
    client = ctx.obj["client"]
    data = client.select_from_table(table, where=where)
    if data_file:
        df = records_to_dataframe(data)
        write_dataframe(df, data_file, sheet=sheet, delimiter=delimiter)
    else:
        # print to stdout
        datas = json.dumps(data, ensure_ascii=False, indent=2)
        datab = datas.encode()
        sys.stdout.buffer.write(datab)

    logging.info("OK")


@main.group("metadata")
def metadata():
    pass


@metadata.command("get")
@click.pass_context
@click.argument("table")
@click.argument("metadata_file", type=click.Path(exists=False), required=False)
@click.option("--encoding", "-e", default=DEFAULT_ENCODING)
def get_metadata(ctx, table, metadata_file, encoding):
    client = ctx.obj["client"]
    metadata = client.get_metadata(table)

    if metadata_file:
        write_json(metadata, metadata_file, encoding=encoding)
    else:
        # print to stdout
        datas = json.dumps(metadata, ensure_ascii=False, indent=2)
        datab = datas.encode()
        sys.stdout.buffer.write(datab)

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
@click.argument("test_schema", default="sandbox")
def test_roundtrip(ctx, test_schema):
    client = ctx.obj["client"]
    TestRoundtrip().test_roundtrip(client=client, schema=test_schema)
    logging.info("OK")


@main.command("move")
@click.pass_context
@click.argument("table")
@click.argument("target_schema")
def move_table(ctx, table, target_schema):
    client = ctx.obj["client"]
    client.move_table(table, target_schema)
    logging.info("OK")


@main.command("count")
@click.pass_context
@click.argument("table")
def count(ctx, table):
    client = ctx.obj["client"]
    n_rows = client.count_rows(table)
    logging.info("%d", n_rows)


@main.command("delete")
@click.pass_context
@click.argument("table")
def delete(ctx, table):
    client = ctx.obj["client"]
    client.delete_from_table(table)
    logging.info("OK")


@main.command("list")
@click.pass_context
def iter_tables(ctx):
    client = ctx.obj["client"]
    for item in client.iter_tables():
        print("%(schema)s.%(table)s" % item)


if __name__ == "__main__":
    try:
        main(prog_name=PROG_NAME)
    except OepApiException as exc:
        logging.error("%s: %s", exc.__class__.__name__, str(exc))
        sys.exit(1)
