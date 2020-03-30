# coding: utf-8

__version__ = "0.0.0"

import sys
import logging
from argparse import ArgumentParser

from .client import OepClient

logger = logging.getLogger()


def setup_logging(loglevel):
    if isinstance(loglevel, str):  # e.g. 'debug'/'DEBUG' -> logger.DEBUG
        loglevel = getattr(logging, loglevel.upper())
    formatter = logging.Formatter("[%(asctime)s %(levelname)7s] %(message)s")
    logger.setLevel(loglevel)

    # remove all existing handlers
    for h in logger.handlers:
        logger.removeHandler(h)

    h = logging.StreamHandler()
    h.setFormatter(formatter)
    h.setLevel(loglevel)
    logger.addHandler(h)


def _main(
    settings,
    metadata,
    validate,
    create,
    delete,
    upload_data,
    download_data,
    update_metadata,
    download_metadata,
    tablename,
    sheet,
    encoding,
    delimiter,
    token,
):
    # configure and validate settings
    if settings:
        settings = OepClient.load_json(settings)
    else:
        settings = {}
    if metadata:
        metadata = OepClient.load_json(metadata)
    else:
        metadata = {}
    if tablename:
        settings["tablename"] = tablename
    if token:
        settings["token"] = token
    if sheet:
        settings["sheet"] = sheet
    if encoding:
        settings["encoding"] = encoding
    if delimiter:
        settings["delimiter"] = delimiter

    cl = OepClient(**settings)

    if delete:
        if any((validate, create, upload_data, download_data, download_metadata)):
            raise Exception("Cannot use action in combination with delete")
        cl.delete()
    elif download_data or download_metadata:  # download workflow
        if any((delete, validate, create, upload_data, update_metadata)):
            raise Exception(
                "Cannot use action in combination with download_data/download_metadata"
            )
        if download_data:
            df = cl.download_data()
            cl.save_dataframe(df, download_data)
        if download_metadata:
            meta = cl.download_metadata()
            cl.save_json(meta, download_metadata)
    elif create or upload_data or update_metadata or validate:  # upload workflow
        if any((delete, download_data, download_metadata)):
            raise Exception(
                "Cannot use action in combination with create/upload_data/update_metadata"
            )
        if validate or create or update_metadata:
            if not metadata:
                raise Exception("upload/validate requires metadata file")
        if validate:
            cl.validate(metadata)
        if create:
            cl.create(metadata)
        if upload_data:
            df = cl.load_dataframe(upload_data)
            cl.upload_data(df)
        if update_metadata:
            cl.update_metadata(metadata)
    else:
        logger.warning("No action selected")


def main():
    # arguments
    ap = ArgumentParser()
    ap.add_argument("--settings", "-s", help="json file with additional configuration")
    ap.add_argument("--metadata", "-m", help="json file with meta data")
    ap.add_argument("--token", "-t", help="user token")
    ap.add_argument(
        "--validate", action="store_true", help="validate meta data before upload"
    )
    ap.add_argument(
        "--create", "-c", action="store_true", help="create table at the beginning"
    )
    ap.add_argument("--delete", action="store_true", help="remove table")
    ap.add_argument(
        "--upload-data", "-u", help="input file with data, can be xlsx, csv, json"
    )
    ap.add_argument(
        "--download-data", "-d", help="file to save data to, can be xlsx, csv, json"
    )
    ap.add_argument(
        "--update-metadata", action="store_true", help="update metadata on platform"
    )
    ap.add_argument(
        "--download-metadata", help="download metadata from platform and save it (json)"
    )
    ap.add_argument("--tablename", "-n", help="table name")
    ap.add_argument("--sheet", help="sheet name, only for xlsx files")
    ap.add_argument("--encoding", "-e", help="text file encoding, only csv")
    ap.add_argument("--delimiter", help="field sparator, only csv")
    ap.add_argument(
        "--loglevel",
        "-l",
        default="INFO",
        type=str,
        help="ERROR, WARNING, INFO, or DEBUG",
    )

    kwargs = vars(ap.parse_args())
    setup_logging(kwargs.pop("loglevel"))

    rc = 0
    try:
        _main(**kwargs)
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        if logger.level == logging.DEBUG:
            logger.error(e, exc_info=e)  # show error trace
        else:
            logger.error(e)
        rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
