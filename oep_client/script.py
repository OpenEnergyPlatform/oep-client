# coding: utf-8
import sys
import logging
import argparse
from .client import OepClient
from .test import test as testscript
from . import __version__

logger = logging.getLogger()

# show version in help
class ArgumentParser(argparse.ArgumentParser):
    version_line = "oep-client version %s" % __version__

    def format_help(self):
        help = super().format_help()
        return self.version_line + "\n" + help


def setup_logging(loglevel="INFO"):
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
    test,
    test_rows,
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

    cl = OepClient(**settings)
    if test:
        if any(
            (delete, validate, create, upload_data, download_data, download_metadata)
        ):
            raise Exception("Cannot use action in combination with test")
        testscript(token, test_rows=test_rows)
    elif delete:
        if any((test, validate, create, upload_data, download_data, download_metadata)):
            raise Exception("Cannot use action in combination with delete")
        cl.delete(metadata=metadata)
    elif download_data or download_metadata:  # download workflow
        if any((test, delete, validate, create, upload_data, update_metadata)):
            raise Exception(
                "Cannot use action in combination with download_data/download_metadata"
            )
        if download_data:
            df = cl.download_data(metadata=metadata)
            cl.save_dataframe(
                df, download_data, sheet=sheet, delimiter=delimiter, encoding=encoding
            )
        if download_metadata:
            meta = cl.download_metadata(metadata=metadata)
            cl.save_json(meta, download_metadata)
    elif create or upload_data or update_metadata or validate:  # upload workflow
        if any((test, delete, download_data, download_metadata)):
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
            df = cl.load_dataframe(
                upload_data, sheet=sheet, delimiter=delimiter, encoding=encoding
            )
            cl.upload_data(df, metadata=metadata)
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
    ap.add_argument("--test", action="store_true", help="run test script")
    ap.add_argument("--test_rows", help="number of test rows to upload", type=int)

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
