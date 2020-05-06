# coding: utf-8
import datetime
from .client import OepClient
import pandas as pd
import random
from argparse import ArgumentParser


example_metadata = {
    "id": "test",
    "description": "Test Table",
    "keywords": ["test"],
    "resources": [
        {
            "name": "test",
            "schema": {
                "fields": [
                    {
                        "name": "field1",
                        "type": "string",
                        "description": "column description",
                    },
                    {"name": "field2", "type": "integer", "unit": "none"},
                ],
                "foreignKeys": [],
            },
        }
    ],
}

exacmple_record = {"field1": "test", "field2": 999}


def test(token, test_rows=10, batch_size=None):
    example_data = pd.DataFrame([exacmple_record] * test_rows)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    tablename = "test_%s_%d" % (timestamp, random.randint(100000, 999999))
    cl = OepClient(token=token, tablename=tablename)
    cl.create(metadata=example_metadata)
    try:
        cl.upload_data(dataframe=example_data, batch_size=batch_size)
        cl.update_metadata(metadata=example_metadata)
        data = cl.download_data()
        metadata = cl.download_metadata()
        print(len(data))
        print(data[0])
        print(metadata)
    finally:
        cl.delete()
