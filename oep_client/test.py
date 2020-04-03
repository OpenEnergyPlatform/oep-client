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

example_data = pd.DataFrame([{"field1": "test"}, {"field1": "test2", "field2": 9}])


def test(token):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    tablename = "test_%s_%d" % (timestamp, random.randint(100000, 999999))
    cl = OepClient(token=token, tablename=tablename)
    cl.create(metadata=example_metadata)
    try:
        cl.upload_data(dataframe=example_data)
        cl.update_metadata(metadata=example_metadata)
        data = cl.download_data()
        metadata = cl.download_metadata()
        print(data)
        print(metadata)
    finally:
        cl.delete()
