# OEP Client

This tool eases data sharing with the Open Energy Platform (OEP). Common tasks on the OEP are:

- creating a table
- uploading data
- updating a table's metadata
- downloading data
- retrieving a table's metadata
- deleting a table (that you created)

You can also always just use the API `(TODO: link to documentation)` directly if your tasks are more complex.

## Notes for Windows Users

All the example commands below use `python3`, because we need python 3. Under Windows, it's most likely to be `python.exe` or just `python`.

## Installation

Install package `oep-client` from python package index with pip:

```bash
python3 -m pip install --upgrade oep-client
```

## Authentification

You need to be [registered on the OEP platform](https://openenergyplatform.org/user/register) and have a valid API token.
You can find your token in your user profile on the OEP under _Your Security Information_.

## Test

There is a short test script that creates a table on the platform, uploads data and metadata, downloads them again
and finally deletes the table.

You can run it either directly from the command prompt using

```
oep-client -t OEP_API_TOKEN test
```

## Notes on Data and Metadata

Supported filetypes that the client can work with are are: xslx, csv, json.
Your metadata must be a json file that complies with the [metadata specification of the OEP](https://github.com/OpenEnergyPlatform/metadata).

## Notes on Usage

All tasks can be executed either directly as a comand line script (CLI) `oep-client` that comes with this package, or in a python environment.

The CLI is very handy for standardized tasks as it requires just one command line, but is somewhat limited when for instance your input data is not in a very specific format.
To see avaiblabe command line options, use

```
oep-client --help
```

In a python environment, you have more flexibility to prepare / clean your data before uploading it.

# Using the CLI

## Creating a table

Requires a valid metadata file.

You need to specify names and data types of your columns in the metadata, which also must be valid [postgres data types](https://www.postgresql.org/docs/9.5/datatype.html "postgres data types").

`metadata.json`

```
{
  "resources": [
    {
      "schema": {
        "fields": [
          {
            "name": "id",
            "type": "bigserial"
          },
          {
            "name": "field_1",
            "type": "varchar(32)",
            "description": "column description",
            "unit": "unit name"
          }
        ]
      }
    }
  ]
}
```

```bash
oep-client -t OEP_API_TOKEN create TABLE_NAME metadata.json
```

## Uploading data

```bash
oep-client -t OEP_API_TOKEN insert TABLE_NAME FILENAME
```

if `FILENAME` is a

- `xlsx`, you _have to_ also specify `--sheet SHEETNAME`
- `csv`, you _may_ also specify `--delimiter DELIMITER` and or `--encoding ENCODING`

## Updating a table's metadata

This of course requires a valid metadata file.

```bash
oep-client -t OEP_API_TOKEN metadata set TABLE_NAME metadata.json
```

## Downloading data

Note: you do not need an API_TOKEN to downlad data. Also, the table might not be in the `model_draft` schema, in which case you can specify the table name as `schema_name.table_name`. -> [List of schemas](https://openenergyplatform.org/dataedit/schemas).

```bash
oep-client -t OEP_API_TOKEN select TABLE_NAME FILENAME
```

if `FILENAME` is a

- `xlsx`, you _have to_ also specify `--sheet SHEETNAME`
- `csv`, you _may_ also specify `--delimiter DELIMITER` and or `--encoding ENCODING`

## Retrieving a table's metadata

Note: you do not need an API_TOKEN to downlad metadata. Also, the table might not be in the `model_draft` schema, in which case you can specify the table name as `schema_name.table_name`. -> [List of schemas](https://openenergyplatform.org/dataedit/schemas).

```bash
oep-client -t OEP_API_TOKEN metadata get TABLE_NAME FILENAME
```

## Deleting a table (that you created)

```bash
oep-client -t OEP_API_TOKEN drop TABLE_NAME
```

# Using the Package in Python

All examples assume that you import the package and create a client instance first:

```
from oep_client import OepClient
cl = OepClient(token='API_TOKEN', ...)
```

`... TODO`

# More Information - Use the API without the oep-client

This section describes how to upload data to the [OEP](https://openenergyplatform.org "OEP") using Python and the REST-API.

## Create and upload data table(s)

- The REST-API can be used with any language than can make HTTP(s) requests.

- Most requests require you to add an authorization header:
  Authorization: `Token API_TOKEN`, where you substitute `API_TOKEN` with your token.

- All requests (and most responses) will use json data as payload. A paylpad is the actual data content of the request.

- An example is provided below. For it, we use python and the [requests package](https://2.python-requests.org/en/master/ "Python request package"). All requests will use a requests session with the authorization header.

```
import requests
API_URL = 'https://openenergyplatform.org/api/v0'
session = requests.Session()
session.headers = {'Authorization': 'Token %s' % API_TOKEN}
```

- The requests in the following sections use roughly the same pattern:
  - Prepare your request payload as a json object
  - Prepare your request url
  - Send your request using the correct verb (get, post, put, delete)
  - Check if the request was successful

### Create a new table

- You will create the tables at first in the [_model_draft_](https://openenergyplatform.org/dataedit/view/model_draft) schema. After a successful review later, the table will be moved to the final target schema.

- You need to specify the name of the new table (`TABLE_NAME`), which should be a valid
  post-gresql table name, without spaces, ideally only containing lower case letters, numbers and underscores.

# make request and check using PUT

res = session.put(url, json=data)
res.raise_for_status() # check: throws exception if not successful

```
### Upload data
* To upload data, you must first load it into a json structure as a [list](https://www.w3schools.com/python/python_lists.asp "python lists") representing data rows, each of which is a [dictionary](https://www.w3schools.com/python/python_dictionaries.asp "python dictionary") mapping column names to values.

* In the example, we will use [pandas](https://pypi.org/project/pandas/ "pandas") to read data from an Excel workbook (`WORKBOOK, WORKSHEET`) into a [data frame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html "data frame") which we will then convert into a json object. Please note that this step will most likely require some modification to accommodate the specifics of your in-put data.

* In addition to that, at the end, you need to load your data into the specified json structure.

* After that, the data can be uploaded making a request to the API:
```

# load data into dataframe, convert into json

df = pd.read_excel(WORKBOOK, WORKSHEET)
records = df.to_json(orient='records')
records = json.loads(records)

# prepare request payload

data = {'query': records}

# prepare api url

url = API_URL + '/schema/model_draft/tables/' + TABLE_NAME + '/rows/new'

# make request

res = session.post(url, json=data)
res.raise_for_status() # check
