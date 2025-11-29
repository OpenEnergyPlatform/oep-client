# OEP Client

This tool eases data sharing with the Open Energy Platform (OEP). Common tasks
on the OEP are:

- creating a table
- uploading data
- updating a table's metadata
- downloading data
- retrieving a table's metadata
- deleting a table (that you created)

You can also always just use the
[API](https://openenergyplatform.github.io/academy/tutorials/01_api/01_api_download/)
directly if your tasks are more complex.

## Notes for Windows Users

All the example commands below use `python3`, because we need python 3. Under
Windows, it's most likely to be `python.exe` or just `python`.

## Installation

Install package `oep-client` from python package index with pip:

```bash
python3 -m pip install --upgrade oep-client
```

## Authentification

You need to be
[registered on the OEP platform](https://openenergyplatform.org/user/register)
and have a valid API token. You can find your token in your user profile on the
OEP under _Your Security Information_.

## Test

There is a short test script that creates a table on the platform, uploads data
and metadata, downloads them again and finally deletes the table.

You can run it either directly from the command prompt using

```bash
oep-client -t OEP_API_TOKEN test
```

## Notes on Data and Metadata

Supported filetypes that the client can work with are are: xslx, csv, json. Your
metadata must be a json file that complies with the
[metadata specification of the OEP](https://github.com/OpenEnergyPlatform/metadata).

## Notes on Usage

All tasks can be executed either directly as a comand line script (CLI)
`oep-client` that comes with this package, or in a python environment.

The CLI is very handy for standardized tasks as it requires just one command
line, but is somewhat limited when for instance your input data is not in a very
specific format. To see avaiblabe command line options, use

```bash
oep-client --help
```

In a python environment, you have more flexibility to prepare / clean your data
before uploading it.

## Using the CLI

### Creating a table

Requires a valid metadata file.

You need to specify names and data types of your columns in the metadata, which
also must be valid
[postgres data types](https://www.postgresql.org/docs/9.5/datatype.html "postgres data types").

`metadata.json`

```json
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

### Uploading data

```bash
oep-client -t OEP_API_TOKEN insert TABLE_NAME FILENAME
```

if `FILENAME` is a

- `xlsx`, you _have to_ also specify `--sheet SHEETNAME`
- `csv`, you _may_ also specify `--delimiter DELIMITER` and or
  `--encoding ENCODING`

### Updating a table's metadata

This of course requires a valid metadata file.

```bash
oep-client -t OEP_API_TOKEN metadata set TABLE_NAME metadata.json
```

### Downloading data

Note: you do not need an API_TOKEN to downlad data

```bash
oep-client -t OEP_API_TOKEN select TABLE_NAME FILENAME
```

if `FILENAME` is a

- `xlsx`, you _have to_ also specify `--sheet SHEETNAME`
- `csv`, you _may_ also specify `--delimiter DELIMITER` and or
  `--encoding ENCODING`

### Retrieving a table's metadata

Note: you do not need an API_TOKEN to downlad metadata.

```bash
oep-client -t OEP_API_TOKEN metadata get TABLE_NAME FILENAME
```

### Deleting a table (that you created)

```bash
oep-client -t OEP_API_TOKEN drop TABLE_NAME
```

### Using the Package in Python

All examples assume that you import the package and create a client instance
first:

```bash
from oep_client import OepClient
cl = OepClient(token='API_TOKEN', ...)
```
