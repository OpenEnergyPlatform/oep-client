
.. figure:: https://user-images.githubusercontent.com/14353512/185425447-85dbcde9-f3a2-4f06-a2db-0dee43af2f5f.png
    :align: left
    :target: https://github.com/rl-institut/super-repo/
    :alt: Repo logo

==========
OEP Client
==========

.. list-table::
   :widths: auto

   * - License
     - |badge_license|
   * - Documentation
     - |badge_documentation|
   * - Publication
     -
   * - Development
     - |badge_issue_open| |badge_issue_closes| |badge_pr_open| |badge_pr_closes|
   * - Community
     - |badge_contributing| |badge_contributors| |badge_repo_counts|

.. contents::
    :depth: 2
    :local:
    :backlinks: top

Tools
=======

This tool eases data sharing with the Open Energy Platform (OEP). Common tasks on the OEP are:

- creating a table
- uploading data
- updating a table's metadata
- downloading data
- retrieving a table's metadata
- deleting a table (that you created)

You can also always just use the API (TODO: link to documentation) directly if your tasks are more complex.

Notes for Windows Users
========================

All the example commands below use `python3`, because we need python 3. Under Windows, it's most likely to be `python.exe` or just `python`.

Installation
========================

Install package `oep-client` from python package index with pip:

.. code-block:: bash

    python3 -m pip install --upgrade oep-client

Authentification
========================

You need to be `registered on the OEP platform <https://openenergy-platform.org/user/register>`_ and have a valid API token. You can find your token in your user profile on the OEP under _Your Security Information_.

Test
========================

There is a short test script that creates a table on the platform, uploads data and metadata, downloads them again and finally deletes the table.

You can run it either directly from the command prompt using

.. code-block:: bash

    oep-client -t OEP_API_TOKEN test

Notes on Data and Metadata
========================

Supported filetypes that the client can work with are are: xslx, csv, json. Your metadata must be a json file that complies with the `metadata specification of the OEP <https://github.com/OpenEnergyPlatform/metadata>`_.

Notes on Usage
========================

All tasks can be executed either directly as a command line script (CLI) `oep-client` that comes with this package, or in a python environment.

The CLI is very handy for standardized tasks as it requires just one command line, but is somewhat limited when for instance your input data is not in a very specific format. To see available command line options, use

.. code-block:: bash

    oep-client --help

In a python environment, you have more flexibility to prepare / clean your data before uploading it.

Using the CLI
========================

Creating a Table
-----------------

Requires a valid metadata file.

You need to specify names and data types of your columns in the metadata, which also must be valid `postgres data types <https://www.postgresql.org/docs/9.5/datatype.html>`_.

Example `metadata.json`:

.. code-block:: json

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

To create a table:

.. code-block:: bash

    oep-client -t OEP_API_TOKEN create TABLE_NAME metadata.json

Uploading Data
--------------
.. code-block:: bash

    oep-client -t OEP_API_TOKEN insert TABLE_NAME FILENAME

If `FILENAME` is a...

- `xlsx`, you _have to_ also specify `--sheet SHEETNAME`
- `csv`, you _may_ also specify `--delimiter DELIMITER` and or `--encoding ENCODING`

Updating a Table's Metadata
----------------------------
This of course requires a valid metadata file.

.. code-block:: bash

    oep-client -t OEP_API_TOKEN metadata set TABLE_NAME metadata.json

Downloading Data
-----------------
Note: you do not need an API_TOKEN to download data. Also, the table might not be in the `model_draft` schema, in which case you can specify the table name as `schema_name.table_name`.

.. code-block:: bash

    oep-client -t OEP_API_TOKEN select TABLE_NAME FILENAME

Retrieving a Table's Metadata
-------------------------------
Note: you do not need an API_TOKEN to download metadata.

.. code-block:: bash

    oep-client -t OEP_API_TOKEN metadata get TABLE_NAME FILENAME

Deleting a Table (That You Created)
------------------------------------
.. code-block:: bash

    oep-client -t OEP_API_TOKEN drop TABLE_NAME

Using the Package in Python
========================

All examples assume that you import the package and create a client instance first:

.. code-block:: python

    from oep_client import OepClient
    cl = OepClient(token='API_TOKEN', ...)

... TODO

More Information - Use the API without the oep-client
========================

This section describes how to upload data to the `OEP <https://openenergy-platform.org>`_ using Python and the REST-API.

Create and Upload Data Table(s)
-------------------------------
The REST-API can be used with any language that can make HTTP(s) requests. Most requests require you to add an authorization header: Authorization: `Token API_TOKEN`.

All requests will use json data as payload. An example is provided below using Python and the `requests package <https://2.python-requests.org/en/master/>`.

.. code-block:: python

    import requests
    API_URL = 'https://openenergy-platform.org/api/v0'
    session = requests.Session()
    session.headers = {'Authorization': 'Token %s' % API_TOKEN}

Create a new table:

.. code-block:: python

    # Prepare request payload as a json object and prepare your request url
    # Send your request using the correct verb and check if the request was successful

    # Example: PUT request to create a table
    res = session.put(url, json=data)
    res.raise_for_status()

Upload Data
-----------
To upload data, load it into a json structure as a list representing data rows, each of which is a dictionary mapping column names to values.

Example using pandas to read data from an Excel workbook:

.. code-block:: python

    import pandas as pd
    import json

    # Load data into dataframe and convert into json
    df = pd.read_excel(WORKBOOK, WORKSHEET)
    records = df.to_json(orient='records')
    records = json.loads(records)

    # Prepare request payload
    data = {'query': records}

    # Prepare API url
    url = API_URL + '/schema/model_draft/tables/' + TABLE_NAME + '/rows/new'

    # Make request
    res = session.post(url, json=data)
    res.raise_for_status()




.. |badge_license| image:: https://img.shields.io/github/license/OpenEnergyPlatform/oep-client
    :target: LICENSE.txt
    :alt: License

.. |badge_documentation| image::
    :target:
    :alt: Documentation

.. |badge_contributing| image:: https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat
    :alt: contributions

.. |badge_repo_counts| image:: http://hits.dwyl.com/OpenEnergyPlatform/oep-client.svg
    :alt: counter

.. |badge_contributors| image:: https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square
    :alt: contributors

.. |badge_issue_open| image:: https://img.shields.io/github/issues-raw/OpenEnergyPlatform/oep-client
    :alt: open issues

.. |badge_issue_closes| image:: https://img.shields.io/github/issues-closed-raw/OpenEnergyPlatform/oep-client
    :alt: closes issues

.. |badge_pr_open| image:: https://img.shields.io/github/issues-pr-raw/OpenEnergyPlatform/oep-client
    :alt: closes issues

.. |badge_pr_closes| image:: https://img.shields.io/github/issues-pr-closed-raw/OpenEnergyPlatform/oep-client
    :alt: closes issues

