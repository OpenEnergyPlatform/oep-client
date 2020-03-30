# OEP client
This document describes how to upload data to the [OEP](https://openenergy-platform.org "OEP") using Python and the REST-API. Prerequisite is that you are a registered user on the [OEP](https://openenergy-platform.org/user/register "OEP").
## Create and upload data table(s)
* The REST-API can be used with any language than can make HTTP(s) requests.

* Most requests require you to add an authorization header: 
Authorization: `Token API_TOKEN`, where you substitute `API_TOKEN` with your token. You can find your token in your user profile on the OEP under _Your Security Information_.

* All requests (and most responses) will use json data as payload. A paylpad is the actual data content of the request.

* An example is provided below. For it, we use python and the [requests package](https://2.python-requests.org/en/master/ "Python request package"). All requests will use a requests session with the authorization header.

```
import requests
API_URL = 'https://openenergy-platform.org/api/v0'
session = requests.Session()
session.headers = {'Authorization': 'Token %s' % API_TOKEN}
``` 
* The requests in the following sections use roughly the same pattern: 
    * Prepare your request payload as a json object
    * Prepare your request url
    * Send your request using the correct verb (get, post, put, delete)
    * Check if the request was successful

### Create a new table
* You will create the tables at first in the [_model_draft_](https://openenergy-platform.org/dataedit/view/model_draft) schema. After a successful review later, the table will be moved to the final target schema.

* You need to specify the name of the new table (`TABLE_NAME`), which should be a valid
post-gresql table name, without spaces, ideally only containing lower case letters, numbers and underscores.

* You also need to specify names and data types of your columns, which also must be valid [post-gres data types](https://www.postgresql.org/docs/9.5/datatype.html "postgres data types").
```
# prepare request payload
data = {'query': {  
  'columns': [
    {
      'name': 'id',
      'data_type': 'bigserial'
    }, 
    # add more columns here
    ],
    'constraints': [
      {'constraint_type': 'PRIMARY KEY', 'constraint_parameter': 'id'}
    ]
}}

# prepare api url
url = API_URL + '/schema/model_draft/tables/' + TABLE_NAME

# make request and check using PUT
res = session.put(url, json=data)
res.raise_for_status()  # check: throws exception if not successful
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
res.raise_for_status()  # check
```
* You can repeat this if you want to upload your data in multiple batches.

## Starting over: Deleting your table
* While the table is still in the model draft, you can always delete the table and start over: 
```
# prepare api url
url = API_URL + '/schema/model_draft/tables/' + TABLE_NAME

# make request
res = session.delete(url)
res.raise_for_status() # check
````


