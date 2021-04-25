# darwin_sea_exploration
## Project Description
Schema validation for Taxfix takehome assignment. My approach for both schema validation and large dataset inputs was to use a combination of marshmallow for verbose schema fitting and then storing individual json object into separate files. Then, I use batch processing through pyspark to aggregate all the files into a lazy dataframe and perform all my transformation on top of the spark df. This provides both quality control of the incoming data and efficient management of large source data. Ideally, I would batch the json objects in groups of 200-300 per file, however I will save this as a future improvement.

## Project Structure
```
.
├── darwin_sea_exploration
│   ├── data
│   │   ├── input # Folder for storing individual json files
│   │   ├── report # Folder where the results of the ETL pipeline are stored
│   │   └── input.json
│   ├── tests
│   │   ├── resources # test related resources
│   │   │   ├── sample_data.json 
│   │   │   ├── sample_missing_data.json 
│   │   │   └── sample_null_data.json 
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │   ├── fixtures.py
│   │   └── test_utils.py
│   ├── utils
│   │   ├── __init__.py
│   │   └── schema.py
│   ├── __init__.py
│   ├── main.py # main entrypoint
├── .gitignore   
├── Dockerfile
├── Pipfile
├── Pipfile.lock
└── README.md
```
## How to run
```docker build -t darwin .```<br/>
```docker run --rm -it darwin bash```<br/>
```python -m darwin_sea_exploration.main```
 

####For Testing

```python -m pytest darwin_sea_exploration/tests```


### Assumptions
 1) Due to limited amount of data, required fields are only based on the two provided jsons. Hence if a field is only seen in one, then it is not required and if a field is seen in both, then it is required.