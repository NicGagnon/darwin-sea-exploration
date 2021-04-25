# darwin_sea_exploration
## Project Description
Schema validation for Taxfix takehome assignment

## Project Structure
```
.
├── darwin_sea_exploration
│   ├── data
│   │   ├── ga-sessions
│   │   └── transaction-data
│   ├── sql_queries # Part 1 Answers
│   │   ├── 1.sql
│   │   ├── 2.sql
│   │   ├── 3.sql
│   │   └── 4.sql
│   ├── tests
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │   ├── fixtures.py
│   │   └── test_utils.py
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
```python darwin_sea_exploration/main.py```
 

####For Testing

```python -m pytest darwin_sea_exploration/tests```


### Future Considerations
 