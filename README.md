# darwin_sea_exploration
## Project Description
Schema validation for Taxfix takehome assignment

## Project Structure
```
.
├── darwin_sea_exploration
│   ├── data
│   │   └── input.json
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
 1) Due to limited amount of data, required fields are only based on the two provided jsons. Hence if a field is only seen in one, then it is not required and if a field is seen in both, then it is required.