# Third Party imports
from marshmallow.exceptions import ValidationError
from pathlib import Path
import simplejson

# Local Imports
from darwin_sea_exploration.utils.schema import DataSchema


def load_data():
    datadir = Path(__file__).parent.joinpath('data', 'input.json')
    content = []
    with open(datadir, 'r') as fp:
        for line in fp:
            content.append(simplejson.loads(line))
    return content


def main():
    try:
        input_data = load_data()
        for data in input_data:
            DataSchema().load(data)
        print("Data Validation successful")
    except ValidationError as e:
        print(f"Validation Error has occured: {e}")


if __name__ == "__main__":
    main()