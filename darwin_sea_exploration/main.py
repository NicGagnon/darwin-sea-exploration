# Import pyspark related modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count

# Import other modules
from marshmallow.exceptions import ValidationError
from pathlib import Path
import simplejson
import os

# Local imports
from darwin_sea_exploration.utils.schema import get_schema, DataSchema


def create_pyspark_session():
    """
    Create and return a pyspark session
    :return: Pyspark session
    """
    scSpark = SparkSession \
        .builder \
        .appName("reading JSON") \
        .getOrCreate()
    return scSpark


def load_data(input_folder, scSpark):
    """
    Aggregate data and return the aggregate recipe data set
    :return: Spark Dataframe
    """
    try:
        raw_data_path = input_folder.joinpath('input.json')
        folderpath = input_folder.joinpath('input')
        transfer_valid_files(raw_data_path, folderpath)
        sdf_data = scSpark.read.json(f"{folderpath}/*.json", schema=get_schema()).cache()
    except Exception as e:
        print(f"Error occurred while trying to read dataset: {e}")
        sdf_data = scSpark.createDataFrame([], schema=get_schema())
    return sdf_data


def transfer_valid_files(filepath, folderpath):
    # create the output directory to save data
    os.makedirs(folderpath, exist_ok=True)

    cnt = 0
    with open(filepath, 'r') as fpr:
        for line in fpr:
            try:
                raw_content = simplejson.loads(line)
                DataSchema().load(raw_content)
                with open(folderpath.joinpath(f'part-{cnt}.json'), 'w') as fpw:
                    valid_content = simplejson.dumps(raw_content)
                    fpw.write(valid_content)
                cnt += 1
            except ValidationError as e:
                print(f"Validation Error has occured: {e}")


def save_data(sdf_data, output_folder_path):
    """
    Save the spark dataframe to the provided output folder.
    :param sdf_data: Spark DataFrame
    :param output_folder_path: pathlib Path object - target location to save file
    :return: n/a
    """
    try:
        sdf_data.coalesce(1).write.csv(f"{output_folder_path}/report", mode='overwrite', header=True)
        print("Data Successfully Saved")
    except Exception as e:
        print(f"Error has occurred while trying to save the files: {e}")


def main():
    """
    Orchestrator of the project. This reads in the data, transforms the recipes into desired output and saves them
    :return: n/a
    """
    # Create Folder Paths
    data_folder_path = Path(__file__).parent.joinpath('data')

    # Create PySpark session to extract out recipe related information
    scSpark = create_pyspark_session()
    sdf_data = load_data(data_folder_path, scSpark)
    sdf_data = sdf_data.groupBy(["event", "timestamp"]).count().alias("event_count")
    sdf_data.show()
    save_data(sdf_data, data_folder_path)
    scSpark.stop()


if __name__ == '__main__':
    main()
