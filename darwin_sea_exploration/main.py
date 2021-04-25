# Import pyspark related modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, to_date

# Import other modules
from marshmallow.exceptions import ValidationError
from pathlib import Path
import simplejson
import os
import shutil

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


def load_data(input_folder, scSpark, filename):
    """
    Aggregate data and return the aggregate recipe data set
    :return: Spark Dataframe
    """
    try:
        raw_data_path = input_folder.joinpath(filename)
        folderpath = input_folder.joinpath('input')
        transfer_valid_files(raw_data_path, folderpath)
        sdf_data = scSpark.read.json(f"{folderpath}/*.json", schema=get_schema()).cache()
    except Exception as e:
        print(f"Error occurred while trying to read dataset: {e}")
        sdf_data = scSpark.createDataFrame([], schema=get_schema())
    return sdf_data


def transfer_valid_files(filepath, folderpath):
    """
    transfers individual json objects to independent files for manageable batch processing with spark
    :param filepath: target input file
    :param folderpath: target output folder to store the files
    :return: n/a
    """
    # create the output directory to save data
    if os.path.exists(folderpath):
        shutil.rmtree(folderpath)
    os.makedirs(folderpath)

    with open(filepath, 'r') as fpr:
        cnt = 0
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


def main(input_filename):
    """
    Orchestrator of the project. This reads in the data, transforms the event data into desired output and saves them
    :return: n/a
    """
    # Create Folder Paths
    data_folder_path = Path(__file__).parent.joinpath('data')

    # Create PySpark session to extract out recipe related information
    scSpark = create_pyspark_session()
    sdf_data = load_data(data_folder_path, scSpark, input_filename)
    sdf_data = perform_transformations(sdf_data)

    save_data(sdf_data, data_folder_path)
    scSpark.stop()


def perform_transformations(sdf_data):
    """
    Helper function to perform all transformations.
    :param sdf_data: Pyspark Dataframe
    :return: resulting dataframe after transformations are performed
    """
    sdf_data = sdf_data.withColumn("date", to_date(col("timestamp")))
    sdf_data = sdf_data.groupBy(["event", "date"]).count().alias("event_count")
    return sdf_data


if __name__ == '__main__':
    main(input_filename="input.json")
