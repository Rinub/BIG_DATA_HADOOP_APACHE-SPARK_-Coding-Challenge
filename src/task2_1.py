# Part -2
#===============
# Task -1
#===============
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max
from pyspark.sql.functions import avg
import requests

# create a SparkSession
spark = SparkSession.builder \
    .appName("AirBnB Data Analysis") \
    .getOrCreate()


def get_data(url = 'https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet?raw=true'):

    """  
    This function loads the parquet file to Spark Dataframe API.
    Args: 
      url: input parquet file path mapped from github 
    Returns:
      This function returns Spark Dataframe API
    """
    # download the parquet file 
    response = requests.get(url)
    open('sf-airbnb-clean.parquet', 'wb').write(response.content)
    import pandas as pd
    # read the parquet file into a pandas dataframe
    df = pd.read_parquet('sf-airbnb-clean.parquet')
    # save the dataframe as a CSV file
    df.to_csv('sf-airbnb-clean.csv', index=False)

    Spark_df = spark.read.csv("sf-airbnb-clean.csv", header=True, inferSchema=True)
    return Spark_df

spark.stop()