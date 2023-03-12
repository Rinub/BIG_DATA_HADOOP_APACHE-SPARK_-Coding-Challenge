#=================================================================================
from task2_1 import get_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max
from pyspark.sql.functions import avg

# create a SparkSession
spark = SparkSession.builder \
    .appName("AirBnB Data Analysis") \
    .getOrCreate()

def write_out_2_2(write_path='out/out_2_2.txt'):

  """  
  This function writes the output by executing functions agg_df.
  Args: 
    write_path: output file path for Part 2 task 2
  Returns:
    None
  """

  agg_df_val = agg_df()
  # write to a CSV file
  agg_df_val.write.csv(write_path, header=True)

def agg_df():

  """  
  This function takes the input by excuting get_data and returns the minimum price, maximum price, and total row count.
  Args: 
    None 
  Returns:
    This functions returns aggrigated values such as minimum price, maximum price, and total row count 
  """

  Spark_df = get_data()
# compute aggregates and store in a new DataFrame
  agg_df_val = Spark_df.agg(min("price").alias("min_price"), 
                  max("price").alias("max_price"), 
                  count("*").alias("row_count"))
  return agg_df_val 

write_out_2_2()

spark.stop()


