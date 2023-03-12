from task2_1 import get_data

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max
from pyspark.sql.functions import avg

# create a SparkSession
spark = SparkSession.builder \
    .appName("AirBnB Data Analysis") \
    .getOrCreate()

def write_out_2_3(write_path='out/out_2_3.txt'):

  """  
  This function writes the output out_2_3.txt by executing functions avg_df.
  Args: 
    write_path: output file path for Part 2 task 3
  Returns:
    None
  """

  avg_df_val = avg_df()

    # write to a CSV file
  avg_df_val.write.csv(write_path, header=True)

def avg_df():

  """  
  This function takes the input by excuting get_data
  Args: 
    None 
  Returns:
    This functions returns average number 
    of bathrooms and bedrooms across all the properties listed in this data set with a price of > 5000 
    and a review score being exactly equalt to 10. 
  """

  Spark_df = get_data()
  # filter the relevant rows and compute averages
  avg_df_val = Spark_df.filter((Spark_df.price > 5000) & (Spark_df.review_scores_rating == 10)) \
            .agg(avg("bathrooms").alias("avg_bathrooms"), 
                  avg("bedrooms").alias("avg_bedrooms"))
  return avg_df_val

write_out_2_3()

spark.stop()