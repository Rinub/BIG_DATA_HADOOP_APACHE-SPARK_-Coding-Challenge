from task2_1 import get_data

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max
from pyspark.sql.functions import avg

# create a SparkSession
spark = SparkSession.builder \
    .appName("AirBnB Data Analysis") \
    .getOrCreate()
    
def write_out_2_4(write_path='out/out_2_4.txt'):
 
  """  
  This function writes the output out_2_4.txt by executing functions capacity.
  Args: 
    write_path: output file path for Part 2 task 4
  Returns:
    None
  """

  capacity_val = capacity()
    # write to a text file
  with open(write_path, "w") as f:
      f.write(str(capacity_val))

def capacity():

  """  
  This function takes the input by excuting get_data
  Args: 
    None 
  Returns:
    This functions returns How many people can be accomodated by the property with the lowest price and highest rating. 
  """

  Spark_df = get_data()
  # sort the DataFrame by price and rating and select the first row
  top_property = Spark_df.orderBy(["price", "review_scores_rating"], ascending=[True, False]).first()
  # compute the maximum capacity
  capacity_val = top_property.bedrooms * top_property.beds
  return capacity_val

write_out_2_4()
spark.stop()