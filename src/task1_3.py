# task 1_3.py
# Task 3
#==========
from task1_1 import load_groceries_data
from pyspark import SparkConf, SparkContext
import requests

conf = SparkConf().setAppName("GroceriesData")
sc = SparkContext.getOrCreate(conf)

def write_out_1_3(write_path_1_3 = 'out/out_1_3.txt'):

  """  
  This function writes the output by executing functions item_counts.
  Args: 
    write_path_1_3: output file path for Part 1 task 3
  Returns:
    None
  """

  item_counts_val = item_counts()
  
  sc.parallelize(item_counts_val).saveAsTextFile(write_path_1_3)

def item_counts():

  """  
  This function loads the csv file to spark rdd api and takes the top 5 products based on the cout of their occurance in the list.
  Args: 
    None
  Returns:
    This function returns top 5 products in a tuple.
  """
  
  groceries_data_rdd = load_groceries_data()

  item_counts_val = groceries_data_rdd.flatMap(lambda x: x.split(',')) \
                        .map(lambda x: (x.strip(), 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda x: -x[1]) \
                        .take(5)
  return item_counts_val

write_out_1_3()