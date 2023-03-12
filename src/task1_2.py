# task 1_2.py
# Tash 2
# =========
from task1_1 import load_groceries_data
from pyspark import SparkConf, SparkContext
import requests
conf = SparkConf().setAppName("GroceriesData")
sc = SparkContext.getOrCreate(conf)

def write_out_1_2ab(write_path_1_2a = 'out/out_1_2a.txt', write_path_1_2b ='out/out_1_2b.txt'):

  """ 
  This function writes the output by executing functions unique_products and total_count.
  Args: 
    write_path_1_2a: output file path for Part 1 task 2 A
    write_path_1_2b: output file path for Part 1 task 2 B
  Returns:
    None
  """

  unique_products_val = unique_products()
  total_count_val = total_count()
  unique_products_val.saveAsTextFile(write_path_1_2a)
  sc.parallelize(["Count: {}".format(total_count_val)]).saveAsTextFile(write_path_1_2b)

def unique_products():

  """  
  unique_products
  This function takes the input by excuting load_groceries_data and returns the unique products from the list.
  Args: 
    None 
  Returns:
    This functions returns unique products from the list 
  """
  groceries_data_rdd = load_groceries_data()
  unique_products_val = groceries_data_rdd.flatMap(lambda x: x.split(',')).distinct()
  return unique_products_val

def total_count():

  """  
  This function takes the input by excuting load_groceries_data and returns total count of the list.
  Args: 
    None 
  Returns:
    This function returns total count of values from the list
  """

  groceries_data_rdd = load_groceries_data()
  total_count_val = groceries_data_rdd.flatMap(lambda x: x.split(',')).count()
  return total_count_val

write_out_1_2ab()