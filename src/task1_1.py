# task 1_1.py
from pyspark import SparkConf, SparkContext
import requests

conf = SparkConf().setAppName("GroceriesData")
sc = SparkContext.getOrCreate(conf)

def load_groceries_data(link_csv = 'https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv'):

  """  
  This function loads the csv file to spark rdd api.
  Args: 
    link_csv: input file path mapped from github 
  Returns:
    This function returns spark rdd api variable
  """

  groceries_data_rdd = sc.parallelize(requests.get(link_csv).text.split("\n"))
  return groceries_data_rdd

load_groceries_data()