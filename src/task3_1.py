from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pandas as pd
import urllib.request

spark = SparkSession.builder.appName("LogisticRegressionExample").getOrCreate()

def get_irish_data(url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"):

  """  
  This function loads the irish data to Spark Dataframe API.
  Args: 
    url: input file path mapped from Iris Dataset UCI Machine Learning Repository.
  Returns:
    This function returns spark Dataframe API variable.
  """
  
  # File path to save the dataset
  file_path = "/tmp/iris.csv"

  # Download the file using urllib
  urllib.request.urlretrieve(url, file_path)

  # Load the dataset into a pandas dataframe
  df = pd.read_csv(file_path, header=None, names=["sepal_length", "sepal_width", "petal_length", "petal_width", "class"])
  df.to_csv('iris.csv', index=False)
  # Display the first few rows of the dataframe
  # print(df)
  # Create a SparkSession object
  spark = SparkSession.builder.appName("LogisticRegressionExample").getOrCreate()

  # Load the data into a Spark DataFrame.
  data = spark.read.csv("/content/iris.csv", header=False, inferSchema=True)
  data = data.select(col("_c0").alias("sepal_length"),
                col("_c1").alias("sepal_width"),
                col("_c2").alias("petal_length"),
                col("_c3").alias("petal_width"),
                col("_c4").alias("class"))
  return data

def lr_model_build():

  """  
  This function takes the input by excuting get_irish_data and builds a logistic regression machine learning model
  Args: 
    None
  Returns:
    This function returns machine learning model and the test data for evaluation.
  """

  data = get_irish_data()

  # Cast the columns to double data type
  data = data.withColumn("sepal_length", col("sepal_length").cast("double"))
  data = data.withColumn("sepal_width", col("sepal_width").cast("double"))
  data = data.withColumn("petal_length", col("petal_length").cast("double"))
  data = data.withColumn("petal_width", col("petal_width").cast("double"))

  # Convert class labels to numeric values
  indexer = StringIndexer(inputCol="class", outputCol="label")
  data = indexer.fit(data).transform(data)

  # Create feature vector
  assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")
  data = assembler.transform(data.na.drop())

  # Cast the columns to double data type
  data = data.withColumn("sepal_length", col("sepal_length").cast("double"))
  data = data.withColumn("sepal_width", col("sepal_width").cast("double"))
  data = data.withColumn("petal_length", col("petal_length").cast("double"))
  data = data.withColumn("petal_width", col("petal_width").cast("double"))

  # Split the data into training and test sets
  trainData, testData = data.randomSplit([0.7, 0.3], seed=123)

  # Train the logistic regression model
  lr = LogisticRegression(featuresCol="features", labelCol="label", family="multinomial", maxIter=100, regParam=0.0, elasticNetParam=0.0)
  lrModel = lr.fit(trainData)
  return lrModel, testData



def evaluation():

  """  
  This function takes the input by excuting evaluation and evaluated logistic regression machine learning model
  Args: 
    None
  Returns:
    This function returns machine learning models accuracy.
  """

  lrModel, testData = lr_model_build()
  # Make predictions on test data and evaluate the accuracy
  predictions = lrModel.transform(testData)
  evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
  accuracy = evaluator.evaluate(predictions)
  return accuracy

evaluation()

spark.stop()