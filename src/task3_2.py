from task3_1 import get_irish_data
from task3_1 import lr_model_build
from task3_1 import evaluation
from pyspark.ml.linalg import Vectors
import csv
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LogisticRegressionExample").getOrCreate()


def out_3_2(li_val_1=[5.1, 3.5, 1.4, 0.2], li_val_2=[6.2, 3.4, 5.4, 2.3]):
    """
    This function takes the input by executing lr_model_build
    Args:
      custom value li_val_1 and li_val_2 is used for prediction
    Returns:
      This function returns predicted results.
    """
    lrModel, testData = lr_model_build()

    value1 = Vectors.dense(li_val_1)
    value2 = Vectors.dense(li_val_2)

    # Create a DataFrame with the custom values
    pred_data = spark.createDataFrame([(value1,), (value2,)], ["features"])

    # Use the trained logistic regression model to make predictions on the custom values
    predictions = lrModel.transform(pred_data)

    # # Show the predicted class for each custom value
    # predictions.select("prediction").show()
    prediction_list = [row.prediction for row in predictions.collect()]

    return prediction_list


def write_out_3_2():
    """
    This function takes the input by executing out_3_2
    Args:
      None
    Returns:
      This function saves an output file holding the class names.
    """
    prediction_list = out_3_2()
    with open('out_3_2.txt', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['class'])
        for prediction in prediction_list:
            if prediction == 0.0:
                writer.writerow(['Iris-setosa'])
            elif prediction == 1.0:
                writer.writerow(['Iris-versicolor'])
            elif prediction == 2.0:
                writer.writerow(['Iris-virginica'])


write_out_3_2()

spark.stop()
