# Apache Spark
## Truata Data Engineering Coding Challenge


[![Build Status](https://d33wubrfki0l68.cloudfront.net/f08cac2f234f4a1a4a466615fc28196ee49b9e34/81add/static/uploads/pyspark.webp)]()

The objective of the project is to use Apache Spark to simulate concrete data engineering scenarios for big data processing and analytics due to its high performance, scalability, and ease of use. There are three parts in this project which involves 


 

- working with Spark RDD API
- Working with Spark Datafframe API
- Using Spark MLlib to built a multinomial logistic regression machine learning model

## Tech

The below listed technologies are used to built all the three parts of the project.

- [Pyspark] - PySpark is the Python API for Apache Spark
- [Visual Studio Code] - Visual Studio Code is a free and open-source code editor
- [google Colab] - cloud-based Jupyter notebook environment that allows users to write and run Pyspark.
- [GitHub] - hosting and sharing code repositories, offering version control, project management.

## Installation

To use this project, you'll need to have Python and PySpark installed on your machine. You can install them using the following commands.


```sh
pip install pyspark
pip install requests
pip install apache-airflow
import datetime
import os
```
These commands will do the following:

Update the package lists for the Ubuntu operating system that Colab uses.
Install the Java Development Kit (JDK) that PySpark requires.
Download the PySpark package from the Apache Spark website and extract it.
```sh
!apt-get update
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
!tar xf spark-3.1.1-bin-hadoop3.2.tgz
!pip install -q findspark
```
## Compile the code online
I have used Google Colab a web-based platform that allows users to run the Pyspark job for all the three parts of the project online and examin the output in a Jupyter notebook environment. It provides access to powerful hardware resources such as CPUs, GPUs, and TPUs. 

| Parts | google colab link |
| ------ | ------ |
| Part 1: Spark RDD API | [[visit and excute the code for part_1](https://colab.research.google.com/drive/1CRhbeaxpGV6OMYrV4BqLiwLPud3lFkGD?usp=sharing)] |
| Part 2: Spark Dataframe API | [[visit and excute the code for part_2](https://colab.research.google.com/drive/13Gfs9Nkcy0kcmBgXJMUIgN7jOAmRBtak?usp=sharing)] |
| Part 3: Applied Machine Learning | [[visit and excute the code for part_3](https://colab.research.google.com/drive/1tfcXyM6G3RV6nDzPg37JOXOwq7m3CuD6?usp=sharing)] |

## File structure

#### Source file
##### part_1 
- task1_1.py - Downloaded the data file from the above location and make it accessible to Spark.
- task1_2.py - 
-- a.) Used Spark's RDD API, create a list of all (unique) products present in the transactions. Write out this list to a text file: out/out_1_2a.txt.
-- b.) used Spark only, write out the total count of products to a text file: out/out_1_2b.txt
- task1_3.py - Created an RDD and using Spark APIs, determine the top 5 purchased products along with how often they were purchased (frequency count). Write the results out in descending order of frequency into a file out/out_1_3.txt .

##### part_2 
- task2_1.py - Downloads the parquet data file from this URL and load it into a Spark data frame.
- task2_2.py - Creates CSV output file under out/out_2_2.txt that lists the minimum price, maximum price, and total row count from this data set.
- task2_3.py - Calculates the average number of bathrooms and bedrooms across all the properties listed in this data set with a price of > 5000 and a review score being exactly equalt to 10. Write the results into a CSV file out/out_2_3.txt 
- task2_4.py - returns people can be accomodated by the property with the lowest price and highest rating. Write the resulting number to a text file under out/out_2_4.txt .
- task2_5.py - Used Apache Airflow's Dummy Operator, create an Airflow Dag that runs task 1, followed by tasks 2, and 3 in parallel, followed by tasks 4, 5, 6 all in parallel
- 
##### part_3 
- task3_1.py - Built Logistic Regression model on the Iris data using Pyspark MLib.
- task3_2.py - Implemented a Spark version of the given Python code and demonstrated that it can e correctly predict on the training data, similar to what was done in the preceding section.

#### output file

- out_1_2a - Exported output file of task1_2.py [(unique) products present in the transactions]
- out_1_2b - Exported output file of task1_2.py [total count of products]
- out_1_3 - Exported output file of task1_3.py [top 5 purchased products]
- out_2_2 - Exported output file of task2_2.py [minimum price, maximum price, and total row count]
- out_2_3 - Exported output file of task2_3.py [ average number of bathrooms and bedrooms across all the properties listed in this data set with a price of > 5000 and a review score being exactly equalt to 10]
- out_2_4 - Exported output file of task2_4.py [count of people can be accomodated by the property with the lowest price and highest rating]
- out_3_2 - Exported output file of task3_2.py [predicted class by built logistic regression model]
#### ipynb file
- I have also attached ipynb files (jupyter notebook files) for all the three parts in this repo 

## Author
-- Ibrahim Rinub Babu

## License

Copyright © 2022 Truata Ltd. Version 1.0, January 2022


