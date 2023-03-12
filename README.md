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

## File structure

#### Source file

- task1_1.py - Download the data file from the above location and make it accessible to Spark.
-- functions used 
    -
---
- task1_2.py -
-- functions used 
    -
---
- task1_3.py -
-- functions used 
    -
---
- task2_1.py -
- task2_2.py -
- task2_3.py -
- task2_4.py -
- task3_1.py -
- task3_2.py -
- 
#### output file

- out_1_2a -
- out_1_2b -
- out_1_3 -
- out_2_2 -
- out_2_3 -
- out_2_4 - 
- out_3_3 - 


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




#### Building for source

For production release:

```sh
gulp build --prod
```

Generating pre-built zip archives for distribution:

```sh
gulp build dist --prod
```
## Author
-- Ibrahim Rinub Babu

## License

Copyright © 2022 Truata Ltd. Version 1.0, January 2022


