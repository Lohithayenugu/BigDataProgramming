from pyspark.sql import SparkSession
from operator import add
from pyspark import SparkContext
from pyspark.sql import *
import os
import numpy as np
import pandas as pd
os.environ["SPARK_HOME"] = "C:\spark\spark-2.4.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="c:\\winutils"


## 1. Import the dataset and create data framesdirectly on import.
spark = SparkSession .builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
sc=SparkContext.getOrCreate()
# noinspection PyInterpreter
df = spark.read.csv("D:\Drivers\github\Big-Data-Programming\Module2\ICP3-pyspark\\survey.csv",header=True);

## Bonus-1:
df.createOrReplaceTempView("survey")

## 2.save to file:
df.write.csv("D:\Drivers\github\Big-Data-Programming\Module2\ICP3-pyspark\\out.csv")

#3.Remove duplicates
df.dropDuplicates()
print(df.count())

#4.UnionAll
df1 = df.limit(5)
df2 = df.limit(10)
unionDf = df1.unionAll(df2)
unionDf.orderBy('Country').show()

#5GroupBy
print(df.groupBy('treatment'))

## part-2 1. Join operation
joined_df = df1.join(df2, df1.Country == df2.Country)

# Aggregate functions
df.groupby('Country').agg({'Age': 'mean'}).show()
df.createOrReplaceTempView("survey")
sqlDF = spark.sql("SELECT max(`Age`) FROM survey")
sqlDF.show()

sqlDF = spark.sql("SELECT avg(`Age`) FROM survey")
sqlDF.show()

sqlDF = spark.sql("SELECT min(`Age`) FROM survey")
sqlDF.show()

## part-2 2. 13th row
df13=df.take(13)
print(df13[-1])

## bonus 1. Write aparseLinemethod to split the comma-delimited row and create a Data frame
#dfd=pd.read_csv("D:\Drivers\github\Big-Data-Programming\Module2\ICP3-pyspark\\survey.csv",sep=',')
#dfd.registerTempTable("survey")
#df.registerTempTable("survey")
