import sys
import os

from pyspark.sql import SparkSession

os.environ["SPARK_HOME"] = "C:\spark\spark-2.4.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 777)
ssc.checkpoint("checkpoint")

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
#pairs = words.map(lambda word: (word, 1))
pairs = words.map(lambda word : (len(word), word))
wordCounts = pairs.reduceByKey(lambda x, y: x +","+ y)
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
Spark = SparkSession.builder.getOrCreate()
#rdd = SparkSession.read.csv("sample1.csv", header=True).rdd
windowedStream1 = words.window(20)
windowedStream2 = words.window(60)
joinedStream = windowedStream1.join(windowedStream2)
joinedStream.pprint()
# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
windowedWordCounts.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()