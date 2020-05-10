import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from collections import namedtuple

os.environ["SPARK_HOME"] = "C:\Spark"

def main():
    sc = SparkContext(appName="WordCount")
    ssc = StreamingContext(sc, 5)
    lines = ssc.socketTextStream("localhost", 6000)
    fields = ("word", "count")
    Tweet = namedtuple('Text', fields)

    counts = lines.flatMap(lambda text: text.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y).map(lambda rec: Tweet(rec[0], rec[1]))
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
