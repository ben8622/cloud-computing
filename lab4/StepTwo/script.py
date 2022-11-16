# Goal:
# List the 10 most frequent words in file1.txt in descending order based on frequency

# stop_words = ['and', 'or', 'that', 'the', 'a', 'an', 'is', 'are', 'have']
stop_words = [("and",), ("or",), ("that",), ("the",), ("a",)]
stop_words_cols = ["value"]

from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("script").getOrCreate()

stop_df = spark.createDataFrame(stop_words, ["word"])
stop_df.show()

df = spark.read.text("file1.txt")

# seperate into column with individual words
df = df.withColumn('word', func.explode(func.split(func.col('value'), '\s+')))

# df = df.filter(func.col('word').contains())

# group by words
df = df.groupBy('word')

# count each group
df = df.count()

# sort by count
df = df.sort(func.col('count').desc())

# display top 10 results
df.show(10)

spark.stop()