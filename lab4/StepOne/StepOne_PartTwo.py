# GOAL:
# List the 10 last longest words
from pyspark.sql import SparkSession
from pyspark.sql.functions import flatten, split, length

inputFile = "words.txt"
spark = SparkSession.builder.appName("StepOne_PartTwo").getOrCreate()
inputData = spark.read.text(inputFile).cache()

df_length = inputData.withColumn('length', length('value'))
df_length.orderBy(df_length.length.desc()).limit(10).show()

spark.stop()