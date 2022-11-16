# GOAL:
# List the most occuring 4 char sequence
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("StepOne_PartThree").getOrCreate()

df = spark.read.text("words.txt")

# Create a new column with the first 4 chars of each string
df = df.withColumn('substr', func.substring('value', 1, 4))

# Group rows by said substr & count the occurences
df = df.groupBy('substr').count()

# Sort the new df
df = df.sort(func.col('count').desc())

# Show top ten
df.show(10)

spark.stop()