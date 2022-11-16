# GOAL:
# Find 3 common words between files 1, 2, and 3 with the most occurences
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

input = "file1.txt"
input = "file1.txt"
input = "file1.txt"
spark = SparkSession.builder.appName("StepOne_PartThree").getOrCreate()

df1 = spark.read.text("file1.txt")
df2 = spark.read.text("file2.txt")
df3 = spark.read.text("file3.txt")

# Combine all rows of each file's df
df_combined = df1.union(df2).union(df3)

# Get rid of empty strings
df_combined = df_combined.filter(func.col('value') != '')

# Explode strings into separate words
df_combined = df_combined.withColumn('word', func.explode(func.split(func.col('value'), '\s+')))

# Clean up words by removing non alpha-numeric chars
df_combined = df_combined.withColumn('word', func.regexp_replace('word', '[^\w]', ''))

# Group similar words together and count them
df_combined = df_combined.groupBy('word').count()

df_combined = df_combined.sort(func.col('count').desc())

print(df_combined.show(3))

spark.stop()