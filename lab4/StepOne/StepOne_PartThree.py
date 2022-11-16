# GOAL:
# Calculate number of lines in the file and number of distinct words
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

input = "file1.txt"
spark = SparkSession.builder.appName("StepOne_PartThree").getOrCreate()

df = spark.read.text(input)

# the num  of lines is just equal to the rows in the df
num_of_lines = df.count()

df = df.filter(func.col('value') != "")

# Create a new row for each word in the line
df = df.withColumn('word', func.explode(func.split(func.col('value'), '\s+')))

# Remove any non-word characters such as ',' or '.'
df = df.withColumn('word', func.regexp_replace('word', '[^\w]', ''))

# Group similar words together and count them
df = df.groupBy('word').count()

# grab distinct words based off unique groups
distinct_words = df.count()

df.show()

print(f'Number of lines = {num_of_lines}, distinct words = {distinct_words}')

spark.stop()