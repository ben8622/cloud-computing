# GOAL:
# List the 5 first words (in ascending order) and from the words.txt which start with âbâ and end with âtâ.

from pyspark.sql import SparkSession
from pyspark.sql.functions import flatten, split

inputFile = "words.txt"
spark = SparkSession.builder.appName("StepOne_PartOne").getOrCreate()
inputData = spark.read.text(inputFile).cache()

startsWithB = inputData.filter(inputData.value.startswith('b'))
endsWithT = startsWithB.filter(startsWithB.value.endswith('t'))

endsWithT.show(5)

spark.stop()