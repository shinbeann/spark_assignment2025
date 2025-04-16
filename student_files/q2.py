import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
import os
from pyspark.sql.functions import col, desc, asc, rank
from pyspark.sql.window import Window


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
base_path = os.getcwd()
input_path = f"{base_path}/data/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

# Filter and prepare data
df = df.filter(col("Price Range").isNotNull() & (col("Price Range") != "null"))
df = df.withColumn("Rating", col("Rating").cast("float"))

# Define window functions
window_spec = Window.partitionBy("City", "Price Range").orderBy(desc("Rating"))
best = df.withColumn("rank", rank().over(window_spec)).filter(col("rank") == 1).drop("rank")

window_spec = Window.partitionBy("City", "Price Range").orderBy(asc("Rating"))
worst = df.withColumn("rank", rank().over(window_spec)).filter(col("rank") == 1).drop("rank")

# Combine and write
result = best.union(worst)
output_path = f"{base_path}/output/question2/"
result.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()