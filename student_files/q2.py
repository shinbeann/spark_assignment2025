import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, desc, asc, rank
from pyspark.sql.window import Window

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

input_path = (
    f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
)
df = spark.read.option("header", True).csv(input_path)

# Filter and prepare data
df = df.filter(col("Price Range").isNotNull() & (col("Price Range") != "null"))
df = df.withColumn("Rating", col("Rating").cast("float"))

# Define window functions
window_spec = Window.partitionBy("City", "Price Range").orderBy(desc("Rating"))
best = (
    df.withColumn("rank", rank().over(window_spec))
    .filter(col("rank") == 1)
    .drop("rank")
)

window_spec = Window.partitionBy("City", "Price Range").orderBy(asc("Rating"))
worst = (
    df.withColumn("rank", rank().over(window_spec))
    .filter(col("rank") == 1)
    .drop("rank")
)

# Combine and write
result = best.union(worst)
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question2/"
result.write.mode("overwrite").option("header", True).csv(output_path)
print("Row count after filtering:", result.count())

# check ans
# single_csv_path = f"{output_path}/q2_output.csv"
# result.coalesce(1).write.mode("overwrite").option("header", True).csv(single_csv_path)

spark.stop()
