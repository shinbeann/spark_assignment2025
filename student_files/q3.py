import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
import os
from pyspark.sql.functions import col, avg, rank, when
from pyspark.sql.window import Window

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
base_path = os.getcwd()
input_path = f"{base_path}/data/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

# Filter and prepare data
df = df.filter(col("Price Range").isNotNull() & (col("Price Range") != "null"))
df = df.withColumn("Rating", col("Rating").cast("float"))

# Calculate average rating
avg_rating_df = df.groupBy("City").agg(avg("Rating").alias("AverageRating"))

# Define window specs
window_spec_desc = Window.orderBy(col("AverageRating").desc())
window_spec_asc = Window.orderBy(col("AverageRating").asc())

# Rank cities
ranked_df = avg_rating_df.withColumn(
    "rank_desc", rank().over(window_spec_desc)
).withColumn("rank_asc", rank().over(window_spec_asc))

# Filter top and bottom cities
result = ranked_df.filter((col("rank_desc") <= 2) | (col("rank_asc") <= 2)).withColumn(
    "RatingGroup", when(col("rank_desc") <= 2, "Top").otherwise("Bottom")
)

# Select and order columns for output
result = result.select("City", "AverageRating", "RatingGroup")

# Sort the result
result = result.orderBy(["RatingGroup", "AverageRating"], ascending=[False, False])

print("Row count after filtering:", result.count())

# Write into the target path
output_path = f"{base_path}/output/question3/"
result.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
