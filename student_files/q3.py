import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
import os
from pyspark.sql.functions import col, avg, row_number, when
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
ranked_df = avg_rating_df.withColumn("rank_desc", row_number().over(window_spec_desc)) \
                         .withColumn("rank_asc", row_number().over(window_spec_asc))

# Filter top and bottom cities
result = ranked_df.filter((col("rank_desc") <= 3) | (col("rank_asc") <= 3)) \
                         .withColumn("RatingGroup", 
                                     when(col("rank_desc") <= 3, "Top").otherwise("Bottom"))

# Write into the target path
output_path = f"{base_path}/output/question3/"
result.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()