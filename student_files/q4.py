import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
import os
from pyspark.sql.functions import col, explode, split, trim, regexp_replace

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
base_path = os.getcwd()
input_path = f"{base_path}/data/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

# Filter and prepare data
df = df.filter(col("Price Range").isNotNull() & (col("Price Range") != "null"))
df = df.withColumn("Rating", col("Rating").cast("float"))

# Clean and process Cuisine Style column
# First remove the square brackets, then split by comma
df = df.withColumn(
    "Cuisine Style",
    split(
        trim(regexp_replace(col("Cuisine Style"), "\[|\]|'", "")),
        ",\s*"
    )
)

# Explode the array to get one row per cuisine per restaurant
exploded = df.select(
    "City",
    explode(col("Cuisine Style")).alias("Cuisine")
).filter(
    col("Cuisine") != ""  # Remove empty cuisine entries
)

# Count by city and cuisine
result = exploded.groupBy("City", "Cuisine").count().orderBy("City", "Cuisine") 

# Write into the target path
output_path = f"{base_path}/output/question4/"
result.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()