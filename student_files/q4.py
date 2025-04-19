import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, explode, regexp_replace, trim, split
import os

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
base_path = os.getcwd()
input_path = f"{base_path}/data/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

# Drop null City or Cuisine Style
df_filtered = df.filter(col("City").isNotNull() & col("Cuisine Style").isNotNull())

# Clean square brackets and extra quotes
df_cleaned = df_filtered.withColumn(
    "Cuisine", split(regexp_replace(col("Cuisine Style"), r"[\[\]\']+", ""), ",\\s*")
)

# Explode the Cuisine list to get one row per cuisine
df_exploded = df_cleaned.select("City", explode(col("Cuisine")).alias("Cuisine"))
df_exploded = df_exploded.withColumn("Cuisine", trim(col("Cuisine")))

# Group by City and Cuisine and count
result = df_exploded.groupBy("City", "Cuisine").count()

# Write into the target path
output_path = f"{base_path}/output/question4/"
result.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
