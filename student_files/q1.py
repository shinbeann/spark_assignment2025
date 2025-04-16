import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.sql.functions import col
import os


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
base_path = os.getcwd()
input_path = f"{base_path}/data/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

df.printSchema()

# Filter rows with no reviews or ratings < 3.0
# From df.printSchema():  Rating is casted to String -> Type casting to float must be done
cleaned_df = df.filter(
    (col("Reviews") != "null") &
    (col("Reviews").isNotNull()) &
    (col("Rating").cast("float") >= 3.0 )
)

# Write into the target path
output_path = f"{base_path}/output/question1/"
# output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question1/"
cleaned_df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()