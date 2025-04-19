import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

input_path = (
    f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
)
df = spark.read.option("header", True).csv(input_path)

df.printSchema()
print("Original row count:", df.count())

# Filter rows with no reviews or ratings < 3.0
# From df.printSchema():  Rating is casted to String -> Type casting to float must be done
cleaned_df = df.filter(
    (col("Number of Reviews").isNotNull()) & (col("Rating").cast("float") >= 3.0)
)

print("Row count after filtering:", cleaned_df.count())

# Write into the target path
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question1/"
cleaned_df.write.mode("overwrite").option("header", True).csv(output_path)

# check ans
# single_csv_path = f"{output_path}/q1_output.csv"
# cleaned_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
#     single_csv_path
# )

spark.stop()
