import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
import os
import json
from pyspark.sql.functions import col, explode, array, sort_array
from itertools import combinations

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
base_path = os.getcwd()
input_path = f"{base_path}/data/tmdb_5000_credits.parquet"
df = spark.read.option("header", True).parquet(input_path)

# UDF to parse JSON cast
def parse_cast_json(cast_json):
    try:
        cast_list = json.loads(cast_json.replace("'", '"'))
        return [actor["name"] for actor in cast_list]
    except:
        return []

# Register UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
parse_cast = udf(parse_cast_json, ArrayType(StringType()))

# Get all actor pairs per movie
movies_with_actors = df.select(
    "movie_id",
    "title",
    parse_cast(col("cast")).alias("actors")
)

# Function to generate all unique pairs of actors
def get_actor_pairs(actors):
    return [tuple(sorted(pair)) for pair in combinations(actors, 2)]

# Generate pairs UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
pair_schema = ArrayType(StructType([
    StructField("actor1", StringType()),
    StructField("actor2", StringType())
]))
get_pairs_udf = udf(get_actor_pairs, pair_schema)

# Explode to get one row per pair
pairs_df = movies_with_actors.select(
    "movie_id",
    "title",
    explode(get_pairs_udf(col("actors"))).alias("pair")
).select(
    "movie_id",
    "title",
    col("pair.actor1").alias("actor1"),
    col("pair.actor2").alias("actor2")
)

# Count pairs across movies and filter those appearing in >= 2 movies
pair_counts = pairs_df.groupBy("actor1", "actor2").count().filter(col("count") >= 2)

# Join back to get movie details for qualifying pairs
result = pairs_df.join(
    pair_counts.select("actor1", "actor2"),
    ["actor1", "actor2"],
    "inner"
).distinct().orderBy("movie_id", "actor1", "actor2")

# Write output
output_path = "./output/question5/"
result.write.mode("overwrite").parquet(output_path)

spark.stop()