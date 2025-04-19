import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, explode, from_json, count, collect_list, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType
from pyspark.sql import functions as F

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

# Define the schema for the 'cast' column
cast_schema = ArrayType(
    StructType(
        [
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("order", IntegerType(), True),
            StructField("profile_path", StringType(), True),
        ]
    )
)

# Load the data from Parquet file
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/tmdb_5000_credits.parquet"
df = spark.read.parquet(input_path)

# Parse the cast JSON and explode to get individual actors
df_parsed = df.withColumn("cast_parsed", from_json(col("cast"), cast_schema))
df_exploded = df_parsed.withColumn("actor", explode("cast_parsed"))

df_actors = df_exploded.select(
    "movie_id",
    "title",
    col("actor.name").alias("actor_name"),
    col("actor.character").alias("character_name"),
)

# Find actor pairs who appeared in the same movies
df_pairs = (
    df_actors.alias("a")
    .join(
        df_actors.alias("b"),
        (F.col("a.movie_id") == F.col("b.movie_id"))
        & (F.col("a.actor_name") < F.col("b.actor_name")),
    )
    .select(
        F.col("a.movie_id"),
        F.col("a.title"),
        F.col("a.actor_name").alias("actor1"),
        F.col("b.actor_name").alias("actor2"),
    )
    .dropDuplicates(["movie_id", "actor1", "actor2"])
)

# Group by actor pairs and count their co-starring appearances
df_coactors = df_pairs.groupBy("actor1", "actor2").agg(
    collect_list(struct("movie_id", "title")).alias("movies"),
    count("*").alias("collaboration_count"),
)

# Find pairs who collaborated in 2 or more movies
frequent_collaborators = df_coactors.filter(col("collaboration_count") >= 2).orderBy(
    col("collaboration_count").desc()
)

# Select the required columns
result = frequent_collaborators.select(
    "actor1", "actor2", "collaboration_count", "movies"
)

# Define the output path
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question5/"

# Write the result to Parquet format
result.write.mode("overwrite").parquet(output_path)

spark.stop()
