from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc


spark = (
    SparkSession.builder
    .appName("WordCount")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.text("/opt/spark/work-dir/data/wordcount.txt")

words = (
    df.select(
        explode(
            split(col("value"), r"\s+")
        ).alias("word")
    )
    .filter(col("word") != "")
)

result = (
    words.groupBy("word")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    .limit(20)
)

print("\n=== Top 20 Word Count ===")
result.show(20, truncate=False)

spark.stop()