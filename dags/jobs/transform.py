import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV path",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output Parquet directory",
    )

    parser.add_argument(
        "--baseline-year",
        type=int,
        default=2015,
        help="Minimum release year. Default: 2015",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("NetflixWeeklyTransform")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )

    filtered_df = df.filter(
        F.col("release_year").cast("int") >= args.baseline_year
    )

    exploded_df = (
        filtered_df
        .withColumn(
            "genre",
            F.explode(F.split(F.col("listed_in"), ",")),
        )
        .withColumn(
            "genre",
            F.trim(F.col("genre")),
        )
    )

    result_df = (
        exploded_df
        .groupBy("type", "genre")
        .agg(F.count("*").alias("count"))
        .orderBy(
            F.col("type"),
            F.col("count").desc(),
            F.col("genre"),
        )
    )

    aggregate_row_count = result_df.count()

    print(f"Baseline year: {args.baseline_year}")
    print(f"Aggregate row count: {aggregate_row_count}")

    result_df.show(50, truncate=False)

    (
        result_df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output)
    )

    print(f"Parquet output: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()