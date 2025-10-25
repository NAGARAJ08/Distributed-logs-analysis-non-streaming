from pyspark.sql import SparkSession

def get_spark_session(app_name="OutputWriter"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_partitioned_output(df, output_dir, partition_cols):
    print(f"Writing partitioned output → {output_dir}")
    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_cols)
        .parquet(output_dir)
    )
    print(f"Data written successfully: {output_dir}")


if __name__ == "__main__":
    spark = get_spark_session()


    metrics_df = spark.read.parquet("../data/metrics/")
    anomalies_df = spark.read.parquet("../data/anomalies/")

    write_partitioned_output(metrics_df, "../data/final/metrics/", ["service", "env"])
    write_partitioned_output(anomalies_df, "../data/final/anomalies/", ["service", "env"])

    spark.stop()
