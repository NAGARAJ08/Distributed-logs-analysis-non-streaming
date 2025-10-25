from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, avg, stddev, when, count, round, lag, abs as F_abs
)


def get_spark_session(app_name="AnomalyDetection"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def detect_anomalies(df,time_col="timestamp", metric_col="responseTime"):
    print("Detecting anomalies...")

    w = Window.partitionBy("service","env").orderBy(time_col).rowsBetween(-20,0)
    df_stats = (
        df.withColumn("mean_rt",avg(col(metric_col)).over(w))
        .withColumn("std_rt",stddev(col(metric_col)).over(w))
        .withColumn(
            "z_score",
            (col(metric_col)-col("mean_rt"))/col("std_rt")
        )
        .withColumn(
              "is_anomaly",
            when(col("z_score").isNotNull() & (F_abs(col("z_score")) > 3), 1).otherwise(0)

          )
    )

    anomaly_df = df_stats.filter(col("is_anomaly") == 1)
    print("Anomaly detection done.")
    return anomaly_df, df_stats



def detect_spikes(df):
    print("Detecting response-time spikes...")
    w = Window.partitionBy("service","env").orderBy("timestamp")
    df_spike = df.withColumn("prev_rt", lag("responseTime").over(w))
    df_spike = df_spike.withColumn(
        "delta",
        (col("responseTime") - col("prev_rt")) / col("prev_rt")
    )
    df_spike = df_spike.withColumn(
        "is_spike",
        when(col("delta") > 0.5, 1).otherwise(0)
    )
    spikes = df_spike.filter(col("is_spike") == 1)
    print("Spike detection complete.")
    return spikes




if __name__ == "__main__":
    spark = get_spark_session()

    df = spark.read.option("inferSchema", "true").json("../data/raw_logs/*.json")

    df = df.select("service", "env", "responseTime", "timestamp", "level")

    anomalies, enriched = detect_anomalies(df)
    print("Top anomalies:")
    anomalies.select("service", "env", "responseTime", "z_score", "timestamp").show(10, truncate=False)

    spikes = detect_spikes(df)
    print("Spike sample:")
    spikes.select("service", "env", "responseTime", "prev_rt", "delta").show(10, truncate=False)

    anomalies.write.mode("overwrite").parquet("../data/anomalies/")
    enriched.write.mode("overwrite").parquet("../data/enriched/")
    print("Anomalies written to ../data/anomalies/")
    print("Enriched data written to ../data/enriched/")

    spark.stop()
