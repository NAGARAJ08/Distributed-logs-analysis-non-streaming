"""
Compute metrics such as:
    Total log count per service
    Average response time
    Error count and error rate (%)
    Average response time by environment

"""


from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(app_name = "LogIntelligence"):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions","8")
        .config("spark.executor.memory","2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def compute_service_metrics(df):

    print("Computing per-service metrics...")


    df = df.withColumn(
        "isError",F.when(F.col("level")=="ERROR",1).otherwise(0)
    )


    metrics_df = (
        df.groupBy("service","env")
        .agg(
            F.count("*").alias("totalLogs"),
            F.round(F.avg("responseTime"),2).alias("avgResponseTime"),
            F.count(F.when(F.col("isError") == 1, True)).alias("errorCount"),
        ).withColumn(
            "errorRate",F.round((F.col("errorCount")/F.col("totalLogs"))*100,2),
        )
        .orderBy(F.col("errorRate").desc())
    )

    print("Metrics computed successfully")
    return metrics_df


def compute_metrics_with_rdd(df):

    print("computing the metrics using the RDD transformations...............")

    rdd  = df.select("service","responseTime","level","env").rdd

    print("columns are : ")
    print(df.columns)

    def map_to_pair(row):
        is_error= 1 if row['level'] == "ERROR" else 0
        return ((row["service"], row["env"]), (1, row["responseTime"], is_error))
    
    mapped_rdd = rdd.map(map_to_pair)

    # agg counts , sum(requestTime) , sum(errors)

    reduced_rdd = mapped_rdd.reduceByKey(
        lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2])
    )

    result_rdd = reduced_rdd.mapValues(
        lambda v:{
            
            "totalLogs": v[0],
            "avgResponseTime": round(v[1] / v[0], 2),
            "errorRate": round((v[2] / v[0]) * 100, 2),

        }
    )
    return result_rdd


if __name__ == "__main__":
    spark = get_spark_session()

    df = spark.read.option("inferSchema", "true").json("../data/raw_logs/*.json")

    df = df.withColumn("isError", F.when(F.col("level") == "ERROR", 1).otherwise(0))

    metrics_df = compute_service_metrics(df)
    metrics_df.show(10, truncate=False)

    rdd_metrics = compute_metrics_with_rdd(df)
    print("\n Sample RDD output:")
    for item in rdd_metrics.take(5):
        print(item)

    metrics_df.write.mode("overwrite").parquet("../data/metrics/")
    print("Metrics written to ../data/metrics/")

    spark.stop()

"""
Computing per-service metrics...                                                
Metrics computed successfully
+-----------------+-------+---------+---------------+----------+---------+      
|service          |env    |totalLogs|avgResponseTime|errorCount|errorRate|
+-----------------+-------+---------+---------------+----------+---------+
|auth-service     |staging|333294   |1005.07        |33592     |10.08    |
|inventory-service|prod   |333288   |1005.11        |33489     |10.05    |
|inventory-service|dev    |333514   |1005.2         |33499     |10.04    |
|auth-service     |prod   |332898   |1006.7         |33340     |10.02    |
|order-service    |dev    |333325   |1005.72        |33388     |10.02    |
|payment-service  |prod   |333198   |1005.02        |33369     |10.01    |
|search-service   |staging|332196   |1005.59        |33217     |10.0     |
|order-service    |staging|334563   |1004.89        |33436     |9.99     |
|order-service    |prod   |334046   |1005.69        |33276     |9.96     |
|auth-service     |dev    |333037   |1003.84        |33164     |9.96     |
+-----------------+-------+---------+---------------+----------+---------+
only showing top 10 rows

computing the metrics using the RDD transformations...............

 Sample RDD output:
('order-service', {'totalLogs': 1001934, 'avgResponseTime': 1005.43, 'errorRate': 9.99})
('search-service', {'totalLogs': 998380, 'avgResponseTime': 1004.57, 'errorRate': 9.96})
('auth-service', {'totalLogs': 999229, 'avgResponseTime': 1005.2, 'errorRate': 10.02})
('payment-service', {'totalLogs': 999898, 'avgResponseTime': 1005.35, 'errorRate': 9.96})
('inventory-service', {'totalLogs': 1000559, 'avgResponseTime': 1005.25, 'errorRate': 10.01})

"""