from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def get_spark_session(app_name="parser"):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions","8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def make_env_cleaner(valid_Env):

    def clean_env(env):

        if env in valid_Env:
            return env
        else:
            return "unknown"
        
    return F.udf(clean_env,StringType())
    


def clean_logs(df,spark):

    print("Clearning logs.................")

    # convert the timestamp steing to proper timestap
    df = df.withColumn("timestamp",F.to_timestamp(F.col("timestamp")))

    df = df.withColumn("date",F.date_format(F.col("timestamp"),"yyyy-MM-dd"))
    df = df.withColumn("hour",F.hour(F.col("timestamp")))

    df = df.withColumn(
        "responseTime",F.when(F.col("responseTime").isNull(),F.lit(0)).otherwise(F.col("responseTime")))
    

    df = df.withColumn("statusCode",F.when(F.col("statusCode").isNull(),F.lit(0)).otherwise(F.col("statusCode")))

    # apply closures based udf for env cleaning

    valid_env = ["prod","dev"]
    env_cleaner_udf = make_env_cleaner(valid_env)
    df = df.withColumn("env",env_cleaner_udf(F.col("env")))
    df = df.na.drop(subset=["timestamp","service","statusCode"])

    print(f"Clearned log count {df.count()}")
    return df


if __name__ == '__main__':
    input_path = "../data/raw_logs/*.json"

    spark = get_spark_session()
    df  = spark.read.option("inferSchema","true").json(input_path)
    df_Clearn = clean_logs(df,spark)

    df_Clearn.show(5,truncate=False)

    df_Clearn.printSchema()

    spark.stop()


"""
ing logs.................                                                 
Clearned log count 5000000                                                      
+-------+--------------+-----+-----------------------------------------------+------------+---------------+----------+--------------------------+----------+----+
|env    |host          |level|message                                        |responseTime|service        |statusCode|timestamp                 |date      |hour|
+-------+--------------+-----+-----------------------------------------------+------------+---------------+----------+--------------------------+----------+----+
|prod   |81.154.20.149 |WARN |payment-service handled request with status 400|1114.76     |payment-service|400       |2025-10-17 18:33:51.950773|2025-10-17|18  |
|unknown|70.143.71.233 |INFO |search-service handled request with status 200 |613.08      |search-service |200       |2025-10-16 23:37:36.312337|2025-10-16|23  |
|prod   |210.200.51.240|INFO |payment-service handled request with status 200|1513.15     |payment-service|200       |2025-10-17 20:21:12.671427|2025-10-17|20  |
|prod   |69.255.26.86  |ERROR|search-service handled request with status 400 |413.58      |search-service |400       |2025-10-17 16:12:56.106151|2025-10-17|16  |
|unknown|8.108.176.251 |WARN |auth-service handled request with status 400   |1089.67     |auth-service   |400       |2025-10-16 22:21:59.642046|2025-10-16|22  |
+-------+--------------+-----+-----------------------------------------------+------------+---------------+----------+--------------------------+----------+----+
only showing top 5 rows

root
 |-- env: string (nullable = true)
 |-- host: string (nullable = true)
 |-- level: string (nullable = true)
 |-- message: string (nullable = true)
 |-- responseTime: double (nullable = true)
 |-- service: string (nullable = true)
 |-- statusCode: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- date: string (nullable = true)
 |-- hour: integer (nullable = true)


"""