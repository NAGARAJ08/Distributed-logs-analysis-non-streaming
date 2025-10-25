
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField, IntegerType, StructType, DoubleType, TimestampType


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



def manual_schema():
    # adding the manual schema becz this could reduce the spark inference 
    # so saprk will not have an overhead to see and chekc waht is the current data type for the data that we received
    # bcz we already specified the schema and it know what to expect 
    # this willreduce the spark load and cpu usage
    return StringType([
        StructField("timestamp",StringType(),True),
        StructField("service",StringType(),True),
        StructField("level",StringType(),True),
        StructField("message",StringType(),True),
        StructField("responseTime",DoubleType(),True),
        StructField("statusCode",IntegerType(),True),
        StructField("host",StringType(),True),
        StructField("env",StringType(),True),
    ])


def load_logs(spark,input_path,user_manual_schema=False):

    if user_manual_schema:
        schema = manual_schema()
        df = spark.read.schema(schema).json(input_path)
    else:
        df = spark.read.option("inferSchema","true").json(input_path)

    
    print(f"Loaded {df.count()} records")
    print("schema")
    df.printSchema()

    return df 

def explore_partitions(df):
    print(f"Initial paritions :{df.rdd.getNumPartitions()}")
    df = df.repartition(8)
    print(f"After reparitions :{df.rdd.getNumPartitions()}")
    return df



if __name__ =="__main__":

    input_path = "../data/raw_logs/*.json"

    spark = get_spark_session("LogIntelligence")
    df = load_logs(spark=spark,input_path=input_path,user_manual_schema=False)
    df.show(5,truncate=False)

    df = explore_partitions(df)

    print(f"Total logs: {df.count()}")
    spark.stop()


"""
Loaded 5000000 records                                                          
schema
root
 |-- env: string (nullable = true)
 |-- host: string (nullable = true)
 |-- level: string (nullable = true)
 |-- message: string (nullable = true)
 |-- responseTime: double (nullable = true)
 |-- service: string (nullable = true)
 |-- statusCode: long (nullable = true)
 |-- timestamp: string (nullable = true)

+-------+--------------+-----+-----------------------------------------------+------------+---------------+----------+--------------------------+
|env    |host          |level|message                                        |responseTime|service        |statusCode|timestamp                 |
+-------+--------------+-----+-----------------------------------------------+------------+---------------+----------+--------------------------+
|prod   |81.154.20.149 |WARN |payment-service handled request with status 400|1114.76     |payment-service|400       |2025-10-17T18:33:51.950773|
|staging|70.143.71.233 |INFO |search-service handled request with status 200 |613.08      |search-service |200       |2025-10-16T23:37:36.312337|
|prod   |210.200.51.240|INFO |payment-service handled request with status 200|1513.15     |payment-service|200       |2025-10-17T20:21:12.671427|
|prod   |69.255.26.86  |ERROR|search-service handled request with status 400 |413.58      |search-service |400       |2025-10-17T16:12:56.106151|
|staging|8.108.176.251 |WARN |auth-service handled request with status 400   |1089.67     |auth-service   |400       |2025-10-16T22:21:59.642046|
+-------+--------------+-----+-----------------------------------------------+------------+---------------+----------+--------------------------+
only showing top 5 rows

Initial paritions :20
After reparitions :8                                              (1 + 19) / 20]
Total logs: 5000000                                                             

"""

"""

| Concept                      | Meaning                                   |
| ---------------------------- | ----------------------------------------- |
| Partition                    | Logical chunk of data                     |
| Number of rows per partition | Not fixed                                 |
| Executors                    | Workers that process partitions           |
| Partition count              | Controls parallelism                      |
| repartition(n)               | Change total partitions by shuffling data |


Number of Partitions = 2 to 4 × Number of CPU cores in cluster


Spark Partitions – Quick Notes

    A partition is a logical chunk of data for parallel processing.

    Spark runs one task per partition.

    Executors process partitions. Partitions are not executors.

    Partition count affects parallelism and speed.

    Spark creates initial partitions while reading data based on file size, not number of rows.

    Use .repartition(n) to increase or decrease partitions (full shuffle, expensive).

    Use .coalesce(n) to reduce partitions only (no shuffle, faster).

"""

