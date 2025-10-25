import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

def get_spark_session(app_name="LogOptimize"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8") 
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def time_action(action_fn, *args, label="action"):
    """
    Run a function that triggers Spark actions and measure elapsed time.
    action_fn should perform the action (e.g., df.count(), df.write...).
    """
    start = time.time()
    result = action_fn(*args)
    elapsed = time.time() - start
    print(f"{label} took {elapsed:.2f}s")
    return result, elapsed


def repartition_vs_coalesce(df,target_partitions=8):
    print("=== Repartition vs Coalesce comparison ===")

    # Repartition (full shuffle)
    df_Repart = df.repartition(target_partitions)
    _, t_repart = time_action(lambda d:d.count(), df_Repart, label=f"repartition({target_partitions}) count")

    # Coalesce (no shuffle if decreasing partitions)
    df_cal = df.coalesce(max(1,target_partitions//2))
    _,t_cal = time_action(lambda d:d.count(),df_cal,label=f"coalesce({max(1,target_partitions//2)}) count")

    return {"repartition_s": t_repart, "coalesce_s": t_cal}

def caching_examples(df):

    print("=== Caching examples ===")

    df_mem = df.persist(StorageLevel.MEMORY_ONLY)

    _,t1 = time_action(lambda d: d.count(),df_mem, label="cache MEMORY_ONLY first count")
    _, t2 = time_action(lambda d: d.count(), df_mem, label="cache MEMORY_ONLY second count (should be faster)")

    df_mem_disk =  df.persist(StorageLevel.MEMORY_AND_DISK)
    _,t3 = time_action(lambda d: d.count(), df_mem_disk, label="cache MEMORY_AND_DISK first count")
    _,t4 = time_action(lambda d: d.count(), df_mem_disk, label="cache MEMORY_AND_DISK second count")

    df_mem.unpersist()
    df_mem_disk.unpersist()

    return {"mem_first": t1, "mem_second": t2, "memdisk_first": t3, "memdisk_second": t4}


def join_with_bradcast(df,lookup):

    print("=== Broadcast join example ===")

    _,t_non_broadcast = time_action(lambda: df.join(lookup,on="statusCode").count(),label="non-broadcast join count")
    _,t_broadcast = time_action(lambda: df.join(F.broadcast(lookup),on="statusCode"),label="broadcast join count")

    return {"non_broadcast_s": t_non_broadcast, "broadcast_s": t_broadcast}



def fix_skew_with_salt(df, skewed_keys="service", salt_buckets=8):
    print("=== Skew handling via salting example ===")

    salted = df.withColumn("salt", (F.floor(F.rand() * salt_buckets)).cast("int"))
    salted = salted.withColumn("salted_key", F.concat(F.col(skewed_keys), F.lit("_"), F.col("salt")))

    def salted_agg_action():
        return salted.groupBy("salted_key").count().count()
    
    _, t_salted = time_action(salted_agg_action, label=f"salted groupBy ({salt_buckets} buckets)")

    return {"salted_groupby_s": t_salted}




def optimize_Workflow(inputh_apth):

    spark = get_spark_session("OptimizeWorkflow")

    df = spark.read.option("inferSchema","true").json(inputh_apth)

    df_small = df.select("service", "env", "responseTime", "statusCode", "level", "timestamp")

    print(f"Initial partitions: {df_small.rdd.getNumPartitions()}")
    

    # 1 reparition vs coalesce

    repl = repartition_vs_coalesce(df_small,target_partitions=8)


    # caching comparisions

    cahce_resutls = caching_examples(df_small)


    # broadcast join demonstration

    # create a tiny look df

    lookup = spark.createDataFrame(
        [(200, "ok"), (400, "bad_request"), (401, "auth_error"), (403, "forbidden"), (404, "not_found"), (500, "server_error")],
        schema =["statusCode","statusLabel"]
    )


    join_res = join_with_bradcast(df_small,lookup)


    # skew data example

    skew_res = fix_skew_with_salt(df_small,skewed_keys="service",salt_buckets=8)

    spark.stop()

    return {"repartition_vs_coalesce": repl, "caching": cahce_resutls, "joins": join_res, "skew": skew_res}


if __name__ == "__main__":
    import sys
    input_path = "../data/raw_logs/*.json"

    res = optimize_Workflow(inputh_apth=input_path)
    print("\n=== Summary ===")
    print(res)




"""
Initial partitions: 20                                                          
=== Repartition vs Coalesce comparison ===
repartition(8) count took 2.03s                                                 
coalesce(4) count took 1.96s                                                    
=== Caching examples ===
cache MEMORY_ONLY first count took 4.88s                                        
cache MEMORY_ONLY second count (should be faster) took 0.19s
25/10/19 12:31:49 WARN CacheManager: Asked to cache already cached data.
cache MEMORY_AND_DISK first count took 0.15s
cache MEMORY_AND_DISK second count took 0.17s
=== Broadcast join example ===
non-broadcast join count took 3.05s                                             
broadcast join count took 0.03s
=== Skew handling via salting example ===
salted groupBy (8 buckets) took 1.48s                                           

=== Summary ===
{'repartition_vs_coalesce': {'repartition_s': 2.0317299365997314, 'coalesce_s': 1.9591665267944336}, 'caching': {'mem_first': 4.875173330307007, 'mem_second': 0.18973112106323242, 'memdisk_first': 0.14597249031066895, 'memdisk_second': 0.16691207885742188}, 'joins': {'non_broadcast_s': 3.0461387634277344, 'broadcast_s': 0.028740406036376953}, 'skew': {'salted_groupby_s': 1.4841501712799072}}

"""
