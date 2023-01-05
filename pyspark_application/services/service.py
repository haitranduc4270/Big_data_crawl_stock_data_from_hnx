import json

def process (data_dir, config_dir, spark_context):
    spark = spark_context.getOrCreate()
    data = spark.read.json("hdfs://namenode:9000/project20221" + data_dir)
    config = spark.read.json("hdfs://namenode:9000/project20221" + config_dir)
    
    # Do something with data
    data.write.format("json").mode("overwrite").save("hdfs://namenode:9000/project20221/clean" + data_dir[4:])

    spark.stop()

def start (work, spark_context):
    work = json.loads(work.value.decode('utf-8'))
    process(work['data_dir'], work['config_dir'], spark_context)
