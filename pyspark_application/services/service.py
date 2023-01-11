import json
from constant.constant import hadoop_namenode
from services.ssi_stock_data import process_ssi_stock_data
from constant.constant import ssi_stock_data_api


def process(spark, data_dir, config_dir, time_stamp):
    data = spark.read.json(hadoop_namenode + data_dir)

    config = (spark
              .read
              .option("multiLine", "true")
              .json(hadoop_namenode + config_dir)
              .rdd
              .map(lambda row: row.asDict()).collect())

    if (config[0]['data'] == ssi_stock_data_api):
        process_ssi_stock_data(spark, data, config[0], time_stamp)


def start(work, spark_sess):
    work = json.loads(work.value.decode('utf-8'))
    process(spark_sess, work['data_dir'],
            work['config_dir'], work['time_stamp'])
