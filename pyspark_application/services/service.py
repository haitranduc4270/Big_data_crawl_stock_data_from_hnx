import json
from services.ssi_stock_data import process_ssi_stock_data, pre_process_ssi_stock_data
from constant.constant import ssi_stock_data_api, hadoop_namenode


def process(spark, data_dir, config_dir, time_stamp, stock_info):
    data = spark.read.json(hadoop_namenode + data_dir)

    config = (spark
              .read
              .option("multiLine", "true")
              .json(hadoop_namenode + config_dir)
              .rdd
              .map(lambda row: row.asDict()).collect())

    if (config[0]['data'] == ssi_stock_data_api):
        clean_data = pre_process_ssi_stock_data(data)
        process_ssi_stock_data(
            spark, clean_data, config[0], time_stamp, stock_info)


def start(work, stock_info, spark_sess):
    work = json.loads(work.value.decode('utf-8'))
    process(spark_sess, work['data_dir'],
            work['config_dir'], work['time_stamp'], stock_info)
