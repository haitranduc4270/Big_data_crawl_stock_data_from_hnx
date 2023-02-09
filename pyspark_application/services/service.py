import json
from services.ssi_stock_data import process_ssi_stock_data, pre_process_ssi_stock_data
from services.article import pre_process_article_data, extract_article_data
from constant.constant import ssi_stock_data_api, hadoop_namenode
from pyspark.sql.types import *


def process_ssi_data(spark, data_dir, config_dir, time_stamp, stock_info):
    # Đọc data từ file lưu trung gian
    data = (spark
            .read
            .format('parquet')
            .load(hadoop_namenode + data_dir))

    # Đọc file config để lấy thông tin hướng dẫn xử lý
    config = (spark
              .read
              .option("multiLine", "true")
              .json(hadoop_namenode + config_dir)
              .rdd
              .map(lambda row: row.asDict()).collect())

    if (config[0]['data'] == ssi_stock_data_api):
        # Tiền xử lý dữ liệu
        clean_data = pre_process_ssi_stock_data(data)

        # Xử lý dữ liệu
        process_ssi_stock_data(
            spark, clean_data, config[0], time_stamp, stock_info)


def process_article_data(spark, data, stock_info, save_df_to_mongodb):
    # Đọc dữ liệu từ kafka vào dataframe
    schema = StructType([
        StructField("content", StringType(), True),
        StructField("description", StringType(), True),
        StructField("guid", StringType(), True),
        StructField("link", StringType(), True),
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("pubDate", StringType(), True),
    ])

    rdd = spark.sparkContext.parallelize(json.loads(data))
    data = spark.createDataFrame(rdd, schema)

    # Tiền xử lý dữ liệu
    new_article = pre_process_article_data(spark, data)

    # Xử lý dữ liệu
    extract_article_data(spark, new_article, stock_info, save_df_to_mongodb)


def start(msg, stock_info, spark_sess, save_df_to_mongodb):
    # Topic article là thông tin các bài báo thu thập
    if msg.topic == 'article':
        process_article_data(spark_sess, msg.value.decode(
            'utf-8'), stock_info, save_df_to_mongodb)

    # Topic crawl_data là thông tin chứng khoán từ sàn ssi
    if msg.topic == 'crawl_data':
        work = json.loads(msg.value.decode('utf-8'))
        process_ssi_data(spark_sess, work['data_dir'],
                         work['config_dir'], work['time_stamp'], stock_info)
