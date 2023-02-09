# Ghi các file config vào hadoop
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from constant.constant import works, hadoop_namenode

sc = SparkContext('local')
spark = SparkSession(sc)

data = ['ssi_stock_data.json', 'ssi_stock_data_HOSE.json',
        'ssi_stock_data_HNX.json', 'analys_news_vndirect.json', 'news_vndirect.json']

for file in data:
    config = spark.read.option(
        "multiLine", "true").json('config/' + file)
    config.write.format("json").mode("overwrite").save(
        hadoop_namenode + "config/" + file)
