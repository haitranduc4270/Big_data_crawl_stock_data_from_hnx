# import time
# from datetime import datetime

# print('Craler stared')

# while True:
#     print(datetime.now())
#     time.sleep(60)

import time

from dependencies import spark
from services.crawl.crawl import start_crawl
from constant.constant import works, hadoop_namenode


def crawl(spark_sess, work):
    config = (spark_sess
              .read
              .option("multiLine", "true")
              .json(hadoop_namenode + work['config'])
              .rdd
              .map(lambda row: row.asDict()).collect())

    if (len(config) != 0):
        start_crawl(spark_sess, config[0])


def start_crawler():
    spark_sess = spark.start_spark()

    while True:
        for work in works:
            crawl(spark_sess, work)

        time.sleep(6)


if __name__ == '__main__':
    try:
        start_crawler()
    except Exception as e:
        print(e)
