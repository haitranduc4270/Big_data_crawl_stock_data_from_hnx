import json
from services.apis.apis import get_stock_real_times_by_group, get_analys_news_vndirect
from constant.constant import ssi_stock_data_api, news_vndirect_api, analys_news_vndirect_api, hadoop_namenode, time_format, kafka_bootstrap_servers, kafka_topic
from dependencies import kafka_producer


def process_ssi_data(spark, data, work):
    kafka_producer_instance = kafka_producer.start_kafka_producer(
        kafka_bootstrap_servers)

    df = spark.sparkContext.parallelize(
        data['data']).map(lambda x: json.dumps(x))
    df = spark.read.json(df)

    data_dir = hadoop_namenode + work['hadoop_dir'] + \
        data['timestamp'].strftime(time_format) + '.json'

    (df
        .write
        .format('json')
        .mode('overwrite')
        .save(data_dir))

    kafka_producer_instance.send(kafka_topic, {
        'data_dir': work['hadoop_dir'] + data['timestamp'].strftime(time_format) + '.json',
        'config_dir': work['config_dir'],
        'time_stamp': data['timestamp'].strftime(time_format)
    })

    print('Success get data from ' +
          work['data'] + ' at ' + data['timestamp'].strftime(time_format))


def process_analys_news_vndirect(spark, data, work):
    kafka_producer_instance = kafka_producer.start_kafka_producer(
        kafka_bootstrap_servers)

    df = spark.sparkContext.parallelize(
        data['data']).map(lambda x: json.dumps(x))
    df = spark.read.json(df)

    data_dir = hadoop_namenode + work['hadoop_dir'] + \
        data['timestamp'].strftime(time_format) + '.json'

    (df
        .write
        .format('json')
        .mode('overwrite')
        .save(data_dir))

    kafka_producer_instance.send(kafka_topic, {
        'data_dir': work['hadoop_dir'] + data['timestamp'].strftime(time_format) + '.json',
        'config_dir': work['config_dir'],
        'time_stamp': data['timestamp'].strftime(time_format)
    })

    print('Success get data from ' +
          work['data'] + ' at ' + data['timestamp'].strftime(time_format))


def start_crawl(spark, work):
    if work['data'] == ssi_stock_data_api:
        data = get_stock_real_times_by_group(
            work['source']['url'], work['source']['body'])
        if (data):
            process_ssi_data(spark, data, work)

    if work['data'] == analys_news_vndirect_api:
        data = get_analys_news_vndirect(work['source']['url'])
        if (data):
            process_analys_news_vndirect(spark, data, work)
