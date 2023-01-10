ssi_stock_data_api = 'ssi_stock_data_api'
news_vndirect_api = 'news_vndirect_api'
analys_news_vndirect_api = 'analys_news_vndirect_api'

works = [
    {
        'data': ssi_stock_data_api,
        'config': 'config/ssi_stock_data.json'
    },
    {
        'data': news_vndirect_api,
        'config': 'config/news_vndirect.json'
    },
    {
        'data': analys_news_vndirect_api,
        'config': 'config/analys_news_vndirect.json'
    },

]

hadoop_namenode = 'hdfs://namenode:9000/project20221/'
spark_master = 'spark://spark-master:7077'
app_name = 'crawler'
kafka_bootstrap_servers = ['kafka:9092']
kafka_topic = 'crawl_data'

time_format = '%m-%d-%Y-%H-%M-%S'
time_zone = 7