
from datetime import datetime, timedelta
from pyspark.sql.functions import to_timestamp, col, lit
from constant.constant import hadoop_namenode, time_format, date_format, time_format, elasticsearch_time_format, elasticsearch_index
from services.udfs.ssi import price_ssi, volume_ssi
from dependencies.elasticsearch import save_dataframes_to_elasticsearch


def process_ssi_stock_data(spark, data, config, time_stamp):
    try:
        print('Start')
        print(datetime.now())
        time_stamp = datetime.strptime(time_stamp, '%m-%d-%Y-%H-%M-%S')

        data = data\
            .distinct()\
            .withColumn('best1Bid', price_ssi(col('best1Bid')))\
            .withColumn('best2Bid', price_ssi(col('best2Bid')))\
            .withColumn('best3Bid', price_ssi(col('best3Bid')))\
            .withColumn('best1Offer', price_ssi(col('best1Offer')))\
            .withColumn('best2Offer', price_ssi(col('best2Offer')))\
            .withColumn('best3Offer', price_ssi(col('best3Offer')))\
            .withColumn('lowest', price_ssi(col('lowest')))\
            .withColumn('highest', price_ssi(col('highest')))\
            .withColumn('refPrice', price_ssi(col('refPrice')))\
            .withColumn('floor', price_ssi(col('floor')))\
            .withColumn('ceiling', price_ssi(col('ceiling')))\
            .withColumn('matchedPrice', price_ssi(col('matchedPrice')))\
            .withColumn('time_stamp', lit(time_stamp.strftime(elasticsearch_time_format)))\
            .withColumnRenamed('exchange', 'stock_exchange')\
            .withColumnRenamed('stockSymbol', 'stock_code')

        stock_info_clean = data.select(
            col('time_stamp'),
            col('stock_code'),
            col('stock_exchange'),
            col('best1Bid'),
            col('best1BidVol'),
            col('best2Bid'),
            col('best2BidVol'),
            col('best3Bid'),
            col('best3BidVol'),
            col('best1Offer'),
            col('best1OfferVol'),
            col('best2Offer'),
            col('best2OfferVol'),
            col('best3Offer'),
            col('best3OfferVol'),
            col('priceChange'),
            col('priceChangePercent'),
            col('nmTotalTradedQty'),
            col('lowest'),
            col('highest'),
            col('matchedPrice'),
            col('matchedVolume'),
            col('refPrice'),
            col('floor'),
            col('ceiling'))

        data_dir = hadoop_namenode + config['hadoop_clean_dir'] + \
            config['source']['body']['variables']['exchange'] + '/' + \
            time_stamp.strftime(date_format) + '/' + \
            time_stamp.strftime(time_format) + '.json'

        (stock_info_clean
            .write
            .format('json')
            .mode('overwrite')
            .save(data_dir))

        save_dataframes_to_elasticsearch(stock_info_clean, elasticsearch_index, {
            'es.nodes': 'elasticsearch',
            'es.port': '9200',
            "es.input.json": 'yes',
            "es.nodes.wan.only": 'true'
        })

        print('Success save to ' + data_dir)

        print('End')
        print(datetime.now())
    except Exception as e:
        print(e)
