
from datetime import datetime, timedelta
from pyspark.sql.functions import to_timestamp, col, lit
from constant.constant import hadoop_namenode, time_format


def process_ssi_stock_data(spark, data, config, time_stamp):
    try:
        time_stamp = datetime.strptime(time_stamp, '%m-%d-%Y-%H-%M-%S')

        data = data\
            .distinct()\
            .withColumn('best1Bid', col('best1Bid') / 1000)\
            .withColumn('best2Bid', col('best2Bid') / 1000)\
            .withColumn('best3Bid', col('best3Bid') / 1000)\
            .withColumn('best1Offer', col('best1Offer') / 1000)\
            .withColumn('best2Offer', col('best2Offer') / 1000)\
            .withColumn('best3Offer', col('best3Offer') / 1000)\
            .withColumn('lowest', col('lowest') / 1000)\
            .withColumn('highest', col('highest') / 1000)\
            .withColumn('refPrice', col('refPrice') / 1000)\
            .withColumn('floor', col('floor') / 1000)\
            .withColumn('ceiling', col('ceiling') / 1000)\
            .withColumn('matchedPrice', col('matchedPrice') / 1000)\
            .withColumn('time_stamp', to_timestamp(lit(time_stamp)))\
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
            time_stamp.strftime(time_format) + '.json'

        (stock_info_clean
            .write
            .format('json')
            .mode('overwrite')
            .save(data_dir))

        print('Success save to ' + data_dir)
    except Exception as e:
        print(e)
