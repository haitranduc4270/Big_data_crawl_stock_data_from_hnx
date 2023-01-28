
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from constant.constant import hadoop_namenode, time_format, date_format, time_format, elasticsearch_time_format, elasticsearch_index
from dependencies.elasticsearch import save_dataframes_to_elasticsearch


def pre_process_ssi_stock_data(data):
    def fill_data(column, df, data_type):
        if not column in df.columns:
            ret = lit(None).cast(data_type)
        else:
            ret = col(column).cast(data_type)

        return ret

    data = (data
            .withColumn('stockSymbol', fill_data('stockSymbol', data, StringType()))
            .withColumn('exchange', fill_data('exchange', data, StringType()))
            .withColumn('priceChange', fill_data('priceChange', data, DoubleType()))
            .withColumn('priceChangePercent', fill_data('priceChangePercent', data, DoubleType()))
            .withColumn('nmTotalTradedQty', fill_data('nmTotalTradedQty', data, LongType()))
            .withColumn('best1Bid', fill_data('best1Bid', data, DoubleType()))
            .withColumn('best2Bid', fill_data('best2Bid', data, DoubleType()))
            .withColumn('best3Bid', fill_data('best3Bid', data, DoubleType()))
            .withColumn('best1Offer', fill_data('best1Offer', data, DoubleType()))
            .withColumn('best2Offer', fill_data('best2Offer', data, DoubleType()))
            .withColumn('best3Offer', fill_data('best3Offer', data, DoubleType()))
            .withColumn('lowest', fill_data('lowest', data, DoubleType()))
            .withColumn('highest', fill_data('highest', data, DoubleType()))
            .withColumn('refPrice', fill_data('refPrice', data, DoubleType()))
            .withColumn('floor', fill_data('floor', data, DoubleType()))
            .withColumn('ceiling', fill_data('ceiling', data, DoubleType()))
            .withColumn('matchedPrice', fill_data('matchedPrice', data, DoubleType()))
            .withColumn('best1BidVol', fill_data('best1BidVol', data, LongType()))
            .withColumn('best2BidVol', fill_data('best2BidVol', data, LongType()))
            .withColumn('best3BidVol', fill_data('best3BidVol', data, LongType()))
            .withColumn('best1OfferVol', fill_data('best1OfferVol', data, LongType()))
            .withColumn('best2OfferVol', fill_data('best2OfferVol', data, LongType()))
            .withColumn('best3OfferVol', fill_data('best3OfferVol', data, LongType()))
            .withColumn('matchedVolume', fill_data('matchedVolume', data, LongType()))
            .withColumn('currentBidQty', fill_data('currentBidQty', data, DoubleType()))
            .withColumn('currentOfferQty', fill_data('currentOfferQty', data, DoubleType()))
            .withColumn('session', fill_data('session', data, StringType()))
            .withColumn('stockType', fill_data('stockType', data, StringType()))
            )

    data = (data
            .na.fill(value=0, subset=([
                'nmTotalTradedQty',
                'best1BidVol',
                'best2BidVol',
                'best3BidVol',
                'best1OfferVol',
                'best2OfferVol',
                'best3OfferVol',
                'matchedVolume',
                'currentBidQty',
            ]))
            .na.fill(value=0.0, subset=([
                'priceChange',
                'priceChangePercent',
                'best1Bid',
                'best2Bid',
                'best3Bid',
                'best1Offer',
                'best2Offer',
                'best3Offer',
                'lowest',
                'highest',
                'refPrice',
                'floor',
                'ceiling',
                'matchedPrice',
                'currentBidQty',
                'currentOfferQty',
            ]))
            .na.fill(value='undefined', subset=['stockSymbol', 'exchange', 'session', 'stockType'])
            )

    return data


def process_ssi_stock_data(spark, data, config, time_stamp, stock_info):

    try:
        print('Start')
        print(datetime.now())
        time_stamp = datetime.strptime(time_stamp, '%m-%d-%Y-%H-%M-%S')

        def price_ssi(price):
            return price * 10

        def volume_ssi(volume):
            return volume * 1

        def reformat(row):
            index = row.stockSymbol + '-' + row.exchange
            return ([
                row.stockSymbol,
                row.exchange,
                row.priceChange,
                row.priceChangePercent,
                row.nmTotalTradedQty,
                price_ssi(row.best1Bid),
                price_ssi(row.best2Bid),
                price_ssi(row.best3Bid),
                price_ssi(row.best1Offer),
                price_ssi(row.best2Offer),
                price_ssi(row.best3Offer),
                price_ssi(row.lowest),
                price_ssi(row.highest),
                price_ssi(row.refPrice),
                price_ssi(row.floor),
                price_ssi(row.ceiling),
                price_ssi(row.matchedPrice),
                volume_ssi(row.best1BidVol),
                volume_ssi(row.best2BidVol),
                volume_ssi(row.best3BidVol),
                volume_ssi(row.best1OfferVol),
                volume_ssi(row.best2OfferVol),
                volume_ssi(row.best3OfferVol),
                volume_ssi(row.matchedVolume),
                stock_info[index]['companyProfile']["subsectorcode"],
                stock_info[index]['companyProfile']["industryname"],
                stock_info[index]['companyProfile']["supersector"],
                stock_info[index]['companyProfile']["sector"],
                stock_info[index]['companyProfile']["subsector"],
                stock_info[index]['companyProfile']["chartercapital"],
                stock_info[index]['companyProfile']["numberofemployee"],
                stock_info[index]['companyProfile']["issueshare"],
                stock_info[index]['companyProfile']["firstprice"],
                stock_info[index]['companyStatistics']["sharesoutstanding"],
                stock_info[index]['companyStatistics']["marketcap"],
                row.currentBidQty,
                row.currentOfferQty,
                row.session,
                row.stockType,
            ])

        data = data.rdd.map(reformat).toDF([
            'stockSymbol',
            'exchange',
            'priceChange',
            'priceChangePercent',
            'nmTotalTradedQty',
            'best1Bid',
            'best2Bid',
            'best3Bid',
            'best1Offer',
            'best2Offer',
            'best3Offer',
            'lowest',
            'highest',
            'refPrice',
            'floor',
            'ceiling',
            'matchedPrice',
            'best1BidVol',
            'best2BidVol',
            'best3BidVol',
            'best1OfferVol',
            'best2OfferVol',
            'best3OfferVol',
            'matchedVolume',
            'subsectorcode',
            'industryname',
            'supersector',
            'sector',
            'subsector',
            'chartercapital',
            'numberofemployee',
            'issueshare',
            'firstprice',
            'sharesoutstanding',
            'marketcap',
            'currentBidQty',
            'currentOfferQty',
            'session',
            'stockType',
        ])

        data = (data
                .withColumn('chartercapital', col('chartercapital').cast(DoubleType()))
                .withColumn('numberofemployee', col('numberofemployee').cast(LongType()))
                .withColumn('issueshare', col('issueshare').cast(DoubleType()))
                .withColumn('firstprice', col('firstprice').cast(DoubleType()))
                .withColumn('sharesoutstanding', col('sharesoutstanding').cast(DoubleType()))
                .withColumn('marketcap', col('marketcap').cast(DoubleType()))
                .withColumn('time_stamp', lit(time_stamp.strftime(elasticsearch_time_format))))

        data = (data
                .na.fill(value='undefined', subset=([
                    'subsectorcode',
                    'industryname',
                    'supersector',
                    'sector',
                    'subsector']))
                .na.fill(value=0.0, subset=([
                    'chartercapital',
                    'issueshare',
                    'firstprice',
                    'sharesoutstanding',
                    'marketcap']))
                .na.fill(value=0, subset=['numberofemployee']))

        data_dir = (hadoop_namenode + config['hadoop_clean_dir'] +
                    config['source']['body']['variables']['exchange'] + '/' +
                    time_stamp.strftime(date_format) + '/' +
                    time_stamp.strftime(time_format) + '.json')

        (data
            .write
            .format('json')
            .mode('overwrite')
            .save(data_dir))

        save_dataframes_to_elasticsearch(data, elasticsearch_index, {
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
