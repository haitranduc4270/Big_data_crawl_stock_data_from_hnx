
from datetime import datetime, timedelta
from pyspark.sql.functions import to_timestamp, col, lit
from constant.constant import hadoop_namenode, time_format, date_format, time_format, elasticsearch_time_format, elasticsearch_index
from dependencies.elasticsearch import save_dataframes_to_elasticsearch


def process_ssi_stock_data(spark, data, config, time_stamp, stock_info):

    try:
        print('Start')
        print(datetime.now())
        time_stamp = datetime.strptime(time_stamp, '%m-%d-%Y-%H-%M-%S')

        def price_ssi(price):
            if price is None:
                return 0
            return price * 1

        def volume_ssi(volume):
            if volume is None:
                return 0
            return volume * 1

        def to_number(string):
            if string is None:
                return 0
            else:
                try:
                    number = round(float(string), 4)
                    return number
                except Exception as e:
                    print(e)
                    print(string)

        def reformat(row):
            index = row.stockSymbol + '-' + row.exchange
            return ([
                row.stockSymbol,
                row.exchange,
                to_number(row.priceChange),
                to_number(row.priceChangePercent),
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
                to_number(stock_info[index]
                          ['companyProfile']["chartercapital"]),
                to_number(stock_info[index]
                          ['companyProfile']["numberofemployee"]),
                to_number(stock_info[index]['companyProfile']["issueshare"]),
                to_number(stock_info[index]['companyProfile']["firstprice"]),
                to_number(
                    stock_info[index]['companyStatistics']["sharesoutstanding"]),
                to_number(stock_info[index]['companyStatistics']["marketcap"]),
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
        ])

        stock_info_clean = data\
            .withColumn('time_stamp', lit(time_stamp.strftime(elasticsearch_time_format)))

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
