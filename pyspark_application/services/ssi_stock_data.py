
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from constant.constant import hadoop_namenode, time_format, date_format, time_format, elasticsearch_time_format, elasticsearch_index
from dependencies.elasticsearch import save_dataframes_to_elasticsearch


def pre_process_ssi_stock_data(data):

    # Hàm tạo thêm cột nếu cột không tồn tại, cast dữ liệu trong cột về đúng dataType mặc định
    def fill_data(column, df, data_type):
        if not column in df.columns:
            ret = lit(None).cast(data_type)
        else:
            ret = col(column).cast(data_type)

        return ret

    # Fill data bằng giá trị mặc định, tạo cột nếu cột không tồn tại
    data = (data
            .withColumn('stockSymbol', fill_data('ss', data, StringType()))
            .withColumn('exchange', fill_data('e', data, StringType()))
            .withColumn('priceChange', fill_data('pc', data, DoubleType()))
            .withColumn('priceChangePercent', fill_data('cp', data, DoubleType()))
            .withColumn('nmTotalTradedQty', fill_data('mtq', data, LongType()))
            .withColumn('best1Bid', fill_data('b1', data, DoubleType()))
            .withColumn('best2Bid', fill_data('b2', data, DoubleType()))
            .withColumn('best3Bid', fill_data('b3', data, DoubleType()))
            .withColumn('best1Offer', fill_data('o1', data, DoubleType()))
            .withColumn('best2Offer', fill_data('o2', data, DoubleType()))
            .withColumn('best3Offer', fill_data('o3', data, DoubleType()))
            .withColumn('lowest', fill_data('l', data, DoubleType()))
            .withColumn('highest', fill_data('h', data, DoubleType()))
            .withColumn('refPrice', fill_data('r', data, DoubleType()))
            .withColumn('floor', fill_data('f', data, DoubleType()))
            .withColumn('ceiling', fill_data('c', data, DoubleType()))
            .withColumn('matchedPrice', fill_data('mp', data, DoubleType()))
            .withColumn('best1BidVol', fill_data('b1v', data, LongType()))
            .withColumn('best2BidVol', fill_data('b2v', data, LongType()))
            .withColumn('best3BidVol', fill_data('b3v', data, LongType()))
            .withColumn('best1OfferVol', fill_data('o1v', data, LongType()))
            .withColumn('best2OfferVol', fill_data('o2v', data, LongType()))
            .withColumn('best3OfferVol', fill_data('o3v', data, LongType()))
            .withColumn('matchedVolume', fill_data('mv', data, LongType()))
            .withColumn('currentBidQty', fill_data('b1', data, DoubleType()))
            .withColumn('currentOfferQty', fill_data('o1', data, DoubleType()))
            .withColumn('session', fill_data('s', data, StringType()))
            .withColumn('stockType', fill_data('st', data, StringType()))
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

        # Sử dụng spark map để đổi lại tên và thêm thông tin tương ứng của từng công ty theo mã cổ phiếu
        def reformat(row):
            index = row.stockSymbol + '-' + row.exchange
            return ([
                row.stockSymbol,
                row.exchange,
                row.priceChange,
                row.priceChangePercent,
                row.nmTotalTradedQty,
                row.best1Bid,
                row.best2Bid,
                row.best3Bid,
                row.best1Offer,
                row.best2Offer,
                row.best3Offer,
                row.lowest,
                row.highest,
                row.refPrice,
                row.floor,
                row.ceiling,
                row.matchedPrice,
                row.best1BidVol,
                row.best2BidVol,
                row.best3BidVol,
                row.best1OfferVol,
                row.best2OfferVol,
                row.best3OfferVol,
                row.matchedVolume,
                
                stock_info[index]['companyProfile']["subSectorCode"] if index in stock_info else "",
                stock_info[index]['companyProfile']["industryName"] if index in stock_info else "",
                stock_info[index]['companyProfile']["superSector"] if index in stock_info else "",
                stock_info[index]['companyProfile']["sector"] if index in stock_info else "",
                stock_info[index]['companyProfile']["subSector"] if index in stock_info else "",
                stock_info[index]['companyProfile']["charterCapital"] if index in stock_info else 0,
                stock_info[index]['companyProfile']["numberOfEmployee"] if index in stock_info else 0,
                stock_info[index]['companyProfile']["issueShare"] if index in stock_info else 0,
                stock_info[index]['companyProfile']["firstPrice"] if index in stock_info else 0,
                # stock_info[index]['companyStatistics']["sharesoutstanding"] if index in stock_info else 0,
                # stock_info[index]['companyStatistics']["marketcap"] if index in stock_info else 0,
                stock_info[index]['companyStatistics']["firstPrice"] if index in stock_info else 0,
                stock_info[index]['companyStatistics']["firstPrice"] if index in stock_info else 0,
                
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

        # Cast lại các giá trị thông tin công ty string về double
        data = (data
                .withColumn('chartercapital', col('chartercapital').cast(DoubleType()))
                .withColumn('numberofemployee', col('numberofemployee').cast(LongType()))
                .withColumn('issueshare', col('issueshare').cast(DoubleType()))
                .withColumn('firstprice', col('firstprice').cast(DoubleType()))
                .withColumn('sharesoutstanding', col('sharesoutstanding').cast(DoubleType()))
                .withColumn('marketcap', col('marketcap').cast(DoubleType()))
                .withColumn('time_stamp', lit(time_stamp.strftime(elasticsearch_time_format))))

        # Fill các giá trị mới thêm bằng giá trị mặc định
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

        # Ghi dữ liệu sau xử lý vào hadoop
        data_dir = (hadoop_namenode + config['hadoop_clean_dir'] +
                    config['source']['body']['variables']['exchange'] + '/' +
                    time_stamp.strftime(date_format) + '/' +
                    time_stamp.strftime(time_format) + '.parquet')

        (data
            .write
            .format('parquet')
            .mode('overwrite')
            .save(data_dir))

        # Ghi dữ liệu ra elasticsearch
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
