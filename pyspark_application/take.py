from pyspark.sql.functions import to_timestamp, col, concat, lit, from_json, schema_of_json, flatten, json_tuple
from pyspark.sql.types import *
import json
from dependencies.elasticsearch import save_dataframes_to_elasticsearch

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from constant.constant import spark_master

spark = (SparkSession
         .builder
         .master(spark_master)
         .appName('muon ti')
         .config("spark.executor.cores", 4)
         .config("spark.cores.max", 4)
         .config("spark.jars", "elasticsearch-hadoop-7.15.1.jar")
         .config("spark.driver.extraClassPath", "elasticsearch-hadoop-7.15.1.jar")
         .config("spark.es.nodes", 'elasticsearch')
         .config("spark.es.port", '9200')
         .config("spark.es.nodes.wan.only", 'true')
         .getOrCreate())


with open('data/stock.json', 'r') as openfile:
    stock_info = json.load(openfile)
stock_info = stock_info['data']

json = spark.read.json(
    'hdfs://namenode:9000/project20221-data/19-1/a.json')


a = json.select(col('_source'))


stock_info_clean = a\
    .withColumn('time_stamp', col('_source.time_stamp'))\
    .withColumn('stock_code', col('_source.stock_code'))\
    .withColumn('stock_exchange', col('_source.stock_exchange'))\
    .withColumn('best1Bid', col('_source.best1Bid'))\
    .withColumn('best1BidVol', col('_source.best1BidVol'))\
    .withColumn('best2Bid', col('_source.best2Bid'))\
    .withColumn('best2BidVol', col('_source.best2BidVol'))\
    .withColumn('best3Bid', col('_source.best3Bid'))\
    .withColumn('best3BidVol', col('_source.best3BidVol'))\
    .withColumn('best1Offer', col('_source.best1Offer'))\
    .withColumn('best1OfferVol', col('_source.best1OfferVol'))\
    .withColumn('best2Offer', col('_source.best2Offer'))\
    .withColumn('best2OfferVol', col('_source.best2OfferVol'))\
    .withColumn('best3Offer', col('_source.best3Offer'))\
    .withColumn('best3OfferVol', col('_source.best3OfferVol'))\
    .withColumn('priceChange', col('_source.priceChange'))\
    .withColumn('priceChangePercent', col('_source.priceChangePercent'))\
    .withColumn('nmTotalTradedQty', col('_source.nmTotalTradedQty'))\
    .withColumn('lowest', col('_source.lowest'))\
    .withColumn('highest', col('_source.highest'))\
    .withColumn('matchedPrice', col('_source.matchedPrice'))\
    .withColumn('matchedVolume', col('_source.matchedVolume'))\
    .withColumn('refPrice', col('_source.refPrice'))\
    .withColumn('floor', col('_source.floor'))\
    .withColumn('ceiling', col('_source.ceiling'))

stock_info_clean = stock_info_clean.select(
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
    index = row.stock_code + '-' + row.stock_exchange
    return ([
        row.stock_code,
        row.stock_exchange,
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
    ])


stock_info_clean = stock_info_clean.rdd.map(reformat).toDF([
    'stock_code',
    'stock_exchange',
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

stock_info_clean = stock_info_clean.withColumn(
    "priceChange", stock_info_clean.priceChange.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "priceChangePercent", stock_info_clean.priceChangePercent.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "chartercapital", stock_info_clean.chartercapital.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "numberofemployee", stock_info_clean.numberofemployee.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "issueshare", stock_info_clean.issueshare.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "firstprice", stock_info_clean.firstprice.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "sharesoutstanding", stock_info_clean.sharesoutstanding.cast(DoubleType()))
stock_info_clean = stock_info_clean.withColumn(
    "marketcap", stock_info_clean.marketcap.cast(DoubleType()))

stock_info_clean.printSchema()
print(stock_info_clean.count())

save_dataframes_to_elasticsearch(stock_info_clean, '19_11', {
    'es.nodes': 'elasticsearch',
    'es.port': '9200',
    "es.input.json": 'yes',
    "es.nodes.wan.only": 'true'
})
