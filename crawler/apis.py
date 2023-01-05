import requests
import json
from datetime import datetime

def get_analys_news_vndirect() :
    news = requests.get('https://finfo-api.vndirect.com.vn/v4/news?q=newsType:company_report~locale:VN~newsSource:VNDIRECT&sort=newsDate:desc~newsTime:desc&size=20')
    return json.loads(news.text)['data']

def get_news_vndirect() :
    news = requests.get('https://finfo-api.vndirect.com.vn/v4/news?q=newsGroup:company_news~locale:VN&sort=newsDate:desc~newsTime:desc&size=20')
    return json.loads(news.text)['data']

def get_stock_real_times_by_group (spark_context):
    try:
        spark_instance = spark_context.getOrCreate()
        stock_real_times_by_group = requests.post('https://wgateway-iboard.ssi.com.vn/graphql', json = {
            'operationName' : 'stockRealtimesByGroup',
            'query':  'query stockRealtimesByGroup($group: String) {\n  stockRealtimesByGroup(group: $group) {\n    stockNo\n    ceiling\n    floor\n    refPrice\n    stockSymbol\n    stockType\n    exchange\n    matchedPrice\n    matchedVolume\n    priceChange\n    priceChangePercent\n    highest\n    avgPrice\n    lowest\n    nmTotalTradedQty\n    best1Bid\n    best1BidVol\n    best2Bid\n    best2BidVol\n    best3Bid\n    best3BidVol\n    best4Bid\n    best4BidVol\n    best5Bid\n    best5BidVol\n    best6Bid\n    best6BidVol\n    best7Bid\n    best7BidVol\n    best8Bid\n    best8BidVol\n    best9Bid\n    best9BidVol\n    best10Bid\n    best10BidVol\n    best1Offer\n    best1OfferVol\n    best2Offer\n    best2OfferVol\n    best3Offer\n    best3OfferVol\n    best4Offer\n    best4OfferVol\n    best5Offer\n    best5OfferVol\n    best6Offer\n    best6OfferVol\n    best7Offer\n    best7OfferVol\n    best8Offer\n    best8OfferVol\n    best9Offer\n    best9OfferVol\n    best10Offer\n    best10OfferVol\n    buyForeignQtty\n    buyForeignValue\n    sellForeignQtty\n    sellForeignValue\n    caStatus\n    tradingStatus\n    remainForeignQtty\n    currentBidQty\n    currentOfferQty\n    session\n    tradingUnit\n    __typename\n  }\n}\n',
            'variables': {
                'group': 'VN30'
            }
        })
        timestamp =  datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
        result = json.loads(stock_real_times_by_group.text)['data']['stockRealtimesByGroup']

        df = spark_instance.sparkContext.parallelize(result).map(lambda x: json.dumps(x))
        df = spark_instance.read.json(df)

        (df
            .write
            .format('json')
            .mode('overwrite')
            .save('hdfs://namenode:9000/project20221/raw/stock_price_realtime/ssiDataApi-' + timestamp + '.json'))

        spark_instance.stop()
        return '/raw/stock_price_realtime/ssiDataApi-' + timestamp + '.json'

    except:
        print('Something wrong when get_stock_real_times_by_group may be request or hadoop or spark :>')
