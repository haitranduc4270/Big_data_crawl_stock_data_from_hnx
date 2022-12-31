import requests
import json
stockRealtimesByGroup = requests.post('https://wgateway-iboard.ssi.com.vn/graphql', json = {
    "operationName" : "stockRealtimesByGroup",
    "query":  "query stockRealtimesByGroup($group: String) {\n  stockRealtimesByGroup(group: $group) {\n    stockNo\n    ceiling\n    floor\n    refPrice\n    stockSymbol\n    stockType\n    exchange\n    matchedPrice\n    matchedVolume\n    priceChange\n    priceChangePercent\n    highest\n    avgPrice\n    lowest\n    nmTotalTradedQty\n    best1Bid\n    best1BidVol\n    best2Bid\n    best2BidVol\n    best3Bid\n    best3BidVol\n    best4Bid\n    best4BidVol\n    best5Bid\n    best5BidVol\n    best6Bid\n    best6BidVol\n    best7Bid\n    best7BidVol\n    best8Bid\n    best8BidVol\n    best9Bid\n    best9BidVol\n    best10Bid\n    best10BidVol\n    best1Offer\n    best1OfferVol\n    best2Offer\n    best2OfferVol\n    best3Offer\n    best3OfferVol\n    best4Offer\n    best4OfferVol\n    best5Offer\n    best5OfferVol\n    best6Offer\n    best6OfferVol\n    best7Offer\n    best7OfferVol\n    best8Offer\n    best8OfferVol\n    best9Offer\n    best9OfferVol\n    best10Offer\n    best10OfferVol\n    buyForeignQtty\n    buyForeignValue\n    sellForeignQtty\n    sellForeignValue\n    caStatus\n    tradingStatus\n    remainForeignQtty\n    currentBidQty\n    currentOfferQty\n    session\n    tradingUnit\n    __typename\n  }\n}\n",
    "variables": {
        "group": "VN30"
    }
})
print(json.loads(stockRealtimesByGroup.text)['data']['stockRealtimesByGroup'])

