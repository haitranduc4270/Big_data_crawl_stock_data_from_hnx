import pymongo

mongo_client = pymongo.MongoClient(
    'mongodb+srv://hai4270:hai4270@cluster-big-data.m420aoy.mongodb.net/?retryWrites=true&w=majority')

db = mongo_client['big-data']
print('Connect to db success')


def save_df_to_mongodb(modal, data):
    modal = db[modal]
    insert_data = data.collect()

    for data in insert_data:
        try:
            modal.insert_one(data.asDict())
        except Exception as e:
            print(e)
    print('Success save to mongodb')
