from pyspark.sql.functions import col, lit, coalesce, explode, split
from pyspark.sql.types import *
from datetime import datetime, timedelta
import re
from dependencies.elasticsearch import save_dataframes_to_elasticsearch
from constant.constant import hadoop_namenode


def pre_process_article_data(spark, data):
    print('Start article')

    # Đọc dữ liệu các bài báo đã có từ hadoop
    schema = StructType([
        StructField("content", StringType(), True),
        StructField("description", StringType(), True),
        StructField("guid", StringType(), True),
        StructField("link", StringType(), True),
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("id", StringType(), True),
        StructField("pubDate", StringType(), True),
    ])

    articles = 0

    try:
        articles = (spark
                    .read
                    .format('parquet')
                    .schema(schema)
                    .load(hadoop_namenode + 'articles.parquet'))

    except Exception as e:
        print(e)

    data = data.na.fill(value='')

    print('Read data success')

    # Sử dụng spark map để tính time stamp và định danh duy nhất của từng bài báo
    def get_article_id(row):
        return ([
            row.content,
            row.description,
            row.link,
            row.source,
            row.title,
            (datetime.strptime(
                row.pubDate[5:][:-6], '%d %b %Y %H:%M:%S') - timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S'),
            row.source + row.link.split('-')[-1].split('.')[0],
        ])

    new_article = data.rdd.map(get_article_id).toDF([
        'content',
        'description',
        'link',
        'source',
        'title',
        'pubDate',
        'id',
    ])

    # Tìm các bài báo chưa được ghi trong hadoop tức là các bài báo mới để tránh trùng lắp
    if articles != 0:

        new_article = new_article.join(articles.withColumn('new', lit(False)).select(col('id'), col('new')), on="id", how='left')\
            .withColumn('new', coalesce('new', lit(True)))\
            .filter(col('new') == True)

    new_article.show()

    new_article = new_article.select(col('id'), col('content'), col(
        'description'), col('link'), col('source'), col('title'), col('pubDate'))

    # Ghi lại các bài báo ra hadoop
    (new_article
        .write
        .format('parquet')
        .mode('append')
        .save(hadoop_namenode + 'articles.parquet'))

    print('Success append to articles.parquet')

    return new_article


def extract_article_data(spark, new_article, stock_info, save_df_to_mongodb):
    # Lấy tất cả các mã cô phiếu từ stock_info ghi vào mảng để xử dụng
    stock_codes = []
    for stock in stock_info.values():
        stock_codes.append(stock['companyProfile']['symbol'])

    stock_codes = list(filter(lambda score: score != None, stock_codes))

    # Sử dụng spark map để lặp qua các bài báo và xác định số lần mà một mã cổ phiếu được nhắc đến trong nội dung
    def get_tag(row):
        result = []
        # Chỉ lấy ra chữ, số, dấu cách từ nội dung
        string = re.sub(r'[^\w\s]', '', row.content)
        # Tách thành từng từ
        string = string.split(' ')

        # Kiểm tra từ khớp với mã cổ phiếu
        for word in string:
            if word in stock_codes:
                result.append(word)

        return ([
            ' '.join(map(str, result)),
            row.content,
            row.description,
            row.link,
            row.source,
            row.title,
            row.pubDate,
            row.id,
        ])

    new_article = new_article.rdd.map(get_tag).toDF([
        'tag',
        'content',
        'description',
        'link',
        'source',
        'title',
        'pubDate',
        'id',
    ])

    # Ghi dữ liệu từng bài báo cùng tag vào mongodb
    save_df_to_mongodb('articles', new_article)

    # Tách theo từng lần một mã cổ phiếu xuất hiện
    new_article = (new_article
                   .select(col('id'), col('tag'), col('description'), col('link'), col('source'), col('title'), col('pubDate'))
                   .withColumn('tag', explode(split('tag', ' ')))
                   .filter(col('tag') != ''))

    # Lưu thông tin vào elasticseach
    save_dataframes_to_elasticsearch(new_article, 'article', {
        'es.nodes': 'elasticsearch',
        'es.port': '9200',
        "es.input.json": 'yes',
        "es.nodes.wan.only": 'true'
    })
