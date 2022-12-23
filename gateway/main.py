from flask import Flask, jsonify
from controller.news import get_analyst_new
from dependencies.spark import start_spark

app = Flask(__name__)

spark, config = start_spark(
    app_name='my_etl_job',
    files=['configs/etl_config.json'])


@app.route('/<stock_code>', methods=['GET'])
def index(stock_code):
    print(stock_code)
    return jsonify(get_analyst_new(spark))


if __name__ == '__main__':
    app.run(debug=True)
