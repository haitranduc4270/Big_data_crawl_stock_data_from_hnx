from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def start_spark ():
  spark = (SparkSession
    .builder
    .master('spark://spark-master-crawler:7077')
    .appName('crawler')
    .getOrCreate())

  return spark
