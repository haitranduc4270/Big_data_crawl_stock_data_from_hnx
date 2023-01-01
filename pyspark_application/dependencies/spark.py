from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def start_spark (master = 'local', app_name):
        
  spark_builder = (
    SparkSession
    .builder
    .master(master)
    .appName(app_name))

  spark_sess = spark_builder.getOrCreate()

  return spark_sess