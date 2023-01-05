from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def start_spark ():
  spark_context = (SparkSession
    .builder
    .master("spark://spark-master:7077")
    .appName('pyspark_application'))

  return spark_context
