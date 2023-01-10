from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def start_spark ():
  spark_context = (SparkSession
    .builder
    .master("spark://spark-master:7077")
    .appName('pyspark_application')
    .config("spark.executor.cores", 4)
    .config("spark.cores.max", 4)
    .getOrCreate())

  return spark_context
