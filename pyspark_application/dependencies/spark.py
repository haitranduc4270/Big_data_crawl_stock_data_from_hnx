from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from constant.constant import spark_master, app_name


def start_spark():
    spark = (SparkSession
             .builder
             .master(spark_master)
             .appName(app_name)
             .config("spark.executor.cores", 4)
             .config("spark.cores.max", 4)
             .config("spark.jars", "elasticsearch-hadoop-7.15.1.jar")
             .config("spark.driver.extraClassPath", "elasticsearch-hadoop-7.15.1.jar")
             .config("spark.es.nodes", 'elasticsearch')
             .config("spark.es.port", '9200')
             .config("spark.es.nodes.wan.only", 'true')
             .getOrCreate())

    return spark
