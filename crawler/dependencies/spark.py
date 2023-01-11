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
             .getOrCreate())

    return spark
