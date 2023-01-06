from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def start_spark ():
  spark = (SparkSession
    .builder
    .master('spark://localhost:8077')
    .appName('crawler')
    .getOrCreate())

  return spark

spark = start_spark()

dept = [("Finance",10), 
        ("Marketing",20), 
        ("Sales",30), 
        ("IT",40) 
      ]

deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

deptDF.write.format("csv").option("header","true").mode("overwrite").save("hdfs://localhost:9000/project20221/clean/ssiDataApi.csv")

stock_info1 = spark.read.option("header","true").csv("hdfs://localhost:9000/project20221/clean/ssiDataApi.csv")
stock_info1.show(60, False)