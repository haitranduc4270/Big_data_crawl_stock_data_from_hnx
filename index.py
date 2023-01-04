from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


spark_app = (SparkSession
.builder.master("spark://172.20.0.5:7077")
.appName('pyspark_application_test2')
.getOrCreate())

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df = spark_app.createDataFrame(data=data, schema = columns)
df.show()