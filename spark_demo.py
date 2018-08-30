from pyspark.sql import SparkSession
from pyspark.sql import Row

df = spark.createDataFrame( [(1,),(2,),(3,)] , ('id',))
df = df.rdd.map(lambda x: Row(idd=x['id']))
print(df.collect())
