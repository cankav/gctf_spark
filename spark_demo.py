from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("spark_demo").getOrCreate()

df_f = spark.read.load('hdfs://spark-master0-dsl05:9000/gctf_data/gtp_full_tensor.csv', format="csv", sep=",", header="true",
                     schema='i INT ,k INT ,j INT , value DOUBLE')
#df_f.show(n=1000)

df_i1 = spark.read.load('hdfs://spark-master0-dsl05:9000/gctf_data/gtp_test_input1.csv', format="csv", sep=",", header="true",
                     schema='i INT ,k INT , value DOUBLE')
#df_i1.show()

df_i2 = spark.read.load('hdfs://spark-master0-dsl05:9000/gctf_data/gtp_test_input2.csv', format="csv", sep=",", header="true",
                     schema='j INT ,k INT , value DOUBLE')
#df_i2.show()

df = df_f.join( df_i1, ( ((df_f.i == df_i1.i)) & (df_f.k == df_i1.k) ),'inner').join( df_i2, ( ((df_f.j == df_i2.j)) & (df_f.k == df_i2.k) ),'inner' ).withColumn('output', df_i1.value * df_i2.value)

df.show(n=1000)

df = df_f.join( df_i1, ( ((df_f.i == df_i1.i)) & (df_f.k == df_i1.k) ),'inner').join( df_i2, ( ((df_f.j == df_i2.j)) & (df_f.k == df_i2.k) ),'inner' ).withColumn('output', df_i1.value * df_i2.value).groupBy([df_f['i'], df_f['j']]).sum('output').show(n=1000)

+---+---+-----------+
|  i|  j|sum(output)|
+---+---+-----------+
|  1|  1|    11800.0|
|  1|  2|    13400.0|
|  1|  3|    15000.0|
|  2|  1|    14000.0|
|  2|  2|    16000.0|
|  2|  3|    18000.0|
+---+---+-----------+
