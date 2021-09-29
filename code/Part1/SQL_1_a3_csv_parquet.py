from pyspark.sql import SparkSession
import sys
import time

start=time.time()

spark = SparkSession.builder.appName("query1-sql").getOrCreate()


read = sys.argv[1]

if (read == "csv"):
	data = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/yellow_tripdata_1m.csv")
elif (read=="parquet"):
	data = spark.read.parquet("hdfs://master:9000/project/tripdata.parquet")
else:
	raise Exception ("This setting is not available.")

data.registerTempTable("data")

sqlString = \
"select HOUR(d._c1) as Hour, AVG(d._c3) as Longitude, AVG(d._c4) AS Latitude " +\
"from data as d " +\
"where d._c3 >= -75.0 and d._c3<= -73.0 and d._c4>=40.0 and d._c4<=41.0 " +\
"group by HOUR(d._c1) " +\
"order by HOUR(d._c1)"

res = spark.sql(sqlString)

res.show(24)

print("Execution Time: ", time.time()-start)