from pyspark.sql import SparkSession
import datetime
import time

start=time.time()
spark = SparkSession.builder.appName("Query1_RDD").getOrCreate()

sc = spark.sparkContext


data= sc.textFile("hdfs://master:9000/project/yellow_tripdata_1m.csv"). \
				map(lambda line: (float(datetime.datetime.strptime(line.split(',')[1],"%Y-%m-%d %H:%M:%S").strftime("%H")),(float(line.split(',')[3]), float(line.split(',')[4]),1))). \
				filter(lambda line: (-75.0<=line[1][0] and line[1][0]<=-73.0) and (40.0<=line[1][1] and line[1][1]<=41.0)). \
				reduceByKey(lambda accum, data: (accum[0] + data[0], accum[1] + data[1], accum[2] + data[2])). \
				map(lambda x: (x[0],x[1][0]/x[1][2],x[1][1]/x[1][2]))



#data.cache()
for i in data.collect():
	print(i)


print("Execution Time: ", time.time()-start)