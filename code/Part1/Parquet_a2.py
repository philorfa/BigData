from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("parquet").getOrCreate()


data_read = time.time()
tripdata = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/yellow_tripdata_1m.csv")

vendors_read = time.time()
tripvendors = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/yellow_tripvendors_1m.csv")

data_write=time.time()
tripdata.write.parquet("hdfs://master:9000/project/tripdata.parquet")

vendors_write=time.time()
tripvendors.write.parquet("hdfs://master:9000/project/tripvendors.parquet")

complete=time.time()

print("Tripdata Read Time: ", vendors_read-data_read)

print("Tripdata Convert and Write Time: ",vendors_write-data_write)

print("Tripvendors Read Time: ", data_write-vendors_read)

print("Tripvendors Convert and Write Time: ", complete-vendors_write )