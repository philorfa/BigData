from pyspark.sql import SparkSession
import sys
from math import radians, cos, sin, asin, sqrt

import time

start=time.time()

spark = SparkSession.builder.appName("query2-sql").getOrCreate()


read = sys.argv[1]


if (read == "csv"):
    data = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/yellow_tripdata_1m.csv")
    vendors = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/project/yellow_tripvendors_1m.csv")
elif (read=="parquet"):
    data = spark.read.parquet("hdfs://master:9000/project/tripdata.parquet")
    vendors = spark.read.parquet("hdfs://master:9000/project/tripvendors.parquet")
else:
    raise Exception ("This setting is not available.")

data.registerTempTable("data")
vendors.registerTempTable("vendors")


def harv(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

spark.udf.register("harv_dist", harv)


transformed_table = \
"(select t._c0 as id,cast(harv_dist(cast(t._c3 as float),cast(t._c4 as float),cast(t._c5 as float),cast(t._c6 as float)) as float) as dist, cast(unix_timestamp(t._c2) - unix_timestamp(t._c1) as int) as travel " +\
"from data as t " +\
"where cast(t._c3 as float) >= -75.0 and cast(t._c3 as float)<= -73.0 and cast(t._c4 as float)>=40.0 and cast(t._c4 as float)<=41.0 and cast(t._c5 as float)>= -75.0 and cast(t._c5 as float)<= -73.0 and cast(t._c6 as float)>=40.0 and cast(t._c6 as float)<=41.0)"

joined_table = \
"(select v._c1 as Vendor, d.dist as Distance, d.travel AS TimeNeeded " +\
"from " + transformed_table + " as d " +\
"inner join vendors as v on d.ID=v._c0)"

table_of_maxes = \
"(select fin.Vendor as Vendor, MAX(fin.Distance) as Distance " +\
"from " + joined_table + " as fin " +\
"group by fin.Vendor)"

find_max_rows = \
"select f.Vendor as Vendor, f.Distance as Distance, f.TimeNeeded as TimeNeeded " +\
"from " + joined_table + " as f " +\
"inner join " + table_of_maxes +" as tmax on f.Vendor=tmax.Vendor and f.Distance=tmax.Distance"


res = spark.sql(find_max_rows)

res.show()

print("Execution Time: ", time.time()-start)