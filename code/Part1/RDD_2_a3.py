from pyspark.sql import SparkSession
import datetime
from math import radians, cos, sin, asin, sqrt
import time

start=time.time()
spark = SparkSession.builder.appName("Query2_RDD").getOrCreate()

sc = spark.sparkContext


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

def maximum(x1,y1):

	if(x1[0]>y1[0]): 
		return (x1[0],x1[1])
	else: 
		return (y1[0],y1[1])


data= sc.textFile("hdfs://master:9000/project/yellow_tripdata_1m.csv"). \
				map(lambda line: (int(line.split(',')[0]),(
					datetime.datetime.strptime(line.split(',')[1],"%Y-%m-%d %H:%M:%S"),
					datetime.datetime.strptime(line.split(',')[2],"%Y-%m-%d %H:%M:%S"),
					float(line.split(',')[3]),
					float(line.split(',')[4]),
					float(line.split(',')[5]),
					float(line.split(',')[6])))). \
				filter(lambda line: (-75.0<=line[1][2] and line[1][2]<=-73.0) and 
									(40.0<=line[1][3] and line[1][3]<=41.0) and 
									(-75.0<=line[1][4] and line[1][4]<=-73.0) and 
									(40.0<=line[1][5] and line[1][5]<=41.0)). \
				map(lambda line:(line[0],(
					float(harv(line[1][2],line[1][3],line[1][4],line[1][5])),
					float(abs(line[1][0]-line[1][1]).total_seconds()))))

vendors = sc.textFile("hdfs://master:9000/project/yellow_tripvendors_1m.csv"). \
					map(lambda line: (int(line.split(",")[0]),
						int(line.split(",")[1])))


res = data.join(vendors). \
		map(lambda x: (x[1][1],(x[1][0][0],x[1][0][1]))). \
		reduceByKey(maximum). \
		sortByKey()

for i in res.collect():
	print(i)
	
print("Execution Time: ", time.time()-start)


