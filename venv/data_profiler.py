import findspark
from pyspark import SparkContext
import pyspark

   # makes pyspark importable as a regular library
#Get a Spark session
# from pyspark.sql import SparkSession
#
# sc = SparkContext("local", "count app")
#
# sc.conf.set("spark.executor.cores", 4)
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("PySpark App").setMaster("local")

sc = SparkContext(conf=conf)
sc._conf.set("spark.executor.core",1)

# 'spark.executor.cores','1'


#
# import platform, sys, os
# print('Platform = ',platform.platform())
# print('Version of Spark = ',sc.version)
# print('Python version = ',sys.version)

#
# rdd2 = sc.parallelize([],10) #This creates 10 partitions
# print(rdd2.getNumPartitions())
#
# rdd3=sc.parallelize([1,2,3,4,56,7,8,9,12,3], 10).collect()
# print(rdd3)
#
# reparRdd = rdd2.repartition((2))
# print("re-partition count:"+str(reparRdd.getNumPartitions()))
# rdd = sc.textFile("test.txt")
rdd = sc.parallelize([["a",1],["a",2],["b",1],["a",3],["b",3],["c",5]], 5)
#
# print(rdd.groupByKey())
print(rdd.collect())
rdd.groupByKey()

p=key.hashCode()