from pyspark import SparkContext
import pyspark

#
# sc = SparkContext("local", "count app")
#
# sc.setLogLevel("WARN")
# words = sc.parallelize (
#     ["scala",
#      "java",
#      "hadoop",
#      "spark",
#      "akka",
#      "spark vs hadoop",
#      "pyspark",
#      "pyspark and spark"]
# )
# counts = words.count()
# print "Number of elements in RDD -> %i" % (counts)
#
#

# #
# # words = sc.parallelize (
#  ["scala",
#   "java",
#   "hadoop",
#   "spark",
#   "akka",
#   "spark vs hadoop",
#   "pyspark",
#   "pyspark and spark"]
# # )
# def f(x): print(x)
# fore = words.foreach(f)

#
# words_filter = words.filter(lambda x: 'spark' in x)
# filtered = words_filter.collect()
# print ("Fitered RDD -> %s" % (filtered))

# words_filter=words.filter((lambda x:'py' in x)).collect()
# print ('filter %s is',words_filter)
#
#
# words_filter=words.filter(lambda  x:'spark' in x).collect()
# print(words_filter)

#
# words_map = words.map(lambda x: (x, 1))
# mapping = words_map.collect()
# print "Key value pair -> %s" % (mapping)
#
#
# wordsmap=words.map(lambda x:x.upper()).collect()
# print (wordsmap)
#
# from operator import sub
#
# nums = sc.parallelize([1, 2, 3, 4, 5])
# adding = nums.reduce(sub)
# print ("Adding all the elements -> %i" % (adding))
#
# x = sc.parallelize([("spark", 1), ("hadoop", 4)])
# y = sc.parallelize([("spark", 2), ("hadoop", 5)])
# joined = x.join(y)
# final = joined.collect()
#
# print ("Join RDD -> %s" % (final))


#
# words = sc.parallelize (
#  ["scala",
#   "java",
#   "hadoop",
#   "spark",
#   "akka",
#   "spark vs hadoop",
#   "pyspark",
#   "pyspark and spark"]
# )
# words.cache()
# caching = words.persist().is_cached
# print ("Words got chached > %s" % (caching))
# words=words.collect()
# print(words)


#
# words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
# data = words_new.value
# print "Stored data -> %s" % (data)
# elem = words_new.value[0]
# print ("Printing a particular element in RDD -> %s" % (elem))



#
# num = sc.accumulator(10)
# print(num)
# def f(x):
#  global num
#  num+=x
# rdd = sc.parallelize([20,30,40,50])
# rdd.foreach(f)
# final = num.value
# print "Accumulated value is -> %i" % (final)

#
# rdd1 = sc.parallelize([1,2])
# rdd1.persist( pyspark.StorageLevel.MEMORY_AND_DISK_2 )
# rdd1.getStorageLevel()
# print(rdd1.getStorageLevel())



from pyspark.serializers import MarshalSerializer,PickleSerializer
sc = SparkContext("local", "serialization app", serializer =PickleSerializer())
print(sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10))
sc.stop()