from pyspark import SparkContext
sc = SparkContext("local", "count app")
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


words = sc.parallelize (
 ["scala",
  "java",
  "hadoop",
  "spark",
  "akka",
  "spark vs hadoop",
  "pyspark",
  "pyspark and spark"]
)
def f(x): print(x)
fore = words.foreach(f)

#
# words_filter = words.filter(lambda x: 'spark' in x)
# filtered = words_filter.collect()
# print ("Fitered RDD -> %s" % (filtered))

words_filter=words.filter((lambda x:'py' in x)).collect()
print ('filter %s is',words_filter)


words_filter=words.filter(lambda  x:'spark' in x).collect()
print(words_filter)


words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print "Key value pair -> %s" % (mapping)