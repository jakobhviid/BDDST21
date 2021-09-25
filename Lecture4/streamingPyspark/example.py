from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()
import time

conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g')
sc = SparkContext(master='spark://spark-master:7077', appName='myAppName', conf=conf)
ssc = StreamingContext(sc, 3)

select_words = lambda s : s[1] > 400

files = "hdfs://namenode:9000/stream-in/"
lines = ssc.textFileStream(files)
counts = lines.flatMap(lambda line: line.split(" ")) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(select_words)
counts.pprint()
counts.foreachRDD(lambda rdd: rdd.saveAsTextFile('hdfs://namenode:9000/stream-out/' + str(time.time()) + '/'))
ssc.start()
ssc.awaitTermination()