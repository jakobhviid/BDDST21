from pyspark import SparkConf, SparkContext
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g').set('spark.driver.host', '127.0.0.1')
sc = SparkContext(master='local', appName='pyspark-local', conf=conf)

files = "hdfs://namenode:9000/txt/"
txtFiles = sc.wholeTextFiles(files, 20)
words_in_files = txtFiles.map(lambda s: s[1].split())
all_word = txtFiles.flatMap(lambda s: s[1].split())
word_map = all_word.map(lambda s: (s, 1))
word_reduce = word_map.reduceByKey(lambda s, t: s+t)
print(word_reduce.sortBy(lambda s: s[1]).collect())