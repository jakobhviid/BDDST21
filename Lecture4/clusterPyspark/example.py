from pyspark import SparkConf, SparkContext
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g')
sc = SparkContext(master='spark://spark-master:7077', appName='myAppName', conf=conf)

select_words = lambda s : s[1] > 400

files = "hdfs://namenode:9000/txt/"
txtFiles = sc.wholeTextFiles(files, 20)
words_in_files = txtFiles.map(lambda s: s[1].split())
all_word = txtFiles.flatMap(lambda s: s[1].split())
word_map = all_word.map(lambda s: (s, 1))
word_reduce = word_map.reduceByKey(lambda s, t: s+t)
top_words = word_reduce.filter(select_words).sortBy(lambda s: s[1])
top_words.saveAsTextFile('hdfs://namenode:9000/txt-out')
print(top_words.collect())

