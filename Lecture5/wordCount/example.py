# from pyspark import SparkConf, SparkContext
# from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, split, to_json, array, col
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

files = "hdfs://namenode:9000/stream-in/"

schema = StructType().add("value", "string")
# wordCountSchema = StructType().add("word", "string").add("count", "int")

spark = SparkSession.builder.appName('streamTest') \
    .config('spark.master','spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max',1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation','hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "lines") \
  .load()
lines = df.selectExpr("CAST(value AS STRING)")

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count().sort(col('count'))

# Start running the query that prints the running counts to the console
# query = wordCounts \
    # .writeStream \
    # .outputMode("complete") \
    # .format("console") \
    # .start()
columns = [col('word'), col('count')]
mergedColumns = wordCounts.withColumn('value', array(columns))
mergedColumns.select(to_json(mergedColumns.value).alias('value')).selectExpr("CAST(value AS STRING)").writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "word-counts") \
    .outputMode("complete") \
    .start().awaitTermination()

# query.awaitTermination()