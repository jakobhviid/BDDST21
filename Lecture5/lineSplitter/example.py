# from pyspark import SparkConf, SparkContext
# from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

files = "hdfs://namenode:9000/stream-in/"

schema = StructType().add("value", "string")

spark = SparkSession.builder.appName('streamTest') \
    .config('spark.master','spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max',1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation','hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()
df = spark.readStream.option('path',files).option('sep','\n').schema(schema).format('text').load()
df.selectExpr("CAST(value AS STRING)").writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "lines") \
    .start().awaitTermination()
