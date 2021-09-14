from hdfs import InsecureClient
from collections import Counter

import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

df = None

with client.read('/alice.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)
    # Create the dataframe
    df = pd.DataFrame(wordcount)

# Create a Pandas table
table = pa.Table.from_pandas(df)

# Note: writing a local file, and then uploading it is a bad way to do it for Big Data, however, it is a solution.
# The proper solution would be to use the Filesystem Interface in pyarrow (however, it requires a bit more tinkering with the docker container):
# https://arrow.apache.org/docs/python/filesystems.html
pq.write_table(table, 'word-count.pq') # Write a pandas table to a local file using Parquet
client.upload('/word-count.pq', 'word-count.pq') # Upload local file to the HDFS

# Download the file again into a different name
client.download('/word-count.pq', 'word-count2.pq')
# Use Parquet to read the local file
table2 = pq.read_table('word-count2.pq')
print(table2.to_pandas())