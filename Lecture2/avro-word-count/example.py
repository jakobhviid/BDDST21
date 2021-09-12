from hdfs import InsecureClient
from hdfs.ext.avro import AvroReader, AvroWriter
from collections import Counter

client = InsecureClient('http://namenode:9870', user='root')

with client.read('/alice.txt', encoding='utf-8') as reader, AvroWriter(client, '/word-count.avro', overwrite=True) as writer:
    wordcount = Counter(reader.read().split()).most_common(10)
    
    for (key, count) in wordcount:
        writer.write({'word':key,'count':count})
    
with AvroReader(client, '/word-count.avro') as reader:
    schema = reader.schema # The inferred schema.
    content = reader.content # The remote file's HDFS content object.
    print(schema)
    print('\n')
    print(list(reader))
  
