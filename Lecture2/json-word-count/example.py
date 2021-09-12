from hdfs import InsecureClient
from collections import Counter
from json import dumps

client = InsecureClient('http://namenode:9870', user='root')

with client.read('/alice.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)
    
    client.write('/word-count.json', dumps(wordcount), encoding='utf-8', overwrite=True)