from hdfs import InsecureClient
from collections import Counter

client = InsecureClient('http://namenode:9870', user='root')

with client.read('/alice.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)

    
# To-Do!