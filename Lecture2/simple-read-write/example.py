from hdfs import InsecureClient

client = InsecureClient('http://namenode:9870', user='root')

first_line = 'wow'

with client.read('/alice.txt', encoding='utf-8', delimiter='\n') as reader:
  for line in reader:
    print(line)
    first_line = line
    break
    
# client.write('/write2.txt', first_line, encoding='utf-8')

with client.write('/write.txt', encoding='utf-8') as writer:
    writer.write(first_line)
    writer.write('\n')
    writer.write(first_line)
    writer.write('\n')