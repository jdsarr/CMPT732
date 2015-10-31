'''
Created on Oct 30, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext, SQLContext, Row
import sys, re, datetime

inputs = sys.argv[1]
output = sys.argv[2]
linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

def parse(match, n):
    x = match.group(n)
    if n == 1 or n == 3:
        return x
    elif n == 2:
        return datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S')
    elif n == 4:
        return long(x)


conf = SparkConf().setAppName('load logs')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

matched_lines   = text.map(lambda line: linere.match(line))
f_matched_lines = matched_lines.filter(lambda match: match != None)
rows            = f_matched_lines.map(lambda m: Row(host=parse(m,1), date=parse(m,2), path=parse(m,3), bytes=parse(m,4)) )


df = sqlContext.createDataFrame(rows)
df.write.format('parquet').save(output)