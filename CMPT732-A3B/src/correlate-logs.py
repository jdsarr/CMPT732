'''
Created on Oct 20, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext
import sys, re, math, json

inputs = sys.argv[1]
output = sys.argv[2]
linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

conf = SparkConf().setAppName('correlate logs')
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)

matched_lines          = text.map(lambda line: linere.match(line))
filtered_matched_lines = matched_lines.filter(lambda match: match != None)
host_count_bytes       = filtered_matched_lines.map(lambda match: (match.group(1),(long(1),long(match.group(4)))) )
sum_host_count_bytes   = host_count_bytes.reduceByKey(add_tuples)

n_x_y_x2_y2_xy        = sum_host_count_bytes.map(lambda (h,(x,y)): (1 , x , y , x*x , y*y , x*y) )
(n,Sx,Sy,Sx2,Sy2,Sxy) = n_x_y_x2_y2_xy.reduce(add_tuples)


r  = (1.0*n*Sxy-1.0*Sx*Sy)/ (math.sqrt(n*Sx2-Sx*Sx))/(math.sqrt(n*Sy2-Sy*Sy))
r2 = r*r

answer = sc.parallelize([ ('n',n), ('Sx',Sx), ('Sx2',Sx2), ('Sy',Sy), ('Sy2',Sy2), ('Sxy',Sxy), ('r',r), ('r2',r2)]).coalesce(1)
outData = answer.map(lambda t: json.dumps(t))
outData.saveAsTextFile(output)