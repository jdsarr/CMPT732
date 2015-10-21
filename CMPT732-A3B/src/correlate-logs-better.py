'''
Created on Oct 21, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext 
import sys, re, math, json

inputs = sys.argv[1]
output = sys.argv[2]
linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

conf = SparkConf().setAppName('correlate logs better')
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)

matched_lines          = text.map(lambda line: linere.match(line))
filtered_matched_lines = matched_lines.filter(lambda match: match != None)
host_count_bytes       = filtered_matched_lines.map(lambda match: (match.group(1),(long(1),long(match.group(4)))) )
sum_host_count_bytes   = host_count_bytes.reduceByKey(add_tuples).cache()

n_x_y     = sum_host_count_bytes.map(lambda (h,(c,b)): (1 , c , b))
(n,Sx,Sy) = n_x_y.reduce(add_tuples)

xbar = 1.0*Sx/n
ybar = 1.0*Sy/n

diff_sums                    = sum_host_count_bytes.map(lambda (h,(x,y)): ((x-xbar)*(y-ybar) , math.pow((x-xbar),2), math.pow((y-ybar),2)) )
(Sxy_diff,Sx2_diff,Sy2_diff) = diff_sums.reduce(add_tuples) 

r  = 1.0*Sxy_diff/math.sqrt(Sx2_diff)/math.sqrt(Sy2_diff)
r2 = r*r

answer = sc.parallelize([ ('n',n), ('Sx',Sx), ('Sy',Sy), ('xbar', xbar), ('ybar',ybar), ('r',r), ('r2',r2)]).coalesce(1)
outData = answer.map(lambda t: json.dumps(t))
outData.saveAsTextFile(output)