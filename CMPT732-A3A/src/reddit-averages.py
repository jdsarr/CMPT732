'''
Created on Oct 15, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext
import sys, operator, json

inputs = sys.argv[1]
output = sys.argv[2]

def add_pairs((a,b),(c,d)):
    return (a+c,b+d)


conf = SparkConf().setAppName('reddit average')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)

json_objs  = text.map(lambda line: json.loads(line))
subred     = json_objs.map(lambda json_obj: ( json_obj['subreddit'] , (json_obj['score'],1) ))

subred_count = subred.reduceByKey(add_pairs).coalesce(1)
subred_avg   = subred_count.map(lambda (subreddit , (score,count)): (subreddit , 1.0*score/count))

outdata      = subred_avg.map(lambda t: json.dumps(t))

outdata.saveAsTextFile(output)


