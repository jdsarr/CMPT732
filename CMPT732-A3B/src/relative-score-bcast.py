'''
Created on Oct 21, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext
import sys, json

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('relative score with broadcast')
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)

def add_pairs((a,b),(c,d)):
    return (a+c,b+d)

commentdata       = text.map(lambda line: json.loads(line)).cache()
score_count       = commentdata.map(lambda c: ( c['subreddit'], (c['score'],1) ))
total_score_count = score_count.reduceByKey(add_pairs)
avg_score         = total_score_count.map(lambda (sub, (s,c)): (sub, 1.0*s/c)).filter(lambda (sub,avg): avg != 0)
broadcast_avg     = sc.broadcast(dict(avg_score.collect()))



def calculate_score(c):
    sub            = c['subreddit']
    if sub in broadcast_avg.value:      
        relative_score = 1.0*c['score']/broadcast_avg.value[sub]
        return (c['author'],relative_score)
    else:
        return None

author_score = commentdata.map(lambda c: calculate_score(c)).filter(lambda t: t != None)

outData = author_score.sortBy(lambda (a,s): s, False).map(lambda t: json.dumps(t))
outData.saveAsTextFile(output)