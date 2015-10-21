'''
Created on Oct 21, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext
import sys, json

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('relative score')
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)

def add_pairs((a,b),(c,d)):
    return (a+c,b+d)

commentdata       = text.map(lambda line: json.loads(line)).cache()
score_count       = commentdata.map(lambda c: ( c['subreddit'], (c['score'],1) ))
total_score_count = score_count.reduceByKey(add_pairs)
avg_score         = total_score_count.map(lambda (sub, (s,c)): (sub, 1.0*s/c)).filter(lambda (sub,avg): avg != 0)

commentbysub      = commentdata.map(lambda c: ( c['subreddit'], c))

join_comment_score = commentbysub.join(avg_score)

author_score = join_comment_score.map(lambda (sub, (c, avg)): ( c['author'], 1.0*c['score']/avg ) )

outData = author_score.sortBy(lambda (a,s): s, False).map(lambda t: json.dumps(t))
outData.saveAsTextFile(output)