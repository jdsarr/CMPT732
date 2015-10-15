'''
Created on Oct 14, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext
import sys, operator, re, string, unicodedata
 
inputs = sys.argv[1]
output = sys.argv[2]
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
 
conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
 
text = sc.textFile(inputs)
 
words_split      = text.flatMap(lambda line: wordsep.split(line.lower()))
words_filtered   = words_split.filter(lambda w: w != '')
words_normal     = words_filtered.map(lambda w:unicodedata.normalize('NFD', w)) 
words_mapped     = words_filtered.map(lambda w: (w,1)) 


wordcount = words_mapped.reduceByKey(operator.add).coalesce(1).cache()
 
wordcount_by_alpha = wordcount.sortBy(lambda (w,c): w)
wordcount_by_freq  = wordcount_by_alpha.sortBy(lambda (w,c): c, ascending=False)

outdata1 = wordcount_by_alpha.map(lambda (w,c): u"%s %i" % (w, c))
outdata1.saveAsTextFile(output+'/by-word')

outdata2 = wordcount_by_freq.map(lambda (w,c): u"%s %i" % (w, c))
outdata2.saveAsTextFile(output+'/by-freq')

