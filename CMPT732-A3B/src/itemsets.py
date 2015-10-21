'''
Created on Oct 21, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext
from pyspark.mllib.fpm import FPGrowth
import sys, json

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('itemsets')
sc = SparkContext(conf=conf)

text = sc.textFile(inputs).map(lambda line: line.split())

model = FPGrowth.train(text,0.00227)

freq_itemsets = model.freqItemsets().map(lambda f: (sorted(f[0]),f[1]))
sorted_itemsets = freq_itemsets.sortBy(lambda (a,b): a).sortBy(lambda (a,b): b, False)

top_sorted_itemsets = sorted_itemsets.take(10000)

outData = sc.parallelize(top_sorted_itemsets).map(lambda t: json.dumps(t)).coalesce(1)
outData.saveAsTextFile(output)

