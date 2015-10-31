'''
Created on Oct 30, 2015

@author: Juan Sarria
'''
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys


def main(inputs, output, sc, sqlContext):
    assert sc.version >= '1.5.1'

    schema = StructType([
    StructField('subreddit', StringType(), False),
    StructField('score',IntegerType(),False),
    ])
    
    comments = sqlContext.read.json(inputs,schema)
    averages = comments.select('subreddit', 'score').groupby('subreddit').avg().coalesce(1)
    averages.write.save(output, format='json',mode='overwrite')
    
    
if __name__ == "__main__":
    inputs = sys.argv[1]
    output = sys.argv[2]
    conf = SparkConf().setAppName('reddit average sql')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    main(inputs,output,sc,sqlContext)
    
    
    
    
    