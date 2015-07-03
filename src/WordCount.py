'''
Created on Jul 3, 2015

@author: saeed
'''
from pyspark import SparkConf,SparkContext
import os
from distutils.text_file import TextFile

sparkConf=SparkConf().setAppName("WordCounts").setMaster("local")
sc=SparkContext(conf=sparkConf)

textFile=sc.textFile(os.environ["SPARK_HOME"]+"/README.md")
wordCounts=textFile.flatMap(lambda line:line.split()).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
for wc in wordCounts.collect(): print(wc)