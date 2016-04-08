#Name: Srinivas Avireddy 
# Email: saviredd@eng.ucsd.edu
# PID: A53101356
from pyspark import SparkContext
sc = SparkContext()
import re
import string
textRDD = sc.newAPIHadoopFile('/data/Moby-Dick.txt',
                              'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text',
                               conf={'textinputformat.record.delimiter': "\r\n\r\n"}) \
            .map(lambda x: x[1])

sentences=textRDD.flatMap(lambda x: x.split(". "))

def printOutput(n,freq_ngramRDD):
    top=freq_ngramRDD.take(5)
    print '\n============ %d most frequent %d-grams'%(5,n)
    print '\nindex\tcount\tngram'
    for i in range(5):
        print '%d.\t%d: \t"%s"'%(i+1,top[i][0]," ".join(top[i][1]))

def removePunctuation(line):
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    out = regex.sub(' ', line)
    return out

def ngram(line,n):
    line = removePunctuation(line.lower())
    array = line.split()
    ngrams = []
    for i in xrange(0,len(array)-n+1):
        ngrams.append(tuple(array[i:i+n]))
    return ngrams

for n in range(1,6):
    # Put your logic for generating the sorted n-gram RDD here and store it in freq_ngramRDD variable
    RDD = sentences.flatMap(lambda sentence: ngram(sentence,n))\
                    .map(lambda ngram: (ngram,1))\
                    .reduceByKey(lambda x,y:x+y)\
                    .map(lambda (c,v):(v,c))\
                    .sortByKey(False)
    #print RDD.take(5)
    printOutput(n,RDD)

