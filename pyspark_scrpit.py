import findspark
import nltk
from nltk.corpus import stopwords
from pyspark import SparkConf, SparkContext 
from textblob.utils import strip_punc
from operator import add, itemgetter

findspark.init()

nltk.download('stopwords')
stop_words = stopwords.words('english')

#create spark context and connect to spark master node
configuration = SparkConf().setAppName('RomeoAndJulietCounter').setMaster('spark://spark-master:7077')
sc = SparkContext.getOrCreate(conf=configuration)

#create rdd from hdfs text file
#delete all the punctuation and change input to list of words
tokenized = sc.textFile('hdfs://namenode:9000/test_spark/romeo-and-juliet.txt').map(lambda line: strip_punc(line, all=True).lower()).flatMap(lambda line: line.split())

#filter out all english stop words (I, so, can ...)
filtered = tokenized.filter(lambda word: word not in stop_words)

#change every word to touple (word,1) and add by word
word_counts = filtered.map(lambda word: (word, 1)).reduceByKey(add)

#filter out all words that occured less than 60 times
filtered_counts = word_counts.filter(lambda item: item[1] >= 60)

#run action in spark to apply all tranformation and sort by amount
sorted_items = sorted(filtered_counts.collect(),key=itemgetter(1), reverse=True)

max_len = max([len(word) for word, count in sorted_items])
for word, count in sorted_items:
    print(f'{word:>{max_len}}: {count}')