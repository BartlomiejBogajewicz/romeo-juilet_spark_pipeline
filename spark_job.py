import findspark
from os.path import abspath
import nltk
from nltk.corpus import stopwords
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from operator import add, itemgetter
from textblob.utils import strip_punc

findspark.init()

warehouse_location = abspath('spark-warehouse')

nltk.download('stopwords')
stop_words = stopwords.words('english')


configuration = SparkConf().setAppName('RomeoAndJulietCounter').setMaster('spark://spark-master:7077')
sc = SparkContext.getOrCreate(conf=configuration)

spark_sql = SparkSession \
    .builder \
    .appName("RomeoHive") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

tokenized = sc.textFile('hdfs://namenode:9000/test_spark/romeo-and-juliet.txt').map(lambda line: strip_punc(line, all=True).lower()).flatMap(lambda line: line.split())

filtered = tokenized.filter(lambda word: word not in stop_words)

word_counts = filtered.map(lambda word: (word, 1)).reduceByKey(add)

filtered_counts = word_counts.filter(lambda item: item[1] >= 60)


sorted_items = sorted(filtered_counts.collect(),key=itemgetter(1), reverse=True)

max_len = max([len(word) for word, count in sorted_items])
for word, count in sorted_items:
    print(f'{word:>{max_len}}: {count}')

df = filtered_counts.toDF()

df.write.mode("append").insertInto("ROMEO_COUNT")