from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import numpy as np

import math
import time

import re


import nltk
nltk.download('stopwords')
stop_words = nltk.corpus.stopwords.words('english')



#####   HYPERPARAMETERS   #####
lexicon_size = 200 
stop_words.append('xxxx')
stop_words.append('xx')
####-----------------------####


spark = SparkSession.builder.appName("complaints").getOrCreate()
sc = spark.sparkContext



def clean_text(d_text):
	ret_text = re.sub(r'[^A-Za-z]+',' ', d_text)
	ret_text= ret_text.lower()
	ret_text = ' '.join([word for word in ret_text.split() if word not in stop_words])
	return (ret_text)

start=time.time()


complaints = sc.textFile("hdfs://master:9000/project/customer_complaints.csv"). \
					filter(lambda x: x.startswith("201") and len(x.split(',')) == 3). \
					filter(lambda x: x.split(',')[2]). \
					map(lambda line: (line.split(',')[1],line.split(',')[2]))




										   #(parapono,keimeno)
complaints = complaints.map(lambda line: (line[0],clean_text(line[1])))


most_common_words = complaints.flatMap(lambda x : x[1].split(" ")). \
                					map(lambda word : (word, 1))

most_common_words1 = most_common_words.reduceByKey(lambda accum, data: (accum + data)). \
										sortBy(lambda x : x[1], ascending = False).map(lambda x : x[0]).take(lexicon_size)
                
broad_com_words = sc.broadcast(most_common_words1)


										

# 1.(parapono,(lekseis))
# 2.(parapono,(lekseis mesa sto leksiko))
# 3.kratame osa keimena exoun toulxaxiston mia leksi sto leksiko

# 4. Vazw sto value to plhthos twn leksewn (parapono,lekseis,plhthos_leksewn)

# 5.((string_label,(lekseis),plhthos_leksewn),sentence_index)
# 6. dhmiourgw ((leksi,string_label,sentence_index,plhthos_leksewn),1)
# 7. ((leksi,string_label,sentence_index,plhthos_leksewn),#emfaniseis mesa sth protash) 

# 8. (leksi,(strin_label,sentence_index,td,1))




complaints = complaints.map(lambda x : (x[0], x[1].split(" "))). \
						map(lambda x : (x[0], [y for y in x[1] if y in broad_com_words.value])). \
						filter(lambda x : len(x[1]) != 0)
Keimena = complaints.count()

complaints=complaints.map(lambda x: (x[0],x[1],len(x[1]))). \
						zipWithIndex(). \
						flatMap(lambda x : [((y, x[0][0], x[1],x[0][2]), 1) for y in x[0][1]]). \
						reduceByKey(lambda x, y : x + y)
						
complaints=complaints.map(lambda x:(x[0][0],(x[0][1],x[0][2],x[1]/x[0][3],1)))

idf=complaints.map(lambda x:(x[0],x[1][3])). \
				reduceByKey(lambda x,y:x+y). \
				map(lambda x: (x[0],math.log10(Keimena/x[1])))

# (leksi,((strin_label,sentence_index,tf,1),idf))
res=complaints.join(idf)
# (leksi,string_label,sentence_index),td*idf))

res=res.map(lambda x:((x[0],x[1][0][0],x[1][0][1]),x[1][0][2]*x[1][1]))

# 1.((word, string_label, sentence_index), (tfidf, word_index_in_lexicon))
# 2.((sentence_index, string_label), [(word_index_in_lexicon, tfidf)])
# 3.((sentence_index, string_label), listof((word_index_in_lexicon, tfidf)))
# 4.(string_label, sorted_on_word_index_in_lexicon_listof((word_index_in_lexicon, tfidf)))
# 5.(string_label, SparseVector(lexicon_size, list_of(word_index_in_lexicon), list_of(tfidf)))

res=res.map(lambda x : (x[0], (x[1], broad_com_words.value.index(x[0][0])))). \
		map(lambda x : ((x[0][2], x[0][1]), [(x[1][1], x[1][0])])). \
		reduceByKey(lambda x, y : x + y). \
		map(lambda x : (x[0][1], sorted(x[1], key = lambda y : y[0]))). \
		map(lambda x : (x[0], (lexicon_size, [y[0] for y in x[1]], [y[1] for y in x[1]])))

print(res.take(5))

res=res.map(lambda x : (x[0], SparseVector(x[1][0],x[1][1],x[1][2])))


res = res.toDF(["string_label", "features"])
res.write.parquet("hdfs://master:9000/project/ml_dataset_200lex.parquet")

print("Execution Time: ", time.time()-start)