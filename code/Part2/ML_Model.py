from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier
import sys

cache = sys.argv[1]

import numpy as np

import time

spark = SparkSession.builder.appName("Model-Creation").getOrCreate()


dataset = spark.read.parquet("hdfs://master:9000/project/ml_dataset_200lex.parquet")


stringIndexer = StringIndexer(inputCol="string_label", outputCol="label")
stringIndexer.setHandleInvalid("skip")
stringIndexerModel = stringIndexer.fit(dataset)
dataset = stringIndexerModel.transform(dataset)

fraction={0.0: 0.7, 1.0: 0.7, 2.0: 0.7,3.0: 0.7, 4.0: 0.7, 5.0: 0.7,6.0: 0.7, 7.0: 0.7, 8.0: 0.7,9.0: 0.7, 10.0: 0.7, 11.0: 0.7,12.0: 0.7, 13.0: 0.7, 14.0: 0.7,15.0: 0.7, 16.0: 0.7, 17.0: 0.7}

if (cache == "Y"):
	train = dataset.sampleBy("label", fractions=fraction, seed=10).cache()
elif (cache=="N"):
	train = dataset.sampleBy("label", fractions=fraction, seed=10)
else:
	raise Exception ("This setting is not available.")


test = dataset.subtract(train)

print("Train Rows:", train.count())
print("Test Rows:", test.count())

train.groupBy("label").count().orderBy("label").show()
test.groupBy("label").count().orderBy("label").show()


layers = [200, 100, 50, 18]

# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, labelCol="label", featuresCol="features")

start=time.time()
# train the model
model = trainer.fit(train)

# compute accuracy on the test set
result = model.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))


print("MLP Classifier Train-Inference Time: ", time.time()-start)
