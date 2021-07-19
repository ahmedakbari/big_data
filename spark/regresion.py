import os

os.environ["PYSPARK_PYTHON"]="/home/ahmad/anaconda3/bin/python"
import subprocess
# subprocess.run(["pyspark","--packages anguenot/pyspark-cassandra:2.4.0"])

from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# conf = SparkConf()
#
# conf.set("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")  # spark-cassandra-connect jar
#
#
# sc = SparkContext( conf=conf)
#
# ss = SparkSession(sc)


spark = SparkSession.builder.appName('demo').master("local").getOrCreate()
# spark.conf.set("date", "cas")
a=spark.read.options(table="date", keyspace="cas")
a=0
#
# data = spark.read.format("org.apache.spark.sql.cassandra").options(table="t2", keyspace="test").load()
# print(data.count())
# wineDF = spark.read.format("org.apache.spark.sql.cassandra").options(table="date", keyspace="cas").load()
#
# print ("Table Wine Row Count: ")
# print (wineDF.count())
#
#
#
#
# sc = SparkContext("local", "Simple App")
# # Load and parse the data
# data = sc.textFile("lpsa.data")
# parsedData = data.map(lambda line: array([float(x) for x in line.replace(',', ' ').split(' ')]))
#
# # Build the model
# clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")
#
# model = LinearRegressionWithSGD.train(parsedData)
#
# # Evaluate the model on training data
# valuesAndPreds = parsedData.map(lambda point: (point.item(0),
#         model.predict(point.take(range(1, point.size)))))
# # MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y)/valuesAndPreds.count()
# # print("Mean Squared Error = " + str(MSE))
