
import cassandra
import pyspark

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

cluster = Cluster(['localhost'])
session = cluster.connect()

spark = SparkSession.builder.appName('demo').master("local").getOrCreate()
df = spark.read.format("org.apache.spark.sql.cassandra").options(table="date", keyspace="cas").load()
print ("Table Row Count: ")
print (df.count())


