from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
#import os
#os.environ['SPARK_HOME'] = '/opt/spark/spark-3.1.2-bin-hadoop3.2'

spark = SparkSession.builder.master("local[*]").appName("castColumnToDifferentDataType").getOrCreate()

df=spark.read.format('csv').option('header','True').option('escape','"').load("/home/bluepi/Downloads/dummy.csv")
df.show()
fifa_df = spark.read.csv("sparkhowto/datasets/archive/dummy.csv", inferSchema = True, header = True, escape='"', multiLine=True)
fifa_df.select("id","pricing_parameters").show(5, False)