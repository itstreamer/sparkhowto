from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType
from pyspark.sql import SparkSession

import os
os.environ['SPARK_HOME'] = '/opt/spark/spark-3.1.2-bin-hadoop3.2'

spark = SparkSession.builder \
.master("local[*]") \
.config("spark.executor.memory", "25g")\
.appName("Word Count") \
.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7") \
.config("spark.memory.offHeap.enabled","true")\
.config("spark.memory.offHeap.size","16g") \
.getOrCreate()


"""df = spark.read.format("com.crealytics.spark.excel") \
.option("useHeader", "true") \
.option("inferSchema", "true") \
.option("dataAddress", "NameOfYourExcelSheet") \
.load("your_file"))"""
#df =spark.read.format("com.crealytics.spark.excel") .options(header='true', inferSchema='true').load("/home/bluepi/Downloads/opportunity_packageMappingv2.xlsx")
#df.show()
#df = spark.read.format("com.crealytics.spark.excel")\
df = spark.read.format("com.crealytics.spark.excel") \
.option("useHeader", "true") \
.option("inferSchema", "true") \
.options(header="true")\
.load("/home/bluepi/Downloads/dummy.xlsx")

print("Fdsfsdf")
df.show()