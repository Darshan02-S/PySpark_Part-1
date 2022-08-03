import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession
spark1 = spark.builder.appName('DataFrame').getOrCreate()
data = spark1.read.csv('Student_Data.csv', header = True, inferSchema = True)
data.show()
data.printSchema()
data.select(['Student ID','E-mail Address']).show()
#adding colums ---> withColumn(attribute1 = 'new column name',attribute2 = 'values')
data = data.withColumn('Converted Student ID',data['Student ID']*2)
data.select('Converted Student ID').show()
#droping columns ---> drop('column name')  
#even a single column can be dropped or else multiple columns can be dropped by giving list of columns
data = data.drop('Full Name')
data.show()
#renaming colums ---> withColumnRenamed(attribute1 = 'existing column name',attribute2 = 'new column name')
data = data.withColumnRenamed('Preferred Name','Actual Name')
data.select('Actual Name').show()