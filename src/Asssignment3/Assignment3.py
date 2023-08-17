# Databricks notebook source
from pyspark.sql.functions import first,desc,col

# COMMAND ----------

data = [('James','Sales',3000),('Michael','Sales',4600),('Robert','Sales',4100),('Maria','Finance',3000),('Roman','Finance',3000),('Scott','Finance',3300),('Jen','Finance',3900),('Jeff','Marketing',3000),('Kumar','Marketing',2000)]
schema = ['employee_name','department','salary']
df = spark.createDataFrame(data,schema)
display(df)

df1 = df.groupby('department').agg(first('employee_name').alias('employee_name'),first('salary').alias('salary'))
display(df1)

df2 = df.orderBy(col('salary').desc()).limit(1).show()


