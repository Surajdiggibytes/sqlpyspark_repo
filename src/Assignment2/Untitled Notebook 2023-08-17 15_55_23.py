# Databricks notebook source
from pyspark.sql.functions import expr

# COMMAND ----------

data = [('Banana',1000,'USA'),('Carrots',1500,'INDIA'),('Beans',1600,'Swedan'),('Orange',2000,'UK'),('Orange',2000,'UAE'),('Banana',400,'China'),('Carrots',1200,'China')]
schema = ('Product','Amount','Country')
df =spark.createDataFrame(data,schema)
display(df)


pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
display(pivotDF)

unpivotexp = "stack(5,'China',China,'INDIA',INDIA,'Swedan',Swedan,'UAE',UAE,'UK',UK) as (Country,Total)"

unPivotDF = pivotDF.select("Product", expr(unpivotexp)) \
    .where("Total is not null")
unPivotDF.show(truncate=False)

