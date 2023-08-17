# Databricks notebook source
from pyspark.sql.functions import lit,col,struct
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

def quest1(df):
    df = df.select('name.firstname','name.lastname','salary','dob')
    df2 = df.withColumn('country',lit('India'))\
        .withColumn('department',lit('DE'))\
            .withColumn('age',lit('22'))\
                .withColumn('new_salary',col('salary')*2)\
                    .withColumn('salary',col('salary').cast('Integer')).drop('department','age')
    display(df)
    df3 = df.select('salary','dob').distinct()
    display(df3)

    # new_struct = struct(col("name.firstname").alias("firstposition"),
    #             col("name.middlename").alias("middleposition"),
    #             col("name.lastname").alias("lastposition"))
    # df = df.withColumn("name",new_struct)
    # display(df)

    

# COMMAND ----------

data = [({"firstname":"James;","middlename":"","lastname":"Smith"},'03011998','M',3000),({"firstname":"Michael;","middlename":"Rose","lastname":""},'10111998','M',20000),({"firstname":"Robert;","middlename":"","lastname":"Williams"},'02012000','M',3000),({"firstname":"Maria;","middlename":"Anne","lastname":"Jones"},'03011998','F',11000),({"firstname":"Jen;","middlename":"Mary","lastname":"Brown"},'04101998','F',10000)]
schema = StructType([
    StructField("name",StructType([
        StructField("firstname",StringType()),
        StructField("middlename",StringType()),
        StructField("lastname",StringType())
    ])),
    StructField("dob",StringType()),
    StructField("gender",StringType()),
    StructField("salary",IntegerType())
])
df = spark.createDataFrame(data,schema)
quest1(df)
