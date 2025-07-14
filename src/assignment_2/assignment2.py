# Databricks notebook source
import requests
import json

all_data = []
page = 2

while True:
    url = f"https://reqres.in/api/users?page={page}"
    response = requests.get(url)
    
    if response.status_code != 200:
        break
    
    result = response.json()
    
    data = result.get("data", [])
    
    if not data:
        break  
    
    all_data.extend(data)
    page += 1

# COMMAND ----------

print(all_data[0])

# COMMAND ----------

import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row
from pyspark.sql.functions import current_date, lit

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# Convert to list of Rows
rows = [Row(**item) for item in all_data]

# Create DataFrame
df = spark.createDataFrame(rows, schema=schema)

# COMMAND ----------

from pyspark.sql.functions import lit, current_date

df = df.withColumn("site_address", lit("reqres.in")) \
       .withColumn("load_date", current_date())

display(df)

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save("/Volumes/workspace/assignment_2/assignment_2/site_info/person_info/")

df2 = spark.read.format("delta").load("/Volumes/workspace/assignment_2/assignment_2/site_info/person_info/")
display(df2)