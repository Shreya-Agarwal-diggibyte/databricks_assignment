# Databricks notebook source
def read_files(path: str):
   return spark.read.option("header", True).option("inferSchema", True).csv(path)

# COMMAND ----------

import re

def camel_to_snake(col_name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def convert_columns_to_snake_case(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, camel_to_snake(col))
    return df
