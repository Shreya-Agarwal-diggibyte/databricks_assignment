# Databricks notebook source
# MAGIC %run /Workspace/Users/shreyaagarwal.g@diggibyte.com/databricks_assignment/assignment_1/source_to_bronze/util

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

department_schema = StructType([
    StructField("DepartmentID", StringType(), True),
    StructField("DepartmentName", StringType(), True)
])

country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("CountryName", StringType(), True)
])


# COMMAND ----------

# Read employee.csv
employee_df = spark.read \
    .schema(employee_schema) \
    .option("header", True) \
    .csv("/Volumes/workspace/default/resource_data/Employee-Q1.csv")

# Read department.csv
department_df = spark.read \
    .schema(department_schema) \
    .option("header", True) \
    .csv("/Volumes/workspace/default/resource_data/Department-Q1.csv")

# Read country.csv
country_df = spark.read \
    .schema(country_schema) \
    .option("header", True) \
    .csv("/Volumes/workspace/default/resource_data/Country-Q1.csv")


# COMMAND ----------

# MAGIC %run /Workspace/Users/shreyaagarwal.g@diggibyte.com/databricks_assignment/assignment_1/source_to_bronze/util
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_date


# COMMAND ----------

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])
employee_df = spark.read \
    .schema(employee_schema) \
    .option("header", True) \
    .csv("/Volumes/workspace/default/resource_data/Employee-Q1.csv")


# COMMAND ----------

# Apply snake_case conversion (with fixed function)
employee_snake_df = convert_columns_to_snake_case(employee_df)

# Check column names
print(employee_snake_df.columns)

# Add load_date
from pyspark.sql.functions import current_date
employee_final_df = employee_snake_df.withColumn("load_date", current_date())

# Preview schema
employee_final_df.printSchema()


# COMMAND ----------

employee_df = employee_df.withColumn("load_date", current_date())

spark.sql("CREATE DATABASE IF NOT EXISTS Employee_info")

employee_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/employee_info/dim_employee")