# Databricks notebook source
# MAGIC %run "../source_to_bronze/util"

# COMMAND ----------

employee_df = spark.read.format("delta").load("/Volumes/workspace/employee_info/dim_employee")
display(employee_df)

# COMMAND ----------

from pyspark.sql.functions import sum

dept_salary_df = employee_df.groupBy("department") \
    .agg(sum("salary").alias("total_salary")) \
    .orderBy("total_salary", ascending=False)

display(dept_salary_df)


# COMMAND ----------

dept_country_count_df = employee_df.groupBy("department", "country") \
    .count() \
    .withColumnRenamed("count", "employee_count")

display(dept_country_count_df)

# COMMAND ----------

department_df = read_files("//Volumes/workspace/default/resource_data/Department-Q1.csv")
country_df = read_files("/Volumes/workspace/default/resource_data/Country-Q1.csv")

department_df = department_df \
    .withColumnRenamed("DepartmentID", "department_id") \
    .withColumnRenamed("DepartmentName", "department_name")


country_df = country_df \
    .withColumnRenamed("CountryCode", "country_id") \
    .withColumnRenamed("CountryName", "country_name")

emp_dept_country_df = employee_df.select("department_id", "country_id").distinct() \
    .join(department_df, on="department_id", how="inner") \
    .join(country_df, on="country_id", how="inner") \
    .select("department_name", "country_name")

# COMMAND ----------

from pyspark.sql.functions import avg

avg_age_df = employee_df.groupBy("department") \
    .agg(avg("age").alias("average_age"))

display(avg_age_df)

# COMMAND ----------


from pyspark.sql.functions import current_date

dept_salary_df = dept_salary_df.withColumn("at_load_date", current_date())
dept_country_count_df = dept_country_count_df.withColumn("at_load_date", current_date())
employee_df =employee_df.withColumn("at_load_date", current_date())
avg_age_df = avg_age_df.withColumn("at_load_date", current_date())

display(dept_salary_df)
display(dept_country_count_df)
display(employee_df)
display(avg_age_df)


# COMMAND ----------

final_df = dept_salary_df.unionByName(dept_country_count_df, allowMissingColumns=True) \
    .unionByName(emp_dept_country_df, allowMissingColumns=True) \
    .unionByName(avg_age_df, allowMissingColumns=True)

final_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("replaceWhere", "at_load_date = current_date()") \
    .save("/Volumes/workspace/default/gold_employee/fact_employee/")

display(final_df)