# Databricks notebook source
# MAGIC %run ./util

# COMMAND ----------

emp_details="/Volumes/workspace/default/resource_data/Employee-Q1.csv"
country_details="/Volumes/workspace/default/resource_data/Country-Q1.csv"
dept_details="/Volumes/workspace/default/resource_data/Department-Q1.csv"

read_emp_details=read_files(emp_details)
read_country_details=read_files(country_details)
read_dept_details=read_files(dept_details)

display(read_emp_details)
display(read_country_details)
display(read_dept_details)