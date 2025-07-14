# Databricks Assignment: ETL and API Ingestion
 
This project demonstrates a structured data engineering pipeline using **Databricks (Free Edition)** and **Delta Lake**, covering:
 
- Bronze → Silver → Gold layered ETL

- External API data ingestion

- Schema transformation, UDF usage, and Delta table writing
 
## Assignment 1: ETL Pipeline
 
### Step 1: Source to Bronze
 
- Read raw CSVs: `employee`, `department`, `country`

- Wrote them to:  

  ```

  /source_to_bronze/employee.csv  

  /source_to_bronze/department.csv  

  /source_to_bronze/country.csv  

  ```
 
### Step 2: Bronze to Silver
 
- Read CSVs with **custom schema**

- Converted column names from CamelCase → snake_case using UDF

- Added `load_date` column

- Wrote to Delta format:

  ```

  /silver/employee_info/dim_employee

  ```
 
### Step 3: Silver to Gold
 
- Read cleaned data from silver layer

- Performed analytics:

  - Salary by department (descending)

  - Employee count per department-country

  - Department-country mapping

  - Average age by department

- Added `at_load_date` column

- Wrote final fact table:

  ```

  /gold/employee/fact_employee

  ```
 
## Assignment 2: API Ingestion
 
### API Used:

`https://reqres.in/api/users?page=N`
 
### Pipeline Tasks:
 
- Fetched paginated user data until empty

- Dropped metadata fields:

  - `page`, `per_page`, `total`, `total_pages`, and `support`

- Flattened nested `data` structure

- Derived `site_address` from email (`reqres.in`)

- Added `load_date` for audit

- Wrote to Delta table:

  ```

  /Volumes/workspace/default/databricks_assignment/site_info/person_info

  ```
 
### Final Schema:
 
| Column        | Type    |

|---------------|---------|

| id            | Integer |

| email         | String  |

| first_name    | String  |

| last_name     | String  |

| emp_name    | String  |

| avatar        | String  |

| site_address  | String  |

| load_date     | Date    |
 
---
 
## Storage Layout (Free Edition using Volumes)
 
```text

/Volumes/workspace/default/databricks_assignment/

├── source_to_bronze/

├── silver/employee_info/dim_employee

├── gold/employee/fact_employee

├── site_info/person_info
 
