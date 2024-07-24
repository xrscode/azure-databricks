-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Learning Objectives**
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/f1dl9072024/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Data Manipulation Statements \
-- MAGIC https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(*) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;
