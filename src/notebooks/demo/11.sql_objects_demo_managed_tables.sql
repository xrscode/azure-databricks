-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Learning Objectives** 
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;
DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create managed table using SQL**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;
-- Notice that race_results_sql has been removed form hive metastore.
