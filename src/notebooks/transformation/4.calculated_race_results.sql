-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Calculated Race Results**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ensure f1_processed exists:

-- COMMAND ----------

-- %run "../ingestion/0.ingest_all_files"

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

SELECT races.race_year, constructors.name, drivers.name, results.position, results.points;
FROM results
JOIN drivers ON (results.driver_id = drivers.driver_id)
JOIN constructors ON (results.constructor_id = constructors.constructor_id)
JOIN races ON (results.race_id = races.race_id)
