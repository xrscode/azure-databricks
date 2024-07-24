-- Databricks notebook source
-- MAGIC %md
-- MAGIC **SQL Objects** /
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Database** \
-- MAGIC https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-database.html

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;


-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;
-- Can provide more information:
-- DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SHOW TABLES;
-- SHOW TABLES IN demo;

-- Use this command to switch to 'demo' database.
USE demo;

SELECT CURRENT_DATABASE()
