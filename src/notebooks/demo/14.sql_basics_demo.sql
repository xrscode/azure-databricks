-- Databricks notebook source
-- MAGIC %md
-- MAGIC **SQL BASICS**

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed;
SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers AS date_of_birth
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY dob;

-- COMMAND ----------

SELECT name, dob AS date_of_birth, nationality FROM drivers
ORDER BY nationality ASC, dob DESC;

-- COMMAND ----------

SELECT name, nationality, dob
FROM drivers
WHERE (nationality = 'British' AND dob >= '1990-01-01')
OR nationality = 'Indian'
ORDER BY dob DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **SQL Functions**

-- COMMAND ----------

USE f1_processed;
SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CONCAT**

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref 
FROM drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **SPLIT**

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[1] surname
FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CURRENT_TIMESTAMP**

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy') AS date_of_birth
FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **JOINS**

-- COMMAND ----------

-- MAGIC %run "../transformation/2.driver_standings"

-- COMMAND ----------

USE f1_presentation;
SELECT current_database()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- TABLE 1:
CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018;

-- TABLE 2:
CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018

-- COMMAND ----------

SELECT * FROM v_driver_Standings_2020

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **LEFT JOIN**

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RIGHT JOIN**

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **FULL OUTER JOIN**

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **SEMI JOIN**

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **ANTI JOIN**

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CROSS JOIN**

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2020 d_2020
