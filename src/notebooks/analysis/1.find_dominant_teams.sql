-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Find_dominant_teams**

-- COMMAND ----------

SHOW Databases;
USE f1_presentation;

-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

SELECT team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- Last 10 years:

SELECT team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- Between 2001 and 2011:

SELECT team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------


