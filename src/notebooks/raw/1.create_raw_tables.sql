-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Circuits Table**
-- MAGIC CSV

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/f1dl9072024/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CREATE RACES TABLE**
-- MAGIC CSV

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
race_id INT,
year INT,
round INT,
circuit_id STRING,
name STRING,
date STRING,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/f1dl9072024/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create constructors Table** \
-- MAGIC JSON
-- MAGIC Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1.raw_constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/f1dl9072024/raw/constructors.json")

-- COMMAND ----------

-- SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CREATE drivers TABLE** \
-- MAGIC JSON 
-- MAGIC Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1.raw_drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/f1dl9072024/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create results table** \
-- MAGIC JSON
-- MAGIC Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1.raw_results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING 
)
USING JSON
OPTIONS(path "/mnt/f1dl9072024/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create pit stops table** \
-- MAGIC JSON multi-line, simple structure.

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS(path "/mnt/f1dl9072024/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create tables for list of files**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Lap Times Table**
-- MAGIC 1. CSV file
-- MAGIC 2. Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/f1dl9072024/raw/lap_times")


-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Qualifying Table**
-- MAGIC 1. JSON file
-- MAGIC 2. MultiLine JSON
-- MAGIC 3. Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "/mnt/f1dl9072024/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Table - Parquet Source**

-- COMMAND ----------


