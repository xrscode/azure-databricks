# Databricks notebook source
# MAGIC %md
# MAGIC **Preperation for Incremental Loading**

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Storage

# COMMAND ----------

# MAGIC %run "../set-up/8.mount_adls_containers_for_project"

# COMMAND ----------

# MAGIC %md
# MAGIC Drop all tables from f1_processed:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC Create table f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/mnt/f1dl9072024/processed";
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Drop all tables from f1_presentation:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION "/mnt/f1dl9072024/presentation";