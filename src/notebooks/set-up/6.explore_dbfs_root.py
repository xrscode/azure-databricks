# Databricks notebook source
# MAGIC %md
# MAGIC **Explore DBFS Root**
# MAGIC 1. List all the folders in DBFS root.
# MAGIC 2. Interact with the DBFS File Browser. 
# MAGIC 3. Upload file to DBFS root.

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

# Upload Files:

# First enable: DBFS File Browser.
# Then upload 'circuits.csv'.

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/tables/circuits.csv'))
