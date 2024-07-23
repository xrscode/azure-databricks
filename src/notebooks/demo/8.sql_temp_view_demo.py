# Databricks notebook source
# MAGIC %md
# MAGIC **Access dataframes using SQL**
# MAGIC
# MAGIC 1. Create temporary views on dataframes.
# MAGIC 2. Access the view from SQL cell. 
# MAGIC 3. Access the view from Python cell. 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# race_results_df.createTempView("v_race_results")

race_results_df.createOrReplaceTempView("v_race_results")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results 
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")
display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Global Temporary View**
# MAGIC 1. Create global temporary views on dataframes. 
# MAGIC 2. Access the view from SQL cell. 
# MAGIC 3. Access the view from Python cell. 
# MAGIC 4. Access the view from another notebook. 
# MAGIC
# MAGIC

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results;").show()
