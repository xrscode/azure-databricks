# Databricks notebook source
# MAGIC %md
# MAGIC **Filter Transformations**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Data**

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
# display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add Filter**

# COMMAND ----------

# 1. SQL Way:
# races_filterd_df = races_df.filter("race_year = 2019 and round <= 5")

# 2. Pythonic Way:
races_filterd_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

display(races_filterd_df)

# COMMAND ----------


