# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Circuits.csv** \
# MAGIC Widget: "Ergast API"

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_circuits = f"{mount_dict['processed']}/circuits"        

# COMMAND ----------

# dbutils.widgets.help()

# COMMAND ----------

# Create Widget
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Ensure 'Mount' function is called**

# COMMAND ----------

# raw_folder_path
# add_ingestion_date
display(raw_folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Display Mount Points**
# MAGIC Locate: '/mnt/f1dl9072024/raw \ 
# MAGIC Raw is where we can access the raw data.

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC **3. List files in: /mnt/f1dl9072024/raw** \ 
# MAGIC This command shows all of the files we have access to.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dl9072024/raw

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Create dataframe**

# COMMAND ----------

circuits_df_one = spark.read.csv(f"{mount_dict['raw']}/circuits.csv", header='true')

# Displays the type of 'circuits_df'.  pyspark.sql.dataframe.DataFrame
display(type(circuits_df_one))

# # Shows the tables:
display(circuits_df_one.show())


# COMMAND ----------

# MAGIC %md
# MAGIC **5. Print Schema**
# MAGIC Provides information about the schema.

# COMMAND ----------

circuits_df_one.printSchema()



# COMMAND ----------

# Perform a summary of basic descriptive statistics:
circuits_df_one.describe() \
# .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **6. inferSchema**
# MAGIC inferSchema can be used to automatically detect the data types of each column in the dataset.

# COMMAND ----------

# Save variable with inferSchema set to true:
circuits_df = spark.read.csv(f"{mount_dict['raw']}/circuits.csv", header='true', inferSchema='true')
circuits_df.printSchema()
circuits_df.describe() \
# .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **7. import from Pyspark**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC **8. Specify Schema**

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC **9. Re-define 'circuits_df'**

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{mount_dict['raw']}/circuits.csv")
display(circuits_df)  

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **10. Select columns** \
# MAGIC There are some columns we do not need.  We need to drop them. \
# MAGIC One method is to use; DataFrame.select

# COMMAND ----------

# # Method 1:
# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# display(circuits_selected_df)


# COMMAND ----------

# # Method 2:
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
# display(circuits_selected_df)

# COMMAND ----------

# # Method 3:
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
# display(circuits_selected_df)

# COMMAND ----------

# Method 4: requires 'col' to be imported
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC **11. Rename Columns**

# COMMAND ----------


from pyspark.sql.functions import lit
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **13. Add Ingestion Date Data**

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **14. Write to DataLake as Parquet**

# COMMAND ----------


final_df.write.mode("overwrite").parquet(processed_circuits)

# """
# This command writes the DataFrame to the specified directory in parquet format. 
# 1. Location: data is written to; /mnt/f1dl9072024/processed/circuits
# 2. Format: saved as parquet. 
# 3. Table Registration: The data is NOT registered as a table in the Hive metastore. 
# 4.  This will only write data to directory - it will not make it SQL accessbile.
# """

# COMMAND ----------

# MAGIC %md
# MAGIC **FOR CREATE PROCESSED DATABASE!!!!**

# COMMAND ----------

# Use dbutils to delete:
path = processed_circuits
end_path = 'circuits'
print(path)

try:
    final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.{end_path}")
    print(f"{end_path.capitalize()} table successfully created.")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        # If folder exists at path: /mnt/f1dl9072024/processed/circuits
        if dbutils.fs.ls(path):
            # Delete folder:
            dbutils.fs.rm(path, True)
        # Re-write Table:
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.{end_path}")
        print(f"{end_path.capitalize()} table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")

# """
# This command writes the DataFrame to a location managed by Hive and registers it ias a table in the Hive metastore. 
# 1. 
# 2. Format: data saved as parquet. 
# 3. Table Registration: The data is registered as a table; ('f1_processed.circuits') in the Hive metastore making it accessible via SQL queries. 
# 4. Useful when you want ot save hte data and make it queryable through spark SQL, allowing for easier data manipulation and access through SQL commands.

# NOTE: DIRECTORY CONFLICT: when you write the DataFrame directly to a directory first, that directory might not align with the managed table location expected by the Hive metastore.  The 'saveAsTable' method manages its own directory structure and locations for tables. 

# LOCATION ALREADY EXISTS ERROR: The Hive metastore expects a clean directory for its managed tables.  If the directory already contains data, it might throw an error when trying to save the table.
# """

# COMMAND ----------

# %fs
# ls /mnt/formula1dl/processed/circuits

# COMMAND ----------

# # Read parquet to verify:
display(spark.read.parquet(processed_circuits))

# COMMAND ----------

dbutils.fs.ls(processed_circuits)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
