# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def hello():
    print('Hello world!')

def add_ingestion_date(input_df):
    """
    Args: dataframe.
    Outputs: dataframe with additional collumn; 'ingestion_date' set to current time.
    """
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

def set_partition(partition_by, dataframe):
    df_list = dataframe.schema.names
    df_list.remove(partition_by)
    df_list.append(partition_by)
    output_df = dataframe.select(df_list)
    return output_df

def overwrite_partition(df, db, table, part):
    """
    Overwrites the specified partition in a Spark SQL table. If the table does not exist,
    creates a new partitioned table.
    
    Parameters:
    db (str): The name of the database.
    table (str): The name of the table.
    column (str): The column to partition by.
    df (DataFrame): The Spark DataFrame to write.
    
    Returns:
    None
    """
    table_full_name = f"{db}.{table}"

    output_df = set_partition(part, df)
    
    try:
        print('Setting spark config...')
        partition_overwrite_mode_set = spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        partition_overwrite_mode_get = spark.conf.get("spark.sql.sources.partitionOverwriteMode")
    except Exception as e:
        return f"Error! Message: {e}"


    if partition_overwrite_mode_get == 'dynamic':
        if spark._jsparkSession.catalog().tableExists(table_full_name):
            # If table exist try...
            try:
                # Writing...
                output_df.write.mode("overwrite").insertInto(table_full_name)
                print(f"Successfully inserted into existing table: {table_full_name}")
            except Exception as e:
                print(f"Failed to insert into existing table: {table_full_name}. Error: {e}")
                return e
        else:
            try:
                # Creating...
                output_df.write.mode("overwrite").partitionBy(part).format("parquet").saveAsTable(table_full_name)
                print(f"Successfully created and inserted into new partitioned table: {table_full_name}")
            except Exception as e:
                print(f"Failed to create or insert into new partitioned table: {table_full_name}. Error: {e}")
                return e
    else:
        print('Could not set to dynamic config.')

    
def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
        .distinct() \
        .collect() 
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list
        



