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


def overwrite_partition(db, table, column, df):
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
    
    if spark._jsparkSession.catalog().tableExists(table_full_name):
        try:
            df.write.mode("overwrite").insertInto(table_full_name)
            print(f"Successfully inserted into existing table: {table_full_name}")
        except Exception as e:
            print(f"Failed to insert into existing table: {table_full_name}. Error: {e}")
            return e
    else:
        try:
            df.write.mode("overwrite").partitionBy(column).format("parquet").saveAsTable(table_full_name)
            print(f"Successfully created and inserted into new partitioned table: {table_full_name}")
        except Exception as e:
            print(f"Failed to create or insert into new partitioned table: {table_full_name}. Error: {e}")
            return e
        

def set_partition(partition_by, dataframe):
    df_list = dataframe.schema.names
    df_list.remove(partition_by)
    df_list.append(partition_by)
    return df_list

