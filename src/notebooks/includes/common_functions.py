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


def overwrite_partition(db, table, column):
    if (spark._jsparkSession.catalog().tableExists(f"{db}.{table}")):
        try:
            select_final_df.write.mode("overwrite").insertInto(f"{db}.{table}")
        except Exception as e:
            return e
    else:
        try:
            select_final_df.write.mode("overwrite").partitionBy(f"{column}").format("parquet").saveAsTable(f"{db}.{table}")
        except Exception as e:
            return e
        

def set_partition(partition_by, dataframe):
    df_list = dataframe.schema.names
    df_list.remove(partition_by)
    df_list.append(partition_by)
    return df_list

