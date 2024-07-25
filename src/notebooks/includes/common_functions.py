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