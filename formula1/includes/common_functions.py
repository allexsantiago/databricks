# Databricks notebook source
from pyspark.sql.functions import from_utc_timestamp, current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))
    return output_df
