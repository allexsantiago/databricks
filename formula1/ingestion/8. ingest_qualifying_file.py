# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),                                       
                                      StructField("q3", StringType(), True)
                                      ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(f"{raw_folder_path}/qualifying", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename columns and add new columns

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("constructorId", "constructor_id") \
                                 .withColumn("data_source", lit(v_data_source))
                                 #.withColumn("ingestion_date", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
