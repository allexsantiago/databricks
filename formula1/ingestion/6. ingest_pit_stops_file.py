# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the JSON file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                        ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).json(f"{raw_folder_path}/pit_stops.json", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 - Rename columns and add new columns
# MAGIC ###### 1. Rename driverId and raceId
# MAGIC ###### 2. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("data_source", lit(v_data_source))
                                 #.withColumn("ingestion_date", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
