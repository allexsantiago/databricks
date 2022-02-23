# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json File

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create an schema for the data

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False)
                                   ,StructField("raceId", IntegerType(), True)
                                   ,StructField("driverId", IntegerType(), True)
                                   ,StructField("constructorId", IntegerType(), True)
                                   ,StructField("number", IntegerType(), True)
                                   ,StructField("grid", IntegerType(), True)
                                   ,StructField("position", IntegerType(), True)
                                   ,StructField("positionText", StringType(), True)
                                   ,StructField("positionOrder", IntegerType(), True)
                                   ,StructField("points", FloatType(), True)
                                   ,StructField("laps", IntegerType(), True)
                                   ,StructField("time", StringType(), True)
                                   ,StructField("milliseconds", IntegerType(), True)
                                   ,StructField("fastestLap", IntegerType(), True)
                                   ,StructField("rank", IntegerType(), True)
                                   ,StructField("fastestLapTime", StringType(), True)
                                   ,StructField("fastestLapSpeed", FloatType(), True)
                                   ,StructField("statusId", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import Data and add the schema

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename and Add columns

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("constructorId", "constructor_id") \
                               .withColumnRenamed("positionText", "position_text") \
                               .withColumnRenamed("positionOrder", "position_order") \
                               .withColumnRenamed("fastesLap", "fastest_lap") \
                               .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                               .withColumnRenamed("FastestLapSpeed", "fastest_lap_speed") \
                               .withColumn("data_source", lit(v_data_source))
#.withColumn("Ingestion_date", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# COMMAND ----------

results_renamed_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop unwanted columns

# COMMAND ----------

results_renamed_df = results_renamed_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data on storage

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

dbutils.notebook.exit("Success")
