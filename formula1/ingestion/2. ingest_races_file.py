# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest races.csv File

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Show mounted items

# COMMAND ----------

#display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ######Show files

# COMMAND ----------

#%fs
#ls /mnt/datalakelabalex2/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Import necessary Items

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql.functions import concat, to_timestamp, lit, col

# COMMAND ----------

# MAGIC %md
# MAGIC ######Create an schema to receive data

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", StringType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True),
                                 ])

# COMMAND ----------

# MAGIC %md
# MAGIC ######Import data

# COMMAND ----------

racesdf = spark.read.schema(races_schema).csv(f"{raw_folder_path}/races.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ######Add columns ingestion_date and race_timestamp

# COMMAND ----------

races_with_timestamp_df = racesdf.withColumn("race_timestamp", to_timestamp(concat("date", lit(" "), "time"), "yyyy-MM-dd HH:mm:ss")) \
                                 .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Select only the columns required & rename as required

# COMMAND ----------

racesdf_final = races_with_timestamp_df.select(col("raceId").alias("race_id")
                                              ,col("year").alias("race_year")
                                              ,col("round")
                                              ,col("circuitId").alias("circuit_id")
                                              ,col("name")
                                              ,col("race_timestamp")
                                              ,col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Save Data

# COMMAND ----------

# MAGIC %md
# MAGIC ######Write the output to processed container in parquet file format

# COMMAND ----------

#racesdf_final.write.mode("overwrite").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

racesdf_final.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
