# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest JSON Drivers File

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 01 - Read the Json file

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True)
                                ,StructField("surname", StringType(), True)
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 02 - Rename columns and add new columns
# MAGIC ###### 1. driverId renamed to driver_id
# MAGIC ###### 2. driverRef renamed to driver_ref
# MAGIC ###### 3. ingestion date added
# MAGIC ###### 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, from_utc_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat("name.forename",lit(" "),"name.surname")) \
                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

drivers_with_columns_df = add_ingestion_date(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC ###### 1. name.forename
# MAGIC ###### 2. name.surename
# MAGIC ###### 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers/")

# COMMAND ----------

dbutils.notebook.exit("Success")
