# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuits.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 -Read the CSV file using the spark dataframe reader

# COMMAND ----------

#Ver as pastas montadas
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalakelabalex2/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

circuits_df = spark.read.schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Select required columns and Alias in a column

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat").alias("latitude"), col("lng"), col("alt"))

# COMMAND ----------

#display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the column as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuits_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - add new column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Add a new column with an default value, lit function

# COMMAND ----------

#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
#.withColumn("env", lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

#%fs
#ls /mnt/datalakelabalex2/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/datalakelabalex2/processed/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
