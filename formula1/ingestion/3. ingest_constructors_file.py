# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read Json Data

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Create an schema using a DDL-FORMATTED string intead of use StructType/StructField

# COMMAND ----------

constructors_schema = ("constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING")

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Rename columns and add columns ingestion date

# COMMAND ----------

contructors_df_renamed = add_ingestion_date(constructors_df).withColumnRenamed("constructorId","constructor_id") \
                                                            .withColumnRenamed("constructorRef","constructor_ref") \
                                                            .withColumn("data_source", lit(v_data_source))
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to parquet file

# COMMAND ----------

contructors_df_renamed.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors/")

# COMMAND ----------

dbutils.notebook.exit("Success")
