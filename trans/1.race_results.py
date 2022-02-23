# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
lap_times_df = spark.read.parquet(f"{processed_folder_path}/lap_times")
pit_stops_df = spark.read.parquet(f"{processed_folder_path}/pit_stops")
qualifying_df = spark.read.parquet(f"{processed_folder_path}/qualifying")
races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year in (2019, 2020)")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

races_df.filter(races_df.name.like('%Brazi%')).show()

# COMMAND ----------

pit_stops_df2 = pit_stops_df.groupBy("race_id","driver_id").max("stop").select("race_id", "driver_id", col("max(stop)").alias("pit_stops"))

# COMMAND ----------

df1 = results_df \
.join(races_df, results_df.race_id == races_df.race_id, "inner") \
.join(circuits_df, races_df.circuit_id == circuits_df.circuits_id, "inner") \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
.join(pit_stops_df2, [drivers_df.driver_id == pit_stops_df.driver_id, races_df.race_id == pit_stops_df.race_id], "inner") \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
.select(races_df.race_year \
       ,circuits_df.country \
       ,races_df.name.alias("race_name") \
       ,races_df.race_timestamp.alias("race_date") \
       ,circuits_df.location.alias("circuit_location") \
       ,drivers_df.name.alias("driver_name") \
       ,drivers_df.number.alias("driver_number") \
       ,drivers_df.nationality.alias("driver_nationality") \
       ,constructors_df.name.alias("team") \
       ,results_df.grid \
       ,results_df.fastest_lap_time \
       ,results_df.time.alias("race_time") \
       ,pit_stops_df2.pit_stops \
       ,results_df.points \
       ,results_df.position) \
       .withColumn("created_date", current_timestamp())
#.filter(races_df["race_id"] == 1029)

# COMMAND ----------

#df1.sort(col("position").asc_nulls_last()).show(truncate=False)

# COMMAND ----------

display(df1.filter("race_name = 'Brazilian Grand Prix'").orderBy(col("Points").desc()))

# COMMAND ----------

df1.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))
