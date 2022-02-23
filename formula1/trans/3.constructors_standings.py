# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc

# COMMAND ----------

constructors_standings_df = race_results_df.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points") \
    ,count(when(col("position") == 1, True)).alias("wins")) \
.orderBy(desc("total_points"))

# COMMAND ----------

display(constructors_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructors_standings_df.withColumn("rank", rank().over(constructor_rank))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------


