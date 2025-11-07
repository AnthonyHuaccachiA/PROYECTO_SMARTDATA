# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

catalogo = "catalog_dev"
esquema_source = "silver"
esquema_sink = "golden"

# COMMAND ----------

spark.sql(f""" create volume if not exists {catalogo}.{esquema_sink}.golden_volume""")

# COMMAND ----------

df_circuits_transformed = spark.table(f"{catalogo}.{esquema_source}.circuits_transformed")

# COMMAND ----------

df_transformed = df_circuits_transformed.groupBy(col("race_year")).agg(
                                                     count(col("location")).alias("conteo"),
                                                     max(col("altitude")).alias("max_altitude"),
                                                     min(col("altitude")).alias("min_altitude"),
                                                     max(col("country")).alias("country"),
                                                     max(col("race_type")).alias("race_type"),
                                                     max(col("near_equator")).alias("near_equator")
                                                     ).orderBy(col("race_year").desc())

# COMMAND ----------

dbutils.fs.rm("/Volumes/catalog_dev/golden/golden_volume/golden_raced_partitioned",True)

# COMMAND ----------

df_transformed.write.\
                format("delta").\
                mode("overwrite").\
                partitionBy("race_year").\
                save(f"/Volumes/catalog_dev/golden/golden_volume/golden_raced_partitioned")

# COMMAND ----------

df_transformed.write.\
                format("parquet").\
                mode("overwrite").\
                partitionBy("race_year").\
                save(f"/Volumes/catalog_dev/golden/golden_volume/golden_raced_partitioned")

# COMMAND ----------

df_transformed.write.\
                format("avro").\
                mode("overwrite").\
                partitionBy("race_year").\
                save(f"/Volumes/catalog_dev/golden/golden_volume/golden_raced_partitioned")