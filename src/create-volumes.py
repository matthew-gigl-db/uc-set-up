# Databricks notebook source
dbutils.widgets.text("bundle.workspace.file_path", "", "")
dbutils.widgets.text("bundle.target", "dev", "")

# COMMAND ----------

workspace_file_path = dbutils.widgets.get("bundle.workspace.file_path")
sourcePath = workspace_file_path + "/src"
fixturePath = workspace_file_path + "/fixtures"
target_env = dbutils.widgets.get("bundle.target")

# COMMAND ----------

print(f"""
   workspace_file_path = {workspace_file_path}
   sourcePath = {sourcePath}
   fixturePath = {fixturePath}   
   target_env = {target_env}
""")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(sourcePath))

import ucSetUp
import pandas as pd

# COMMAND ----------

volumes_fixture = fixturePath + "/volumes.json"
df = pd.read_json(volumes_fixture)
sdf = spark.createDataFrame(df)

# COMMAND ----------

display(sdf)

# COMMAND ----------

from pyspark.sql.functions import explode, col

volumes_sdf = (
  sdf
  .withColumn("name", col("volume.name"))
  .withColumn("target", col("volume.target"))
  .withColumn("catalog", col("volume.catalog"))
  .withColumn("schema", col("volume.schema"))
  .withColumn("comment", col("volume.comment"))
  .withColumn("external_location", col("volume.external_location"))
  .withColumn("permissions", explode(col("volume.permissions")))
  .filter(col("target") == target_env)
  .select("catalog", "schema", "name", "comment", "external_location", "permissions")
)

# COMMAND ----------

display(volumes_sdf)

# COMMAND ----------

distinct_volumes_list = volumes_sdf.select("catalog", "schema", "name", "comment", "external_location").distinct().collect()

# COMMAND ----------

for i in distinct_volumes_list:
  catalog = i['catalog']
  schema = i['schema']
  volume = i['name']
  comment = i['comment']
  external_location = i['external_location']
  sql_str = f"VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}"
  if external_location != "":
    sql_str = "EXTERNAL " + sql_str + f" LOCATION '{external_location}'"
  if comment != "":
    sql_str += f" COMMENT '{comment}'"
  sql_str = "CREATE " + sql_str + f";"
  print(sql_str)
  spark.sql(sql_str)

# COMMAND ----------

volumes_list = volumes_sdf.select("catalog", "schema", "name", "permissions").collect()

# COMMAND ----------

for i in volumes_list:
  catalog = i['catalog']
  schema = i['schema']
  volume = i['name']
  grant = i['permissions']['grant']
  for principal in i['permissions']['principals']:
    sql_str = f"GRANT {grant} ON VOLUME {catalog}.{schema}.{volume} TO `{principal}`;"
    print(sql_str)
    # spark.sql(sql_str)

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField

# Define the schema for the empty DataFrame
schema = StructType([
    StructField("Principal", StringType(), nullable=True),
    StructField("ActionType", StringType(), nullable=True),
    StructField("ObjectType", StringType(), nullable=True),
    StructField("ObjectKey", StringType(), nullable=True),
])

# Create an empty DataFrame with the desired schema
appended_df = spark.createDataFrame([], schema)  

for i in distinct_volumes_list:
  catalog = i['catalog']
  schema = i['schema']
  volume = i['name']
  grants = spark.sql(f"SHOW GRANTS ON VOLUME {catalog}.{schema}.{volume};")
  # Append the grants DataFrame to the empty DataFrame
  appended_df = appended_df.union(grants)  

display(appended_df)
