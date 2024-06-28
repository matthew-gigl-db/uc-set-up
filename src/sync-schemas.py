# Databricks notebook source
dbutils.widgets.text("bundle.workspace.file_path", "", "")
dbutils.widgets.text("bundle.target", "dev", "")
dbutils.widgets.text("bundle.sync_dry_run", "true", "")

# COMMAND ----------

workspace_file_path = dbutils.widgets.get("bundle.workspace.file_path")
sourcePath = workspace_file_path + "/src"
fixturePath = workspace_file_path + "/fixtures"
target_env = dbutils.widgets.get("bundle.target")
if dbutils.widgets.get("bundle.sync_dry_run") == "true":
  sync_dry_run = True
else:
  sync_dry_run = False

# COMMAND ----------

print(f"""
   workspace_file_path = {workspace_file_path}
   sourcePath = {sourcePath}
   fixturePath = {fixturePath}   
   target_env = {target_env}
   sync_dry_run = {sync_dry_run}
""")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(sourcePath))

import ucSetUp
import pandas as pd

# COMMAND ----------

schema_sync_fixture = fixturePath + "/schema_sync.json"
df = pd.read_json(schema_sync_fixture)
sdf = spark.createDataFrame(df)

# COMMAND ----------

display(sdf)

# COMMAND ----------

from pyspark.sql.functions import explode, col

schema_sync_sdf = (
  sdf
  .withColumn("target", col("schema_sync.target"))
  .withColumn("source_catalog", col("schema_sync.source_catalog"))
  .withColumn("source_schema", col("schema_sync.source_schema"))
  .withColumn("external", col("schema_sync.external"))
  .withColumn("owner", col("schema_sync.owner"))
  .withColumn("target_catalog", col("schema_sync.target_catalog"))
  .withColumn("target_schema", col("schema_sync.target_schema"))
  .filter(col("target") == target_env)
  .select("source_catalog", "source_schema", "external", "owner", "target_catalog", "target_schema")
)

# COMMAND ----------

display(schema_sync_sdf)

# COMMAND ----------

schema_sync_list = schema_sync_sdf.distinct().collect()

# COMMAND ----------

for i in schema_sync_list:
  source_catalog = i['source_catalog']
  source_schema = i['source_schema']
  target_catalog = i['target_catalog']
  target_schema = i['target_schema']
  owner = i['owner']
  external = i['external']
  sql_str = f"SYNC SCHEMA {target_catalog}.{target_schema} "
  if external: 
    sql_str = sql_str + "AS EXTERNAL "
  sql_str += f"FROM {source_catalog}.{source_schema}"
  if owner != "":
    sql_str += f" SET OWNER {owner}"
  if sync_dry_run:
    sql_str += " DRY RUN"
  print(sql_str)
  spark.sql(sql_str)

