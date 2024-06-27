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

schemas_fixture = fixturePath + "/schemas.json"
df = pd.read_json(schemas_fixture)
sdf = spark.createDataFrame(df)

# COMMAND ----------

display(sdf)

# COMMAND ----------

from pyspark.sql.functions import explode, col

schemas_sdf = (
  sdf
  .withColumn("name", col("schema.name"))
  .withColumn("target", col("schema.target"))
  .withColumn("catalog", col("schema.catalog"))
  .withColumn("permissions", explode(col("schema.permissions")))
  .filter(col("target") == target_env)
  .select("catalog", "name", "permissions")
)

# COMMAND ----------

display(schemas_sdf)

# COMMAND ----------

distinct_schemas_list = schemas_sdf.select("catalog", "name").distinct().collect()

# COMMAND ----------

for i in distinct_schemas_list:
  sql = f"USE CATALOG {i['catalog']}; CREATE SCHEMA IF NOT EXISTS {i['name']};"
  print(sql)
  # spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")

# COMMAND ----------

schemas_list = schemas_sdf.collect()

# COMMAND ----------

for i in schemas_list:
  catalog = i['catalog']
  schema = i['name']
  grant = i['permissions']['grant']
  for principal in i['permissions']['principals']:
    sql = f"USE CATALOG {catalog}; USE SCHEMA {schema}; GRANT {grant} ON {schema} TO `{principal}`;"
    print(sql)
