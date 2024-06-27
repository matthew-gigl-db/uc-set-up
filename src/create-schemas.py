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
  .withColumn("comment", col("schema.comment"))
  .withColumn("properties", col("schema.properties"))
  .withColumn("external_location", col("schema.external_location"))
  .withColumn("permissions", explode(col("schema.permissions")))
  .filter(col("target") == target_env)
  .select("catalog", "name", "comment", "properties", "external_location", "permissions")
)

# COMMAND ----------

display(schemas_sdf)

# COMMAND ----------

distinct_schemas_list = schemas_sdf.select("catalog", "name", "comment", "properties", "external_location").distinct().collect()

# COMMAND ----------

for i in distinct_schemas_list:
  catalog = i['catalog']
  schema = i['name']
  comment = i['comment']
  properties = i['properties']
  external_location = i['external_location']
  sql_str = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
  if external_location != "":
    sql_str += f" MANAGED LOCATION '{external_location}'"
  if properties != "":
    sql_str += f" WITH DBPROPERTIES {properties}"
  if comment != "":
    sql_str += f" COMMENT '{comment}'"
  sql_str += f";"
  print(sql_str)
  spark.sql(sql_str)

# COMMAND ----------

schemas_list = schemas_sdf.select("catalog", "name", "permissions").collect()

# COMMAND ----------

for i in schemas_list:
  catalog = i['catalog']
  schema = i['name']
  grant = i['permissions']['grant']
  for principal in i['permissions']['principals']:
    sql_str = f"GRANT {grant} ON SCHEMA {catalog}.{schema} TO `{principal}`;"
    print(sql_str)
    spark.sql(sql_str)

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField

# Define the schema for the empty DataFrame
schema = StructType([
    StructField("Column1", StringType(), nullable=True),
    StructField("Column2", StringType(), nullable=True),
    StructField("Column3", StringType(), nullable=True)
])

# Create an empty DataFrame with the desired schema
empty_df = spark.createDataFrame([], schema)

# Use the empty_df DataFrame as needed
display(empty_df)

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

for i in distinct_schemas_list:
  catalog = i['catalog']
  schema = i['name']
  grants = spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{schema};")
  # Append the grants DataFrame to the empty DataFrame
  appended_df = appended_df.union(grants)  

display(appended_df)
