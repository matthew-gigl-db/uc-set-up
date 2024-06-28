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
