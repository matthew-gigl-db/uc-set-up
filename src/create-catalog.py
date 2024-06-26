# Databricks notebook source
dbutils.widgets.text("sourcePath", "", "")
dbutils.widgets.text("catalog", "", "")
dbutils.widgets.text("target", "dev", "")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(dbutils.widgets.get("sourcePath")))

import ucSetUp

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
target = dbutils.widgets.get("target")

# COMMAND ----------

if catalog == target:
  catalog = catalog
elif target == "prod":
  catalog = catalog
else:
  catalog = f"{catalog}_{target}"

# COMMAND ----------

ucSetUp.hello_world(catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists ${catalog};
