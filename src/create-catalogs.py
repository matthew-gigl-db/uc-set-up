# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("bundle.sourcePath", "", "")
dbutils.widgets.text("bundle.fixturePath", "", "")
dbutils.widgets.text("bundle.target", "dev", "")

# COMMAND ----------

sourcePath = dbutils.widgets.get("bundle.sourcePath")
fixturePath = dbutils.widgets.get("bundle.fixturePath")
target_env = dbutils.widgets.get("bundle.target")

# COMMAND ----------

print(f"""
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

catalogs_fixture = fixturePath + "/catalogs.json"
df = pd.read_json(catalogs_fixture)
sdf = spark.createDataFrame(df)

# COMMAND ----------

from pyspark.sql.functions import explode, col

catalogs_sdf = (
  sdf
  .withColumn("name", col("catalog.name"))
  .withColumn("target", col("catalog.target"))
  .withColumn("permissions", explode(col("catalog.permissions")))
  .filter(col("target") == target_env)
  .select("name", "permissions")
)

# COMMAND ----------

display(catalogs_sdf)

# COMMAND ----------

catalogs_list = catalogs_sdf.collect()

# COMMAND ----------

distinct_catalogs = set([i["name"] for i in catalogs_list])
for catalog in distinct_catalogs:
  print(f"CREATE CATALOG IF NOT EXISTS {catalog};")
  # spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")


# COMMAND ----------

for i in catalogs_list:
  catalog = i["name"]
  grant = i["permissions"]["grant"]
  for principal in i["permissions"]["principals"]:
    print(f"GRANT {grant} ON CATALOG {catalog} TO `{principal}`;")
    # spark.sql(f"GRANT {grant} ON CATALOG {catalog} TO `{principal}`;")
