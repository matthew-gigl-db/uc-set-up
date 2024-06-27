# Databricks notebook source
dbutils.widgets.text("sourcePath", "", "")
dbutils.widgets.text("fixturePath", "", "")

# COMMAND ----------

sourcePath = dbutils.widgets.get("sourcePath")
fixturePath = dbutils.widgets.get("fixturePath")

# COMMAND ----------

print(f"""
   sourcePath = {sourcePath}
   fixturePath = {fixturePath}    
""")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(dbutils.widgets.get("sourcePath")))

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
  .withColumn("permissions", explode(col("catalog.permissions")))
  .select("name", "permissions")
)

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
