# Databricks notebook source
# Input Widgets for the Repo URL, Project Name, and Workspace URL
dbutils.widgets.text(name = "repo_url", defaultValue="")
dbutils.widgets.text(name = "project", defaultValue="")
dbutils.widgets.text(name = "workspace_url", defaultValue="")

# Add a widget for the Databricks Secret representing the Databricks Personal Access Token  
dbutils.widgets.text("pat_secret", "databricks_pat", "DB Secret for PAT")

# COMMAND ----------

repo_url = dbutils.widgets.get(name="repo_url")
project = dbutils.widgets.get(name="project")
workspace_url = dbutils.widgets.get(name="workspace_url")
print(
f"""
  repo_url = {repo_url}
  project = {project}
  workspace_url = {workspace_url}
"""
)

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0]
secret_scope = user_name.split(sep="@")[0].replace(".", "-")
secret_scope

# COMMAND ----------

db_pat = dbutils.secrets.get(
  scope = secret_scope
  ,key = dbutils.widgets.get("pat_secret")
)

db_pat

# COMMAND ----------

import json

# Create a JSON string
dab_init_config = {
    "project_name": "my_first_dab",
    "include_notebook": "yes",
    "include_dlt": "yes",
    "include_python": "yes"
}
dab_init_config = json.dumps(dab_init_config)

# Print dab_init_config as formatted JSON
print(json.dumps(json.loads(dab_init_config), indent=4))

# COMMAND ----------

import dabAssist
import subprocess
from tempfile import TemporaryDirectory

# COMMAND ----------



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
