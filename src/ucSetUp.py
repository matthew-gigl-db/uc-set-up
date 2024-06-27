import pandas as pd
import os

def hello_world(text):
  print(text)

def get_absolute_path(*relative_parts):
    if 'dbutils' in globals():
        base_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()) # type: ignore
        path = os.path.normpath(os.path.join(base_dir, *relative_parts))
        return path if path.startswith("/Workspace") else "/Workspace" + path
    else:
        return os.path.join(*relative_parts)
      
