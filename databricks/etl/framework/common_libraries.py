# Databricks notebook source
# MAGIC %md
# MAGIC #Common Libraries
# MAGIC ## Overview
# MAGIC The purpose of this notebook is to host common libraries used throughout the projects
# MAGIC
# MAGIC
# MAGIC | Details | Information |
# MAGIC | ----------- | ----------- |
# MAGIC | Created By | Richard Thai (richard.thai@powersecure.com) |
# MAGIC | External References |  |
# MAGIC | Input Datasets |  |
# MAGIC | Ouput Datasets |  |
# MAGIC
# MAGIC ## History
# MAGIC  
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |2024-10-28 | Richard Thai (richard.thai@powersecure.com) | Initial development started.|

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common libraries

# COMMAND ----------

# Import functions for working with PySpark DataFrames
from pyspark.sql.functions import *

# Import functions from PySpark SQL module
from pyspark.sql import *

# Import data types for PySpark DataFrame columns
from pyspark.sql.types import *

# Import from utils module
from pyspark.sql.utils import *

# Import datetime module for working with date and time in Python
from datetime import *
import time

# Import Delta Lake functionality for working with Delta tables in PySpark
from delta.tables import *

# Import concurrent futures for parallel execution within a notebook
import concurrent.futures

# # Import Delta Live Tables
# import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ###Frequently used libraries

# COMMAND ----------

# Import relativedelta from dateutil for working with relative differences between dates
from dateutil.relativedelta import relativedelta

# Import json module for working with JSON data in Python
import json

# Import pandas library and alias it as pd for working with Pandas DataFrames
import pandas as pd

# Import uuid module for generating UUIDs (Universally Unique Identifiers)
import uuid

# Import reduce function from functools for functional programming operations
from functools import reduce

# Import pytz module for working with time zones in Python
import pytz

# Import os module for interacting with the operating system, such as file operations
import os

# Import retrying module, if DNE (does not exists), pip install, else import
# TODO: add library to dependy on cluster instead of pip install if DNE
try:
    from retrying import retry
except ModuleNotFoundError as e:
    import subprocess

    subprocess.check_call(["pip", "install", "retrying"])
    from retrying import retry

# Import glob module for interacting with the adls dir for file operations
import glob

# Import math module for math operations
from math import *

# Import delta module(s)
from delta.tables import DeltaTable