# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Power Control Landing to Raw
# MAGIC ## Overview
# MAGIC The purpose of this notebook is to extract, transform, and load data from the Power Control system into the raw data layer.
# MAGIC
# MAGIC | Details | Information |
# MAGIC | ----------- | ----------- |
# MAGIC | Created By | Richard Thai (richard.thai@powersecure.com) |
# MAGIC | External References |  |
# MAGIC | Input Datasets | Power Control System Data |
# MAGIC | Output Datasets | Raw Data Layer |
# MAGIC
# MAGIC ## History
# MAGIC  
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |2024-10-23 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Initial development started |
# MAGIC |2024-12-12 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Final review. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependencies

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "../.."))
from framework.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

cluster_env = get_environment_from_cluster(spark) # get the environment from the cluster name
env = spark.conf.get("env", cluster_env)  # from DLT Advanced Configuration key-value
env_catalog = spark.conf.get("catalog", get_catalog(env, "datawarehouse")) # environment-specific catalog name
source_catalog = "powercontrol" # rename
source_schema = "public"

# COMMAND ----------

# MAGIC %md
# MAGIC ##DLT Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def customer():
    return spark.read.table(f"{source_catalog}.{source_schema}.customers").select(
        col("customerid").alias("customer_id"),
        col("customername").alias("customer_name")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Site

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def site():
    return spark.read.table(f"{source_catalog}.{source_schema}.sites").select(
        col("SiteId").alias("site_id"),
        col("SiteName").alias("site_name"),
        col("SiteAddress1").alias("site_address1"),
        col("SiteCity").alias("site_city"),
        col("SiteZip").alias("site_zip"),
        col("SiteStateId").alias("site_state_id"),
        col("MobileSite").alias("mobile_site"),
        col("AssetOwned").alias("asset_owned"),
        col("SiteContract").alias("site_contract"),
        col("SiteLive").alias("site_live"),
        col("ControlsDisabled").alias("controls_disabled"),
        col("DisabledSite").alias("disabled_site"),
        col("SiteLiveDate").alias("site_live_date"),
        col("ControlsDisabledDate").alias("controls_disabled_date"),
        col("DisabledDate").alias("disabled_date"),
        col("ProjectCode").alias("project_code"),
        col("IfsProjectCode").alias("ifs_project_code"),
        col("CustomerId").alias("customer_id"),
        col("FuelMgmt").alias("fuel_mgmt")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###State

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def state():
    return spark.read.table(f"{source_catalog}.{source_schema}.states").select(
        col("stateid").alias("state_id"),
        col("stateabbreviation").alias("state_abbreviation")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Generator

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def generator():
    return spark.read.table(f"{source_catalog}.{source_schema}.generators").select(
        col("GeneratorId").alias("generator_id"),
        col("EquipmentId").alias("equipment_id"),
        col("UnitId").alias("unit_id"),
        col("GeneratorNumber").alias("generator_number"),
        col("GeneratorManufacturerId").alias("generator_manufacturer_id"),
        col("GeneratorSize").alias("generator_size"),
        col("SiteId").alias("site_id")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Generator Manufacturer

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def generatormanufacturer():
    return spark.read.table(f"{source_catalog}.{source_schema}.generatormanufacturers").select(
        col("generatormanufacturerid").alias("generator_manufacturer_id"),
        col("generatormanufacturername").alias("generator_manufacturer_name")
    )
