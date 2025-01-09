# Databricks notebook source
# MAGIC %md
# MAGIC # DLT IFS Landing to Raw
# MAGIC ## Overview
# MAGIC This document outlines the process of transferring data from the IFS landing zone to the raw data layer using Databricks Delta Live Tables (DLT).
# MAGIC
# MAGIC | Details | Information |
# MAGIC | ----------- | ----------- |
# MAGIC | Created By | Richard Thai (richard.thai@powersecure.com) |
# MAGIC | External References |  |
# MAGIC | Input Datasets | IFS Landing Zone |
# MAGIC | Output Datasets | Raw Data Layer |
# MAGIC
# MAGIC ## History
# MAGIC  
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |2024-10-24 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Initial development started |
# MAGIC |2024-12-03 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Bring in object version column to each table |
# MAGIC |2024-12-12 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Final review. |
# MAGIC |2024-12-16 | Dan Gardner (dgardner@powersecure.com)| Move company_person_latest from curated to ifs_raw, rename Location |

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
source_container = "landing"
storage_account = f"psdw{env}stordata"

print(f"Environment Catalog: {env_catalog}")
print(f"Environment: {env}")
print(f"Source Container: {source_container}")
print(f"Storage Account: {storage_account}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##DLT Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer Info

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def customer_info():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
            f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/ifs/customer_info/"
        )
        .select(
            col("Name").cast("string").alias("name"),
            col("CUSTOMER_ID").cast("string").alias("customer_id"),
            col("CUSTOMER_CATEGORY").cast("string").alias("customer_category"),
            col("CREATION_DATE").cast("timestamp").alias("creation_date"),
            col("OBJID").alias("_obj_id"),
            col("OBJVERSION").alias("_obj_version"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Equipment Functional

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def equipment_functional():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
            f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/ifs/equipment_functional/"
        )
        .select(
            col("MCH_CODE").alias("mch_code"),
            col("CONTRACT").alias("contract"),
            col("MCH_NAME").alias("mch_name"),
            col("SUP_MCH_CODE").alias("sup_mch_code"),
            col("MCH_TYPE").alias("mch_type"),
            col("LOCATION_ID").alias("location_id"),
            col("MANUFACTURER_NO").alias("manufacturer_no"),
            col("SERIAL_NO").alias("serial_no"),
            col("OPERATIONAL_STATUS").alias("operational_status"),
            col("OBJID").alias("_obj_id"),
            col("OBJVERSION").alias("_obj_version"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Equipment Functional CFV

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def equipment_functional_cfv():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
            f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/ifs/equipment_functional_cfv/"
        )
        .select(
            col("MCH_CODE").alias("mch_code"),
            col("CF$_OWNERSHIP").alias("cf_ownership"),
            col("CF$_OWNER").alias("cf_owner"),
            col("CF$_PWS_EQUIPMENT_STATUS").alias("cf_pws_equipment_status"),
            col("CF$_MODEL_NO").alias("cf_model_no"),
            col("CF$_KW").cast("double").alias("cf_kw"),
            col("CF$_REGION").alias("cf_region"),
            col("CF$_PROJECT_NO").alias("cf_project_no"),
            col("CF$_ASSET_DESIGNATION").alias("cf_asset_designation"),
            col("CONTRACT").alias("contract"),
            col("OBJ_LEVEL").alias("obj_level"),
            col("SUP_MCH_CODE").alias("sup_mch_code"),
            col("CF$_OBJ_CLASS").alias("cf_obj_class"),
            col("LOCATION_ID").alias("location_id"),
            col("OPERATIONAL_STATUS").alias("operational_status"),
            col("MCH_NAME").alias("mch_name"),
            col("OBJID").alias("_obj_id"),
            col("OBJVERSION").alias("_obj_version"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Location Party Address Pub

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def location_party_address_pub():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
            f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/ifs/location_party_address_pub/"
        )
        .select(
            col("location_id"),
            col("description"),
            col("identity"),
            col("address1"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("country_code"),
            col("location_specific_address"),
            col("primary_address"),
            col("OBJID").alias("_obj_id"),
            col("OBJVERSION").alias("_obj_version"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Company Person

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def company_person():
    df_ifs_company_person = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
            f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/ifs/company_person/"
        )
    )

    df_ifs_company_person = df_ifs_company_person.toDF(
        *[c.lower() for c in df_ifs_company_person.columns]
    )

    return df_ifs_company_person.select(
        col("emp_no"),
        col("person_id"),
        col("internal_display_name"),
        col("org_code"),
        col("employee_status"),
        col("OBJID").alias("_obj_id"),
        col("OBJVERSION").alias("_obj_version"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Company Person Last Change - load only the most recent record from company_person

# COMMAND ----------

@dlt.view()
def ifs_company_person_vw():
    return dlt.read_stream(
        "company_person",
    ).select(
        col("emp_no").alias("employee_id"),  # ifs_employee_id
        col("person_id"),
        col("internal_display_name").alias("full_name"),
        col("org_code"),
        col("employee_status").alias("status"),
        col("_obj_id"),
        col("_obj_version")
    )

dlt.create_streaming_table("company_person_last_change")

dlt.apply_changes(
  target = "company_person_last_change",
  source = "ifs_company_person_vw",
  keys = ["_obj_id"],
  sequence_by = col("_obj_version"),
  stored_as_scd_type = 1
)
