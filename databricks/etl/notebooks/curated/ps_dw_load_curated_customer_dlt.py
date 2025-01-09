# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Raw to Curated Customer
# MAGIC ## Overview
# MAGIC This document outlines the process of transforming raw data into curated datasets using Delta Live Tables (DLT). The purpose is to ensure data quality and consistency for downstream analytics and reporting.
# MAGIC
# MAGIC | Details | Information |
# MAGIC | ----------- | ----------- |
# MAGIC | Created By | Richard Thai (richard.thai@powersecure.com) |
# MAGIC | External References |  |
# MAGIC | Input Datasets | Raw data from various sources |
# MAGIC | Output Datasets | Curated datasets for data warehouse |
# MAGIC
# MAGIC ## History
# MAGIC  
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |2024-10-31 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Initial development started |
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
schema_name = "curated_dw"
source_container = "landing"
storage_account = f"psdw{env}stordata"

print(f"Environment Catalog: {env_catalog}")
print(f"Environment: {env}")
print(f"Source Container: {source_container}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##DLT Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Base Views/Tables

# COMMAND ----------

@dlt.view
def sf_users_view():
    return spark.read.table(f"{env_catalog}.raw_sf.user").select(
        col("user_id__c").alias("customer_name"),
        col("employee_number").alias("emp_no"),  # sf_employee_id
        col("ifs_person_id__c").alias("person_id"),
        col("name").alias("full_name"),  # drop
        col("manager_id"),
        col("role_id__c").alias("role_id"),
        col("is_active").alias("active"),
        # col("created_date"),
        # col("last_modified_date")
    )

@dlt.view
def pc_customer_view():
    return (
        spark.read.table(f"{env_catalog}.raw_pc.customer")
        .withColumnRenamed("customer_id", "pc_customer_id")
        .withColumnRenamed("customer_name", "pc_customer_name")
    )


@dlt.view
def ifs_customer_info_view():
    return spark.read.table(f"{env_catalog}.raw_ifs.customer_info").select(
        col("name").alias("ifs_customer_name"),
        col("customer_id").alias("ifs_customer_id"),
        col("customer_category").alias("category"),
        col("creation_date").alias("ifs_created_date"),
        col("_obj_id").alias("ifs_obj_id")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Accounts

# COMMAND ----------

@dlt.view
def sf_account_unfiltered_view():
    df_users = dlt.read("sf_users_view")
    df_sf = spark.read.table(f"{env_catalog}.raw_sf.account")

    df_out = df_sf.join(
        df_users, df_sf.owner_id == df_users.customer_name, how="left"
    ).select(
        col("account_full_id__c").alias("sf_account_id"),
        col("name").alias("sf_account_name"),
        col("ifs_customer_no__c").alias("ifs_customer_no"),
        col("parent_id"),
        col("person_id"),
        col("segment__c").alias("segment"),
        col("subsegment__c").alias("subsegment"),
        col("soco_opco__c").alias("southern_opco"),
        col("shipping_street").alias("street"),
        col("shipping_city").alias("city"),
        col("shipping_state_code").alias("state"),
        col("shipping_postal_code").alias("zip"),
        col("shipping_country_code").alias("country"),
        col("shipping_latitude").alias("latitude"),
        col("shipping_longitude").alias("longitude"),
        col("created_date"),
        col("last_modified_date"),
    )

    return df_out


@dlt.view
def sf_account_view():
    return dlt.read("sf_account_unfiltered_view").filter("parent_id is null")


@dlt.view
def exact_match_view():
    df_sf_account = dlt.read("sf_account_view")
    df_pc_customer = dlt.read("pc_customer_view")
    return df_sf_account.join(
        df_pc_customer,
        lower(col("sf_account_name")) == lower(col("pc_customer_name")),
        "inner",
    )


@dlt.view
def unmatched_sf_view():
    df_sf_account = dlt.read("sf_account_view")
    df_exact_match = dlt.read("exact_match_view")
    return df_sf_account.join(df_exact_match, on="sf_account_id", how="left_anti")


@dlt.view
def unmatched_pc_view():
    df_pc_customer = dlt.read("pc_customer_view")
    df_exact_match = dlt.read("exact_match_view")
    return df_pc_customer.join(df_exact_match, on="pc_customer_id", how="left_anti")


@dlt.view
def fuzzy_match_view():
    df_unmatched_sf = dlt.read("unmatched_sf_view")
    df_unmatched_pc = dlt.read("unmatched_pc_view")
    df_cross = df_unmatched_sf.crossJoin(df_unmatched_pc)
    return df_cross.withColumn(
        "levenshtein_distance",
        when(
            (length(col("sf_account_name")) > 5)
            & (length(col("pc_customer_name")) > 5),
            levenshtein(lower(col("sf_account_name")), lower(col("pc_customer_name"))),
        ).otherwise(lit(None)),
    )


@dlt.view
def filtered_fuzzy_match_view():
    df_fuzzy_match = dlt.read("fuzzy_match_view")
    return df_fuzzy_match.filter(
        (col("levenshtein_distance") <= 2) & col("levenshtein_distance").isNotNull()
    )


@dlt.view
def unmatched_sf_fuzzy_view():
    df_unmatched_sf = dlt.read("unmatched_sf_view")
    df_filtered_fuzzy_match = dlt.read("filtered_fuzzy_match_view")
    return df_unmatched_sf.join(
        df_filtered_fuzzy_match, on="sf_account_id", how="left_anti"
    )


@dlt.view
def unmatched_pc_fuzzy_view():
    df_unmatched_pc = dlt.read("unmatched_pc_view")
    df_filtered_fuzzy_match = dlt.read("filtered_fuzzy_match_view")
    return df_unmatched_pc.join(
        df_filtered_fuzzy_match, on="pc_customer_id", how="left_anti"
    )


@dlt.view
def joined_accounts_view():
    df_exact_match = dlt.read("exact_match_view")
    df_filtered_fuzzy_match = dlt.read("filtered_fuzzy_match_view")
    df_unmatched_sf_fuzzy = dlt.read("unmatched_sf_fuzzy_view")
    df_unmatched_pc_fuzzy = dlt.read("unmatched_pc_fuzzy_view")

    return (
        df_exact_match.withColumn("match_quality", lit(0))
        .unionByName(
            df_filtered_fuzzy_match.withColumn("match_quality", col("levenshtein_distance")), 
            allowMissingColumns=True
            )
        .unionByName(
            df_unmatched_sf_fuzzy.withColumn("match_quality", lit(None)),
            allowMissingColumns=True
        )
        .unionByName(
            df_unmatched_pc_fuzzy.withColumn("match_quality", lit(None)),
            allowMissingColumns=True
        )
    )


@dlt.table(
    schema="""
        dw_account_id STRING,
        sf_account_id STRING COMMENT 'Primary identifier for dw_account_id',
        ifs_customer_id STRING,
        ifs_obj_id STRING COMMENT 'Secondary identifier for dw_account_id',
        pc_customer_id INT COMMENT 'Tertiary identifier for dw_account_id with md5 hashing',
        account_name STRING,
        category STRING,
        parent_id STRING,
        person_id STRING,
        segment STRING,
        subsegment STRING,
        southern_opco BOOLEAN,
        created_date TIMESTAMP,
        last_modified_date TIMESTAMP,
        match_quality INT COMMENT 'Match quality score: 0 indicates an exact match, higher values indicate lower match quality'
    """,
    table_properties={
        "quality": "curated",
        "description": "This table consolidates account information from Salesforce, PowerControl, and IFS systems. It performs exact and fuzzy matching to merge records, ensuring a comprehensive view of accounts with match quality scores.",
    }
)
def account():
    df_join = dlt.read("joined_accounts_view")
    df_ifs_customer_info = dlt.read("ifs_customer_info_view")

    df_final = (
        df_join.join(
            df_ifs_customer_info,
            df_join.ifs_customer_no == df_ifs_customer_info.ifs_customer_id,
            how="outer",
        )
        .filter(
            ~( # to get the opposite
                (col("ifs_customer_no").isNull() & col("ifs_customer_id").isNotNull() & (col("category") == "Prospect")) # if there is no match, and is a prospect, it's a customer
            )
        )
        .select(
            "sf_account_id",
            coalesce(
                col("ifs_customer_id"), col("ifs_customer_no"), col("ifs_customer_name")
            ).alias("ifs_customer_id"),
            "ifs_obj_id",
            "pc_customer_id",
            coalesce(
                col("sf_account_name"),
                col("pc_customer_name"),
                col("ifs_customer_name"),
            ).alias("account_name"),
            "category",
            "parent_id",
            "person_id",
            "segment",
            "subsegment",
            "southern_opco",
            "created_date",
            "last_modified_date",
            "match_quality",
        )
        .orderBy(
            desc("match_quality"), "sf_account_id", "pc_customer_id", "ifs_customer_id"
        )
    )

    df_final = df_final.withColumn(
        "dw_account_id",
        coalesce(
            "sf_account_id", "ifs_obj_id", md5((col("pc_customer_id").cast("string")))
        ),
    ).select("dw_account_id", *df_final.columns)

    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer

# COMMAND ----------

@dlt.table(
    schema="""
        dw_customer_id STRING,
        sf_account_id STRING COMMENT 'Primary identifier for dw_customer_id',
        ifs_obj_id STRING COMMENT 'Secondary identifier for dw_customer_id',
        ifs_customer_no STRING,
        ifs_customer_name STRING,
        sf_customer_name STRING,
        category STRING,
        segment STRING,
        subsegment STRING,
        southern_opco BOOLEAN,
        street STRING,
        city STRING,
        state STRING,
        zip STRING,
        country STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        parent_id STRING,
        person_id STRING,
        ifs_created_date TIMESTAMP,
        sf_created_date TIMESTAMP,
        last_modified_date TIMESTAMP
    """,
    table_properties={
        "quality": "curated",
        "description": "This table consolidates customer information from IFS and Salesforce systems. It merges records based on customer IDs and filters out non-customer categories, ensuring a comprehensive view of customer accounts."
    }
)
def customer():
    df_ifs_customer_info = dlt.read("ifs_customer_info_view")

    df_sf_account = (
        dlt.read("sf_account_unfiltered_view")
        .filter("parent_id is not null")
        .withColumnRenamed("sf_account_name", "sf_customer_name")
        .withColumnRenamed("created_date", "sf_created_date")
    )

    df_join = (
        df_ifs_customer_info.join(
            df_sf_account,
            df_ifs_customer_info.ifs_customer_id == df_sf_account.ifs_customer_no,
            how="outer",
        )
        .filter(~(col("sf_account_id").isNull() & (col("category") == "Customer")))
        .withColumn("dw_customer_id", coalesce("sf_account_id", "ifs_obj_id"))
        .select(
            "dw_customer_id",
            "sf_account_id",
            "ifs_obj_id",
            "ifs_customer_no",
            "ifs_customer_name",
            "sf_customer_name",
            "category",
            "segment",
            "subsegment",
            "southern_opco",
            "street",
            "city",
            "state",
            "zip",
            "country",
            "latitude",
            "longitude",
            "parent_id",
            "person_id",
            "ifs_created_date",
            "sf_created_date",
            "last_modified_date",
        )
    )
    return df_join

# COMMAND ----------

# MAGIC %md
# MAGIC ### Employee

# COMMAND ----------

@dlt.table(
    schema="""
        dw_employee_id STRING,
        person_id STRING,
        emp_no STRING COMMENT 'Primary identifier for dw_employee_id with md5 hashing',
        manager_id STRING,
        role_id STRING,
        active BOOLEAN,
        manager_person_id STRING,
        employee_id STRING,
        full_name STRING,
        org_code STRING,
        status STRING
    """,
    table_properties={
        "quality": "curated",
        "description": "This table consolidates employee information from Salesforce and IFS systems. It merges records based on person IDs and includes manager information from Salesforce, ensuring a comprehensive view of employees with their respective details."
    }
)
def employee():
    df_sf_user = dlt.read("sf_users_view").drop("full_name")

    df_sf_user = (
        df_sf_user.alias("a")
        .join(
            df_sf_user.select(
                col("customer_name").alias("mgr_id"),
                col("person_id").alias("manager_person_id"),
            ).alias("b"),
            col("a.manager_id") == col("b.mgr_id"),
            "left",
        )
        .select(col("a.*"), col("b.manager_person_id"))
    )

    df_ifs_employee = spark.read.table(
        f"{env_catalog}.raw_ifs.company_person_last_change").drop("_obj_id", "_obj_version")
    df_out = df_sf_user.join(df_ifs_employee, on="person_id", how="left").drop("customer_name")
    df_out = df_out.select(
        md5(col("emp_no").cast("string")).alias("dw_employee_id"),
        "*"
        )
    return df_out
