# Databricks notebook source
# MAGIC %pip install usaddress-scourgify

# COMMAND ----------

# MAGIC %md
# MAGIC # DLT Raw to Curated Commercial
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
# MAGIC |2024-12-16 | dgardner@powersecure.com) | Rename Location curated table |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependencies

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from scourgify import normalize_address_record
from scourgify.exceptions import UnParseableAddressError, AddressNormalizationError
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
# MAGIC ## User Defined Functions

# COMMAND ----------

def normalize_address_udf(address):
    try:
        return dict(normalize_address_record(address))
    except (KeyError, UnParseableAddressError, AddressNormalizationError) as e:
        return {"address_line_1": address}

# COMMAND ----------

# MAGIC %md
# MAGIC ##DLT Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Base Views/Tables

# COMMAND ----------

@dlt.view
def equipment_functional_cfv_view():
    return spark.read.table(f"{env_catalog}.raw_ifs.equipment_functional_cfv")


@dlt.view
def location_party_address_pub_view():
    return (
        spark.read.table(f"{env_catalog}.raw_ifs.location_party_address_pub")
        .withColumn("state", when(col("state") == "NULL", None).otherwise(col("state")))
        .withColumn(
            "clean_address",
            concat_ws(", ", "address1", "city", "state", "zip_code", "country_code"),
        )
    )


@dlt.view
def pc_state_view():
    return spark.read.table(f"{env_catalog}.raw_pc.state")


@dlt.view
def pc_site_view():
    df_pc_state = dlt.read("pc_state_view")
    df_pc_site = spark.read.table(f"{env_catalog}.raw_pc.site").withColumnRenamed("site_state_id", "state_id")
    df_pc_site_enriched = df_pc_site.join(
        broadcast(df_pc_state), on="state_id", how="left"
    ).withColumn(
        "clean_address",
        concat_ws(", ", "site_address1", "site_city", "state_abbreviation", "site_zip"),
    )
    return df_pc_site_enriched


@dlt.view
def pc_gen_view():
    df_pc_gen = spark.read.table(f"{env_catalog}.raw_pc.generator").select(
        col("site_id").alias("pc_site_id"),
        md5(col("generator_id").cast("string")).alias("dw_equipment_id"),
        col("generator_size").cast("double").alias("total_power"),
        lit("780-GenSets").alias("object_level"),
        lit("PC").alias("classification"),
        lit("PC_Generators").alias("identifier")
    )
    return df_pc_gen

# COMMAND ----------

@dlt.table
def location():
  df_pc_site = dlt.read("pc_site_view")
  df_location = dlt.read("location_party_address_pub_view")

  df_clean_addresses = (df_pc_site.select("clean_address")).union(df_location.select("clean_address")).distinct()
  normalize_address = udf(normalize_address_udf, MapType(StringType(), StringType()))

  # Apply the UDF to your DataFrame
  df_address_inferred = df_clean_addresses.withColumn("full_address", normalize_address(col("clean_address")))

  # Map out full_address to columns
  df_address_inferred = df_address_inferred.select(
      "clean_address",
      col("full_address")["address_line_1"].alias("address_line_1"),
      col("full_address")["address_line_2"].alias("address_line_2"),
      col("full_address")["city"].alias("city"),
      col("full_address")["state"].alias("state"),
      col("full_address")["postal_code"].alias("postal_code")
  ).withColumn("mapped_address", concat_ws(", ", "address_line_1", "address_line_2", "city", "state", "postal_code"))

  return df_address_inferred

# COMMAND ----------

# MAGIC %md
# MAGIC ### Equipment

# COMMAND ----------

@dlt.view
def psgens_view():
    df_equip_func_cfv = dlt.read("equipment_functional_cfv_view")
    df_equip_func_cfv.createOrReplaceTempView("EQUIPMENT_FUNCTIONAL_CFV")

    df_psgens = spark.sql(
        """
        SELECT 
            EF.mch_code AS ifs_object_id,
            EF._obj_id AS dw_equipment_id,
            EF.contract AS division,
            EF.obj_level AS object_level,
            EF.sup_mch_code AS parent_object,
            SUP.mch_name AS description,
            EF.cf_obj_class AS classification,
            EF.cf_owner AS object_owner,
            EF.cf_ownership AS ownership,
            EF.location_id AS location_id,
            SUM(EF.cf_kw) AS total_power,
            COUNT(EF.mch_code) AS no_of_engines,
            EF.cf_project_no AS project_id,
            EF.operational_status AS equipment_status,
            CASE 
                WHEN SUP.obj_level = '120-Cust Location' THEN SUP.mch_code
                ELSE CUST.mch_code
            END AS customer_location,
            CASE 
                WHEN SUP.obj_level = '120-Cust Location' THEN SUP.mch_name
                ELSE CUST.mch_name
            END AS customer_location_name,
            "PS_Generators" AS identifier
        FROM 
            EQUIPMENT_FUNCTIONAL_CFV EF
        LEFT JOIN 
            EQUIPMENT_FUNCTIONAL_CFV SUP ON EF.sup_mch_code = SUP.mch_code
        LEFT JOIN 
            EQUIPMENT_FUNCTIONAL_CFV CUST ON SUP.sup_mch_code = CUST.mch_code AND CUST.obj_level = '120-Cust Location'
        WHERE 
            EF.contract = '700'
            AND EF.obj_level = '780-GenSets'
            AND (EF.cf_obj_class = 'PS' OR (EF.cf_obj_class IS NULL AND EF.cf_project_no IS NOT NULL))
        GROUP BY
            EF.mch_code,
            EF._obj_id,
            EF.contract,
            EF.obj_level,
            EF.sup_mch_code,
            SUP.mch_name,
            EF.cf_obj_class,
            EF.cf_owner,
            EF.cf_ownership,
            EF.location_id,
            EF.cf_project_no,
            EF.operational_status,
            SUP.obj_level,
            SUP.mch_code,
            SUP.mch_name,
            CUST.mch_code,
            CUST.mch_name
        """
    )

    return df_psgens


@dlt.view
def erggens_view():
    df_equip_func_cfv = dlt.read("equipment_functional_cfv_view")
    df_equip_func_cfv.createOrReplaceTempView("EQUIPMENT_FUNCTIONAL_CFV")
    
    df_erg_gens = spark.sql(
        """
    SELECT 
        EF.mch_code AS ifs_object_id,
        EF._obj_id AS dw_equipment_id,
        EF.contract AS division,
        EF.obj_level AS object_level,
        EF.sup_mch_code AS parent_object,
        SUP.mch_name AS description,
        EF.cf_obj_class AS classification,
        EF.cf_owner AS object_owner,
        EF.cf_ownership AS ownership,
        EF.location_id AS location_id,
        SUM(EF.cf_kw) AS total_power,
        COUNT(EF.mch_code) AS no_of_engines,
        EF.cf_project_no AS project_id,
        EF.operational_status AS equipment_status,
        CASE 
            WHEN SUP.obj_level = '120-Cust Location' THEN SUP.mch_code
            ELSE CUST.mch_code
        END AS customer_location,
        CASE 
            WHEN SUP.obj_level = '120-Cust Location' THEN SUP.mch_name
            ELSE CUST.mch_name
        END AS customer_location_name,
        "ERG_Generators" AS identifier
    FROM 
        EQUIPMENT_FUNCTIONAL_CFV EF
    LEFT JOIN 
        EQUIPMENT_FUNCTIONAL_CFV SUP ON EF.sup_mch_code = SUP.mch_code
    LEFT JOIN 
        EQUIPMENT_FUNCTIONAL_CFV CUST ON SUP.sup_mch_code = CUST.mch_code AND CUST.obj_level = '120-Cust Location'
    WHERE 
        EF.contract = '700'
        AND EF.obj_level = '780-GenSets'
        AND (EF.cf_obj_class = 'ERG' OR (EF.cf_obj_class IS NULL AND EF.cf_project_no IS NULL))
        AND EF.operational_status <> 'Scrapped'
    GROUP BY 
        EF.mch_code,
        EF._obj_id,
        EF.contract,
        EF.obj_level,
        EF.sup_mch_code,
        SUP.mch_name,
        EF.cf_obj_class,
        EF.cf_owner,
        EF.cf_ownership,
        EF.location_id,
        EF.cf_project_no,
        EF.operational_status,
        SUP.obj_level,
        SUP.mch_code,
        SUP.mch_name,
        CUST.mch_code,
        CUST.mch_name;
    """
    )

    return df_erg_gens

@dlt.table(
    table_properties={"quality": "curated", "description": "Curated table containing PS and ERG Generators data"},
    schema="""
        dw_equipment_id STRING,
        ifs_object_id STRING,
        pc_site_id INT,
        division STRING,
        object_level STRING,
        parent_object STRING,
        description STRING,
        classification STRING,
        object_owner STRING,
        ownership STRING,
        location_id STRING,
        total_power DOUBLE,
        no_of_engines BIGINT,
        project_id STRING,
        equipment_status STRING,
        customer_location STRING,
        customer_location_name STRING,
        identifier STRING
    """
)
def equipment():
    df_psgens = dlt.read("psgens_view")
    df_erg_gens = dlt.read("erggens_view")
    df_pc_gen = dlt.read("pc_gen_view")

    df_union = df_psgens.unionByName(df_erg_gens, allowMissingColumns=True).unionByName(df_pc_gen, allowMissingColumns=True)

    return df_union

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opportunity

# COMMAND ----------

@dlt.table(
    table_properties={"quality": "curated"},
    schema="""
        dw_opportunity_id STRING,
        opportunity_no STRING,
        opportunity_name STRING,
        primary_sales_rep STRING,
        account_id STRING,
        record_type_id STRING,
        amount DECIMAL(10,2),
        close_date DATE,
        margin_percentage DECIMAL(9,6),
        rank INT,
        stage STRING,
        desired_installation_date DATE,
        closed_won BOOLEAN,
        closed BOOLEAN,
        company STRING,
        division_id STRING,
        project_type STRING,
        type_of_sale STRING,
        created_date TIMESTAMP,
        last_modified_date TIMESTAMP,
        last_stage_change_date DATE
    """
)
def opportunity():
    df_sf_opportunity = spark.read.table(f"{env_catalog}.raw_sf.opportunity")
    df_sf_opportunity = df_sf_opportunity.select(
        col("opp_full_id__c").alias("dw_opportunity_id"),
        col("ifs_opp_no__c").alias("opportunity_no"),
        col("name").alias("opportunity_name"),
        col("owner_id").alias("primary_sales_rep"),
        col("account_id"),
        col("record_type_id"),
        col("amount"),
        col("close_date"),
        (col("margin__c") / 100).alias("margin_percentage"),
        col("rank__c").alias("rank"),
        col("stage_name").alias("stage"),
        col("desired_installation_date__c").alias("desired_installation_date"),
        col("is_won").alias("closed_won"),
        col("is_closed").alias("closed"),
        col("company__c").alias("company"),
        col("site__c").alias("division_id"),
        col("project_type__c").alias("project_type"),
        col("type_of_sale__c").alias("type_of_sale"),
        col("created_date"),
        col("last_modified_date"),
        col("last_stage_change_date"),
    )
    return df_sf_opportunity


# COMMAND ----------

# MAGIC %md
# MAGIC ### Site

# COMMAND ----------

@dlt.view
def ps_sites_view():
    df_equip_func_cfv = dlt.read("equipment_functional_cfv_view")
    df_location = dlt.read("location_party_address_pub_view")

    df_equip_func_cfv.createOrReplaceTempView("EQUIPMENT_FUNCTIONAL_CFV")
    df_location.createOrReplaceTempView("LOCATION_PARTY_ADDRESS_PUB")

    df_ps_sites = spark.sql(
        """
    SELECT
        EF.mch_code AS ifs_site_id,
        EF._obj_id AS ifs_obj_id,
        EF.mch_name AS site_name,
        EF.contract AS division,
        EF.obj_level AS object_level,
        EF.sup_mch_code AS ifs_customer_no,
        EF.location_id AS location_id,
        L.address1 AS street,
        L.city AS city,
        L.zip_code AS zip,
        L.state AS state,
        L.country_code AS country,
        L.clean_address,
        "PS_Sites" AS identifier
    FROM
        EQUIPMENT_FUNCTIONAL_CFV EF
    LEFT JOIN
        LOCATION_PARTY_ADDRESS_PUB L
    ON
        EF.location_id = L.location_id

    WHERE
        EF.contract ='700' --Only PSS Sites
        AND EF.cf_obj_class = 'ERG' --Only ERG PSS Sites currently under Contract, if ERG Sites not under contract are desired then need to identify use case for the 15,000+added records
        AND LOCATION_SPECIFIC_ADDRESS = 'TRUE' --Identifies this is a physical address, move this to ON to include records without Location_ID
        AND PRIMARY_ADDRESS = 'TRUE' --Identifies this is the primary address, move this to ON to include records without Location_ID
        AND EF.obj_level = '120-Cust Location'
        AND EF.operational_status <> 'Scrapped'; --Does not include 'Scrapped' Sites which are those without an active contract
    """
    )

    return df_ps_sites


@dlt.view
def opco_sites_view():
    df_equip_func_cfv = dlt.read("equipment_functional_cfv_view")
    df_location = dlt.read("location_party_address_pub_view")

    df_equip_func_cfv.createOrReplaceTempView("EQUIPMENT_FUNCTIONAL_CFV")
    df_location.createOrReplaceTempView("LOCATION_PARTY_ADDRESS_PUB")

    df_opco_sites = spark.sql(
        """
    SELECT
        EF.mch_code AS ifs_site_id, --For OPCO Sites, the MCH_CODE = Site_ID = Location_ID = Project_ID
        EF._obj_id AS ifs_obj_id,
        EF.mch_name AS site_name, -- Name of the Project which should equate to the location
        EF.contract AS division, --Tells us which Division did the work here, EES or DI
        EF.obj_level AS object_level, -- Object Level defines where in the hierarchy this record sits
        EF.sup_mch_code AS ifs_customer_no, --Parent Object which for OPCO is the Customer_ID
        EF.LOCATION_ID AS location_id, --For OPCO Sites, the Location_ID= MCH_CODE = PROJECT_ID
        L.address1 AS street, --Address of the Site
        L.city AS city, --
        L.zip_code AS zip,
        L.state As state,
        L.country_code as country,
        L.clean_address,
        "OPCO_Sites" AS identifier
    FROM
        EQUIPMENT_FUNCTIONAL_CFV EF
    JOIN
        LOCATION_PARTY_ADDRESS_PUB L
    ON
        EF.location_id = L.location_id
    WHERE
        EF.contract IN('300','600','605')--Only include DI, EES and EES Canada Sites, excludes Divisions no longer with PowerSecure. PSS Sites will be captured separately.
        AND EF.obj_level = '120-Cust Location'
        AND EF.sup_mch_code NOT IN ('110','200','300','600','605','700'); --Excludes MCH_CODE (Projects) that are Internal Only
    """
    )

    return df_opco_sites


@dlt.view
def pre_ifs_opco_sites_view():
    df_equip_func_cfv = dlt.read("equipment_functional_cfv_view")
    df_location = dlt.read("location_party_address_pub_view")

    df_equip_func_cfv.createOrReplaceTempView("EQUIPMENT_FUNCTIONAL_CFV")
    df_location.createOrReplaceTempView("LOCATION_PARTY_ADDRESS_PUB")

    df_pre_ifs_opco_sites = spark.sql(
        """
    SELECT
        EF.mch_code AS ifs_site_id,
        EF._obj_id AS ifs_obj_id,
        EF.mch_name AS site_name,
        EF.contract AS division,
        EF.obj_level AS object_level,
        EF.sup_mch_code AS ifs_customer_no,
        EF.location_id AS location_id,
        L.address1 AS street,
        L.city AS city,
        L.zip_code AS zip,
        L.state As state,
        L.country_code as country,
        L.clean_address,
        "Pre_IFS_Sites" AS identifier
    FROM
        EQUIPMENT_FUNCTIONAL_CFV EF
    LEFT JOIN
        LOCATION_PARTY_ADDRESS_PUB L
    ON
        EF.location_id = L.location_id
        AND L.location_specific_address = 'TRUE'
        AND L.primary_address = 'TRUE'
    WHERE
        EF.cf_project_no IS NOT NULL
        AND EF.contract = '700'
        AND (EF.cf_obj_class = 'PS' OR EF.cf_obj_class IS NULL)
        AND EF.obj_level = '120-Cust Location'
        AND EF.cf_project_no NOT LIKE '1%';
    """
    )

    return df_pre_ifs_opco_sites


@dlt.table(
    table_properties={
        "quality": "curated",
        "description": "Curated table containing site data including PS, OPCO, Pre-IFS, and PC sites",
    },
    schema="""
        dw_site_id STRING,
        ifs_site_id STRING,
        ifs_obj_id STRING COMMENT 'Primary identifier for dw_site_id', 
        pc_site_id INT COMMENT 'Secondary identifier for dw_site_id with md5 hashing',
        site_name STRING,
        mapped_address STRING,
        street STRING,
        city STRING,
        state STRING,
        state_abbreviation STRING,
        zip STRING,
        country STRING,
        location_id STRING,
        ifs_customer_no STRING,
        pc_customer_id INT,
        division STRING,
        object_level STRING,
        identifier STRING,
        project_id STRING,
        state_id INT,
        mobile_site BOOLEAN,
        asset_owned BOOLEAN,
        site_contract BOOLEAN,
        site_live BOOLEAN,
        controls_disabled BOOLEAN,
        disabled_site BOOLEAN,
        site_live_date TIMESTAMP,
        controls_disabled_date TIMESTAMP,
        disabled_date TIMESTAMP,
        fuel_mgmt BOOLEAN
    """
)
def site():
    df_ps_sites = dlt.read("ps_sites_view")
    df_opco_sites = dlt.read("opco_sites_view")
    df_pre_ifs_opco_sites = dlt.read("pre_ifs_opco_sites_view")
    df_location = dlt.read("location").select(
        "clean_address", "mapped_address"
    )

    df_ifs_union = (
        df_ps_sites.unionByName(df_opco_sites, allowMissingColumns=True)
        .unionByName(df_pre_ifs_opco_sites, allowMissingColumns=True)
        .join(df_location, on="clean_address")
        .drop("clean_address")
    )

    df_pc_sites = (
        dlt.read("pc_site_view")
        .join(df_location, on="clean_address")
        .drop("clean_address")
        .withColumnRenamed("customer_id", "pc_customer_id")
        .withColumnRenamed("site_id", "pc_site_id")
        .withColumnRenamed("site_name", "pc_site_name")
    )

    df_join = (
        (
            df_ifs_union.join(df_pc_sites, on="mapped_address", how="outer")
            .withColumn("site_name", coalesce("site_name", "pc_site_name"))
            .withColumn("street", coalesce("street", "site_address1"))
            .withColumn("city", coalesce("city", "site_city"))
            .withColumn("zip", coalesce("zip", "site_zip"))
            .withColumn("project_id", coalesce("ifs_project_code", "project_code"))
        )
        .drop(
            "pc_site_name",
            "site_address1",
            "site_city",
            "site_zip",
            "ifs_project_code",
            "project_code",
        )
    )

    df_out = df_join.select(
        coalesce("ifs_obj_id", md5(col("pc_site_id").cast("string"))).alias("dw_site_id"),
        "*"
    )
    return df_out

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opportunity Team Member

# COMMAND ----------

# Load data from opportunityteammember, opportunity and user tables
@dlt.table(
    table_properties={
        "quality": "curated",
        "description": "Curated table containing Opportunity Team Member, Opportunity, User and Employee data",
    })
def opportunity_team_member():
    df_opportunity = dlt.read("opportunity")
    df_opportunity.createOrReplaceTempView("opportunity")

    df_employee = dlt.read("employee")
    df_employee.createOrReplaceTempView("employee")
    df_otm = spark.sql(f"""
        SELECT
            otm.id AS opportunity_team_member_id,
            u.IFS_PERSON_ID__c AS person_id,
            e.full_name AS team_member_name,
            otm.teammemberrole AS opportunity_role,
            otm.opportunityid AS opportunity_id,
            o.opportunity_no AS opportunity_no,
            o.opportunity_name AS opportunity_name,
            otm.createddate AS created_date
        FROM {env_catalog}.landing_sf.opportunityteammember otm
        JOIN {env_catalog}.landing_sf.user u ON otm.userid = u.Id
        JOIN opportunity o ON otm.OpportunityId = o.dw_opportunity_id
        JOIN employee e ON u.IFS_PERSON_ID__c = e.person_id
        """)
    return df_otm
