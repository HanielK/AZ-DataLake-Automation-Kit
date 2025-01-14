# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Salesforce Landing to Raw
# MAGIC ## Overview
# MAGIC This notebook is designed to extract data from Salesforce and load it into the raw layer of the data warehouse.
# MAGIC
# MAGIC | Details | Information |
# MAGIC | ----------- | ----------- |
# MAGIC | Created By | Richard Thai (richard.thai@powersecure.com) |
# MAGIC | External References | Salesforce Connector |
# MAGIC | Input Datasets | Salesforce Objects |
# MAGIC | Ouput Datasets | Raw Layer Tables |
# MAGIC
# MAGIC ## History
# MAGIC  
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|---------|----------------|
# MAGIC |2024-12-17 | Dan Gardner | Ingest Product2 into Raw as Product| 
# MAGIC |2024-12-17 | Dan Gardner | Ingest Opportunity Product into Raw| 
# MAGIC |2024-12-12 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Final review. |
# MAGIC |2024-10-31 | Richard Thai (richard.thai@powersecure.com) and Navaneesh Gangala (navaneesh.gangala@powersecure.com) | Initial development started |
# MAGIC

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
source_schema = "landing_sf"

# COMMAND ----------

# MAGIC %md
# MAGIC ##DLT Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ###Account

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def account():
    return spark.read.table(f"{env_catalog}.{source_schema}.account").select(
        col("Account_Full_ID__c").alias("account_full_id__c"),
        col("Name").alias("name"),
        col("IFS_Customer_No__c").alias("ifs_customer_no__c"),
        # col("???").alias("category"), # missing column
        col("ParentId").alias("parent_id"),
        col("OwnerId").alias("owner_id"),
        col("Segment__c").alias("segment__c"),
        col("Subsegment__c").alias("subsegment__c"),
        col("SoCo_Opco__c").alias("soco_opco__c"),
        col("ShippingCity").alias("shipping_city"),
        col("ShippingStreet").alias("shipping_street"),
        col("ShippingStateCode").alias("shipping_state_code"),
        col("ShippingPostalCode").alias("shipping_postal_code"),
        col("ShippingCountryCode").alias("shipping_country_code"),
        col("ShippingLatitude").alias("shipping_latitude"),
        col("ShippingLongitude").alias("shipping_longitude"),
        col("CreatedDate").alias("created_date"),
        col("LastModifiedDate").alias("last_modified_date"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Opportunity

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def opportunity():
    return spark.read.table(f"{env_catalog}.{source_schema}.opportunity").select(
        col("Opp_Full_ID__c").cast("string").alias("opp_full_id__c"),
        col("IFS_Opp_No__c").cast("string").alias("ifs_opp_no__c"),
        col("Name").cast("string").alias("name"),
        col("OwnerId").cast("string").alias("owner_id"),
        col("AccountId").cast("string").alias("account_id"),
        col("RecordTypeId").cast("string").alias("record_type_id"),
        col("Amount").cast("decimal(10,2)").alias("amount"),
        col("CloseDate").cast("date").alias("close_date"),
        col("Margin__c").cast("decimal(5,2)").alias("margin__c"),
        col("Rank__c").cast("int").alias("rank__c"),
        col("StageName").cast("string").alias("stage_name"),
        col("Desired_Installation_Date__c")
        .cast("date")
        .alias("desired_installation_date__c"),
        col("IsWon").cast("boolean").alias("is_won"),
        col("IsClosed").cast("boolean").alias("is_closed"),
        col("Company__c").cast("string").alias("company__c"),
        col("Site__c").cast("string").alias("site__c"),
        col("Project_Type__c").cast("string").alias("project_type__c"),
        col("Type_of_Sale__c").cast("string").alias("type_of_sale__c"),
        col("CreatedDate").cast("timestamp").alias("created_date"),
        col("LastModifiedDate").cast("timestamp").alias("last_modified_date"),
        col("LastStageChangeDate").cast("date").alias("last_stage_change_date"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###User

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def user():
    return spark.read.table(f"{env_catalog}.{source_schema}.user").select(
            col("UserID__c").alias("user_id__c"),
            col("IsActive").alias("is_active"),
            col("EmployeeNumber").alias("employee_number"),
            col("IFS_PERSON_ID__c").alias("ifs_person_id__c"),
            col("Name").alias("name"),
            col("ManagerId").alias("manager_id"),
            col("RoleID__c").alias("role_id__c"),
            col("CreatedDate").alias("created_date"),
            col("LastModifiedDate").alias("last_modified_date"),
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ###OpportunityTeamMember

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def opportunity_team_member():
    return spark.read.table(f"{env_catalog}.{source_schema}.opportunityteammember").select(
            col("Id").alias("id"),
            col("OpportunityID").alias("opportunity_id"),
            col("UserId").alias("user_id"),
            col("TeamMemberRole").alias("team_member_role"),
            col("CreatedDate").alias("created_date"),
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ###OpportunityLineItem - (Opportunity Product)

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def opportunity_product():
    return spark.read.table(f"{env_catalog}.{source_schema}.opportunitylineitem").select(
        col("Id").alias("id"),
        col("product2Id").alias("product2_id"),
        col("OpportunityID").alias("opportunity_id"),
        col("ProductCode").alias("product_code"),
        col("Product_Family__c").alias("product_family__c"),
        col("Quantity").alias("quantity"),
        col("CreatedDate").alias("created_date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Product2 - (Product)

# COMMAND ----------

@dlt.table(table_properties={"quality": "raw"})
def product():
    return spark.read.table(f"{env_catalog}.{source_schema}.product2").select(
        col("Id").alias("id"),
        col("Name").alias("name"),
        col("ProductCode").alias("product_code"),
        col("Product_Family__c").alias("product_family__c"),
        col("Product_Subcategory__c").alias("product_subcategory__c"),
        col("Description").alias("description"),        
        col("IsActive").alias("is_active"),
        col("CPQ_Product__c").alias("cpq_product__c"),
        col("LGK__IsConfigurable__c").alias("lgk__is_configurable__c"),
        col("CreatedDate").alias("created_date"),
        col("LastModifiedDate").alias("last_modified_date") 

    )
