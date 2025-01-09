from pyspark.sql import SparkSession

def get_environment_from_cluster(spark: SparkSession) -> str:
    """
    Determines the environment based on the cluster name in Spark configuration.
    
    Args:
        None.
    Returns:
        str: The environment name ('dv', 'ua', 'pr') or an empty string if not found.
    """
    try:
        cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
        if '-dv-' in cluster_name:
            return "dv"
        elif '-ua-' in cluster_name:
            return "ua"
        elif '-pr-' in cluster_name:
            return "pr"
        else:
            return ""
    except Exception as e:
        raise e
    
def get_environment(catalog_name):
    mapping = {
        "datawarehouse_dv": "dv",
        "datawarehouse_ua": "ua",
        "datawarehouse": "pr"
    }
    return mapping.get(catalog_name, "unknown")
    
def get_catalog(env: str, catalog: str) -> str:
    """
    Adjusts the catalog name based on the environment.

    Args:
        catalog: The original catalog name.

    Returns:
        catalog: The adjusted catalog name based on the environment ('catalog_env' if not 'pr', else 'catalog').
    """
    try:
        if env != 'pr':
            catalog = catalog + '_' + env
        else:
            catalog
        
        return catalog
    except Exception as e:
        raise e