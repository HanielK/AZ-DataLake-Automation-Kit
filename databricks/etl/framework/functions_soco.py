# Databricks notebook source
# MAGIC %md
# MAGIC #Common Functions
# MAGIC ## Overview
# MAGIC The purpose of this notebook is to host common functions used throughout the project
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
# MAGIC |2024-10-28 | Richard Thai (richard.thai@powersecure.com) | Initial development started. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependencies

# COMMAND ----------

# MAGIC %run /etl/framework/common_libraries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get_environment() -> str:
    """
    Determines the environment based on the cluster name in Spark configuration.
    
    Args:
        None.
    Returns:
        str: The environment name ('dv', 'ua', 'pr') or an empty string if not found.
    """
    try:
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterName").find('-dv-') > -1:
            return "dv"
        elif spark.conf.get("spark.databricks.clusterUsageTags.clusterName").find('-ua-') > -1:
            return "ua"
        elif spark.conf.get("spark.databricks.clusterUsageTags.clusterName").find('-pr-') > -1:
            return "pr"
        else:
            return ""
    except Exception as e:
        raise e


# COMMAND ----------

def get_catalog(catalog: str) -> str:
    """
    Adjusts the catalog name based on the environment.

    Args:
        catalog: The original catalog name.

    Returns:
        catalog: The adjusted catalog name based on the environment ('catalog_env' if not 'pr', else 'catalog').
    """
    env = get_environment()
    try:
        if env != 'pr':
            catalog = catalog + '_' + env
        else:
            catalog
        
        return catalog
    except Exception as e:
        raise e


# COMMAND ----------

def get_workspace() -> str:
    """
    Determines the workspace based on the cluster name in Spark configuration.

    Args:
        None.
        
    Returns:
        str: The workspace name ('de', 'da') or an empty string if not found.
    """
    try:
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterName").find('-de-') > -1:
            return "de"
        elif spark.conf.get("spark.databricks.clusterUsageTags.clusterName").find('-da-') > -1:
            return "da"
        else:
            return ""
    except Exception as e:
        raise e


# COMMAND ----------

def to_bool(value: str) -> bool:
    """
    Converts a string representation of a boolean value to a Python boolean.

    Args:
        value: The string representation of a boolean ('true', 'false', 't', 'f', '1', '0').

    Returns:
        bool: The corresponding Python boolean value.

    Raises:
        ValueError: If the input value is not a valid boolean string.
    """
    try:
        valid = {'true': True, 't': True, '1': True, 'false': False, 'f': False, '0': False}

        if isinstance(value, bool):
            return value

        if not isinstance(value, str):
            raise ValueError('Invalid literal for boolean. Not a string.')

        lower_value = value.lower()
        
        if lower_value in valid:
            return valid[lower_value]
        else:
            raise ValueError('Invalid literal for boolean: "%s"' % value)
    except Exception as e:
        raise e


# COMMAND ----------

def bool_to_str(value):
    """
    Converts a boolean value to its string representation.

    Args:
        value (bool): The boolean value to convert.

    Returns:
        str: The string representation of the boolean ('True' or 'False').
    """
    try:
        if value == 1:
            return "True"
        else:
            return "False"
    except Exception as e:
        raise e


# COMMAND ----------

def print_variable(name: str, value) -> None:
    """
    Prints a variable name and its corresponding value in a formatted message.

    Args:
        name (str): The name of the variable.
        value: The value of the variable (can be of any type).

    Returns:
        None
    """
    try:
        message = '{:<30}: {:<60}'
        print(message.format(name, value))
    except Exception as e:
        raise e


# COMMAND ----------

def list_files(path: str) -> list:
    """
    Lists files in the specified path using dbutils.fs.ls.

    Args:
        path: The path to list files from.

    Returns:
        list: A list of file information objects, or None if an error occurs.
    """
    try:
        return dbutils.fs.ls(path)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return None
        else:
            raise e

# COMMAND ----------

def file_exists(path: str) -> bool:
    """
    Checks if a file or directory exists at the specified path.
    This function is intended to return True if a file in the provided path exists,
    and False if the file or directory does not exist.
    Example of use:
    if not file_exists(credentials_file_path):
                credentials = self.get_temporary_credentials(api_endpoint)
                self.store_credentials(credentials, credentials_file_path)
    in this case, if the credentials file does not exist, it will be created.
    
    Args:
        path: The path to check for existence.

    Returns:
        bool: True if the file or directory exists, False otherwise.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return False
        else:
            raise e

# COMMAND ----------

def run_parallel_notebooks(notebooks: list, max_workers: int) -> list:
    """
    Runs notebooks in parallel using ThreadPoolExecutor.

    Args:
        notebooks: List of Notebook objects to run.
        max_workers: Maximum number of worker threads to use.

    Returns:
        list: List of futures representing the notebook execution tasks.
    """
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as tpe:
            return [tpe.submit(Notebook.run, notebook) for notebook in notebooks]
    except Exception as e:
        raise e

# COMMAND ----------

def run_parallel_functions(functions: list) -> None:
    """
    Executes a list of functions in parallel using a ThreadPoolExecutor.

    Args:
        functions: A list of functions to be executed in parallel.

    Returns:
        None

    Raises:
        None

    Example:
        functions = [function1, function2, function3]
        run_parallel_functions(functions)
    """
    try:
        # Create a ThreadPoolExecutor
        executor = concurrent.futures.ThreadPoolExecutor()

        # Submit the functions to the ThreadPoolExecutor
        futures = [executor.submit(func) for func in functions]

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)
    except Exception as e:
        raise e