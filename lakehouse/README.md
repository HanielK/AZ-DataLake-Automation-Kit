# DataLake Automation Kit

# Overview
This project contains a collection of bicep modules, environment specific configurations, scripts and pipelines to automate the deployment of a Data Lake in Azure. Below are the step by step instructions to deploy the Data Lake and also extend if we need to deploy additional components.

# General Guidance
- The kit is designed to be used with Azure DevOps pipelines. This gives us a set of pipelines to create and also manage the datalake environment.
The pipelines are as follows:
    - `infra-pipeline` - This pipeline is used to deploy the infrastructure components like VNET, Subnets, NSG, etc.
    - `datalake-compute-pipeline` - This pipeline is used to deploy the compute components in the databricks workspace.
    - `datalake-sql-warehouse-pipeline` - This pipeline is used to deploy the sql warehouse components.
    - `databricks-allowlist-pipeline` - This pipeline is used to AllowList specific IPs on the databricks workspace.


## Prerequisites
- Resoruce Group in Azure per environment should already be provisioned.
- Azure DevOps Repo.
- A Service Principal with Owner Access on the resource group.


## Usage
`1.)`The first step is to clone the "datalake-kit" repository to your local machine and push it to your desierd project location. The repo consists of the following folders:
- `common-config` - Contains the common bicep configurations which are used across all the environments. We define these only once and these are used in all the environments.
- `environments` - Contains the environment specific configurations.
- `modules` - Contains the bicep modules. Acutal resource code is written in these modules and wont change unless needed.
- `pipelines` - Contains the Azure DevOps pipelines.


`2.)`Once this repository is cloned, navigate to the `environments` folder and look at the existing environments. The environments are named as per the environment name i.e sb,dv,ua,prd.
Each environment folder containes  parameter files for a specific component. The bicep configuration file which is under common-config folder is used to deploy the component and the parameter  file under environment is used to pass the parameters to the bicep configuration file.

`3.)`The folder has a file called as _parameters.ENVNAME.json. This file contains the parameters which are commond for all the components in the environment. The parameters are as follows:
- `org` - The organization name, value is "sc"
- `project` - The project name, replace the value with the project name.
- `environment` - The environment name, replace the value with the environment name. Example - sb,dv,ua,prd
- `deploymentLocation` - The location where the resources will be deloyed, replace the value with the location name. Example - eastus, westus

#### `Note`: 
Each componenet has its own dedicated parameter json file as well so that the specific paramters related to the componets are passed in that file and also if we need to override any parameters which are coming from the "paramters.ENVNAME.json" file.


`4.)`Please check the parameters file and change it according to the environment. Like sku chnages, vnet cidr changes, etc.
     For the databricks compute and the sql warehouse cluster, please check the config files under the databricks-compute and databricks-sqlwarehouse folder in each env. They are json files and have the configurations for the databricks compute and the sql warehouse cluster respectively. Please update the configurations as per the requirement.

`5.)` Update the Variables file which are named in the format of `variables.ENVNAME.yml` in the `pipelines` folder. Please update the azureServiceConnection and agentPool to the correct values.

`6.)` Once the parameters and variables are updated, commit the changes to the repository.

`7.)` Create a new pipeline in Azure DevOps and select the repository where the code is pushed.

`8.)` Select the `infra-pipeline.yml` file from the `pipelines` folder and click on save.

`9.)` Once the pipeline is created, click on the `Run` button to run the pipeline.

`10.)` Once the pipeline is completed, the infrastructure components will be deployed in the Azure environment.

`11.)` Repeat the steps 7 to 10 for the `datalake-compute-pipeline.yml` and `datalake-sql-warehouse-pipeline.yml` files.

`12.)` Once all the pipelines are completed, the Data Lake environment will be deployed in the Azure environment.

`13.)` If we need to AllowList specific IPs on the databricks workspace, we can use the `databricks-allowlist-pipeline.yml` file.

`14.)` The pipeline will deploy the databricks workspace and AllowList the IPs in the databricks workspace.

`17.)` The Data Lake environment is now ready to use.