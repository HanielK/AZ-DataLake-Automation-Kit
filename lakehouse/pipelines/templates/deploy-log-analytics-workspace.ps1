az login

# Dev Parameters - uncomment to Test Dev reosuuece deployments
# $env = "dv"
# $rg  = "ps-dw-dv-rg"
# $sb  = "144af092-409d-455c-b4c2-12a728f76ff5"

# UA Parameters
# $env = "ua"
# $rg  = "ps-dw-ua-rg"
# $sb  = "144af092-409d-455c-b4c2-12a728f76ff5"

# PR Parameters
$env = "pr"
$rg  = "ps-dw-pr-rg"
$sb  = "ceb330fa-91ee-4198-aca6-6d411b8ae0e9"


az account set --subscription $sb
az account show --output table

az deployment group create `
    --resource-group $rg `
    --template-file "./lakehouse/common-config/log_analytics_workspace.bicep" `
    --parameters "./lakehouse/environments/${env}/_parameters.${env}.json" `
    --parameters "./lakehouse/environments/${env}/_parameters.log_analytics_workspace.json" `
    --name "ps-dw-ua-log" `
    --mode Incremental `
    --what-if


# **************************************************************************
#  Notes... deploy resources accodrding to the following sequence
# **************************************************************************
# =-> Deploy Log Analytics Workspace
# =-> Deploy Nat Gateway
# =-> Deploy Virtual Network
# =-> Deploy Data Storage Account
# =-> Deploy Meta Storage Account
# =-> Deploy Key Vault
# =-> Deploy Event Hub Namepsace
# =-> Deploy Data Factory
# =-> Deploy Databricks Workspace
# =-> Deploy DataBricks Access Connector
# ---------------------------------------------------------------------------