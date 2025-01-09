az login

$env = "dv"
$rg  = "ps-dw-dv-rg"
$sb  = "144af092-409d-455c-b4c2-12a728f76ff5"

az account set --subscription $sb
az account show --output table

az deployment group create `
    --resource-group $rg `
    --template-file "./common-config/log_analytics_workspace.bicep" `
    --parameters "./environments/${env}/_parameters.${env}.json" `
    --parameters "./environments/${env}/_parameters.log_analytics_workspace.json" `
    --name "DougLeal-TestDeployment" `
    --mode Incremental `
    --what-if


# **************************************************************************
#  Notes...
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
# az deployment group create `
#     --resource-group $rg `
#     --template-file "./common-config/virtual_network.bicep" `
#     --parameters "./environments/${env}/_parameters.${env}.json" `
#     --parameters "./environments/${env}/_parameters.virtual_network.json" `
#     --name "DougLeal-TestDeployment" `
#     --mode Incremental `
#     --what-if