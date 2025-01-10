az login

# Dev Parameters - uncomment to Test Dev reosuuece deployments
$env = "dv"
$rg  = "ps-dw-dv-rg"
$sb  = "4280aaac-7665-42a5-9a8c-5af0590c5c52"

# UA Parameters
# $env = "ua"
# $rg  = "ps-dw-ua-rg"
# $sb  = "4280aaac-7665-42a5-9a8c-5af0590c5c52"

# PR Parameters
# $env = "pr"
# $rg  = "ps-dw-pr-rg"
# $sb  = "ceb330fa-91ee-4198-aca6-6d411b8ae0e9"


az account set --subscription $sb
az account show --output table

az deployment group create `
    --resource-group $rg `
    --template-file "./lakehouse/common-config/storage_account_metastore.bicep" `
    --parameters "./lakehouse/environments/${env}/_parameters.${env}.json" `
    --parameters "./lakehouse/environments/${env}/_parameters.storage_account_metastore.json" `
    --name "psdwdvstormeta" `
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