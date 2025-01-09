param (
  [Parameter(Mandatory=$true)]
  [string]$DatabricksHost,

  [Parameter(Mandatory=$true)]
  [string]$ClusterConfigPath
)

$ErrorActionPreference = 'Stop'

# Databricks host
$databricksHost = $DatabricksHost
Write-Host "Databricks host: $databricksHost"

# Get the Azure Resource Manager token
$azAccessToken = az account get-access-token --resource=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken --output tsv

if (-not $azAccessToken) {
  throw "Failed to obtain Azure access token"
}

Write-Host "Azure access token obtained successfully"

# Read cluster configuration from JSON file
$clusterConfigPath = $ClusterConfigPath
write-host "Cluster config path: $clusterConfigPath"
if (-Not (Test-Path $clusterConfigPath)) {
  throw "Cluster configuration file not found: $clusterConfigPath"
}

$clusterBody = Get-Content -Path $clusterConfigPath -Raw | ConvertFrom-Json

write-host "Cluster configuration loaded successfully"
write-host "Cluster configuration: $clusterBody"

$databricksHeaders = @{
  "Authorization" = "Bearer $azAccessToken"
  "Content-Type"  = "application/json"
}

$createClusterResponse = Invoke-RestMethod -Uri ($databricksHost + "/api/2.0/clusters/create") -Method Post -Headers $databricksHeaders -Body ($clusterBody | ConvertTo-Json)

if ($createClusterResponse) {
  Write-Host "Cluster created successfully"
} else {
  throw "Error creating cluster: $createClusterResponse"
}