param (
    [Parameter(Mandatory=$true)]
    [string]$DatabricksHost,

    [Parameter(Mandatory=$true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory=$true)]
    [string]$AdminGroupName,

    [Parameter(Mandatory=$true)]
    [string]$UserGroupName,

    [Parameter(Mandatory=$true)]
    [string]$MetastoreName
)

$ErrorActionPreference = 'Stop'

# Function to get Azure AD token
function Get-AzureADToken {
    param (
        [string]$Resource = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
    )
    $azAccessToken = az account get-access-token --resource=$Resource --query accessToken --output tsv
    if (-not $azAccessToken) {
        throw "Failed to obtain Azure access token for resource $Resource"
    }
    return $azAccessToken
}

# Get the Azure AD token for Databricks
$databricksToken = Get-AzureADToken
Write-Host "Databricks token obtained successfully"

# Construct the headers
$databricksHeaders = @{
    "Authorization" = "Bearer $databricksToken"
    "Content-Type"  = "application/json"
}

# Function to get Metastore ID from Metastore Name
function Get-MetastoreId {
    param (
        [string]$MetastoreName
    )
    try {
        $response = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/metastores" -Method Get -Headers $databricksHeaders
        $metastore = $response.metastores | Where-Object { $_.name -eq $MetastoreName }
        if ($metastore) {
            return $metastore.metastore_id
        } else {
            Write-Host "Metastore with name '$MetastoreName' not found."
            return $null
        }
    }
    catch {
        Write-Host "Error retrieving metastore ID: $_"
        return $null
    }
}

# Function to add workspace to Unity Catalog
function Add-WorkspaceToUnityCatalog {
    param (
        [string]$WorkspaceId,
        [string]$MetastoreId
    )
    $body = @{
        workspace_id = $WorkspaceId
        metastore_id = $MetastoreId
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/workspaces/$WorkspaceId/metastore" -Method Put -Headers $databricksHeaders -Body $body
        Write-Host "Workspace $WorkspaceId successfully added to Unity Catalog metastore $MetastoreId"
        return $true
    }
    catch {
        Write-Host "Error adding workspace to Unity Catalog: $_"
        return $false
    }
}

# Function to check if a group exists
function Test-GroupExists {
    param (
        [string]$GroupName
    )
    try {
        $response = Invoke-RestMethod -Uri "$DatabricksHost/api/2.0/groups/list" -Method Get -Headers $databricksHeaders
        return $response.group_names -contains $GroupName
    }
    catch {
        Write-Host "Error checking if group exists: $_"
        return $false
    }
}

# Function to add group to metastore
function Add-GroupToMetastore {
    param (
        [string]$GroupName,
        [string]$MetastoreId,
        [string]$PermissionLevel
    )
    if (-not (Test-GroupExists -GroupName $GroupName)) {
        Write-Host "Group $GroupName not found. Skipping..."
        return $false
    }

    $body = @{
        changes = @(
            @{
                principal = $GroupName
                add = @($PermissionLevel)
            }
        )
    } | ConvertTo-Json -Depth 3

    try {
        $response = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/permissions/$MetastoreId" -Method Patch -Headers $databricksHeaders -Body $body
        Write-Host "Group $GroupName successfully added to metastore $MetastoreId with $PermissionLevel permissions"
        return $true
    }
    catch {
        Write-Host "Error adding group to metastore: $_"
        return $false
    }
}

# Main execution
$metastoreId = Get-MetastoreId -MetastoreName $MetastoreName
if (-not $metastoreId) {
    Write-Host "Script execution failed. Unable to find metastore with name: $MetastoreName"
    exit
}

$workspaceAdded = Add-WorkspaceToUnityCatalog -WorkspaceId $WorkspaceId -MetastoreId $metastoreId

if ($workspaceAdded) {
    # Add admin group
    $adminAdded = Add-GroupToMetastore -GroupName $AdminGroupName -MetastoreId $metastoreId -PermissionLevel "USE_CATALOG"
    
    # Add user group
    $userAdded = Add-GroupToMetastore -GroupName $UserGroupName -MetastoreId $metastoreId -PermissionLevel "USE_CATALOG"

    if ($adminAdded -or $userAdded) {
        Write-Host "Script executed successfully. Workspace added to Unity Catalog and available groups configured."
    } else {
        Write-Host "Script completed. Workspace added to Unity Catalog, but no groups were added. Please check if the specified groups exist."
    }
} else {
    Write-Host "Script execution failed. Unable to add workspace to Unity Catalog."
}