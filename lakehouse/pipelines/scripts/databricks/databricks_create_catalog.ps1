param (
    [string]$DatabricksHost = "https://adb-2682422710237035.15.azuredatabricks.net",
    [string]$JsonFilePath = "C:\Users\X2ATHUMA\Desktop\catalogs.json"
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

# Read the JSON file
$catalogConfigs = Get-Content $JsonFilePath | ConvertFrom-Json

# Function to verify Unity Catalog permissions
function Test-UnityCatalogPermissions {
    try {
        # First, check if the user is a metastore admin
        $metastoreResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/current-metastore-assignment" -Method Get -Headers $databricksHeaders
        $metastoreId = $metastoreResponse.metastore_id

        $permissionsResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/permissions/$metastoreId" -Method Get -Headers $databricksHeaders
        
        $isMetastoreAdmin = $permissionsResponse.privilege_assignments | Where-Object { $_.principal -eq "account users" -and $_.privileges -contains "USE_CATALOG" }

        if ($isMetastoreAdmin) {
            Write-Host "User is a metastore admin. Sufficient permissions verified."
            return $true
        }

        # If not a metastore admin, check for CREATE_CATALOG permission
        $createCatalogPermission = $permissionsResponse.privilege_assignments | Where-Object { $_.principal -eq $env:USERNAME -and $_.privileges -contains "CREATE_CATALOG" }

        if ($createCatalogPermission) {
            Write-Host "User has CREATE_CATALOG permission. Sufficient permissions verified."
            return $true
        }

        Write-Host "User does not have sufficient permissions to create catalogs."
        return $false
    }
    catch {
        Write-Host "Error: Unable to verify Unity Catalog permissions."
        Write-Host "Error details: $_"
        return $false
    }
}

# Function to create or update catalog
function Set-Catalog {
    param (
        [string]$CatalogName,
        [string]$Comment,
        [hashtable]$Properties
    )
    $catalogBody = @{
        name       = $CatalogName
        comment    = $Comment
        properties = $Properties
    } | ConvertTo-Json

    try {
        $createCatalogResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/catalogs" -Method Post -Headers $databricksHeaders -Body $catalogBody
        Write-Host "Catalog created successfully: $($createCatalogResponse.name)"
    }
    catch {
        if ($_.Exception.Response.StatusCode -eq 'Conflict') {
            Write-Host "Catalog $CatalogName already exists. Attempting to update..."
            try {
                $updateCatalogResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/catalogs/$CatalogName" -Method Patch -Headers $databricksHeaders -Body $catalogBody
                Write-Host "Catalog updated successfully: $($updateCatalogResponse.name)"
            }
            catch {
                Write-Host "Error updating catalog: $_"
            }
        }
        else {
            Write-Host "Error creating/updating catalog: $_"
        }
    }
}

# Function to set owner of catalog
function Set-CatalogOwner {
    param (
        [string]$CatalogName,
        [string]$Owner
    )
    $ownerBody = @{
        owner = $Owner
    } | ConvertTo-Json

    try {
        $updateOwnerResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/catalogs/$CatalogName" -Method Patch -Headers $databricksHeaders -Body $ownerBody
        Write-Host "Owner for catalog $CatalogName set successfully to $Owner"
    }
    catch {
        Write-Host "Error setting owner for catalog $($CatalogName): $_"
    }
}

# Main execution
if (Test-UnityCatalogPermissions) {
    # Process each catalog in the JSON
    foreach ($catalog in $catalogConfigs.catalogs) {
        $catalogName = $catalog.name
        $comment = $catalog.comment
        $properties = $catalog.properties
        $owner = $catalog.owner

        # Create or update catalog
        Set-Catalog -CatalogName $catalogName -Comment $comment -Properties $properties

        # If owner is specified, set the owner
        if ($owner) {
            Set-CatalogOwner -CatalogName $catalogName -Owner $owner
        }
    }
    Write-Host "All catalogs processed successfully"
}
else {
    Write-Host "Script execution stopped due to insufficient permissions."
}