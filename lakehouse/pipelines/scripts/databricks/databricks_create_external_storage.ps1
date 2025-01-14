param (
    [Parameter(Mandatory = $true)]
    [string]$DatabricksHost,
    

    [Parameter(Mandatory = $true)]
    [string]$AccessConnectorID,

    [Parameter(Mandatory = $true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true)]
    [string]$JsonFilePath
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

# Get the Azure Management token
$managementToken = Get-AzureADToken -Resource "https://management.azure.com/"
Write-Host "Azure Management token obtained successfully"

# Construct the headers
$databricksHeaders = @{
    "Authorization" = "Bearer $databricksToken"
    "Content-Type"  = "application/json"
}

# Get the Access Connector ID
$accessConnectorId = $AccessConnectorID
#$accessConnectorId = az databricks access-connector show --name $AccessConnectorName --resource-group $ResourceGroupName --query id -o tsv

# if (-not $accessConnectorId) {
#     throw "Failed to retrieve Access Connector ID for connector named: $AccessConnectorName"
# }

Write-Host "Access Connector ID retrieved successfully: $accessConnectorId"

# Verify if the Access Connector exists
# $accessConnectorApiVersion = "2022-04-01-preview"
# $accessConnectorUrl = "https://management.azure.com$accessConnectorId`?api-version=$accessConnectorApiVersion"
# $azureHeaders = @{
#     "Authorization" = "Bearer $managementToken"
#     "Content-Type"  = "application/json"
# }

# try {
#     $accessConnectorResponse = Invoke-RestMethod -Uri $accessConnectorUrl -Method Get -Headers $azureHeaders
#     Write-Host "Access Connector '$AccessConnectorName' found."
# }
# catch {
#     throw "Access Connector '$AccessConnectorName' not found or you don't have permission to access it. Error: $_"
# }

# Read the JSON file
$storageMappings = Get-Content $JsonFilePath | ConvertFrom-Json

# Function to create or update storage credential
function Test-UnityCatalogPermissions {
    try {
        # Attempt to list storage credentials as a permissions check
        $response = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/storage-credentials" -Method Get -Headers $databricksHeaders
        Write-Host "Unity Catalog permissions verified successfully."
        Write-Host $response
        return $true
    }
    catch {
        Write-Host "Error: Unable to verify Unity Catalog permissions. Please ensure you have the necessary permissions to manage storage credentials and external locations."
        Write-Host "Error details: $_"
        return $false
    }
}

# Function to create or update storage credential
# Function to create or update storage credential
function Set-StorageCredential {
    param (
        [string]$CredentialName,
        [string]$AccessConnectorId
    )
    $storageCredentialBody = @{
        name = $CredentialName
        azure_managed_identity = @{
            access_connector_id = $AccessConnectorId
        }
    } | ConvertTo-Json

    try {
        $createCredentialResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/storage-credentials" -Method Post -Headers $databricksHeaders -Body $storageCredentialBody
        Write-Host "Storage credential created successfully: $($createCredentialResponse.name)"
    }
    catch {
        $errorDetails = $_ | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($errorDetails.error_code -eq "PERMISSION_DENIED") {
            Write-Host "Error: Permission denied while creating storage credential '$CredentialName'."
            Write-Host "Please ensure you have the necessary permissions in the Unity Catalog metastore."
            Write-Host "Metastore: $($errorDetails.message.resourceName)"
            
        }
        elseif ($_.Exception.Response.StatusCode -eq 'Conflict') {
            Write-Host "Storage credential $CredentialName already exists. Attempting to update..."
            try {
                $updateCredentialResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/storage-credentials/$CredentialName" -Method Patch -Headers $databricksHeaders -Body $storageCredentialBody
                Write-Host "Storage credential updated successfully: $($updateCredentialResponse.name)"
            }
            catch {
                Write-Host "Error updating storage credential: $_"
            }
        }
        else {
            Write-Host "Error creating/updating storage credential: $_"
        }
    }
}
function Set-ExternalLocation {
    param (
        [string]$LocationName,
        [string]$Url,
        [string]$CredentialName
    )
    $externalLocationBody = @{
        name = $LocationName
        url = $Url
        credential_name = $CredentialName
    }
    
    $externalLocationBodyJson = $externalLocationBody | ConvertTo-Json

    try {
        $createLocationResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/external-locations" -Method Post -Headers $databricksHeaders -Body $externalLocationBodyJson
        Write-Host "External location created successfully: $($createLocationResponse.name)"
        return $true
    }
    catch {
        $errorDetails = $_ | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($errorDetails.error_code -eq "PERMISSION_DENIED") {
            Write-Host "Error: Permission denied while creating external location '$LocationName'."
            Write-Host "Please ensure you have the necessary permissions in the Unity Catalog metastore."
            Write-Host "Metastore: $($errorDetails.message.resourceName)"
        }
        elseif ($_.Exception.Response.StatusCode -eq 'Conflict') {
            Write-Host "External location $LocationName already exists. Attempting to update..."
            try {
                $updateLocationResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/external-locations/$LocationName" -Method Patch -Headers $databricksHeaders -Body $externalLocationBodyJson
                Write-Host "External location updated successfully: $($updateLocationResponse.name)"
                return $true
            }
            catch {
                Write-Host "Error updating external location: $_"
            }
        }
        else {
            Write-Host "Error creating/updating external location: $_"
        }
    }
    return $false
}

# Function to set owner of external location
function Set-ExternalLocationOwner {
    param (
        [string]$LocationName,
        [string]$Owner
    )
    $ownerBody = @{
        owner = $Owner
    } | ConvertTo-Json

    try {
        $updateOwnerResponse = Invoke-RestMethod -Uri "$DatabricksHost/api/2.1/unity-catalog/external-locations/$LocationName" -Method Patch -Headers $databricksHeaders -Body $ownerBody
        Write-Host "Owner for external location $LocationName set successfully to $Owner"
    }
    catch {
        Write-Host "Error setting owner for external location $LocationName $_"
    }
}

# Main execution
if (Test-UnityCatalogPermissions) {
    # Process each storage account in the JSON
    foreach ($storageAccount in $storageMappings.storage_accounts) {
        $storageAccountName = $storageAccount.name
        $credentialName = $storageAccount.credential_name

        # Create or update storage credential
        Set-StorageCredential -CredentialName $credentialName -AccessConnectorId $accessConnectorId

        # Process each container and external location mapping
        foreach ($mapping in $storageAccount.mappings) {
            $containerName = $mapping.container_name
            $externalLocationName = $mapping.external_location_name
            $url = "abfss://$containerName@$storageAccountName.dfs.core.windows.net/"
            $owner = $mapping.owner

            # Create or update external location
            $locationCreated = Set-ExternalLocation -LocationName $externalLocationName -Url $url -CredentialName $credentialName

            # If location was created/updated successfully and owner is specified, set the owner
            if ($locationCreated -and $owner) {
                Set-ExternalLocationOwner -LocationName $externalLocationName -Owner $owner
            }
        }
    }
    Write-Host "All storage accounts, credentials, and external locations processed successfully"
}
else {
    Write-Host "Script execution stopped due to insufficient permissions."
}