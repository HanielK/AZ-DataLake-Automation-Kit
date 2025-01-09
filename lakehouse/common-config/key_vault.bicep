
//common params
param deploymentLocation string
param org string
param environment string
param project string

//keyvault params
param enablePurgeProtection bool

module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org : org
    environment: environment
    project: project
  }
}

module keyVault '../modules/key_vault/main.bicep' = {
  name: 'keyVaultDeployment'
  params: {
    // Required parameters
    name: '${namingModule.outputs.naming_abbrevation}-kv'
    // Non-required parameters
    enablePurgeProtection: enablePurgeProtection
    location: deploymentLocation
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
      virtualNetworkRules: [
        {
          id: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${namingModule.outputs.naming_abbrevation}-vnet', '${namingModule.outputs.naming_abbrevation}-sub-default')
          ignoreMissingVnetServiceEndpoint: false
        }
        {
          id: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${namingModule.outputs.naming_abbrevation}-vnet', '${namingModule.outputs.naming_abbrevation}-sub-dbx-priv')
          ignoreMissingVnetServiceEndpoint: false
        }
        {
          id: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${namingModule.outputs.naming_abbrevation}-vnet', '${namingModule.outputs.naming_abbrevation}-sub-dbx-pub')
          ignoreMissingVnetServiceEndpoint: false
        }
      ]
      ipRules: [
        {
         
          value: '4.59.172.35'
        }
        {
         
          value: '24.143.216.228'
        }
        {
          
          value: '66.44.198.240'
        }
        {
          
          value: '212.176.73.78'
        }
      ]
    }
    publicNetworkAccess: 'Enabled'
  }
}
