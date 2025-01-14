
//common params
param deploymentLocation string
param org string
param environment string
param project string

//storage
param storageAccountName string 
param storageSku string 
param storageKind string 
param containers array

//network
param additionalSubnetIds array = []
param existingSubnetNames array = ['sub-default', 'sub-dbx-priv', 'sub-dbx-pub']

// Prepare the virtual network rules
// Prepare existing subnet rules
// Local variable for VNet name prefix (this doesn't use module output)
var vnetNamePrefix = '${org}-${project}-${environment}'

// Prepare virtual network rules
var existingSubnetRules = [for subnetName in existingSubnetNames: {
  id: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${vnetNamePrefix}-vnet', '${vnetNamePrefix}-${subnetName}')
  ignoreMissingVnetServiceEndpoint: false
}]
var additionalSubnetRules = [for subnetId in additionalSubnetIds: {
  id: subnetId
  ignoreMissingVnetServiceEndpoint: false
}]
var allVirtualNetworkRules = concat(existingSubnetRules, additionalSubnetRules)


module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org : org
    environment: environment
    project: project
  }
}
module storageAccount '../modules/storage_account/main.bicep' = {
  name: 'storageAccountDeployment'
  params:{
    name: '${replace(namingModule.outputs.naming_abbrevation, '-', '')}${storageAccountName}'
    kind: storageKind
    location: deploymentLocation
    skuName: storageSku
    blobServices: {
      containers: containers
    }
    networkAcls: {

      bypass: 'AzureServices'
      defaultAction: 'Deny'
      ipRules: [
        {
          action: 'Allow'
          value: '4.59.172.35'
        }
        {
          action: 'Allow'
          value: '24.143.216.228'
        }
        {
          action: 'Allow'
          value: '66.44.198.240'
        }
        {
          action: 'Allow'
          value: '212.176.73.78'
        }     
      ]
      virtualNetworkRules: allVirtualNetworkRules
    }
    publicNetworkAccess: 'Enabled'
  }
}

