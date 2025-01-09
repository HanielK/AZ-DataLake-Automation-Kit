//common params
param deploymentLocation string
param org string
param environment string
param project string

//workspace specific params

param skuName string = 'premium'
param virtualNetworkName string
param publicSubnetName string
param privateSubnetName string
param logAnalyticsWorkspaceName string

//variables

var workspaceName = '${namingModule.outputs.naming_abbrevation}-dbx'
var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(workspaceName, resourceGroup().id)}'
var subscriptionId = subscription().subscriptionId
var managedResourceGroupResourceId = '/subscriptions/${subscriptionId}/resourceGroups/${managedResourceGroupName}'

module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org: org
    environment: environment
    project: project
  }
}

module workspace '../modules/databricks//workspace/main.bicep' = {
  name: 'workspaceDeployment'
  params: {
    // Required parameters
    name: '${namingModule.outputs.naming_abbrevation}-dbx'
    // Non-required parameters
    location: deploymentLocation
    skuName: skuName
    managedResourceGroupResourceId: managedResourceGroupResourceId
    customVirtualNetworkResourceId: '${resourceGroup().id}/providers/Microsoft.Network/virtualNetworks/${namingModule.outputs.naming_abbrevation}-${virtualNetworkName}'
    customPublicSubnetName: '${namingModule.outputs.naming_abbrevation}-${publicSubnetName}'
    customPrivateSubnetName: '${namingModule.outputs.naming_abbrevation}-${privateSubnetName}'
    disablePublicIp: true

    diagnosticSettings: [
      {
        name: 'diagnosticlogs'
        workspaceResourceId: resourceId(
          resourceGroup().name,
          'Microsoft.OperationalInsights/workspaces',
          '${namingModule.outputs.naming_abbrevation}-${logAnalyticsWorkspaceName}'
        )
        logCategoriesAndGroups: [
          {
            categoryGroup: 'AllLogs'
          }
        ]
      }
    ]
  }
}
