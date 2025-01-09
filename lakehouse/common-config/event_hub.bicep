//common params
param deploymentLocation string
param org string
param environment string
param project string

param logAnalyticsWorkspaceName string

module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org: org
    environment: environment
    project: project
  }
}

module eventHubNamespace '../modules/event_hub/namespace/main.bicep' = {
  name: 'eventHubNamespaceDeployment'
  params: {
    name: '${namingModule.outputs.naming_abbrevation}-evhns'
    location: deploymentLocation
    eventhubs: [
      {
        name: '${namingModule.outputs.naming_abbrevation}-evh'
      }
    ]
    managedIdentities: {
      systemAssigned: true
    }
    publicNetworkAccess: 'Disabled'
    networkRuleSets: {
      defaultAction: 'Deny'
      virtualNetworkRules: [
        {
          subnetResourceId: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${namingModule.outputs.naming_abbrevation}-vnet', '${namingModule.outputs.naming_abbrevation}-sub-default')
          ignoreMissingVnetServiceEndpoint: false
        }
        {
          subnetResourceId: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${namingModule.outputs.naming_abbrevation}-vnet', '${namingModule.outputs.naming_abbrevation}-sub-dbx-priv')
          ignoreMissingVnetServiceEndpoint: false
        }
        {
          subnetResourceId: resourceId(resourceGroup().name, 'Microsoft.Network/virtualNetworks/subnets', '${namingModule.outputs.naming_abbrevation}-vnet', '${namingModule.outputs.naming_abbrevation}-sub-dbx-pub')
          ignoreMissingVnetServiceEndpoint: false
        }
      ]
      ipRules: [
        {
          action: 'Allow'
          ipMask: '146.126.51.51'
        }
        {
          action: 'Allow'  
          ipMask: '146.126.61.241'
        }
      ]
    }
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
            categoryGroup: 'audit'   
          }
        ]
      }
    ]
    
  }
}
