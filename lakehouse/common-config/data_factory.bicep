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

module factory '../modules/data-factory/factory/main.bicep' = {
  name: 'factoryDeployment'
  params: {
    name: '${namingModule.outputs.naming_abbrevation}-dfact'
    location: deploymentLocation
    managedIdentities: {
      systemAssigned: true
    }
    diagnosticSettings: [
      {
        name: 'diagnosticlogs'
        workspaceResourceId: resourceId(
          resourceGroup().name,
          'Microsoft.OperationalInsights/workspaces',
          '${namingModule.outputs.naming_abbrevation}-${logAnalyticsWorkspaceName}'
        )
        metricCategories: [
          { 
            category: 'AllMetrics' 
          }
        ]
        logCategoriesAndGroups: [
          {
            categoryGroup: 'allLogs'
          }
        ]
      }
    ]
  }
}
