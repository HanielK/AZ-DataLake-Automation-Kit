//common params
param deploymentLocation string
param org string
param environment string
param project string

module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org : org
    environment: environment
    project: project
  }
}


module accessConnector '../modules/databricks/access-connector/main.bicep' = {
  name: 'accessConnector'
  params: {
    name: '${namingModule.outputs.naming_abbrevation}-ac-dbx'
    location: deploymentLocation
    managedIdentities: {
      systemAssigned: true
    }
  }
}

resource accessConnectorRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(resourceGroup().id, 'ac-dbx', 'StorageBlobDataContributor')
  properties: {
    principalId: accessConnector.outputs.systemAssignedMIPrincipalId
    principalType: 'ServicePrincipal' // Set the principalType into the Bicep, as ServicePrincipal for Azure platform to wait for the replication to complete
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') 
  }
  scope: resourceGroup()
  dependsOn: [
    accessConnector
  ]
}

