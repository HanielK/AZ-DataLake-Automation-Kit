//common params
param deploymentLocation string
param org string
param environment string
param project string


//log analytics workspace name
param logAnalyticsWorkspaceName string

module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org : org
    environment: environment
    project: project
  }
}


module natGatewayModule '../modules/network/nat-gateway/main.bicep' = {
  name: 'natGatewayDeployment'
  params: {
    location: deploymentLocation
    name: '${namingModule.outputs.naming_abbrevation}-nat'
    zone: 1
    publicIPAddressObjects: [
      {
        name: '${namingModule.outputs.naming_abbrevation}-pip-nat'
        sku: 'Standard'
        publicIPAllocationMethod: 'Static'
        publicIPAddressVersion: 'IPv4'
        skuTier: 'Regional'
        zones: [1,2,3]
        diagnosticSettings: [
          {
            name: '${namingModule.outputs.naming_abbrevation}-nat-pip-diag'
            workspaceResourceId: resourceId(resourceGroup().name, 'Microsoft.OperationalInsights/workspaces', '${namingModule.outputs.naming_abbrevation}-${logAnalyticsWorkspaceName}')
            logs: [
              {
                categoryGroup: 'audit'
                enabled: true
              }
            ]
          }
        ]
      }
    ]

  }
}
