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

module workspace '../modules/log_analytics_workspace/main.bicep' = {
  name: 'workspaceDeployment'
  params: {
    // Required parameters
    name: '${namingModule.outputs.naming_abbrevation}-log'
    // Non-required parameters
    location: deploymentLocation
  }
}
