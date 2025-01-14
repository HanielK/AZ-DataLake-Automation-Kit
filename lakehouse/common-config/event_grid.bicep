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


// module systemTopic '../../modules/event-grid/system-topic/main.bicep' = {
//   name: 'systemTopicDeployment'
//   params: {
//     name: '${namingModule.outputs.naming_abbrevation}-evgrid'
//     location: deploymentLocation
//   }
