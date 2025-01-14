
//common params
param deploymentLocation string
param org string
param environment string
param project string


//virtual network parameters
param vnetAddressPrefix string 
param vnetSubnets array

param natGatewayName string
module namingModule '../modules/naming/main.bicep' = {
  name: 'namingModule'
  params: {
    org : org
    environment: environment
    project: project
  }
}

//virtual network
module virtualNetwork '../modules/network/virtual_network/main.bicep' = {
  name: 'virtualNetworkDeployment'
  params: {
    name: '${namingModule.outputs.naming_abbrevation}-vnet'
    addressPrefixes: [
      vnetAddressPrefix
    ]
    location: deploymentLocation
    subnets: [for subnet in vnetSubnets: {
      name: '${namingModule.outputs.naming_abbrevation}-${subnet.name}'
      addressPrefix: subnet.addressPrefix
      networkSecurityGroupResourceId: '${resourceGroup().id}/providers/Microsoft.Network/networkSecurityGroups/${namingModule.outputs.naming_abbrevation}-${subnet.networkSecurityGroupName}'
      delegations: subnet.delegations
      ServiceEndpoints: subnet.ServiceEndpoints
      natGatewayResourceId: subnet.natGatewayEnabled ?  resourceId(resourceGroup().name, 'Microsoft.Network/natGateways', '${namingModule.outputs.naming_abbrevation}-${natGatewayName}') : ''
    }]
  }
  dependsOn: [networkSecurityGroup, defaultNetworkSecurityGroup]
  // Add the following line to make subnet operations one at a time
}

output vnetId string = virtualNetwork.outputs.resourceId

//Network Security Group
module networkSecurityGroup '../modules/network/network_security_group/main.bicep' = {
  name: 'networkSecurityGroupDeployment'
  params: {
    name: '${namingModule.outputs.naming_abbrevation}-nsg-dbx'
    location: deploymentLocation
    securityRules: [
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound'
        properties: {
          priority: 100
          direction: 'Inbound'
          access: 'Allow'
          protocol: '*'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '*'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-ssh'
        properties: {
          priority: 101
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'AzureDatabricks'
          destinationPortRange: '22'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-proxy'
        properties: {
          priority: 102
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'AzureDatabricks'
          destinationPortRange: '5557'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
      {
        name: 'AllowVnetInBound'
        properties: {
          priority: 103
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '*'
          destinationAddressPrefix: '*'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound'
        properties: {
          priority: 100
          direction: 'Outbound'
          access: 'Allow'
          protocol: '*'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '*'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp'
        properties: {
          priority: 101
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '443'
          destinationAddressPrefix: 'AzureDatabricks'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-sql'
        properties: {
          priority: 102
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '3306'
          destinationAddressPrefix: 'Sql'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-storage'
        properties: {
          priority: 103
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '443'
          destinationAddressPrefix: 'Storage'
        }
      }
      {
        name: 'Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-eventhub'
        properties: {
          priority: 104
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '9093'
          destinationAddressPrefix: 'EventHub'
        }
      }
      {
        name: 'AllowVnetOutBound'
        properties: {
          priority: 105
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: '*'
          destinationPortRange: '*'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
    ]
  }
}

output nsgId string = networkSecurityGroup.outputs.resourceId


//Create a default network security group and a rule in it
module defaultNetworkSecurityGroup '../modules/network/network_security_group/main.bicep' = {
  name: 'defaultNetworkSecurityGroupDeployment'
  params: {
    name: '${namingModule.outputs.naming_abbrevation}-nsg-default'
    location: deploymentLocation
    securityRules: [
      {
        name: 'AllowVnetInBound'
        properties: {
          priority: 100
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationPortRange: '*'
          destinationAddressPrefix: '*'
        }
      }
      {
        name: 'AllowVnetOutBound'
        properties: {
          priority: 101
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: '*'
          destinationPortRange: '*'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
    ]
  }
}
