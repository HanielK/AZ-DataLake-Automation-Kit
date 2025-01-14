#!/bin/bash

# Check if correct number of arguments are passed
if [ "$#" -ne 2 ]; then
    echo "Usage: ./create_file_for_component.sh <component> <environment>"
    exit 1
fi

# Assign arguments to variables
component=$1
environment=$2

# Create the files
touch  ./common-config/${component}.bicep
touch  ./environments/${environment}/_parameters.${component}.json

# Write content to the component.bicep file
echo "//common params
param deploymentLocation string
param org string
param environment string
param project string
param naming_abbrevation string = '\${org}-\${project}-\${environment}'" > "./common-config/${component}.bicep"

# Write content to the parameters.component.json file
echo '{
    "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
      "key": {
        "value": "xx"
      }
    }
}' > "./environments/${environment}/_parameters.${component}.json"

echo "Files created successfully."