


//common params
param org string
param environment string
param project string


var naming_abbrevation = '${org}-${project}-${environment}'

output naming_abbrevation string = naming_abbrevation
