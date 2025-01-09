param (
    [string]$resourceGroup,
    [string]$aclGroupAdmins,
    [string]$aclGroupUsers
)

$htmlTable = @( 
    "<html>
            <body>
            <h1>Databricks Workspace Integration Request (From Devops Automation)</h1>
            <p>Hello Databricks Admin,<br>
                Please add the Databricks workspace to the Unity Catalog which was provisioned in the resource group: <strong><em> $resourceGroup </em></strong></p>
                <p>Also, please add the below ACL groups as Users in the Azure EntraID under the enterprise application named <strong>sc-unitycatalog-pr-dbx-eapp</strong></p>
                <p>ACL Group Admins: <strong> $aclGroupAdmins </strong></p>
                <p>ACL Group Users: <strong> $aclGroupUsers </strong></p>
            <p>Thanks<br>
            Datalake Automation</p>
            </body>
    </html>"
        )
$params = @{
    Body = ($htmlTable -join (' '))
    BodyAsHtml = $true
    Subject = "Add DataBricks Workspace to the Unity Catalog- Resource Group - $resourceGroup"
    From = "tfs2008@southernco.com"
    To = "grfloyd@southernco.com"   
    cc = "x2athuma@southernco.com"    
    SmtpServer = "mail.southernco.com"  
}
Send-MailMessage @params