Install-Module Microsoft.Graph -Force -AllowClobber
Connect-MgGraph -Scopes Application.Read.All, AppRoleAssignment.ReadWrite.All

$MI = "wf-hub-teams-pim-notify" 
$roleName = @("AuditLog.Read.All")

$MIID = if (!([guid]::TryParse("$MI", $([ref][guid]::Empty)))) {
    Get-MgServicePrincipal -Filter "DisplayName eq '$MI'"
} else {
    Get-MgServicePrincipal -ServicePrincipalId $MI
}

$msgraph = Get-MgServicePrincipal -Filter "AppId eq '00000003-0000-0000-c000-000000000000'"

foreach ($role in $roleName) { 
    $role = $Msgraph.AppRoles | Where-Object {$_.Value -eq $role} 
    New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $MIID.id -PrincipalId $MIID.id -ResourceId $msgraph.Id -AppRoleId $role.Id
}

Disconnect-MgGraph




$MI = "c36af955-3ca5-41bd-a9b8-3e975f2f1538"
$MIID = Get-MgServicePrincipal -ServicePrincipalId $MI
$msgraph = Get-MgServicePrincipal -Filter "AppId eq '00000003-0000-0000-c000-000000000000'"
$roleName = "AuditLog.Read.All"
$role = $msgraph.AppRoles | Where-Object {$_.Value -eq $roleName}
New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $MIID.id -PrincipalId $MIID.id -ResourceId $msgraph.Id -AppRoleId $role.Id

Get-MgContext | Select Account, TenantId

Disconnect-MgGraph
Connect-MgGraph -Scopes Application.Read.All, AppRoleAssignment.ReadWrite.All -TenantId "eacd16bf-dc0e-44db-8e3f-be370c71feca"
