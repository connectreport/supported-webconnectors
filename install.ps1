  # Define the download URL for the Node.js installer
$installerUrl = "https://nodejs.org/dist/latest-v16.x/node-v16.20.1-win-x64.zip"
$nssmUrl = "https://nssm.cc/release/nssm-2.24.zip"
$repoUrl = "https://github.com/connectreport/supported-webconnectors/archive/refs/heads/main.zip"

# Define the path to download and save the installer
$installerPath = "$env:TEMP\node-v$nodeVersion-x64.zip"
$repoZipPath = "$(Get-Location)\repo.zip"
$repoUnzipPath = "$(Get-Location)"
$repoPath = "$(Get-Location)\repo"
$nssmZipPath = "$env:TEMP\nssm.zip"

# Download the Node.js installer
Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath

$nodeInstallDir = "$(Get-Location)"

# Install Node.js
Expand-Archive -Path $installerPath -DestinationPath $nodeInstallDir 

Rename-Item -Path "$($nodeInstallDir)\node-v16.20.1-win-x64" -NewName  "node"

$nodeInstallDir = "$(Get-Location)\node"

# Verify the installation
Write-Host "Node.js has been configured"

# Install Repo
Invoke-WebRequest -Uri $repoUrl -OutFile $repoZipPath

Expand-Archive -Path $repoZipPath -DestinationPath $repoUnzipPath

Rename-Item -Path "$($repoUnzipPath)\supported-webconnectors-main" -NewName  "repo"

Remove-Item -Path $repoZipPath

Write-Host "Retrieved repo"

Start-Process "$nodeInstallDir\npm" -ArgumentList "i" -wait  -WorkingDirectory $repoPath 

Start-Process "$nodeInstallDir\npm" -ArgumentList "run","ts" -wait  -WorkingDirectory $repoPath 

Write-Host "Installed and built repo"

#install nssm
Invoke-WebRequest -Uri $nssmUrl -OutFile $nssmZipPath

Expand-Archive -Path $nssmZipPath -DestinationPath $(Get-Location)

Move-Item "$(Get-Location)\nssm-2.24\win64\nssm.exe" -Destination "$(Get-Location)\nssm.exe"

Remove-Item -Path "$(Get-Location)\nssm-2.24" -Recurse

# The path to PowerShell
$Binary = (Get-Command Powershell).Source

# The necessary arguments, including the path to our script
.\nssm.exe install crWebConnectorServiceManager $Binary '-ExecutionPolicy Bypass -NoProfile -File ".\repo\start.ps1"'
.\nssm.exe set crWebConnectorServiceManager DisplayName "ConnectReport Web Connector Service Manager"
.\nssm.exe set crWebConnectorServiceManager Start SERVICE_DELAYED_AUTO_START 
.\nssm.exe set crWebConnectorServiceManager AppDirectory $(Get-Location)
.\nssm.exe set crWebConnectorServiceManager AppStdout "$(Get-Location)\logs\service.log"
.\nssm.exe set crWebConnectorServiceManager AppStdErr "$(Get-Location)\logs\service-error.log"

Write-Host "Service installed"
 
 