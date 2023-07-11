$repoZipPath = "$(Get-Location)\repo.zip"
$repoUnzipPath = "$(Get-Location)"
$repoPath = "$(Get-Location)\repo"
$nodeInstallDir = "$(Get-Location)\node"
$repoUrl = "https://github.com/connectreport/supported-webconnectors/archive/refs/heads/main.zip"


Write-Host "Stopping service"

.\nssm.exe stop crWebConnectorServiceManager 

Write-Host "Updating repo"

# Download Repo
Invoke-WebRequest -Uri $repoUrl -OutFile $repoZipPath

# Unzip to temporary directory
$tempPath = "$(Get-Location)\tempRepo"
Expand-Archive -Path $repoZipPath -DestinationPath $tempPath

# Copy files from temp to repo, only if they are newer or don't exist
Copy-Item -Path "$($tempPath)\supported-webconnectors-main\*" -Destination $repoPath -Recurse -Force

# Clean up downloaded zip and temp directory
Remove-Item -Path $repoZipPath
Remove-Item -Path $tempPath -Recurse

# Install and build using npm
Start-Process "$nodeInstallDir\npm" -ArgumentList "i" -wait  -WorkingDirectory $repoPath 
Start-Process "$nodeInstallDir\npm" -ArgumentList "run","ts" -wait  -WorkingDirectory $repoPath 

Write-Host "Updated and built repo"


.\nssm.exe start crWebConnectorServiceManager 

Write-Host "Stated service" 
