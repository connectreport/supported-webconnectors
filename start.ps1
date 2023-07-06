$nodeInstallDir = "$(Get-Location)\node"
$repoPath = "$(Get-Location)\repo"


Start-Process "$nodeInstallDir\npm" -ArgumentList "run","start","--prefix",".\repo" -Wait 
