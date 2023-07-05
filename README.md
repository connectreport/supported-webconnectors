# ConnectReport Supported Web Connectors
This repo houses the Web Connector services for ConnectReport's supported Web Connectors. 

# Install
Open Powershell and execute the following:
```
cd Documents
mkdir "ConnectReport Web Connectors"
cd "./ConnectReport Web Connectors"
$scriptUrl = "https://raw.githubusercontent.com/connectreport/supported-webconnectors/main/install.ps1"
$outputFile = "install.ps1"

# Download the script
Invoke-WebRequest -Uri $scriptUrl -OutFile $outputFile

# Run the downloaded script as administrator
Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -File `"$outputFile`"" -Verb RunAs
```