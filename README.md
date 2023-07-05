# ConnectReport Supported Web Connectors
This repo houses the Web Connector services for ConnectReport's supported Web Connectors. 

# Install
Open cmd.exe within a working directory and run the following:
```
@echo off
set "url=https://raw.githubusercontent.com/connectreport/supported-webconnectors/main/install.ps1"
set "outputFile=install.ps1"

echo Downloading script...
curl %url% --output %outputFile%

echo Running script as administrator...
powershell -ExecutionPolicy Bypass -File %outputFile%
```