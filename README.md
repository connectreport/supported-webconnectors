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

## Aggregations 
You can add aggregation to a connector as follows
```
{
  "connectors": [
    {
      "name": "BigQuery",
      "type": "bigquery",
      "aggregations": [{
        // Table from data source to attach aggregation to
        "sourceTable": "products",
        // "dimension" || "measure" 
        "fieldType": "measure",
        // Identifier for the aggregation 
        "fieldIdentifier": "products.margin",
        // SQL that will be sent to the data source when the identifier is used
        "sql": "products.retail_price - products.cost"
      }]
    }
  ]
}
```