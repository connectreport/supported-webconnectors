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

## Update
Open PowerShell and execute the following:
```
cd Documents
cd "./ConnectReport Web Connectors"
$scriptUrl = "https://raw.githubusercontent.com/connectreport/supported-webconnectors/main/update.ps1"
$outputFile = "update.ps1"

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

## Snowflake Integration 
The Snowflake integration supports password based authentication. 

> _Future support for Key Pair Authentication may allow the integration to access Snowflake on behalf of users and enforce role-based access controls using Snowflake permissions._

In your settings.json file, configure the integration as indicated below, replacing `ACCOUNT`, `DATABASE`, `SCHEMA`, `USERNAME`, and `PASSWORD` with the appropriate values. 

```
{
  "connectors": [
    {
      "name": "Snowflake",
      "type": "snowflake",
      "config": {
        "ACCOUNT": "example.us-east-1",
        "DATABASE": "SNOWFLAKE_SAMPLE_DATA",
        "SCHEMA": "TPCH_SF1",
        "USERNAME": "EXAMPLE",
        "PASSWORD": "password"
      }
    }
  ]
}
```

To gather the `ACCOUNT` value:
- Navigate to Snowsight
- Open the account selector and review the list of accounts that you have previously signed in to.
- Locate the account for which you want to connect.
- Hover over the account to view additional details and select the link icon to copy the account URL to your clipboard.

![Account selector](images/account-selector.png)

- Your clipboard will contain a value like `https://example.us-east-1.snowflakecomputing.com`. 

  Copy the value between `https://` and `.snowflakecomputing.com` 

  In this example, the `ACCOUNT` value is `example.us-east-1`


