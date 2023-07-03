const settings: { connectors: ConnectorDef[] } = require("../settings.json");
import { BigQuery } from "./connectors/bigQuery";
import { SqlService } from "./models/SqlService";

export type ConnectorDef = {
  name: string;
  type: "databricks" | "snowflake" | "bigquery";
  config: any;
  env: { [key: string]: string}
  sqlService: SqlService;
};

export const connectors = settings.connectors.map((c) => {
  for (const key in c.env) {
    if (Object.prototype.hasOwnProperty.call(c.env, key)) {
      const value = c.env[key];
      process.env[key] = value;
    }
  }
  let sqlService: SqlService;
  switch (c.type) {
    case "bigquery":
      sqlService = new BigQuery(c.config.DATABASE, c.config.LOCATION);
      break;
    default:
      throw new Error(`Unknown connector type ${c.type}`);
  }
  return {
    ...c,
    sqlService,
  };
});

export const getConnector = (name: string) => {
  const connector = connectors.find((c) => c.name === name);
  return connector;
};
