import fs from "fs";
import path from "path";
export const settings: { port?: number; locale?: string; logLevel?: string, connectors: ConnectorDef[] } = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "settings.json"), "utf8")
);
import { BigQuery } from "./connectors/bigQuery";
import { SqlService } from "./models/SqlService";
import { Snowflake } from "./connectors/snowflake";


export type AdditionalFieldDef = { sourceTable: string, fieldType: string; fieldIdentifier: string, sql: string };

export type ConnectorDef = {
  name: string;
  type: "databricks" | "snowflake" | "bigquery";
  config: any;
  env: { [key: string]: string };
  sqlService: SqlService;
  additionalFields?: AdditionalFieldDef[];
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
      sqlService = new BigQuery(c.config.DATABASE, c.config.LOCATION, c.additionalFields);
      break;
    case "snowflake":
      sqlService = new Snowflake(c.config.ACCOUNT, c.config.DATABASE, c.config.USERNAME, c.config.PASSWORD, c.config.SCHEMA, c.additionalFields);
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
