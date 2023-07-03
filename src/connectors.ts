const settings: { connectors: ConnectorDef[] } = require("../settings.json");
import { BigQuery } from "./connectors/bigQuery";
import { Databricks } from "./connectors/databricks";
import { Snowflake } from "./connectors/snowflake";
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
    case "databricks":
      sqlService = new Databricks(
        c.config.API_KEY,
        c.config.DEFAULT_TABLE_NAME,
        c.config.BASEURL,
        c.config.WAREHOUSE_ID,
        c.config.POLL_WAIT_MS,
        c.config.POLL_TIMEOUT_MS
      );
      break;
    case "snowflake":
      sqlService = new Snowflake(
        c.config.BASE_URL,
        c.config.API_KEY,
        c.config.POLL_WAIT_MS,
        c.config.POLL_TIMEOUT_MS,
        c.config.DEFAULT_TABLE_NAME,
        c.config.DEFAULT_SCHEMA,
        c.config.DEFAULT_DATABASE
      );
      break;
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
