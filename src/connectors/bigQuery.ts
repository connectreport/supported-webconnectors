// Setup within Google Cloud console:
// https://codelabs.developers.google.com/codelabs/cloud-bigquery-nodejs
// Local workspace requires Application Default Credentials:
// https://cloud.google.com/docs/authentication/provide-credentials-adc
// All requests need to qualify the dataset

import { BigQuery as BigQueryLib } from "@google-cloud/bigquery";
import { SqlService } from "../models/SqlService";
import { logger } from "../util";
import Knex from "knex";
import { AdditionalFieldDef } from "../connectors";
import { v4 } from "uuid";

const knex = Knex({
  client: "pg",
  wrapIdentifier: (value, origImpl, queryContext) => {
    return `\`${value}\``;
  },
});

export class BigQuery extends SqlService {
  bigqueryClient: BigQueryLib;
  DATABASE: string;
  LOCATION: string;
  additionalFields?: AdditionalFieldDef[];

  constructor(
    DATABASE: string,
    LOCATION: string,
    additionalFields?: AdditionalFieldDef[]
  ) {
    super(knex, additionalFields, "STRING", undefined, DATABASE);
    this.DATABASE = DATABASE;
    this.LOCATION = LOCATION;
    this.additionalFields = additionalFields;

    this.bigqueryClient = new BigQueryLib();
  }

  async makeQuery(query: string): Promise<any> {
    const queryId = v4();
    const queryStart = Date.now();

    const options = {
      query,
      location: this.LOCATION,
    };

    logger.debug("Running query", { queryId, query });

    try {
      const [rows] = await this.bigqueryClient.query(options);
      const out = rows.map((row) => Object.values(row));
      logger.debug("Query complete", {
        queryId,
        durationSeconds: (Date.now() - queryStart) / 1000,
      });
      return out;
    } catch (e) {
      logger.error(e);
      throw e;
    }
  }

  /** list tables available in warehouse */
  public async listTables(): Promise<string[]> {
    const query = knex
      .raw(`SELECT table_name FROM ??.INFORMATION_SCHEMA.TABLES;`, [
        this.DATABASE,
      ])
      .toString();
    const res: string[][] = await this.makeQuery(query);
    return res.map((row) => row[0]);
  }
}
