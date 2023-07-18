// Setup within Google Cloud console:
// https://codelabs.developers.google.com/codelabs/cloud-bigquery-nodejs
// Local workspace requires Application Default Credentials:
// https://cloud.google.com/docs/authentication/provide-credentials-adc
// All requests need to qualify the dataset

import { BigQuery as BigQueryLib } from "@google-cloud/bigquery";
import { MappedField, SqlService } from "../models/SqlService";
import { Field } from "../models/Field";
import { FieldValue } from "../models/FieldValues";
import { Selections } from "../models/Selections";
import { logger } from "../util";
import { TableResponse, TableRow } from "../models/TableResponse";
import Knex from "knex";
import { User } from "../models/User";
import { AggregationDef } from "../connectors";
import { isNumber } from "lodash";

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
  aggregations?: AggregationDef[];

  constructor(
    DATABASE: string,
    LOCATION: string,
    aggregations?: AggregationDef[]
  ) {
    super(knex, aggregations, "STRING", undefined, DATABASE);
    this.DATABASE = DATABASE;
    this.LOCATION = LOCATION;
    this.aggregations = aggregations;

    this.bigqueryClient = new BigQueryLib();
  }

  async makeQuery(query: string): Promise<any> {
    const options = {
      query,
      location: this.LOCATION,
    };

    try {
      const [rows] = await this.bigqueryClient.query(options);
      const out = rows.map((row) => Object.values(row));
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
