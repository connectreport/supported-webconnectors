import axios from "axios";
import { Field } from "../models/Field";
import { FieldValue } from "../models/FieldValues";
import { Selections } from "../models/Selections";
import { MappedField, SqlService } from "../models/SqlService";
import Knex from "knex";
import { TableResponse, TableRow } from "../models/TableResponse";
import { User } from "../models/User";
import snowflake from "snowflake-sdk";
import type { Pool } from "generic-pool";
import { AggregationDef } from "../connectors";
import { logger } from "../util";

class Deferred {
  resolve: any;
  reject: any;
  promise: Promise<any>;
  constructor() {
    this.promise = new Promise((res, rej) => {
      this.resolve = res;
      this.reject = rej;
    });
  }
}

export class Snowflake extends SqlService {
  connection: snowflake.Connection;
  DATABASE: string;
  SCHEMA: string;
  aggregations?: AggregationDef[];

  constructor(
    ACCOUNT: string,
    DATABASE: string,
    USERNAME: string,
    PASSWORD: string,
    SCHEMA: string,
    aggregations?: AggregationDef[]
  ) {
    const knex = Knex({
      client: "pg",
      wrapIdentifier: (value, origImpl, queryContext) => {
        return `\"${value}\"`;
      },
    });

    super(knex, aggregations, "TEXT", SCHEMA);

    this.DATABASE = DATABASE;
    this.SCHEMA = SCHEMA;
    this.aggregations = aggregations;

    this.connection = snowflake.createConnection(
      // connection options
      {
        account: ACCOUNT,
        username: USERNAME,
        password: PASSWORD,
        database: DATABASE,
      }
    );
    this.connection.connect((err, conn) => {
      if (err) {
        logger.error("Failed to connect to snowflake", { err });
      } else {
        logger.info("Successfully connected to Snowflake.");
      }
    });
  }

  /** Send query to Snowflake */
  async makeQuery(query: string): Promise<any> {
    const d = new Deferred();
    // Use the connection pool and execute a statement
    this.connection.execute({
      sqlText: query,
      fetchAsString: ["Boolean", "Number", "Date", "JSON", "Buffer"],
      complete: function (err, stmt, rows) {
        if (err) {
          d.reject(err);
        }
        d.resolve(rows?.map((row) => Object.values(row)));
      },
    });
    return d.promise;
  }

  /** list tables available in warehouse */
  public async listTables(): Promise<string[]> {
    const query = this.knex
      .select("TABLE_NAME")
      .from("INFORMATION_SCHEMA.TABLES")
      .where("TABLE_SCHEMA", this.SCHEMA)
      .toString();
    const res: string[][] = await this.makeQuery(query);
    return res.map((row) => row[0]);
  }
}
