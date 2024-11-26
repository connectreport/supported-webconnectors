import { SqlService } from "../models/SqlService";
import Knex from "knex";
import snowflake, { Connection } from "snowflake-sdk";
import type { Pool } from "generic-pool";
import { AdditionalFieldDef } from "../connectors";
import { logger } from "../util";
import { v4 } from "uuid";

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
  pool: Pool<Connection>;
  DATABASE: string;
  SCHEMA: string;
  additionalFields?: AdditionalFieldDef[];

  constructor(
    ACCOUNT: string,
    DATABASE: string,
    USERNAME: string,
    PASSWORD: string,
    SCHEMA: string,
    additionalFields?: AdditionalFieldDef[]
  ) {
    const knex = Knex({
      client: "pg",
      wrapIdentifier: (value, origImpl, queryContext) => {
        return `\"${value}\"`;
      },
    });

    super(knex, additionalFields, "TEXT", SCHEMA);

    this.DATABASE = DATABASE;
    this.SCHEMA = SCHEMA;
    this.additionalFields = additionalFields;

    this.pool = snowflake.createPool(
      // connection options
      {
        account: ACCOUNT,
        username: USERNAME,
        password: PASSWORD,
        database: DATABASE,
        clientSessionKeepAlive: true
      },
      {
        evictionRunIntervalMillis: 60000, // default = 0, off
        idleTimeoutMillis: 60000, // default = 30000
        max: 2,
        min: 0,
      }
    );

    this.checkPoolHealth()
  }

  async checkPoolHealth() {
    this.pool.use(async (connection) => {
      try {
        const statement = connection.execute({
          sqlText: 'SELECT 1',
        });
  
        const stream = statement.streamRows();
        stream.on('data', (row) => {
          logger.info("Successfully connected to Snowflake.");
        });
  
        stream.on('error', (err) => {
          logger.error("Failed to connect to snowflake", { err });
        });
      } catch (err) {
        logger.error("Failed to connect to snowflake", { err });
      }
    }).catch((err) => {
      logger.error("Failed to connect to snowflake", { err });
    });
  }

  /** Send query to Snowflake */
  async makeQuery(query: string): Promise<any> {
    const queryId = v4();
    const queryStart = Date.now();

    logger.debug("Running query", { queryId, query });

    const d = new Deferred();
    // Use the connection pool and execute a statement

    this.pool.use(async(connection) => {
      connection.execute({
        sqlText: query,
        fetchAsString: ["Boolean", "Number", "Date", "JSON", "Buffer"],
        complete: function (err, stmt, rows) {
          if (err) {
            d.reject(err);
            return;
          }
          logger.debug("Query complete", {
            queryId,
            durationSeconds: (Date.now() - queryStart) / 1000,
          });
          d.resolve(rows?.map((row) => Object.values(row)));
        },
      });
    }).catch((err) => {
      logger.error("Failed to connect to snowflake", { err });
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
