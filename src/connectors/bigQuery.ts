// Setup within Google Cloud console:
// https://codelabs.developers.google.com/codelabs/cloud-bigquery-nodejs
// Local workspace requires Application Default Credentials:
// https://cloud.google.com/docs/authentication/provide-credentials-adc
// All requests need to qualify the dataset

import { BigQuery as BigQueryLib } from "@google-cloud/bigquery";
import { SqlService } from "../models/SqlService";
import { Field } from "../models/Field";
import { FieldValue } from "../models/FieldValues";
import { Selections } from "../models/Selections";
import { debug } from "../util";
import { TableResponse, TableRow } from "../models/TableResponse";
import Knex from "knex";

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

  constructor(DATABASE: string, LOCATION: string) {
    super();
    this.DATABASE = DATABASE;
    this.LOCATION = LOCATION;
    // environemnt variables set directly by the node process
    //process.env["GOOGLE_CLOUD_PROJECT"] = ACCOUNT_IDENTIFIER;
    // const keyPath = path.join(process.cwd(), "$HOME/.config/gcloud/application_default_credentials.json")

    this.bigqueryClient = new BigQueryLib({
      // maxRetries:
    });
  }
  /** Send query to Databricks */
  async makeQuery(query: string): Promise<any> {
    const options = {
      query,
      // Location must match that of the dataset(s) referenced in the query.
      location: this.LOCATION,
      //   params: { corpus: "romeoandjuliet", min_word_count: 250 },
    };

    try {
      const [rows] = await this.bigqueryClient.query(options);
      const out = rows.map((row) => Object.values(row));
      return out;
    } catch (e) {
      debug(e);
      throw e;
    }
  }

  /** list columns names in table */
  public async listTableColumns(tableName: string): Promise<{
    name: string;
    fields: Field[];
  }> {
    const query = knex
      .raw(
        `SELECT COLUMN_NAME, DATA_TYPE FROM ??.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=?;`,
        [this.DATABASE, tableName]
      )
      .toString();
    const res: any[][] = await this.makeQuery(query);
    return {
      name: tableName,
      fields: res.map((row) => ({
        fieldName: `${tableName}.${row[0]}`,
        fieldDef: `${tableName}.${row[0]}`,
        fieldType: row[1] === "STRING" ? "dimension" : "measure",
      })),
    };
  }
  /** list databases available in warehouse */
  async listDatabases(): Promise<string[]> {
    const res: string[][] = await this.makeQuery(`SHOW DATABASES`);
    return res.map((row) => row[0]);
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

  /** list unique field values for a field defintion, useful for filters */
  public async getFieldValues(
    field: string,
    search?: string
  ): Promise<FieldValue[]> {
    const tableName = field.split(".")[0];
    const columnName = field.split(".")[1];
    const searchClause = search
      ? knex.raw("CAST(?? AS string) LIKE ?", [columnName, `%${search}%`])
      : null;
    try {
      const res: string[][] = await this.makeQuery(
        knex
          .distinct(columnName)
          .from(`${this.DATABASE}.${tableName}`)
          .orderBy(columnName, "asc")
          .limit(1000)
          .modify((query) => {
            if (searchClause) {
              query.where(searchClause);
            }
          })
          .toString()
      );
      return res.map((row) => ({
        text: row[0],
      }));
    } catch (e) {
      return [];
    }
  }

  /** Must return a value for every measure,
   * with a non-null value for measure fields with a totalsFunction  */
  async getTotals(
    tableName: string,
    fields: Field[],
    query: string
  ): Promise<TableRow> {
    if (!fields.find((f) => f.totalsFunction)) {
      return [];
    }
    const allowedTotals = ["Sum", "Avg", "Count", "Min", "Max"];
    const aggQuery = knex
      .select(
        fields.reduce((acc: any, f) => {
          if (f.fieldType === "measure") {
            if (f.totalsFunction && allowedTotals.includes(f.totalsFunction)) {
              acc.push(knex.raw(`${f.totalsFunction}(??)`, [f.fieldDef]));
            } else {
              acc.push(knex.raw(`NULL`));
            }
          }
          return acc;
        }, [])
      )
      .from(knex.raw(`(${query}) as ${tableName}`))
      .toString();
    const res: string[][] = await this.makeQuery(aggQuery);
    return res[0].map((r, i) => ({ text: r }));
  }

  /** output a table with the provided fields and filters applied */
  public async getTable(
    fields: Field[],
    limit: number = 100,
    tableName: string,
    selections: Selections = []
  ): Promise<TableResponse> {
    try {
      let orderBy: { column: string; order: string }[] = [];
      if (fields.find((f) => f.sortOrder)) {
        fields.map((f) => {
          if (f.sortOrder) {
            orderBy.push({ column: f.fieldDef, order: f.sortOrder });
          }
        });
      }
      const query = knex
        .distinct(fields.map((f) => f.fieldDef))
        .from(`${this.DATABASE}.${tableName}`)
        .orderBy(orderBy)
        .limit(limit)
        .modify((query) => {
          selections.forEach((s) => {
            query.whereIn(
              s.fieldDef,
              s.fieldValues.map((v) => v.text)
            );
          });
        });

      const res: string[][] = await this.makeQuery(query.toString());
      if (!res.length || !res[0].length) {
        return {
          table: [],
          grandTotalRow: [],
          size: {
            width: 0,
            height: 0,
          },
        };
      }
      const out = res.map((row) =>
        row.map((cell) => {
          // if object and not null
          if (cell !== null && typeof cell === "object") {
            if (cell["value"]) {
              return { text: cell["value"] };
            } else {
              return { text: Object.values(cell)[0] as string };
            }
          } else {
            return { text: cell };
          }
        })
      );
      return {
        table: out,
        grandTotalRow: await this.getTotals(
          tableName,
          fields,
          query.toString()
        ),
        size: {
          width: out[0]?.length || 0,
          height: out.length,
        },
      };
    } catch (err: any) {
      if (err.message.includes("Unrecognized name")) {
        err.disableRetry = true;
      }
      throw err;
    }
  }
}
