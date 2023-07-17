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

const knex = Knex({
  client: "pg",
  wrapIdentifier: (value, origImpl, queryContext) => {
    return `\"${value}\"`;
  },
});

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
    super();

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

  /** list columns names in table */
  public async listTableColumns(
    user: User,
    tableName: string
  ): Promise<{
    name: string;
    fields: Field[];
  }> {
    const query = knex
      .raw(
        `SELECT COLUMN_NAME, DATA_TYPE FROM ??.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? and TABLE_SCHEMA=?;`,
        [this.DATABASE, tableName, this.SCHEMA]
      )
      .toString();
    const res: any[][] = await this.makeQuery(query);

    let additionalFields: Field[] = [];
    if (this.aggregations) {
      additionalFields = this.aggregations
        .filter((a) => a.sourceTable === tableName)
        .map((a) => ({
          fieldName: a.fieldIdentifier,
          fieldDef: a.fieldIdentifier,
          tableName: tableName,
          fieldType: a.fieldType as "dimension" | "measure",
        }));
    }

    return {
      name: tableName,
      fields: [
        ...res.map(
          (row) =>
            ({
              fieldName: `${tableName}.${row[0]}`,
              fieldDef: `${row[0]}`,
              tableName: tableName,
              fieldType: row[1] === "TEXT" ? "dimension" : "measure",
            } as Field)
        ),
        ...additionalFields,
      ],
    };
  }

  /** list tables available in warehouse */
  public async listTables(): Promise<string[]> {
    const query = knex
      .select("TABLE_NAME")
      .from("INFORMATION_SCHEMA.TABLES")
      .where("TABLE_SCHEMA", this.SCHEMA)
      .toString();
    const res: string[][] = await this.makeQuery(query);
    return res.map((row) => row[0]);
  }

  /** list unique field values for a field defintion, useful for filters */
  public async getFieldValues(
    user: User,
    field: string,
    search?: string,
    tableName?: string,
    height?: number,
    top?: number
  ): Promise<FieldValue[]> {
    const columnName = field;
    const searchClause = search
      ? knex.raw("CAST(?? AS string) LIKE ?", [columnName, `%${search}%`])
      : null;
    let raw;

    const isAgg = this.aggregations?.find((a) => a.fieldIdentifier === field);
    if (isAgg) {
      raw = knex.raw(`${isAgg.sql} as value`);
    }

    const resolvedCol = raw || columnName;

    try {
      const res: string[][] = await this.makeQuery(
        knex
          .distinct(resolvedCol)
          .from(`${this.SCHEMA}.${tableName}`)
          .orderBy(
            typeof resolvedCol === "string" ? resolvedCol : "value",
            "asc"
          )
          .limit(height || 300)
          .offset(top || 0)
          .modify((query) => {
            if (searchClause) {
              query.where(searchClause);
            }
          })
          .toString()
      );
      return res.map((row: Array<{ value: string } | string>) => {
        if (typeof row[0] === "object" && row[0]?.value) {
          return { text: row[0].value };
        } else {
          return { text: row[0] as string };
        }
      });
    } catch (e) {
      return [];
    }
  }

  /** Must return a value for every measure,
   * with a non-null value for measure fields with a totalsFunction  */
  async getTotals(
    tableName: string,
    fields: MappedField[],
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
              if (f.raw) {
                acc.push(knex.raw(`${f.totalsFunction}(${f.raw.toString()}`));
              } else {
                acc.push(knex.raw(`${f.totalsFunction}(??)`, [f.fieldDef]));
              }
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
    user: User,
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
      const mappedFields: MappedField[] = fields.map((f) => {
        const field: MappedField = { ...f };
        if (this.aggregations) {
          const isAgg = this.aggregations.find(
            (a) => a.fieldIdentifier === f.fieldDef
          );
          if (isAgg) {
            field.raw = knex.raw(isAgg.sql);
          }
        }
        return field;
      });

      const query = knex
        .distinct(
          mappedFields.map((f, index) => f.raw || `${f.fieldDef} as c${index}`)
        )
        .from(`${this.SCHEMA}.${tableName}`)
        .orderBy(orderBy)
        .limit(limit)
        .modify((query) => {
          selections.forEach((s) => {
            let resolvedCol: any = s.fieldDef;
            if (this.aggregations) {
              const isAgg = this.aggregations.find(
                (a) => a.fieldIdentifier === s.fieldDef
              );
              if (isAgg) {
                resolvedCol = knex.raw(isAgg.sql);
              }
            }
            query.whereIn(
              resolvedCol,
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
              // @ts-ignore
            } else if (cell.constructor?.name === "Big") {
              return { text: cell + "", number: cell };
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
          mappedFields,
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
