import type { Knex } from "knex";
import { Field } from "./Field";
import { FieldValue } from "./FieldValues";
import { Selections } from "./Selections";
import { TableResponse, TableRow } from "./TableResponse";
import { User } from "./User";
import { AggregationDef } from "../connectors";

export type MappedField = Field & {
  raw?: any;
};

export abstract class SqlService {
  knex: Knex;
  sqlTypeForDimension: string = "STRING";
  aggregations?: AggregationDef[];
  schema?: string;
  databaseName?: string;

  constructor(
    knex: Knex,
    aggregations?: AggregationDef[],
    sqlTypeForDimension?: string,
    schema?: string,
    databaseName?: string
  ) {
    this.knex = knex;
    this.aggregations = aggregations;
    this.sqlTypeForDimension = sqlTypeForDimension || this.sqlTypeForDimension;
    this.schema = schema;
    this.databaseName = databaseName;
  }

  abstract makeQuery(query: string): Promise<any>;

  async getTotals(
    tableName: string,
    fields: MappedField[],
    query: string
  ): Promise<TableRow> {
    let knex = this.knex;

    if (!fields.find((f) => f.totalsFunction)) {
      return [];
    }
    const allowedTotals = ["Sum", "Avg", "Count", "Min", "Max"];

    const aggQuery = knex
      .select(
        fields.reduce((acc: any, f, index) => {
          if (
            f.fieldType === "measure" &&
            f.totalsFunction &&
            allowedTotals.includes(f.totalsFunction)
          ) {
            acc.push(knex.raw(`${f.totalsFunction}(??)`, ["c" + index]));
          }
          return acc;
        }, [])
      )
      .from(knex.raw(`(${query}) as TQ`))
      .toString();

    const res: string[][] = await this.makeQuery(aggQuery);

    // response with an array of cells, one for each MEASURE field in the table
    // the cells will be populated if they are set to run a totals function
    let totalIndex = 0;
    let response = fields
      .filter((field) => field.fieldType === "measure")
      .map((f) => {
        if (
          f.fieldType === "measure" &&
          f.totalsFunction &&
          allowedTotals.includes(f.totalsFunction)
        ) {
          const r = { text: res[0][totalIndex] };
          totalIndex++;
          return r;
        } else {
          return { text: "" };
        }
      });
    return response;
  }

  public async getTable(
    user: User,
    fields: Field[],
    limit: number,
    tableName: string,
    selections: Selections
  ): Promise<TableResponse> {
    let knex = this.knex;
    try {
      let from = this.schema ? `${this.schema}.${tableName}` : tableName;
      from = this.databaseName ? `${this.databaseName}.${from}` : from;

      let orderBy: { column: string; order: string }[] = [];
      if (fields.find((f) => f.sortOrder)) {
        fields.map((f) => {
          if (f.sortOrder && f.sortOrder !== "auto") {
            orderBy.push({ column: f.fieldDef, order: f.sortOrder === "descending" ? "DESC" : "ASC" });
          }
        });
      }
      const mappedFields: MappedField[] = fields.map((f, index) => {
        const field: MappedField = { ...f };
        if (this.aggregations) {
          const isAgg = this.aggregations.find(
            (a) => a.fieldIdentifier === f.fieldDef
          );
          if (isAgg) {
            field.raw = knex.raw(isAgg.sql + " as c" + index);
          }
        }
        return field;
      });

      const query = knex
        .distinct(
          mappedFields.map((f, index) => f.raw || `${f.fieldDef} as c${index}`)
        )
        .from(from)
        .orderBy(orderBy)
        .limit(limit || 1000)
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
      // BigQuery
      if (err.message.includes("Unrecognized name")) {
        err.disableRetry = true;
        // Snowflake
      } else if (err.message.includes("SQL compilation error")) {
        err.disableRetry = true;
      }
      throw err;
    }
  }

  public abstract listTables(): Promise<string[]>;

  async getMetadata(user: User) {
    let tables: { name: string; fields: Field[] }[] = [];

    const tableList = await this.listTables();
    if (tableList && tableList.length > 0) {
      let promises = tableList.map(async (table) =>
        this.listTableColumns(user, table)
      );
      tables = await Promise.all(promises);
    }

    return {
      tables,
      filterFields: tables
        .flatMap((table) => table.fields)
        .sort((a, b) =>
          (a.fieldName || a.fieldDef).localeCompare(b.fieldName || b.fieldDef)
        ),
    };
  }

  /** list unique field values for a field defintion, useful for filters */
  public async getFieldValues(
    user: User,
    field: string,
    tableName: string,
    search?: string,
    height?: number,
    top?: number
  ): Promise<FieldValue[]> {
    let from = this.schema ? `${this.schema}.${tableName}` : tableName;
    from = this.databaseName ? `${this.databaseName}.${from}` : from;

    const knex = this.knex;
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
          .from(from)
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

  /** list columns names in table */
  public async listTableColumns(
    user: User,
    tableName: string
  ): Promise<{
    name: string;
    fields: Field[];
  }> {
    let from = "INFORMATION_SCHEMA.COLUMNS";
    if (this.databaseName) {
      from = `${this.databaseName}.${from}`;
    }

    const query = this.knex
      .select("COLUMN_NAME", "DATA_TYPE")
      .from(from)
      .where("TABLE_NAME", tableName)
      .modify((qb) => {
        if (this.schema) {
          qb.where("TABLE_SCHEMA", this.schema);
        }
      })
      .orderBy("COLUMN_NAME")
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
              fieldType:
                row[1] === this.sqlTypeForDimension ? "dimension" : "measure",
            } as Field)
        ),
        ...additionalFields,
      ],
    };
  }
}
