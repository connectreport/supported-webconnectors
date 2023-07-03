import axios from "axios";
import { Field } from "../models/Field";
import { FieldValue } from "../models/FieldValues";
import { Selections } from "../models/Selections";
import { SqlService } from "../models/SqlService";
import { debug } from "../util";
import axiosRetry from "axios-retry";
import { TableResponse } from "../models/TableResponse";

axiosRetry(axios, { retries: 3 });

export class Databricks extends SqlService {
  API_KEY: string;
  BASEURL: string;
  DEFAULT_TABLE_NAME: string;
  POLL_WAIT_MS: number;
  POLL_TIMEOUT_MS: number;
  WAREHOUSE_ID: string;

  constructor(
    API_KEY: string,
    DEFAULT_TABLE_NAME: string,
    BASEURL: string,
    WAREHOUSE_ID: string,
    POLL_WAIT_MS: number,
    POLL_TIMEOUT_MS: number
  ) {
    super();
    this.API_KEY = API_KEY;
    this.BASEURL = BASEURL;
    this.WAREHOUSE_ID = WAREHOUSE_ID;
    this.POLL_WAIT_MS = POLL_WAIT_MS;
    this.POLL_TIMEOUT_MS = POLL_TIMEOUT_MS;
    this.DEFAULT_TABLE_NAME = DEFAULT_TABLE_NAME;
  }
  /** Send query to Databricks */
  async makeQuery(query: string): Promise<any> {
    const options = {
      baseURL: this.BASEURL,
      // path: "/api/2.1/unity-catalog/tables",
      // path: "/api/2.0/workspace/get-status",
      // path: "/api/2.0/sql/warehouses/", // list warehouse ids
      url: "/api/2.0/sql/statements/",
      method: "POST",
      data: {
        statement: query,
        warehouse_id: this.WAREHOUSE_ID,
      },
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.API_KEY}`,
      },
    };
    const request = await axios.request(options);
    const id = request.data.statement_id;
    if (request.data?.status?.state === "SUCCEEDED") {
      return request.data.result.data_array;
      //check for next chunks
    } else {
      return await this.loopCheckQuery(
        id,
        this.POLL_WAIT_MS,
        this.POLL_TIMEOUT_MS
      );
    }
  }

  /** list columns names in table */
  public async listTableColumns(tableName?: string): Promise<{
    name: string;
    fields: Field[];
  }> {
    const res: string[][] = await this.makeQuery(
      `SHOW COLUMNS FROM ${tableName || this.DEFAULT_TABLE_NAME}`
    );
    return {
      name: tableName || this.DEFAULT_TABLE_NAME,
      fields: res.map((row) => ({
        fieldName: row[0],
        fieldDef: row[0],
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
    const res: string[][] = await this.makeQuery(`SHOW TABLES`);
    return res.map((row) => row[0]);
  }

  /** list unique field values for a field defintion, useful for filters */
  public async getFieldValues(
    field: string,
    search?: string,
    tableName?: string
  ): Promise<FieldValue[]> {
    const searchClause = search ? ` WHERE ${field} LIKE '%${search}%'` : "";
    const res: string[][] = await this.makeQuery(
      `SELECT DISTINCT ${field} FROM ${
        tableName || this.DEFAULT_TABLE_NAME
      }${searchClause} ORDER BY ${field} ASC LIMIT 1000;`
    );
    return res.map((row) => ({
      text: row[0],
    }));
  }

  /** output a table with the provided fields and filters applied */
  public async getTable(
    fields: Field[],
    limit: number = 100,
    tableName: string = this.DEFAULT_TABLE_NAME,
    selections: Selections = []
  ): Promise<TableResponse> {
    let where = "";
    if (selections.length) {
      where = " WHERE 1=1";
      selections.forEach((s) => {
        const valueLength = s.fieldValues.length;
        const clause = s.fieldValues.map(
          (v, i) =>
            `${s.fieldDef} = '${v.text}' ${i < valueLength - 1 ? "OR" : ""}`
        );
        if (valueLength > 1) {
          where += `AND (${clause.join(" ")})`;
        } else {
          where += ` AND ${clause[0]}`;
        }
      });
    }
    const query = `SELECT ${fields
      .map((f) => f.fieldDef)
      .join(",")} FROM ${tableName}${where} LIMIT ${limit};`;
    console.log("query", query);
    const res: string[][] = await this.makeQuery(query);
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
    const out = res.map((row) => row.map((cell) => ({ text: cell })));
    return {
      table: out,
      grandTotalRow: [],
      size: {
        width: out[0]?.length || 0,
        height: out.length,
      },
    };
  }

  /** Auto check for pending query until completed */
  async loopCheckQuery(id: string, wait: number, timeout: number) {
    let elapsedTime = 0;
    let attempts = 0;
    while (elapsedTime < timeout) {
      const results = await this.checkQuery(id, wait);
      debug(
        `try attempt ${attempts} over ${Math.floor(elapsedTime / 1000)} seconds`
      );
      const status = results?.data?.status?.state;
      if (status === "SUCCEEDED") {
        return results.result.data_array;
        //check for next chunks
      } else if (status === "FAILED") {
        throw new Error(
          `Query failed, error code: ${results?.data?.status?.error?.error_code}, message: ${results?.data?.status?.error?.message}`
        );
      } else {
        elapsedTime += wait;
        attempts++;
      }
    }

    throw new Error(
      `Request timeout after elapsed ${Math.floor(timeout / 1000)} seconds`
    );
  }

  /** Check query status */
  checkQuery(id: string, wait: number): Promise<any> {
    const promise = new Promise((resolve, reject) => {
      setTimeout(async () => {
        const results = await axios.request({
          baseURL: this.BASEURL,
          url: `/api/2.0/sql/statements/${id}`,
          method: "GET",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${this.API_KEY}`,
          },
        });
        resolve(results);
      }, wait);
    });
    return promise;
  }
}
