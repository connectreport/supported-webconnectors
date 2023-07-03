import axios from "axios";
import { Field } from "../models/Field";
import { FieldValue } from "../models/FieldValues";
import { Selections } from "../models/Selections";
import { SqlService } from "../models/SqlService";
import { debug } from "../util";

import axiosRetry from "axios-retry";
import { TableResponse } from "../models/TableResponse";

axiosRetry(axios, { retries: 3 });

export class Snowflake extends SqlService {
  statementURL = "/api/v2/statements";
  counter = 0;
  API_KEY: string;
  BASEURL: string;
  DEFAULT_TABLE_NAME: string;
  POLL_WAIT_MS: number;
  POLL_TIMEOUT_MS: number;
  DEFAULT_DATABASE: string;
  DEFAULT_SCHEMA: string;

  constructor(
    BASE_URL: string,
    API_KEY: string,
    POLL_WAIT_MS: number,
    POLL_TIMEOUT_MS: number,
    DEFAULT_TABLE_NAME: string,
    DEFAULT_SCHEMA: string,
    DEFAULT_DATABASE: string
  ) {
    super();
    this.API_KEY = API_KEY;
    this.BASEURL = BASE_URL;
    this.POLL_WAIT_MS = POLL_WAIT_MS;
    this.POLL_TIMEOUT_MS = POLL_TIMEOUT_MS;
    this.DEFAULT_TABLE_NAME = DEFAULT_TABLE_NAME;
    this.DEFAULT_SCHEMA = DEFAULT_SCHEMA;
    this.DEFAULT_DATABASE = DEFAULT_DATABASE;
  }

  buildHeader(key: string) {
    return {
      "Content-Type": "application/json",
      Authorization: `Bearer ${key}`,
      Accept: "application/json",
      "User-Agent": "ConnectReport-WebConnector/0.1.0",
      "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    };
  }
  getData(response: any) {
    return response?.data?.data;
  }
  getId(response: any) {
    return response?.data?.statementHandle;
  }
  isValidRequest(response: any) {
    if (
      !(
        response?.status == 200 &&
        response?.data?.message == "Statement executed successfully."
      )
    ) {
      throw new Error(`Request failed: ${response?.data?.message}`);
    }
  }

  /** Send query to Databricks */
  async makeQuery(query: string): Promise<any> {
    const options = {
      baseURL: this.BASEURL,
      // path: "/api/2.1/unity-catalog/tables",
      // path: "/api/2.0/workspace/get-status",
      // path: "/api/2.0/sql/warehouses/", // list warehouse ids
      url: this.statementURL,
      // url: "/api/2.0/sql/statements/",
      method: "POST",
      data: {
        statement: query,
        // warehouse_id: WAREHOUSE_ID,
        // warehouse: WAREHOUSE_ID,
        database: this.DEFAULT_DATABASE.toUpperCase(),
        schema: this.DEFAULT_SCHEMA.toUpperCase(),
      },
      headers: this.buildHeader(this.API_KEY),
    };
    const response = await axios.request(options);
    this.isValidRequest(response);

    const data = this.getData(response);
    if (data) {
      return data;
    } else {
      const id = this.getId(response);
      return await this.loopCheckQuery(
        id,
        this.POLL_WAIT_MS,
        this.POLL_TIMEOUT_MS
      );
    }
  }

  /** list columns names in table */
  async listTableColumns(tableName?: string): Promise<{
    name: string;
    fields: Field[];
  }> {
    const res: string[][] = await this.makeQuery(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='${
        this.DEFAULT_SCHEMA
      }' AND TABLE_NAME='${tableName || this.DEFAULT_TABLE_NAME}'`
    );
    const tableFinalName = tableName || this.DEFAULT_TABLE_NAME;
    const fields = res.map((row) => ({
      fieldName: row[0],
      fieldDef: tableFinalName + "." + row[0],
    }));
    return {
      name: tableFinalName,
      fields,
    };
  }
  /** list databases available in warehouse */
  async listDatabases(): Promise<string[]> {
    const res: string[][] = await this.makeQuery(`SHOW DATABASES`);
    return res.map((row) => row[0]);
  }

  /** list tables available in warehouse */
  async listTables(): Promise<string[]> {
    const query = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='${this.DEFAULT_SCHEMA}'`;
    const res: string[][] = await this.makeQuery(query);
    return res.map((row) => row[0]);
  }

  /** list unique field values for a field defintion, useful for filters */
  async getFieldValues(
    field: string,
    search?: string,
    tableName?: string
  ): Promise<FieldValue[]> {
    // if no table provided attempt to split field into table and field
    if (!tableName) {
      const fieldSplit = field.split(".");
      if (fieldSplit.length > 1) {
        tableName = fieldSplit[0];
        field = fieldSplit[1];
      }
    }

    const searchClause = search ? ` WHERE ${field} LIKE '%${search}%'` : "";
    const tableNameConcat = `${this.DEFAULT_SCHEMA}.${
      tableName || this.DEFAULT_TABLE_NAME
    }`;
    const query = `SELECT DISTINCT ${field} FROM ${tableNameConcat}${searchClause} ORDER BY ${field} ASC LIMIT 1000;`;
    const res: string[][] = await this.makeQuery(query);
    return res.map((row) => ({
      text: row[0],
    }));
  }

  /** output a table with the provided fields and filters applied */
  async getTable(
    fields: Field[],
    limit: number = 100,
    tableName: string = this.DEFAULT_TABLE_NAME,
    selections: Selections = []
  ): Promise<TableResponse> {
    let where = "";
    const tableQuery = `${this.DEFAULT_SCHEMA}.${tableName}`;
    if (selections.length) {
      where = ` WHERE 1=1`;
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
      .join(",")} FROM ${tableQuery}${where} LIMIT ${limit};`;
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
        width: out[0]?.length,
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
      const data = this.getData(results);
      if (data) {
        return data;
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
    // console.log("got id", id);
    const promise = new Promise((resolve, reject) => {
      setTimeout(async () => {
        this.counter++;
        console.log("counter", this.counter);
        const results = await axios.request({
          baseURL: this.BASEURL,
          url: `${this.statementURL}/${id}`,
          method: "GET",
          headers: this.buildHeader(this.API_KEY),
        });
        resolve(results);
      }, wait);
    });
    return promise;
  }
}
