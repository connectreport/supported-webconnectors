import { Field } from "./Field";
import { FieldValue } from "./FieldValues";
import { Selections } from "./Selections";
import { TableResponse } from "./TableResponse";
import { User } from "./User";


export type MappedField = Field & {
  raw?: any;
};

export abstract class SqlService {
  public abstract getTable(
    user: User,
    fields: Field[],
    limit: number,
    tableName?: string,
    selections?: Selections
  ): Promise<TableResponse>;

  public abstract listTableColumns(user: User, tableName?: string): Promise<{
    name: string;
    fields: Field[];
  }>;

  public abstract listTables(): Promise<string[]>;

  public abstract getFieldValues(
    user: User,
    field: string,
    search?: string,
    tableName?: string,
    height?: number,
    top?: number
  ): Promise<FieldValue[]>;

  async getMetadata(user: User) {
    let tables: { name: string; fields: Field[] }[] = [];

    const tableList = await this.listTables();
    if (tableList && tableList.length > 0) {
      let promises = tableList.map(async (table) =>
        this.listTableColumns(user, table)
      );
      tables = await Promise.all(promises);
    } else {
      tables = [await this.listTableColumns(user)];
    }

    return {
      tables,
      filterFields: tables.flatMap((table) =>
        table.fields
      ).sort((a, b) => (a.fieldName || a.fieldDef).localeCompare(b.fieldName || b.fieldDef)),
    };
  }
}
