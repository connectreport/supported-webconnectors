import { Field } from "./Field";
import { FieldValue } from "./FieldValues";
import { Selections } from "./Selections";
import { TableResponse } from "./TableResponse";

export abstract class SqlService {
  public abstract getTable(
    fields: Field[],
    limit: number,
    tableName?: string,
    selections?: Selections
  ): Promise<TableResponse>;

  public abstract listTableColumns(tableName?: string): Promise<{
    name: string;
    fields: Field[];
  }>;

  public abstract listTables(): Promise<string[]>;

  public abstract getFieldValues(
    field: string,
    search?: string,
    tableName?: string
  ): Promise<FieldValue[]>;

  async getMetadata() {
    let tables: { name: string; fields: Field[] }[] = [];

    const tableList = await this.listTables();
    if (tableList && tableList.length > 0) {
      let promises = tableList.map(async (table) =>
        this.listTableColumns(table)
      );
      tables = await Promise.all(promises);
    } else {
      tables = [await this.listTableColumns()];
    }

    return {
      tables,
      filterFields: tables.flatMap((table) =>
        table.fields.reduce((acc, field) => {
          if (field.fieldType === "dimension") {
            acc.push(field);
          }
          return acc;
        }, [] as Field[])
      ).sort((a, b) => (a.fieldName || a.fieldDef).localeCompare(b.fieldName || b.fieldDef)),
    };
  }
}
