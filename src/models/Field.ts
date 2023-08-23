export type FieldType = "dimension" | "measure";

export type Field = {
  fieldDef: string;
  fieldName?: string;
  fieldType?: FieldType;
  columnIndex?: number;
  format?: string;
  formatStyle?: string;
  sortOrder?: string;
  totalsFunction?: "Min" | "Max" | "Avg" | "Sum" | "Count";
  tableName?: string;
  aggregation?: "min" | "max" | "avg" | "sum" | "count";
};
export type Fields = Array<Field>;
