import { Fields } from "./Field";
import { Selections } from "./Selections";

export interface TableRequest {
  fields: Fields;
  height: number;
  top: number;
  selections?: Selections;
  tableName?: string;
  debug?: boolean;
}
