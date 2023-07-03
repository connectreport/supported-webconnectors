import { Field } from "./Field";
// import { FieldValues } from "./FieldValues";

export interface FieldValuesRequest {
  field: Field;
  height?: number;
  top?: number;
  search?: string;
}
