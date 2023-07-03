import { FieldValues } from "./FieldValues";

export type Selection = {
  fieldName: string;
  fieldDef: string;
  fieldValues: FieldValues;
};
export type Selections = Array<Selection>;
