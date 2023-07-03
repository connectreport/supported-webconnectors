import { FieldValue } from "./FieldValues";

export type FieldValuesResponse = {
  fieldValues: Array<FieldValue>;
  size: {
    width: number;
    height: number;
  };
};
