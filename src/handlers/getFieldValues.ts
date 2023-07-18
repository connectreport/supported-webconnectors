import { User } from "../models/User";
import { inspect } from "util";
import { FieldValuesRequest } from "../models/FieldValuesRequest";
import { FieldValuesResponse } from "../models/FieldValuesResponse";
import { SqlService } from "../models/SqlService";

/** Used to retrieve list of fields values to filter on in UI */
export const getFieldValuesHandler = async (
  options: FieldValuesRequest,
  user: User,
  sqlService: SqlService
): Promise<FieldValuesResponse> => {
  let result = await sqlService.getFieldValues(
    user,
    options.field.fieldDef,
    options.field.tableName!,
    options.search,
    options.height,
    options.top,
  );
  return {
    fieldValues: result,
    size: {
      height: result.length,
      width: 1,
    },
  };
};
