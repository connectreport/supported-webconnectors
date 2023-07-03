import { User } from "../models/User";
import { inspect } from "util";
import { FieldValuesRequest } from "../models/FieldValuesRequest";
import { FieldValuesResponse } from "../models/FieldValuesResponse";
import { debug } from "../util";
import { SqlService } from "../models/SqlService";

/** Used to retrieve list of fields values to filter on in UI */
export const getFieldValuesHandler = async (
  options: FieldValuesRequest,
  user: User,
  sqlService: SqlService
): Promise<FieldValuesResponse> => {
  debug("Received getFieldValues request", inspect(options), inspect(user));
  let result = await sqlService.getFieldValues(
    options.field.fieldDef,
    options.search
  );
  return {
    fieldValues: result,
    size: {
      height: result.length,
      width: 1,
    },
  };
};
