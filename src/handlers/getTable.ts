import { TableRequest } from "../models/TableRequest";
import { TableResponse, TableCell } from "../models/TableResponse";
import { User } from "../models/User";
import { inspect } from "util";
import { debug } from "../util";
import { SqlService } from "../models/SqlService";
import { formatCell, formatTable } from "../formatter";

/** Used to fulfill tabular data requests */
export const getTableHandler = async (
  request: TableRequest,
  user: User,
  sqlService: SqlService
): Promise<TableResponse> => {
  debug(
    "Received getTable request",
    inspect(request, { compact: false, depth: 5, breakLength: 80 }),
    inspect(user)
  );
  const response = await sqlService.getTable(
    request.fields,
    request.height,
    request.tableName,
    request.selections
  );

  return {
    ...response,
    table: formatTable(response.table, request.fields),
    grandTotalRow: response.grandTotalRow.map((cell, index) => {
      return formatCell(
        cell,
        request.fields.filter((field) => field.fieldType === "measure")[index]
      );
    }),
  };
};
