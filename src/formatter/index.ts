import numeral from "numeral";
import { TableCell, TableResponse, TableRow } from "../models/TableResponse";
import { Fields } from "../models/Field";
import { DateTime } from "luxon";

export const formatTable = (
  table: TableResponse["table"],
  fields: Fields
): Array<TableRow> => {
  const formattedTable = table.map((row) => {
    return row.map((cell, index) => {
      return formatCell(cell, fields[index]);
    });
  });
  return formattedTable;
};

export const formatCell = (cell: TableCell, field: Fields[number]) => {
  if (cell.text === null) {
    return {
      ...cell,
      text: "-"
    }
  }
  if (field.fieldType === "measure" && (field.format === "M" || field.format === "F") && cell.text && field.formatStyle) {
    return { ...cell, text: numeral(cell.text).format(field.formatStyle) };
  } else if (field.fieldType === "measure" && (field.format === "D") && cell.text && field.formatStyle) {
    // @ts-ignore
    return { ...cell, text: DateTime.fromISO(cell.text).toLocaleString(DateTime[field.formatStyle]) };
  }
  return cell;
}