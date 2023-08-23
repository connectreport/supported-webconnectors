import numeral from "numeral";
import { TableCell, TableResponse, TableRow } from "../models/TableResponse";
import { Fields } from "../models/Field";
import { DateTime } from "luxon";
import "numeral/locales/en-gb";
import { settings } from "../connectors";
import { logger } from "../util";

if (settings.locale) {
  if (!["en-us", "en-gb"].includes(settings.locale)) {
    logger.error("Unsupported locale", { locale: settings.locale });
  }
  if (settings.locale !== "en-us") {
    numeral.locale(settings.locale);
  }
}

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
      text: "-",
    };
  }
  if (cell.text === '') {
    return {
      ...cell
    };
  }
  if (
    typeof cell.text !== "undefined" &&
    !isNaN(parseFloat(cell.text)) &&
    field.fieldType === "measure"
  ) {
    cell.number = parseFloat(cell.text);
  }
  if (
    field.fieldType === "measure" &&
    (field.format === "M" || field.format === "F") &&
    typeof cell.text !== "undefined" &&
    field.formatStyle
  ) {
    return { ...cell, text: numeral(cell.text).format(field.formatStyle) };
  } else if (
    field.fieldType === "measure" &&
    field.format === "D" &&
    typeof cell.text !== "undefined" &&
    field.formatStyle
  ) {
    return {
      ...cell,
      text: DateTime.fromISO(cell.text).toLocaleString(
        // @ts-ignore
        DateTime[field.formatStyle],
        { locale: settings.locale || "en-us" }
      ),
    };
  }
  return cell;
};
