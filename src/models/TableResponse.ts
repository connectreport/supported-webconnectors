export type PlaceholderType = "rowSpan" | "columnSpan" | "leftOfHeader";
export type TableCell = {
  text?: string;
  number?: string;
  level?: number;
  isPlaceholder?: boolean;
  placeholderType?: PlaceholderType;
  isLeft?: boolean;
  colSpan?: number;
  rowSpan?: number;
};
export type TableRow = Array<TableCell>;
export type TableResponse = {
  table: Array<TableRow>;
  grandTotalRow: TableRow;
  size: {
    width: number;
    height: number;
  };
  debug?: any;
};
