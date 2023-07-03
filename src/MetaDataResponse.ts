import { Field } from "./models/Field";

export type MetaDataResponse = {
  /** List of fields available to filter on */
  filterFields?: Field[];
  visualizations?: {
    /** Name of the visualization */
    name: string;
    /** URL of a thumbnail */
    thumbnail?: string;
    /** Unique idenfitier for the visualization */
    id: string;
  }[];
  /** List of tables available to query */
  tables?: {
    /** Name of the table */
    name: string;
    /** List of fields available in the table */
    fields: Field[];
  }[];
  /** List of dimensions available to query */
  dimensions?: Field[];
  /** List of measures available to query */
  measures?: Field[];
};
