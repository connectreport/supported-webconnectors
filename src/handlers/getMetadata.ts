import { User } from "../models/User";
import { inspect } from "util";
import { debug } from "../util";
import { MetaDataResponse } from "../MetaDataResponse";
import { Field } from "../models/Field";
import { SqlService } from "../models/SqlService";

/** Used to deliver metadata to the UI to support report authoring
 */
export const getMetadataHandler = async (
  user: User,
  sqlService: SqlService
): Promise<MetaDataResponse> => {
  debug("Received getMetadata request", inspect(user));

  return await sqlService.getMetadata();
};
