import { User } from "../models/User";
import { MetaDataResponse } from "../MetaDataResponse";
import { SqlService } from "../models/SqlService";

/** Used to deliver metadata to the UI to support report authoring
 */
export const getMetadataHandler = async (
  user: User,
  sqlService: SqlService
): Promise<MetaDataResponse> => {
  return await sqlService.getMetadata(user);
};
