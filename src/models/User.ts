export type User = {
  username: string;
  email?: string;
  id?: string;
  qlikSenseUsername?: string;
  qsApiKey?: string;
  qSess?: string;
  sisenseSession?: {
    cookie: string;
    accessToken: string;
    expires: Date;
  };
  sisenseUsername?: string;
  contentfulApiKey?: string;
  qlikSenseUserDirectory?: string;
};
