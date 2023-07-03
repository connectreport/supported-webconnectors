import * as crypto from "crypto";
import * as fs from "fs";
import * as jwt from "jsonwebtoken";

export function createToken(
  path: string,
  account_identifer: string,
  username: string,
  passphrase?: string
) {
  const qualified_username = (account_identifer + "." + username).toUpperCase();
  var privateKeyFile = fs.readFileSync(path);

  let privateKeyObject = crypto.createPrivateKey({
    key: privateKeyFile,
    format: "pem",
    passphrase,
  });

  let privateKey = privateKeyObject.export({ format: "pem", type: "pkcs8" });

  let publicKeyObject = crypto.createPublicKey({
    key: privateKey,
    format: "pem",
  });
  var publicKey = publicKeyObject.export({ format: "der", type: "spki" });

  console.log("pub key", publicKey);

  var publicKeyFingerprint =
    "SHA256:" +
    // @ts-ignore
    crypto.createHash("sha256").update(publicKey, "utf8").digest("base64");

  var signOptions = {
    iss: qualified_username + "." + publicKeyFingerprint,
    sub: qualified_username,
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 60 * 60,
  };

  console.log("\n");
  console.log(signOptions);
  console.log("\n");

  var token = jwt.sign(signOptions, privateKey, { algorithm: "RS256" });
  console.log("\nToken: \n\n" + token);
  return token;
}
