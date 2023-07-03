import { TableRequest } from "./models/TableRequest";
import { User } from "./models/User";
import { getTableHandler } from "./handlers/getTable";
import { getMetadataHandler } from "./handlers/getMetadata";
import { getFieldValuesHandler } from "./handlers/getFieldValues";
import { debug } from "./util";
import { inspect } from "util";
import { getConnector } from "./connectors";
import express, { json, Router } from "express";
import path from "path";

const app = express();
const router = Router();

app.use("/public", express.static(path.join(process.cwd(), "/public")));
app.use(json(), router);


debug("Public directory: ", path.join(process.cwd(), "/public"));

router.use("/:connector/*", (req, res, next) => {
  const { connector } = req.params;
  debug("Received request", connector, req.url, inspect(req.body));
  if (!getConnector(connector)) {
    return res.status(404).json({ error: "Unknown connector" });
  }
  return next();
});

router.post("/:connector/getTable", async (req, res) => {
  const { options, user } = req.body as { options: TableRequest; user: User };
  const { connector: connectorName } = req.params;
  const connector = getConnector(connectorName)!;

  try {
    const tableResponse = await getTableHandler(
      options,
      user,
      connector.sqlService
    );
    return res.json(tableResponse);
  } catch (err: any) {
    res
      .status(500)
      .json({ message: err.message, disableRetry: err.disableRetry });
  }
});

router.post("/:connector/getMetadata", async (req, res) => {
  const { user } = req.body;
  const { connector: connectorName } = req.params;
  const connector = getConnector(connectorName)!;

  try {
    const response = await getMetadataHandler(user, connector.sqlService);

    const output = res.json(response);
    return output;
  } catch (err) {
    res.status(500).json({ error: "Unknown" });
  }
});

router.post("/:connector/getFieldValues", async (req, res) => {
  const { options, user } = req.body;
  const { connector: connectorName } = req.params;
  const connector = getConnector(connectorName)!;

  try {
    const response = await getFieldValuesHandler(
      options,
      user,
      connector.sqlService
    );
    return res.json(response);
  } catch (err) {
    res.status(500).json({ error: "Unknown" });
  }
});

app.listen(3000, () => {
  console.log("listening on *:3000");
  console.log("Web connector listening on 3000", "– Debug:", process.env.DEBUG);
});
