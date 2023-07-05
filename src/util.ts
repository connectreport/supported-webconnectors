import * as winston from "winston";
import "winston-daily-rotate-file";
import path from "path";
import { settings } from "./connectors";

if (process.platform !== "win32") {
  process.env.ProgramData = "/usr/local/etc/";
}

const transport = new winston.transports.DailyRotateFile({
  filename: "%DATE%.log",
  // @ts-ignore
  dirname: path.join(process.env.ProgramData, "ConnectReport", "log", "native-webconnectors"),
  datePattern: "YYYY-MM-DD-HH",
  zippedArchive: true,
  maxSize: "20m",
  maxFiles: "3d",
  level: settings.logLevel || "info",
});

export const logger = winston.createLogger({
  transports: [transport],
  exitOnError: true,
  format: winston.format.combine(winston.format.timestamp(), winston.format.json())
});

if (process.env.NODE_ENV !== "production") {
  logger.add(
    new winston.transports.Console({
      format: winston.format.simple(),
    })
  );
}

