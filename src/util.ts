import * as winston from "winston";
import "winston-daily-rotate-file";
import path from "path";
import { settings } from "./connectors";
import { serializeError } from "serialize-error";

if (process.platform !== "win32") {
  process.env.ProgramData = "/usr/local/etc/";
}

const transport = new winston.transports.DailyRotateFile({
  filename: "%DATE%.log",
  // @ts-ignore
  dirname: path.join(process.env.ProgramData, "ConnectReport", "log", "native-webconnectors"),
  datePattern: "YYYY-MM-DD-HH",
  zippedArchive: true,
  maxSize: "500m",
  maxFiles: "3d",
  level: settings.logLevel || "info",
});

const customFormat = winston.format((meta) => {
  if (meta.error) {
    meta.error = serializeError(meta.error);
  }
  if (meta.err) {
    meta.err = serializeError(meta.err);
  }
  return meta;
});

export const logger = winston.createLogger({
  transports: [transport],
  exitOnError: true,
  format: winston.format.combine(winston.format.timestamp(), customFormat(), winston.format.json())
});

if (process.env.NODE_ENV !== "production") {
  logger.add(
    new winston.transports.Console({
      format: winston.format.simple(),
      level: settings.logLevel || "info",
    })
  );
}

