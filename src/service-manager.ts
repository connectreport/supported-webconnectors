import { Service } from "node-windows";

const svc = new Service({
  name: "ConnectReport Web Connector Service Manager",
  description: "Manages Web Connector Services",
  script: process.cwd() + "/dist/app.js",
});

export const installService = () => {
  svc.on("install", () => {
    svc.start();
  });

  svc.install();
};

export const uninstallService = () => {
  svc.uninstall();
  svc.on("uninstall", function () {
    console.log("Uninstall complete.");
  });
};

if (process.argv[2] === "install") {
  installService();
} else if (process.argv[2] === "uninstall") {
  uninstallService();
} else {
  console.log("Unknown command");
}
