import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  unlinkSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const owner = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const consumers = [
  { name: "vigil-ui", dir: "/home/vaab/dev/rs/vigil-ui/frontend" },
  { name: "rpg-ui", dir: "/home/vaab/dev/rs/rpg-ui/frontend" },
];

function fail(message) {
  throw new Error(`vendor: ${message}; nothing was written`);
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

const packageJson = readJson(join(owner, "package.json"));
const packageName = packageJson.name;
const version = packageJson.version;
const tarballName = `${packageName.replace(/^@/, "").replaceAll("/", "-")}-${version}.tgz`;
const dependency = `file:vendor/${tarballName}`;

const planned = consumers.map((consumer) => {
  const packagePath = join(consumer.dir, "package.json");
  if (!existsSync(packagePath)) fail(`missing ${consumer.name} package.json`);
  const manifest = readJson(packagePath);
  if (manifest.dependencies?.[packageName] !== dependency) {
    fail(`${consumer.name} must declare ${packageName} as ${dependency} before vendoring`);
  }
  return consumer;
});

execFileSync("npm", ["run", "build"], { cwd: owner, stdio: "inherit" });
const staging = mkdtempSync(join(tmpdir(), "kal-sse-client-vendor-"));
try {
  execFileSync("npm", ["pack", "--pack-destination", staging, "--ignore-scripts"], {
    cwd: owner,
    stdio: "inherit",
  });
  const produced = readdirSync(staging).filter((name) => name.endsWith(".tgz"));
  if (produced.length !== 1 || produced[0] !== tarballName) {
    fail(`expected ${tarballName}, got ${produced.join(", ") || "no tarball"}`);
  }
  const source = join(staging, tarballName);
  for (const consumer of planned) {
    const vendorDir = join(consumer.dir, "vendor");
    mkdirSync(vendorDir, { recursive: true });
    for (const filename of readdirSync(vendorDir)) {
      if (filename.startsWith("0k-kal-sse-client-") && filename.endsWith(".tgz") && filename !== tarballName) {
        unlinkSync(join(vendorDir, filename));
      }
    }
    copyFileSync(source, join(vendorDir, tarballName));
  }
  for (const consumer of planned) {
    execFileSync("npm", ["install", "--no-audit", "--no-fund"], {
      cwd: consumer.dir,
      stdio: "inherit",
    });
  }
} finally {
  rmSync(staging, { recursive: true, force: true });
}
