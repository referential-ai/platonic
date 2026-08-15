import { spawn } from "node:child_process";
import { cp, mkdir, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, extname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dist = resolve(siteRoot, "dist");
const workRoot = await mkdtemp(join(tmpdir(), "platonic-docs-558-"));
const rootDist = resolve(workRoot, "root");
const subpathDist = resolve(workRoot, "subpath");
const subpath = "/platonic-docs/";
const testArgs = process.argv.slice(2);
const updateSnapshots = testArgs.some((argument) => argument.startsWith("--update-snapshots"));
const artifacts =
  process.env.DOCS_BROWSER_ARTIFACTS ?? join(tmpdir(), "platonic-docs-558-browser");
const baselineRoot = resolve(siteRoot, "browser-baselines");

if (updateSnapshots && process.env.DOCS_UPDATE_BASELINES !== "reviewed") {
  throw new Error(
    "baseline updates require DOCS_UPDATE_BASELINES=reviewed and a reviewed manifest rationale",
  );
}

function run(command, args, env = process.env) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, { cwd: siteRoot, env, stdio: "inherit" });
    child.once("error", rejectRun);
    child.once("exit", (code, signal) => {
      if (code === 0) resolveRun();
      else rejectRun(new Error(`${command} exited with ${signal ?? code}`));
    });
  });
}

async function build(base, destination) {
  await rm(dist, { force: true, recursive: true });
  await run("npm", ["run", "build"], { ...process.env, DOCS_BASE: base });
  await cp(dist, destination, { recursive: true });
}

const contentTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".wasm", "application/wasm"],
  [".xml", "application/xml; charset=utf-8"],
]);

async function existingFile(root, relativePath) {
  const candidates =
    relativePath === ""
      ? ["index.html"]
      : relativePath.endsWith("/")
        ? [`${relativePath}index.html`]
        : [relativePath, `${relativePath}/index.html`];

  for (const relative of candidates) {
    const candidate = resolve(root, relative);
    if (candidate !== root && !candidate.startsWith(`${root}${sep}`)) return null;
    try {
      if ((await stat(candidate)).isFile()) return candidate;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  return null;
}

async function serve(request, response) {
  if (!request.url || !["GET", "HEAD"].includes(request.method ?? "")) {
    response.writeHead(405).end();
    return;
  }

  const url = new URL(request.url, "http://127.0.0.1");
  if (url.pathname === subpath.slice(0, -1)) {
    response.writeHead(308, { location: subpath }).end();
    return;
  }

  let pathname;
  try {
    pathname = decodeURIComponent(url.pathname);
  } catch {
    response.writeHead(400).end();
    return;
  }

  const isSubpath = pathname.startsWith(subpath);
  const root = isSubpath ? subpathDist : rootDist;
  const relative = pathname.slice(isSubpath ? subpath.length : 1);
  const file = await existingFile(root, relative);
  const status = file ? 200 : 404;
  const responseFile = file ?? resolve(root, "404.html");
  const body = await readFile(responseFile);

  response.writeHead(status, {
    "cache-control": "no-store",
    "content-length": body.length,
    "content-type": contentTypes.get(extname(responseFile)) ?? "application/octet-stream",
  });
  response.end(request.method === "HEAD" ? undefined : body);
}

const server = createServer((request, response) => {
  serve(request, response).catch((error) => {
    console.error(error);
    response.writeHead(500).end();
  });
});

try {
  await build("/", rootDist);
  await build(subpath, subpathDist);
  if (updateSnapshots) {
    await cp(baselineRoot, resolve(workRoot, "baseline-before"), { recursive: true });
  }

  await new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("browser server has no TCP address");
  const origin = `http://127.0.0.1:${address.port}`;
  console.log(`Browser proof server: ${origin} (root and ${subpath})`);

  await run(
    process.execPath,
    [
      resolve(siteRoot, "node_modules/@playwright/test/cli.js"),
      "test",
      "--config",
      resolve(siteRoot, "scripts/browser-playwright.config.mjs"),
      ...testArgs,
    ],
    { ...process.env, DOCS_BROWSER_ORIGIN: origin },
  );
  if (updateSnapshots) {
    await mkdir(artifacts, { recursive: true });
    await cp(resolve(workRoot, "baseline-before"), resolve(artifacts, "baseline-before"), {
      recursive: true,
    });
    await cp(baselineRoot, resolve(artifacts, "baseline-after"), { recursive: true });
  }
} finally {
  if (server.listening) await new Promise((resolveClose) => server.close(resolveClose));
  await rm(workRoot, { force: true, recursive: true });
  await rm(dist, { force: true, recursive: true });
  await rm(resolve(siteRoot, ".astro"), { force: true, recursive: true });
}
