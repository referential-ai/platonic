import { spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { base, site } from "../config.mjs";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const siteRoot = resolve(scriptDirectory, "..");
const dist = resolve(siteRoot, "dist");
const baseUrl = new URL(base, site);

async function inject(fixture, element) {
  const path = resolve(fixture, "index.html");
  const source = await readFile(path, "utf8");
  if (!source.includes("</main>")) throw new Error("fixture target has no closing main element");
  await writeFile(path, source.replace("</main>", `${element}</main>`));
}

async function expectFailure(name, mutate, script, expected, args = []) {
  const fixture = await mkdtemp(join(tmpdir(), "platonic-docs-552-"));
  try {
    await cp(dist, fixture, { recursive: true });
    await mutate(fixture);
    const result = spawnSync(process.execPath, [resolve(scriptDirectory, script), ...args], {
      cwd: siteRoot,
      encoding: "utf8",
      env: { ...process.env, DOCS_DIST: fixture },
    });
    const output = `${result.stdout}${result.stderr}`;
    if (result.status === 0) throw new Error(`${name}: controlled negative unexpectedly passed`);
    const diagnostic = output.split("\n").find((line) => line.includes(expected));
    if (!diagnostic) {
      throw new Error(`${name}: expected diagnostic "${expected}"\n${output}`);
    }
    console.log(`Negative passed: ${name} -> ${diagnostic.replace(/^- /, "")}`);
  } finally {
    await rm(fixture, { recursive: true, force: true });
  }
}

await expectFailure(
  "broken route",
  async (fixture) =>
    await inject(
      fixture,
      `<a href="${new URL("fixture-missing-route/", baseUrl).pathname}">fixture</a>`,
    ),
  "crawl.mjs",
  `missing page target ${new URL("fixture-missing-route/", baseUrl).pathname}`,
);

await expectFailure(
  "broken fragment",
  async (fixture) => await inject(fixture, '<a href="#fixture-missing-fragment">fixture</a>'),
  "crawl.mjs",
  `missing fragment ${baseUrl.pathname}#fixture-missing-fragment`,
);

await expectFailure(
  "broken asset",
  async (fixture) =>
    await inject(
      fixture,
      `<img src="${new URL("fixture-missing-image.svg", baseUrl).pathname}" alt="">`,
    ),
  "crawl.mjs",
  `missing image target ${new URL("fixture-missing-image.svg", baseUrl).pathname}`,
);

await expectFailure(
  "missing search result",
  async () => {},
  "search.mjs",
  `expected ${new URL("user/not-indexed/", baseUrl).pathname}`,
  [resolve(scriptDirectory, "fixtures/search-missing.json")],
);

console.log("Controlled negatives passed: 4 intended failures");
