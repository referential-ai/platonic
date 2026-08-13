import { readFile } from "node:fs/promises";
import { dirname, matchesGlob, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const workflow = await readFile(resolve(repository, ".github/workflows/docs-site.yml"), "utf8");
const pathBlocks = [...workflow.matchAll(/^ {4}paths:\n((?: {6}- .+\n?)+)/gm)];

if (pathBlocks.length !== 2) {
  throw new Error(`expected pull_request and push path filters, found ${pathBlocks.length}`);
}

function patterns(block) {
  return block[1]
    .trim()
    .split("\n")
    .map((line) => line.trim().slice(2).replace(/^(["'])(.*)\1$/, "$2"));
}

const pullRequestPaths = patterns(pathBlocks[0]);
const pushPaths = patterns(pathBlocks[1]);
if (pullRequestPaths.join("\n") !== pushPaths.join("\n")) {
  throw new Error("pull_request and push documentation path filters differ");
}
const requiredPaths = ["docs-site/**", "**/*.md", ".github/workflows/docs-site.yml"];
if (pullRequestPaths.join("\n") !== requiredPaths.join("\n")) {
  throw new Error(`documentation path filters must be exactly: ${requiredPaths.join(", ")}`);
}

const matrix = [
  ["docs-site/src/content/docs/user/index.mdx", true],
  ["docs-site/astro.config.mjs", true],
  ["docs-site/scripts/crawl.mjs", true],
  ["docs-site/public/favicon.svg", true],
  ["docs-site/package.json", true],
  ["docs-site/package-lock.json", true],
  [".github/workflows/docs-site.yml", true],
  ["README.md", true],
  ["docs/QUICKSTART.md", true],
  ["crates/platonic-core/docs/ARCHITECTURE.md", true],
  ["crates/platonic-server/src/lib.rs", false],
  ["desktop/src/routes/+page.svelte", false],
  ["Cargo.lock", false],
];

const failures = [];
for (const [path, expected] of matrix) {
  const invoked = pullRequestPaths.some((pattern) => matchesGlob(path, pattern));
  console.log(`${invoked ? "invoke" : "skip  "} ${path}`);
  if (invoked !== expected) failures.push(`${path}: expected ${expected ? "invoke" : "skip"}`);
}

if (failures.length > 0) {
  console.error(`CI path matrix failed (${failures.length}):`);
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(`CI path matrix passed: ${matrix.length} representative changes`);
}
