import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { dirname, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { parse } from "parse5";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const dist = resolve(process.env.DOCS_DIST ?? resolve(repository, "docs-site/dist"));
const manifest = JSON.parse(await readFile(resolve(repository, "docs-site/migration.json"), "utf8"));
const failures = [];
const dispositions = new Set([
  "migrate",
  "link-to-existing-authority",
  "replace-with-canonical-starlight-content",
  "archive-in-git-history",
  "intentionally-retire",
]);

function check(condition, message) {
  if (!condition) failures.push(message);
}

function compare(label, actual, expected) {
  const actualSet = new Set(actual);
  const expectedSet = new Set(expected);
  for (const value of expectedSet) {
    if (!actualSet.has(value)) failures.push(`${label}: missing ${value}`);
  }
  for (const value of actualSet) {
    if (!expectedSet.has(value)) failures.push(`${label}: unexpected ${value}`);
  }
  check(actual.length === actualSet.size, `${label}: duplicate value`);
}

async function source(path) {
  try {
    return await readFile(resolve(repository, path), "utf8");
  } catch (error) {
    failures.push(`${path}: ${error.message}`);
    return "";
  }
}

function slug(heading) {
  return heading
    .replace(/`([^`]*)`/g, "$1")
    .replace(/\[([^\]]+)]\([^)]+\)/g, "$1")
    .replace(/[~*_]/g, "")
    .trim()
    .toLowerCase()
    .replace(/\s/g, "-")
    .replace(/[^\p{Letter}\p{Number}_-]/gu, "");
}

function markdownFragments(markdown) {
  const fragments = new Set(
    [...markdown.matchAll(/<(?:a|span)\s+id=["']([^"']+)["']/g)].map((match) => match[1]),
  );
  let fence = null;

  for (const line of markdown.split("\n")) {
    const content = line.replace(/^(?:\s*>\s*)+/, "");
    const marker = content.match(/^\s*(`{3,}|~{3,})/)?.[1] ?? null;
    if (marker) {
      if (!fence) fence = marker[0];
      else if (marker[0] === fence) fence = null;
      continue;
    }
    if (fence) continue;

    const heading = content.match(/^\s*#{1,6}\s+(.+?)\s*#*\s*$/)?.[1];
    if (heading) fragments.add(slug(heading));
  }

  return [...fragments];
}

function walk(node, visit) {
  visit(node);
  for (const child of node.childNodes ?? []) walk(child, visit);
}

function attribute(node, name) {
  return node.attrs?.find((item) => item.name === name)?.value ?? null;
}

async function outputIds(destination) {
  const url = new URL(destination);
  const path = decodeURIComponent(url.pathname.slice(1));
  const file = path === "" ? "index.html" : `${path}index.html`;
  const html = await readFile(resolve(dist, file), "utf8");
  const ids = new Set();
  walk(parse(html), (node) => {
    const id = node.attrs?.find((attribute) => attribute.name === "id")?.value;
    if (id) ids.add(id);
  });
  return ids;
}

async function listFiles(directory, prefix = "") {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = posix.join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...(await listFiles(resolve(directory, entry.name), path)));
    else files.push(path);
  }
  return files;
}

check(manifest.version === 1, "manifest version must be 1");
check(manifest.canonical_origin === "https://docs.referential.ai", "canonical origin changed");
check(manifest.legacy_origin === "https://referential-ai.github.io", "legacy origin changed");
check(manifest.legacy_base === "/platonic/", "legacy base changed");
check(manifest.redirect_origin === "https://referential.ai", "redirect origin changed");
compare("homepage sources", manifest.homepage.sources, ["/docs", "/docs/"]);
check(manifest.homepage.destination === `${manifest.canonical_origin}/`, "homepage destination changed");

const expectedSources = new Set([
  "book.toml",
  "docs/book/SUMMARY.md",
  ".github/workflows/docs-pages.yml",
]);
const summary = await source("docs/book/SUMMARY.md");
const summaryWrappers = [...summary.matchAll(/\]\(([^)]+\.md)\)/g)].map((match) =>
  posix.normalize(posix.join("docs/book", match[1])),
);

const redirectSources = [];
const legacyRoutes = [];
let fragmentCount = 0;

for (const route of manifest.routes) {
  expectedSources.add(route.wrapper);
  expectedSources.add(route.source);
  check(dispositions.has(route.disposition), `${route.title}: invalid disposition`);
  check(
    (await source(route.wrapper)).trim() === `{{#include ${route.include}}}`,
    `${route.wrapper}: include does not match manifest`,
  );
  check(
    posix.normalize(posix.join(posix.dirname(route.wrapper), route.include)) === route.source,
    `${route.wrapper}: include does not resolve to ${route.source}`,
  );
  compare(`${route.source} fragments`, route.fragments, markdownFragments(await source(route.source)));

  const destination = new URL(route.destination);
  check(destination.protocol === "https:", `${route.title}: destination must use HTTPS`);
  check(destination.origin === manifest.canonical_origin, `${route.title}: destination is off-host`);
  check(!destination.search && !destination.hash, `${route.title}: destination must be a page URL`);
  check(destination.pathname.endsWith("/"), `${route.title}: destination must end in /`);
  check(destination.origin !== manifest.redirect_origin, `${route.title}: redirect loop`);

  let ids;
  try {
    ids = await outputIds(route.destination);
  } catch (error) {
    failures.push(`${route.title}: missing generated destination: ${error.message}`);
    ids = new Set();
  }
  for (const fragment of route.fragments) {
    check(ids.has(fragment), `${route.title}: missing destination fragment #${fragment}`);
  }

  for (const sourcePath of route.redirect_sources) {
    const parsed = new URL(sourcePath, manifest.redirect_origin);
    check(parsed.origin === manifest.redirect_origin, `${route.title}: redirect source is off-host`);
    check(parsed.pathname === sourcePath, `${route.title}: redirect source has query or fragment`);
    check(sourcePath.startsWith("/docs/"), `${route.title}: redirect source is outside /docs/`);
    check(posix.normalize(sourcePath) === sourcePath, `${route.title}: redirect source is ambiguous`);
    check(decodeURI(sourcePath) === sourcePath, `${route.title}: redirect source is encoded`);
    check(!sourcePath.includes("*"), `${route.title}: wildcard redirect source`);
    check(
      route.legacy_routes.includes(sourcePath.replace(/^\/docs/, "/platonic")),
      `${route.title}: redirect source has no matching legacy route`,
    );
    redirectSources.push(sourcePath);
  }
  legacyRoutes.push(...route.legacy_routes);
  fragmentCount += route.fragments.length;
}

compare("SUMMARY wrappers", summaryWrappers, manifest.routes.map((route) => route.wrapper));
compare(
  "mdBook source inventory",
  manifest.sources.map((item) => item.path),
  [...expectedSources],
);
for (const item of manifest.sources) {
  check(dispositions.has(item.disposition), `${item.path}: invalid disposition`);
  await source(item.path);
}
compare("redirect sources", redirectSources, [...new Set(redirectSources)]);
compare("legacy routes", legacyRoutes, [...new Set(legacyRoutes)]);

const retiredPaths = manifest.intentional_404.paths;
compare("intentional 404 paths", retiredPaths, [...new Set(retiredPaths)]);
for (const path of retiredPaths) {
  check(path && !path.startsWith("/"), `intentional 404 path must be relative: ${path}`);
  check(posix.normalize(path) === path && !path.startsWith(".."), `ambiguous 404 path: ${path}`);
  check(!path.includes("*") && decodeURI(path) === path, `invalid 404 path: ${path}`);
  check(!redirectSources.includes(`/docs/${path}`), `404 path collides with redirect: ${path}`);
}

for (const asset of manifest.authored_assets) {
  check(dispositions.has(asset.disposition), `${asset.path}: invalid disposition`);
  try {
    const digest = createHash("sha256").update(await readFile(resolve(repository, asset.path))).digest("hex");
    check(digest === asset.sha256, `${asset.path}: digest is ${digest}, expected ${asset.sha256}`);
  } catch (error) {
    failures.push(`${asset.path}: ${error.message}`);
  }
}

for (const license of manifest.licenses) {
  check(dispositions.has(license.disposition), `${license.path}: invalid disposition`);
  if (license.linked_from) {
    await source(license.path);
    for (const path of license.linked_from) {
      check((await source(path)).includes(license.path), `${path}: missing ${license.path} link`);
    }
  } else {
    check(retiredPaths.includes(license.path), `${license.path}: missing retired asset`);
  }
}

for (const entry of manifest.entry_points) {
  check(dispositions.has(entry.disposition), `${entry.path}: invalid disposition`);
  check((await source(entry.path)).includes(entry.needle), `${entry.path}: missing ${entry.needle}`);
}

if (process.env.MDBOOK_DIST) {
  const expected = new Set(retiredPaths);
  const contentFiles = new Set();
  for (const route of manifest.routes) {
    for (const legacy of route.legacy_routes) {
      const file = legacy === "/platonic/" ? "index.html" : legacy.slice("/platonic/".length);
      expected.add(file);
      contentFiles.add(file);
    }
  }
  const root = resolve(process.env.MDBOOK_DIST);
  const outputFiles = await listFiles(root);
  const outputSet = new Set(outputFiles);
  compare("mdBook output inventory", outputFiles, [...expected]);

  const documents = new Map();
  const ids = new Map();
  for (const file of outputFiles.filter((path) => path.endsWith(".html"))) {
    const document = parse(await readFile(resolve(root, file), "utf8"));
    const documentIds = new Set();
    walk(document, (node) => {
      const id = attribute(node, "id");
      if (id) documentIds.add(id);
    });
    documents.set(file, document);
    ids.set(file, documentIds);
  }

  for (const route of manifest.routes) {
    const legacy = route.legacy_routes.find((path) => path.endsWith(".html")) ?? "/platonic/";
    const file = legacy === "/platonic/" ? "index.html" : legacy.slice("/platonic/".length);
    for (const fragment of route.fragments) {
      check(ids.get(file)?.has(fragment), `${file}: missing legacy fragment #${fragment}`);
    }
  }

  for (const [file, document] of documents) {
    if (!contentFiles.has(file)) continue;
    const pageUrl = new URL(file, `${manifest.legacy_origin}${manifest.legacy_base}`);
    let baseHref = null;
    walk(document, (node) => {
      if (!baseHref && node.tagName === "base") baseHref = attribute(node, "href");
    });
    const referenceBase = baseHref ? new URL(baseHref, pageUrl) : pageUrl;

    walk(document, (node) => {
      for (const name of ["href", "src"]) {
        const value = attribute(node, name);
        if (!value || /^(?:data:|mailto:|tel:|javascript:)/.test(value)) continue;
        const target = new URL(value, referenceBase);
        if (target.origin !== manifest.legacy_origin) continue;
        if (!target.pathname.startsWith(manifest.legacy_base)) {
          failures.push(`${file}: target escapes legacy base: ${target.pathname}`);
          continue;
        }
        const path = decodeURIComponent(target.pathname.slice(manifest.legacy_base.length));
        const targetFile = path === "" ? "index.html" : path.endsWith("/") ? `${path}index.html` : path;
        if (!outputSet.has(targetFile)) {
          failures.push(`${file}: missing legacy target ${target.pathname}`);
        } else if (target.hash && targetFile.endsWith(".html")) {
          const fragment = decodeURIComponent(target.hash.slice(1));
          if (fragment && !ids.get(targetFile)?.has(fragment)) {
            failures.push(`${file}: missing legacy fragment ${target.pathname}${target.hash}`);
          }
        }
      }
    });
  }
}

if (failures.length > 0) {
  console.error(`Migration check failed (${failures.length}):`);
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(
    `Migration check passed: ${manifest.routes.length} chapters, ${fragmentCount} fragments, ` +
      `${retiredPaths.length} intentional 404s, ${manifest.authored_assets.length} authored assets`,
  );
}
