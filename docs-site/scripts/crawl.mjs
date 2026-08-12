import { readdir, readFile } from "node:fs/promises";
import { dirname, extname, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { parse } from "parse5";

import { base, site } from "../config.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../dist");
const origin = new URL(site).origin;
const failures = [];
const references = [];

async function listFiles(directory, prefix = "") {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = posix.join(prefix, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listFiles(resolve(directory, entry.name), path)));
    } else {
      files.push(path);
    }
  }
  return files;
}

function walk(node, visit) {
  visit(node);
  for (const child of node.childNodes ?? []) walk(child, visit);
}

function attribute(node, name) {
  return node.attrs?.find((item) => item.name === name)?.value ?? null;
}

function routeForFile(file) {
  if (file === "index.html") return base;
  if (file.endsWith("/index.html")) return `${base}${file.slice(0, -"index.html".length)}`;
  return `${base}${file}`;
}

function localFile(url, files) {
  if (!url.pathname.startsWith(base)) return null;
  const path = decodeURIComponent(url.pathname.slice(base.length));
  const candidates =
    path === ""
      ? ["index.html"]
      : path.endsWith("/")
        ? [`${path}index.html`]
        : [path, `${path}/index.html`];
  return candidates.find((candidate) => files.has(candidate)) ?? null;
}

function addReference(source, value, kind, runtime = false) {
  if (
    !value ||
    value.startsWith("data:") ||
    value.startsWith("mailto:") ||
    value.startsWith("tel:") ||
    value.startsWith("javascript:")
  ) {
    return;
  }
  references.push({ source, value, kind, runtime });
}

function addCssReferences(source, css) {
  for (const match of css.matchAll(/url\(\s*["']?([^"')]+)["']?\s*\)/g)) {
    addReference(source, match[1], "css", true);
  }
  for (const match of css.matchAll(/@import\s+(?:url\()?\s*["']([^"']+)["']/g)) {
    addReference(source, match[1], "css", true);
  }
}

function addJavaScriptReferences(source, javascript) {
  const imports =
    /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s*|\bnew URL\(\s*)["']([^"']+)["']/g;
  for (const match of javascript.matchAll(imports)) {
    if (match[1].startsWith(".") || match[1].startsWith("/")) {
      addReference(source, match[1], "script", true);
    }
  }
}

const fileList = await listFiles(root);
const files = new Set(fileList);
const required = ["404.html", "favicon.svg", "pagefind/pagefind.js", "sitemap-index.xml"];

for (const file of required) {
  if (!files.has(file)) failures.push(`missing required output: ${file}`);
}

const documents = new Map();
const ids = new Map();

for (const file of fileList.filter((path) => extname(path) === ".html")) {
  const source = await readFile(resolve(root, file), "utf8");
  const document = parse(source);
  documents.set(file, document);
  const documentIds = new Set();
  const canonicals = [];
  let headings = 0;
  let mains = 0;

  walk(document, (node) => {
    const id = attribute(node, "id");
    if (id) documentIds.add(id);
    if (node.tagName === "h1") headings += 1;
    if (node.tagName === "main") mains += 1;

    const attributes = {
      a: ["href"],
      area: ["href"],
      audio: ["src"],
      embed: ["src"],
      form: ["action"],
      iframe: ["src"],
      image: ["href", "xlink:href"],
      img: ["src"],
      input: ["src"],
      link: ["href"],
      object: ["data"],
      script: ["src"],
      source: ["src"],
      track: ["src"],
      use: ["href", "xlink:href"],
      video: ["src", "poster"],
    }[node.tagName];

    for (const name of attributes ?? []) {
      const value = attribute(node, name);
      const rel = attribute(node, "rel") ?? "";
      const metadata =
        node.tagName === "link" && /(?:^|\s)(?:canonical|alternate)(?:\s|$)/.test(rel);
      if (node.tagName === "link" && /(?:^|\s)canonical(?:\s|$)/.test(rel) && value) {
        canonicals.push(value);
      }
      if (!metadata) {
        addReference(file, value, node.tagName, !["a", "area", "form"].includes(node.tagName));
      }
    }

    for (const name of ["srcset", "imagesrcset"]) {
      const value = attribute(node, name);
      for (const candidate of value?.split(",") ?? []) {
        addReference(file, candidate.trim().split(/\s+/)[0], node.tagName, true);
      }
    }

    const style = attribute(node, "style");
    if (style) addCssReferences(file, style);
    if (node.tagName === "style") {
      addCssReferences(file, node.childNodes?.map((child) => child.value ?? "").join("") ?? "");
    }
  });

  ids.set(file, documentIds);
  if (headings !== 1) failures.push(`${file}: expected exactly one h1, found ${headings}`);
  if (mains !== 1) failures.push(`${file}: expected exactly one main, found ${mains}`);
  if (canonicals.length !== 1) {
    failures.push(`${file}: expected exactly one canonical link, found ${canonicals.length}`);
  } else {
    const canonicalUrl = new URL(canonicals[0], site);
    const expected = new URL(routeForFile(file), site).href;
    if (file !== "404.html" && canonicalUrl.href !== expected) {
      failures.push(`${file}: canonical is ${canonicalUrl.href}, expected ${expected}`);
    }
    if (canonicalUrl.origin !== origin || !canonicalUrl.pathname.startsWith(base)) {
      failures.push(`${file}: canonical escapes DOCS_SITE or DOCS_BASE`);
    }
  }
}

for (const file of fileList.filter((path) => extname(path) === ".css")) {
  addCssReferences(file, await readFile(resolve(root, file), "utf8"));
}

for (const file of fileList.filter((path) => extname(path) === ".js")) {
  addJavaScriptReferences(file, await readFile(resolve(root, file), "utf8"));
}

for (const file of fileList.filter((path) => extname(path) === ".xml")) {
  const xml = await readFile(resolve(root, file), "utf8");
  for (const match of xml.matchAll(/<loc>([^<]+)<\/loc>/g)) addReference(file, match[1], "xml");
}

for (const reference of references) {
  const sourceUrl = new URL(routeForFile(reference.source), site);
  const targetUrl = new URL(reference.value, sourceUrl);

  if (targetUrl.origin !== origin) {
    if (reference.runtime) {
      failures.push(`${reference.source}: runtime ${reference.kind} loads ${targetUrl.href}`);
    }
    continue;
  }

  const targetFile = localFile(targetUrl, files);
  if (!targetFile) {
    failures.push(`${reference.source}: missing ${reference.kind} target ${targetUrl.pathname}`);
    continue;
  }

  if (targetUrl.hash && targetFile.endsWith(".html")) {
    const fragment = decodeURIComponent(targetUrl.hash.slice(1));
    if (fragment && !ids.get(targetFile)?.has(fragment)) {
      failures.push(`${reference.source}: missing fragment ${targetUrl.pathname}${targetUrl.hash}`);
    }
  }
}

if (failures.length > 0) {
  console.error(`Crawl failed (${failures.length}):`);
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(
    `Crawl passed: ${documents.size} pages, ${references.length} references, ` +
      `${files.size} output files at ${base}`,
  );
}
