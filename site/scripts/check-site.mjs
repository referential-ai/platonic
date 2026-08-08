import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { XMLParser, XMLValidator } from "fast-xml-parser";
import { parse } from "parse5";

const SITE_ORIGIN = "https://referential.ai";
const SITE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const CHECK_EXTERNAL_LINKS = process.argv.includes("--external");
const EXTERNAL_ATTEMPTS = 3;
const EXTERNAL_TIMEOUT_MS = 8_000;

const unexpectedArguments = process.argv.slice(2).filter((argument) => argument !== "--external");
if (unexpectedArguments.length > 0) {
  throw new Error(`unexpected argument${unexpectedArguments.length === 1 ? "" : "s"}: ${unexpectedArguments.join(", ")}`);
}

const routes = new Map([
  ["/", "index.html"],
  ["/blog/", "blog/index.html"],
  ["/blog/introducing-platonic/", "blog/introducing-platonic/index.html"],
]);

const requiredFiles = [
  ...routes.values(),
  "assets/site.css",
  "../docs/images/desktop-plato-agent.png",
  "feed.xml",
  "robots.txt",
  "sitemap.xml",
];

const requiredExternalLinks = [
  "https://docs.rs/plato-agent/0.1.0/plato_agent/",
  "https://docs.rs/platonic-core/0.1.0/platonic_core/",
  "https://github.com/referential-ai/platonic",
  "https://github.com/referential-ai/platonic/blob/main/docs/QUICKSTART.md",
  "https://github.com/referential-ai/platonic/discussions",
  "https://github.com/referential-ai/platonic/releases",
];

const allowedExternalOrigins = new Set([
  "https://docs.rs",
  "https://github.com",
]);

const failures = [];
const publishedFileSources = new Map([
  ["assets/plato-agent-desktop.png", "../docs/images/desktop-plato-agent.png"],
]);

function check(condition, message) {
  if (!condition) {
    failures.push(message);
  }
}

async function readSiteFile(path) {
  try {
    return await readFile(resolve(SITE_ROOT, path));
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    failures.push(`${path}: ${reason}`);
    return null;
  }
}

function readPublishedFile(path) {
  return readSiteFile(publishedFileSources.get(path) ?? path);
}

function walk(node, visit) {
  visit(node);
  for (const child of node.childNodes ?? []) {
    walk(child, visit);
  }
}

function elements(document, tagName) {
  const matches = [];
  walk(document, (node) => {
    if (node.tagName === tagName) {
      matches.push(node);
    }
  });
  return matches;
}

function attribute(node, name) {
  return node.attrs?.find((item) => item.name === name)?.value ?? null;
}

function textContent(node) {
  if (node.nodeName === "#text") {
    return node.value ?? "";
  }
  return (node.childNodes ?? []).map(textContent).join("");
}

function localFileForPath(pathname) {
  if (pathname === "/") {
    return "index.html";
  }
  if (pathname.endsWith("/")) {
    return `${pathname.slice(1)}index.html`;
  }
  return pathname.slice(1);
}

function asArray(value) {
  if (value === undefined || value === null) {
    return [];
  }
  return Array.isArray(value) ? value : [value];
}

function wait(milliseconds) {
  return new Promise((resolveWait) => setTimeout(resolveWait, milliseconds));
}

function shouldRetryStatus(status) {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

async function requestExternalLink(link) {
  let lastFailure = "request did not run";

  for (let attempt = 1; attempt <= EXTERNAL_ATTEMPTS; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), EXTERNAL_TIMEOUT_MS);

    try {
      const response = await fetch(link, {
        headers: {
          accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
          "user-agent": "platonic-site-link-check/1.0",
        },
        redirect: "follow",
        signal: controller.signal,
      });
      await response.body?.cancel();

      if (response.ok) {
        return { attempt, finalUrl: response.url, status: response.status };
      }

      lastFailure = `HTTP ${response.status}`;
      if (!shouldRetryStatus(response.status)) {
        break;
      }
    } catch (error) {
      lastFailure = error instanceof Error ? error.message : String(error);
    } finally {
      clearTimeout(timeout);
    }

    if (attempt < EXTERNAL_ATTEMPTS) {
      await wait(250 * attempt);
    }
  }

  return { failure: lastFailure };
}

async function proveExternalLinks(links) {
  const normalizedLinks = [...new Set([...links].map((link) => {
    const url = new URL(link);
    url.hash = "";
    return url.href;
  }))].sort();

  console.log(`Checking ${normalizedLinks.length} live external links (${EXTERNAL_ATTEMPTS} attempts, ${EXTERNAL_TIMEOUT_MS}ms per attempt).`);

  for (const link of normalizedLinks) {
    const result = await requestExternalLink(link);
    if ("failure" in result) {
      failures.push(`external link ${link}: ${result.failure}`);
      console.error(`FAIL ${link}: ${result.failure}`);
      continue;
    }

    const destination = result.finalUrl === link ? "" : ` -> ${result.finalUrl}`;
    console.log(`PASS ${link}${destination} (HTTP ${result.status}, attempt ${result.attempt})`);
  }

  return normalizedLinks.length;
}

for (const path of requiredFiles) {
  await readSiteFile(path);
}

const documents = new Map();
const discoveredExternalLinks = new Set();
const discoveredLocalReferences = [];

for (const [route, path] of routes) {
  const sourceBuffer = await readSiteFile(path);
  if (!sourceBuffer) {
    continue;
  }

  const source = sourceBuffer.toString("utf8");
  const document = parse(source, { sourceCodeLocationInfo: true });
  documents.set(route, document);

  const html = elements(document, "html");
  const headings = elements(document, "h1");
  const mains = elements(document, "main");
  const scripts = elements(document, "script");
  const links = elements(document, "link");
  const anchors = elements(document, "a");
  const images = elements(document, "img");
  const metas = elements(document, "meta");

  check(html.length === 1 && attribute(html[0], "lang") === "en", `${path}: expected one <html lang="en">`);
  check(headings.length === 1, `${path}: expected exactly one h1`);
  check(mains.length === 1 && attribute(mains[0], "id") === "main-content", `${path}: expected <main id="main-content">`);
  check(scripts.length === 0, `${path}: runtime scripts are outside the static-site contract`);

  const canonical = links.filter((node) => attribute(node, "rel") === "canonical");
  check(canonical.length === 1, `${path}: expected exactly one canonical link`);
  if (canonical.length === 1) {
    check(attribute(canonical[0], "href") === `${SITE_ORIGIN}${route}`, `${path}: canonical URL must be ${SITE_ORIGIN}${route}`);
  }

  const viewport = metas.filter((node) => attribute(node, "name") === "viewport");
  const description = metas.filter((node) => attribute(node, "name") === "description");
  check(viewport.length === 1, `${path}: expected one viewport meta tag`);
  check(description.length === 1 && Boolean(attribute(description[0], "content")?.trim()), `${path}: expected one nonempty description meta tag`);

  const pageText = textContent(document).replace(/\s+/g, " ").trim();
  check(pageText.includes("Platonic"), `${path}: missing Platonic framework name`);
  check(pageText.includes("by Referential.ai"), `${path}: missing exact framework endorsement`);
  check(pageText.includes("Plato Agent"), `${path}: missing Plato Agent runtime name`);
  check(!pageText.includes("Platonic Runtime"), `${path}: must not rename Plato Agent to Platonic Runtime`);
  check(!source.includes("referential-ai/platonic-workspace"), `${path}: must not expose the private workspace authority URL`);
  check(!/discord(?:\.gg|\.com\/invite)/i.test(source), `${path}: Discord requires a separately approved public invite`);

  const references = [
    ...anchors.map((node) => ({ tag: "a", value: attribute(node, "href") })),
    ...links.map((node) => ({ tag: "link", value: attribute(node, "href") })),
    ...images.map((node) => ({ tag: "img", value: attribute(node, "src") })),
  ].filter((reference) => reference.value);

  for (const reference of references) {
    const url = new URL(reference.value, `${SITE_ORIGIN}${route}`);
    if (url.origin !== SITE_ORIGIN) {
      discoveredExternalLinks.add(url.href);
      check(allowedExternalOrigins.has(url.origin), `${path}: external origin is not approved: ${url.origin}`);
      continue;
    }
    discoveredLocalReferences.push({ path, tag: reference.tag, url });
  }

  for (const meta of metas.filter((node) => attribute(node, "property") === "og:image")) {
    const value = attribute(meta, "content");
    if (value) {
      const url = new URL(value, SITE_ORIGIN);
      check(url.origin === SITE_ORIGIN, `${path}: og:image must be hosted by referential.ai`);
      discoveredLocalReferences.push({ path, tag: "meta", url });
    }
  }
}

for (const link of requiredExternalLinks) {
  const present = [...discoveredExternalLinks].some((candidate) => {
    if (link === "https://github.com/referential-ai/platonic") {
      return candidate === link || candidate === `${link}/` || candidate === `${link}#readme`;
    }
    return candidate === link;
  });
  check(present, `site: missing canonical external link ${link}`);
}

const checkedExternalLinks = CHECK_EXTERNAL_LINKS
  ? await proveExternalLinks(discoveredExternalLinks)
  : 0;

for (const reference of discoveredLocalReferences) {
  const targetPath = localFileForPath(reference.url.pathname);
  const target = await readPublishedFile(targetPath);
  if (!target || !reference.url.hash || !targetPath.endsWith(".html")) {
    continue;
  }

  const targetRoute = [...routes].find(([, path]) => path === targetPath)?.[0];
  const targetDocument = targetRoute ? documents.get(targetRoute) : parse(target.toString("utf8"));
  const targetId = decodeURIComponent(reference.url.hash.slice(1));
  let found = false;
  if (targetDocument) {
    walk(targetDocument, (node) => {
      if (attribute(node, "id") === targetId) {
        found = true;
      }
    });
  }
  check(found, `${reference.path}: ${reference.tag} references missing fragment ${reference.url.pathname}${reference.url.hash}`);
}

const home = documents.get("/");
if (home) {
  const h1 = elements(home, "h1")[0];
  check(Boolean(h1) && textContent(h1).trim() === "Platonic", "index.html: h1 must be the framework name Platonic");
  const homeText = textContent(home).replace(/\s+/g, " ");
  check(homeText.includes("plato replay"), "index.html: missing stable plato replay command");

  const installCommands = elements(home, "code")
    .filter((node) => textContent(node).trim() === "cargo install plato-agent --locked");
  check(installCommands.length === 1, "index.html: expected one exact cargo install plato-agent --locked command");

  const primaryActions = elements(home, "a")
    .filter((node) => textContent(node).replace(/\s+/g, " ").trim() === "Start with Plato Agent");
  check(primaryActions.length === 1, "index.html: expected one Start with Plato Agent primary action");
  if (primaryActions.length === 1) {
    check(attribute(primaryActions[0], "href") === "#start", "index.html: Start with Plato Agent must target #start");
  }

  const startSections = elements(home, "section")
    .filter((node) => attribute(node, "id") === "start");
  check(startSections.length === 1, "index.html: expected one #start section");
}

const cssBuffer = await readSiteFile("assets/site.css");
if (cssBuffer) {
  const css = cssBuffer.toString("utf8");
  check(!/@import\b/i.test(css), "assets/site.css: remote or layered CSS imports are not allowed");
  check(!/url\(\s*["']?https?:/i.test(css), "assets/site.css: remote assets are not allowed");
  check(!/(?:linear|radial|conic)-gradient\s*\(/i.test(css), "assets/site.css: gradient decoration is outside the visual contract");
  check(!/letter-spacing\s*:\s*-/i.test(css), "assets/site.css: negative letter spacing is not allowed");
  check(css.includes(":focus-visible"), "assets/site.css: missing visible keyboard focus styling");
  check(css.includes("prefers-reduced-motion: reduce"), "assets/site.css: missing reduced-motion behavior");
}

const image = await readSiteFile("../docs/images/desktop-plato-agent.png");
if (image) {
  const pngSignature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  check(image.length > 10_000, "docs/images/desktop-plato-agent.png: image is unexpectedly small");
  check(image.subarray(0, 8).equals(pngSignature), "docs/images/desktop-plato-agent.png: expected a PNG file");
  if (image.length >= 24 && image.subarray(0, 8).equals(pngSignature)) {
    check(image.readUInt32BE(16) === 1180, "docs/images/desktop-plato-agent.png: expected width 1180");
    check(image.readUInt32BE(20) === 760, "docs/images/desktop-plato-agent.png: expected height 760");
  }
}

const xmlParser = new XMLParser({ ignoreAttributes: false });

const feedBuffer = await readSiteFile("feed.xml");
if (feedBuffer) {
  const feedSource = feedBuffer.toString("utf8");
  const validation = XMLValidator.validate(feedSource);
  check(validation === true, `feed.xml: invalid XML${validation === true ? "" : `: ${validation.err.msg}`}`);
  if (validation === true) {
    const feed = xmlParser.parse(feedSource).feed;
    check(feed?.["@_xmlns"] === "http://www.w3.org/2005/Atom", "feed.xml: expected an Atom feed namespace");
    check(feed?.id === `${SITE_ORIGIN}/feed.xml`, `feed.xml: feed id must be ${SITE_ORIGIN}/feed.xml`);
    const entries = asArray(feed?.entry);
    check(entries.length === 1, "feed.xml: expected exactly one launch entry");
    const entryLinks = entries.flatMap((entry) => asArray(entry?.link).map((link) => link?.["@_href"]));
    check(entryLinks.includes(`${SITE_ORIGIN}/blog/introducing-platonic/`), "feed.xml: launch entry link is missing");
  }
}

const sitemapBuffer = await readSiteFile("sitemap.xml");
if (sitemapBuffer) {
  const sitemapSource = sitemapBuffer.toString("utf8");
  const validation = XMLValidator.validate(sitemapSource);
  check(validation === true, `sitemap.xml: invalid XML${validation === true ? "" : `: ${validation.err.msg}`}`);
  if (validation === true) {
    const sitemap = xmlParser.parse(sitemapSource).urlset;
    check(sitemap?.["@_xmlns"] === "http://www.sitemaps.org/schemas/sitemap/0.9", "sitemap.xml: unexpected sitemap namespace");
    const locations = asArray(sitemap?.url).map((entry) => entry?.loc).sort();
    const expected = [...routes.keys()].map((route) => `${SITE_ORIGIN}${route}`).sort();
    check(JSON.stringify(locations) === JSON.stringify(expected), `sitemap.xml: expected only ${expected.join(", ")}`);
  }
}

const robotsBuffer = await readSiteFile("robots.txt");
if (robotsBuffer) {
  const directives = robotsBuffer
    .toString("utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));
  check(directives.includes("User-agent: *"), "robots.txt: missing User-agent: *");
  check(directives.includes("Allow: /"), "robots.txt: missing Allow: /");
  check(directives.includes(`Sitemap: ${SITE_ORIGIN}/sitemap.xml`), "robots.txt: missing canonical sitemap URL");
  check(!directives.includes("Disallow: /"), "robots.txt: site must not be globally disallowed");
}

if (failures.length > 0) {
  console.error(`Static site checks failed (${failures.length}):`);
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exitCode = 1;
} else {
  console.log(`Static site checks passed: ${routes.size} routes, ${requiredFiles.length} required files.`);
  if (CHECK_EXTERNAL_LINKS) {
    console.log(`Live external link checks passed: ${checkedExternalLinks} links.`);
  }
}
