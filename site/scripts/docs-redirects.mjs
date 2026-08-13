import { readFile, writeFile } from "node:fs/promises";
import { dirname, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const manifestPath = resolve(repository, "docs-site/migration.json");
const nginxPath = resolve(repository, "site/nginx.conf");
const canonicalOrigin = "https://docs.referential.ai";
const redirectOrigin = "https://referential.ai";
const blockStart = "        # BEGIN GENERATED DOCS REDIRECTS";
const blockEnd = "        # END GENERATED DOCS REDIRECTS";
const sourcePattern = /^\/docs(?:\/[-A-Za-z0-9._~]+)*\/?$/;

function requireValue(condition, message) {
  if (!condition) throw new Error(message);
}

function validateSource(source, label, homepage = false) {
  requireValue(typeof source === "string", `${label}: source must be a string`);
  requireValue(!source.includes("*"), `${label}: wildcard/catch-all source`);
  requireValue(
    homepage ? source === "/docs" || source === "/docs/" : source.startsWith("/docs/") && source !== "/docs/",
    `${label}: source must be an exact /docs path`,
  );
  requireValue(
    !source.includes("%") && posix.normalize(source) === source && sourcePattern.test(source),
    `${label}: ambiguous normalized source ${source}`,
  );

  const parsed = new URL(source, redirectOrigin);
  requireValue(
    parsed.origin === redirectOrigin && parsed.pathname === source && !parsed.search && !parsed.hash,
    `${label}: source must be a path without a query or fragment`,
  );
}

function validateDestination(destination, label) {
  requireValue(typeof destination === "string", `${label}: destination must be a string`);

  let parsed;
  try {
    parsed = new URL(destination);
  } catch {
    throw new Error(`${label}: invalid destination URL`);
  }

  requireValue(parsed.origin !== redirectOrigin, `${label}: redirect loop`);
  requireValue(parsed.protocol === "https:", `${label}: destination must use HTTPS`);
  requireValue(parsed.origin === canonicalOrigin, `${label}: destination must use ${canonicalOrigin}`);
  requireValue(!parsed.username && !parsed.password, `${label}: destination must not contain credentials`);
  requireValue(!parsed.search && !parsed.hash, `${label}: destination must not contain a query or fragment`);
  requireValue(parsed.pathname.endsWith("/") && parsed.href === destination, `${label}: destination must be canonical`);
}

export function redirectsFromManifest(manifest) {
  requireValue(manifest && typeof manifest === "object", "manifest must be an object");
  requireValue(manifest.version === 1, "manifest version must be 1");
  requireValue(manifest.canonical_origin === canonicalOrigin, "canonical origin changed");
  requireValue(manifest.redirect_origin === redirectOrigin, "redirect origin changed");
  requireValue(Array.isArray(manifest.homepage?.sources), "homepage sources must be an array");
  requireValue(Array.isArray(manifest.routes), "routes must be an array");

  const homepageSources = manifest.homepage.sources;
  requireValue(
    homepageSources.length === 2 && new Set(homepageSources).size === 2
      && homepageSources.includes("/docs") && homepageSources.includes("/docs/"),
    "homepage sources must be exactly /docs and /docs/",
  );
  requireValue(manifest.homepage.destination === `${canonicalOrigin}/`, "homepage destination changed");
  validateDestination(manifest.homepage.destination, "homepage");

  const redirects = [];
  for (const source of homepageSources) {
    validateSource(source, "homepage", true);
    redirects.push({ source, destination: manifest.homepage.destination });
  }

  for (const [routeIndex, route] of manifest.routes.entries()) {
    const label = typeof route?.title === "string" ? route.title : `route ${routeIndex}`;
    requireValue(Array.isArray(route?.redirect_sources), `${label}: redirect_sources must be an array`);
    validateDestination(route?.destination, label);
    for (const source of route.redirect_sources) {
      validateSource(source, label);
      redirects.push({ source, destination: route.destination });
    }
  }

  const sources = redirects.map(({ source }) => source);
  requireValue(new Set(sources).size === sources.length, "duplicate redirect source");
  return redirects;
}

export async function readRedirectManifest(path = manifestPath) {
  return JSON.parse(await readFile(path, "utf8"));
}

export function renderRedirectBlock(manifest) {
  const locations = redirectsFromManifest(manifest).map(({ source, destination }) => `        location = ${source} {
            if ($request_uri != "$uri$is_args$args") {
                return 404;
            }
            return 308 ${destination}$is_args$args;
        }`);

  return [
    blockStart,
    "        # Generated from docs-site/migration.json by site/scripts/docs-redirects.mjs.",
    "        # Request fragments never reach nginx; #551 preserves their destination anchors.",
    ...locations.flatMap((location, index) => index === 0 ? [location] : ["", location]),
    blockEnd,
  ].join("\n");
}

export function replaceRedirectBlock(nginx, manifest) {
  requireValue(nginx.split(blockStart).length === 2, "nginx config must contain one redirect block start marker");
  requireValue(nginx.split(blockEnd).length === 2, "nginx config must contain one redirect block end marker");
  const start = nginx.indexOf(blockStart);
  const endMarker = nginx.indexOf(blockEnd, start);
  requireValue(endMarker > start, "nginx redirect block markers are out of order");
  const end = endMarker + blockEnd.length;
  return `${nginx.slice(0, start)}${renderRedirectBlock(manifest)}${nginx.slice(end)}`;
}

async function main() {
  const check = process.argv[2] === "--check";
  requireValue(process.argv.length === (check ? 3 : 2), "usage: docs-redirects.mjs [--check]");
  const manifest = await readRedirectManifest();
  const current = await readFile(nginxPath, "utf8");
  const generated = replaceRedirectBlock(current, manifest);

  if (check) {
    requireValue(generated === current, "site/nginx.conf redirects are stale; run npm run generate:redirects");
  } else if (generated !== current) {
    await writeFile(nginxPath, generated);
  }

  console.log(`Docs redirects ${check ? "validated" : "generated"}: ${redirectsFromManifest(manifest).length} exact rules.`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
