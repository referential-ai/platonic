import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { base, site } from "../config.mjs";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const root = resolve(process.env.DOCS_DIST ?? resolve(scriptDirectory, "../dist"));
const expectations = process.argv[2]
  ? JSON.parse(await readFile(resolve(process.argv[2]), "utf8"))
  : [
      { section: "User", query: "User docs", route: "user/" },
      { section: "User install", query: "Current public release", route: "user/install/" },
      { section: "User voice", query: "voice mode", route: "user/operations/voice/" },
      {
        section: "User Discord",
        query: "Discord connector",
        route: "user/operations/discord/",
      },
      { section: "Developer", query: "Developer docs", route: "developer/" },
      {
        section: "Developer lifecycle",
        query: "request lifecycle",
        route: "developer/runtime-boundaries/",
      },
      { section: "Reference", query: "Reference", route: "reference/" },
      {
        section: "Reference configuration",
        query: "Provider fields",
        route: "reference/configuration/",
      },
    ];

const nativeFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => {
  const url = new URL(typeof input === "string" || input instanceof URL ? input : input.url);
  if (url.protocol !== "file:") return nativeFetch(input, init);

  try {
    const body = await readFile(fileURLToPath(url));
    const type = url.pathname.endsWith(".wasm") ? "application/wasm" : "application/octet-stream";
    return new Response(body, { headers: { "content-type": type } });
  } catch (error) {
    return new Response(String(error), { status: error.code === "ENOENT" ? 404 : 500 });
  }
};

const failures = [];
let pagefind;

try {
  pagefind = await import(pathToFileURL(resolve(root, "pagefind/pagefind.js")));
  await pagefind.options({ baseUrl: base });

  for (const expectation of expectations) {
    const search = await pagefind.search(expectation.query);
    const results = await Promise.all(search.results.map((result) => result.data()));
    const urls = results.map((result) => new URL(result.url, site));
    const routes = urls.map((url) => url.pathname);
    const expected = new URL(expectation.route, new URL(base, site)).pathname;

    for (const url of urls) {
      if (url.origin !== new URL(site).origin || !url.pathname.startsWith(base)) {
        failures.push(
          `${expectation.section} query "${expectation.query}": result escapes ` +
            `DOCS_SITE or DOCS_BASE: ${url.href}`,
        );
      }
    }

    if (!routes.includes(expected)) {
      failures.push(
        `${expectation.section} query "${expectation.query}": expected ${expected}; ` +
          `got ${routes.join(", ") || "no results"}`,
      );
    } else {
      console.log(
        `Search passed: ${expectation.section} query "${expectation.query}" found ${expected} ` +
          `among ${routes.length} results`,
      );
    }
  }
} catch (error) {
  failures.push(`Pagefind could not query the generated index: ${error.message}`);
} finally {
  await pagefind?.destroy();
  globalThis.fetch = nativeFetch;
}

if (failures.length > 0) {
  console.error(`Search failed (${failures.length}):`);
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(`Search passed: ${expectations.length} fixed section queries at ${base}`);
}
