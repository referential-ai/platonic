import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  readRedirectManifest,
  redirectsFromManifest,
  renderRedirectBlock,
  replaceRedirectBlock,
} from "./docs-redirects.mjs";

const manifest = await readRedirectManifest();

test("the checked-in nginx rules exactly consume the migration manifest", async () => {
  const redirects = redirectsFromManifest(manifest);
  const expectedCount = manifest.homepage.sources.length
    + manifest.routes.reduce((count, route) => count + route.redirect_sources.length, 0);
  const block = renderRedirectBlock(manifest);
  const nginx = await readFile(new URL("../nginx.conf", import.meta.url), "utf8");

  assert.equal(redirects.length, expectedCount);
  assert.equal((block.match(/location = /g) ?? []).length, expectedCount);
  assert.equal((block.match(/return 308 /g) ?? []).length, expectedCount);
  assert.equal(replaceRedirectBlock(nginx, manifest), nginx);
});

const invalidManifests = [
  ["duplicate", /duplicate redirect source/, (copy) => {
    copy.routes[1].redirect_sources.push(copy.routes[0].redirect_sources[0]);
  }],
  ["non-/docs", /exact \/docs path/, (copy) => {
    copy.routes[0].redirect_sources[0] = "/help/index.html";
  }],
  ["non-HTTPS", /must use HTTPS/, (copy) => {
    copy.routes[0].destination = "http://docs.referential.ai/";
  }],
  ["off-host", /must use https:\/\/docs[.]referential[.]ai/, (copy) => {
    copy.routes[0].destination = "https://example.com/";
  }],
  ["loop", /redirect loop/, (copy) => {
    copy.routes[0].destination = "https://referential.ai/docs/index.html";
  }],
  ["wildcard/catch-all", /wildcard\/catch-all/, (copy) => {
    copy.routes[0].redirect_sources[0] = "/docs/*";
  }],
  ["ambiguous normalization", /ambiguous normalized source/, (copy) => {
    copy.routes[0].redirect_sources[0] = "/docs/a/../index.html";
  }],
];

for (const [name, expected, mutate] of invalidManifests) {
  test(`rejects a ${name} manifest`, () => {
    const copy = structuredClone(manifest);
    mutate(copy);
    assert.throws(() => redirectsFromManifest(copy), expected);
  });
}
