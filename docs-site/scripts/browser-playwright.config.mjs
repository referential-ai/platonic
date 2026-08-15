import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { defineConfig } from "@playwright/test";

const artifacts = process.env.DOCS_BROWSER_ARTIFACTS ?? join(tmpdir(), "platonic-docs-558-browser");
mkdirSync(artifacts, { recursive: true });

export default defineConfig({
  expect: {
    timeout: 5_000,
    toHaveScreenshot: {
      animations: "disabled",
      caret: "hide",
      maxDiffPixelRatio: 0.002,
      scale: "css",
      threshold: 0.2,
    },
  },
  forbidOnly: Boolean(process.env.CI),
  fullyParallel: false,
  outputDir: artifacts,
  reporter: [
    ["line"],
    ["json", { outputFile: join(artifacts, "report.json") }],
  ],
  retries: 0,
  snapshotPathTemplate: "{testDir}/../browser-baselines/{arg}{ext}",
  testDir: ".",
  testMatch: "browser-gate.mjs",
  timeout: 30_000,
  use: {
    actionTimeout: 5_000,
    browserName: "chromium",
    colorScheme: "light",
    headless: true,
    locale: "en-US",
    navigationTimeout: 10_000,
    reducedMotion: "reduce",
    serviceWorkers: "block",
    timezoneId: "UTC",
    trace: "off",
    video: "off",
  },
  workers: 1,
});
