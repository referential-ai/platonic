import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const baselineRoot = resolve(scriptRoot, "../browser-baselines");
const fixtureRoot = resolve(scriptRoot, "fixtures/browser");
const manifest = JSON.parse(await readFile(resolve(baselineRoot, "manifest.json"), "utf8"));
const origin = process.env.DOCS_BROWSER_ORIGIN;
const require = createRequire(import.meta.url);

if (!origin) throw new Error("DOCS_BROWSER_ORIGIN is required; run scripts/browser-run.mjs");

const deployments = [
  { name: "root", base: "/" },
  { name: "subpath", base: "/platonic-docs/" },
];

function route(base, path = "") {
  return new URL(`${base}${path}`, origin).href;
}

function watchPage(page, allowedStatus = null) {
  const failures = [];
  page.on("console", (message) => {
    if (
      allowedStatus &&
      message.type() === "error" &&
      message.text() === "Failed to load resource: the server responded with a status of 404 (Not Found)"
    ) {
      return;
    }
    if (["warning", "error", "assert"].includes(message.type())) {
      failures.push(`console ${message.type()}: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => failures.push(`page error: ${error.message}`));
  page.on("requestfailed", (request) => {
    failures.push(`request failed: ${request.method()} ${request.url()}`);
  });
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (["http:", "https:"].includes(url.protocol) && url.origin !== origin) {
      failures.push(`third-party request: ${request.method()} ${url.href}`);
    }
  });
  page.on("response", (response) => {
    if (response.status() >= 400 && response.url() !== allowedStatus) {
      failures.push(`HTTP ${response.status()}: ${response.url()}`);
    }
  });
  return failures;
}

async function openPage(page, url, status = 200) {
  const failures = watchPage(page, status === 404 ? url : null);
  const response = await page.goto(url, { waitUntil: "networkidle" });
  if (response?.status() !== status) {
    throw new Error(`${url}: expected HTTP ${status}, received ${response?.status() ?? "none"}`);
  }
  await page.locator("body").waitFor({ state: "visible" });
  return failures;
}

function throwFailures(label, failures) {
  if (failures.length > 0) throw new Error(`${label}: ${failures.join("; ")}`);
}

async function assertLandmarks(page, { navigation = true } = {}) {
  if ((await page.locator("main").count()) !== 1) throw new Error("landmarks: expected one main");
  if ((await page.locator("h1").count()) !== 1) throw new Error("landmarks: expected one h1");
  if (navigation && (await page.getByRole("navigation", { name: "Main" }).count()) !== 1) {
    throw new Error("landmarks: expected one Main navigation");
  }
  if (!(await page.locator("header").first().isVisible())) {
    throw new Error("landmarks: header is not visible");
  }
}

async function assertFocusIndicator(locator, label) {
  const style = await locator.evaluate((element) => {
    const computed = getComputedStyle(element);
    return {
      boxShadow: computed.boxShadow,
      outlineStyle: computed.outlineStyle,
      outlineWidth: Number.parseFloat(computed.outlineWidth),
    };
  });
  if (
    !(style.outlineStyle !== "none" && style.outlineWidth >= 2) &&
    (style.boxShadow === "none" || style.boxShadow === "")
  ) {
    throw new Error(`keyboard/focus: ${label} has no visible focus indicator`);
  }
}

async function assertKeyboardEntry(page) {
  const skip = page.getByRole("link", { name: "Skip to content" });
  await page.keyboard.press("Tab");
  if (!(await skip.evaluate((element) => element === document.activeElement))) {
    throw new Error("keyboard/focus: skip link is not the first focus target");
  }
  if (!(await skip.isVisible())) throw new Error("keyboard/focus: focused skip link is hidden");
  await assertFocusIndicator(skip, "skip link");
  await page.keyboard.press("Enter");
  await page.waitForFunction(() => window.location.hash === "#_top");
  if (!(await page.locator("#_top").isVisible())) {
    throw new Error("keyboard/focus: skip link did not expose the main heading");
  }
}

async function assertA11y(page) {
  const results = await new AxeBuilder({ page }).analyze();
  const severe = results.violations.filter((violation) =>
    ["serious", "critical"].includes(violation.impact ?? ""),
  );
  if (severe.length > 0) {
    const summary = severe
      .map((violation) => {
        const targets = violation.nodes.flatMap((node) => node.target).join(", ");
        return `${violation.impact} ${violation.id}: ${targets}`;
      })
      .join("; ");
    throw new Error(`accessibility: serious/critical ${summary}`);
  }
}

async function assertLayout(
  page,
  { diagramSteps = null, mobile = false, navigation = true, print = false } = {},
) {
  const failures = await page.evaluate(
    ({ diagramSteps, mobile, navigation, print }) => {
      const tolerance = 1;
      const root = document.documentElement;
      const failures = [];
      const visible = (element) => {
        const style = getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
      };
      const describe = (element) => {
        const text = element.textContent?.replace(/\s+/g, " ").trim().slice(0, 48);
        return `${element.localName}${text ? ` ${JSON.stringify(text)}` : ""}`;
      };

      if (root.scrollWidth > root.clientWidth + tolerance) {
        failures.push(`mobile overflow: document is ${root.scrollWidth}px wide at ${root.clientWidth}px`);
      }

      const selectors = [
        "header",
        "main",
        "site-search button",
        "starlight-menu-button button",
        ".system-flow",
        ".expected-output",
      ];
      for (const element of document.querySelectorAll(selectors.join(","))) {
        if (!visible(element)) continue;
        const rect = element.getBoundingClientRect();
        if (rect.left < -tolerance || rect.right > root.clientWidth + tolerance) {
          failures.push(`clipped control/content: ${describe(element)} spans ${rect.left}-${rect.right}`);
        }
      }

      if (!print) {
        const search = document.querySelector("site-search button");
        if (!search || !visible(search)) failures.push("missing primary control: Search");
        const menu = document.querySelector("starlight-menu-button button");
        if (mobile && navigation && (!menu || !visible(menu))) {
          failures.push("missing primary control: Menu");
        }
      }

      const diagrams = [...document.querySelectorAll(".system-flow")].filter(visible);
      if (diagramSteps !== null && diagrams.length === 0) {
        failures.push("structural visual: semantic diagram is missing");
      }
      for (const diagram of diagrams) {
        const steps = [...diagram.querySelectorAll(":scope > li")].filter(visible);
        if (diagramSteps !== null && steps.length !== diagramSteps) {
          failures.push(`structural visual: expected ${diagramSteps} visible steps, found ${steps.length}`);
        }
        for (const step of steps) {
          const label = step.querySelector("strong");
          if (!label || !visible(label) || Number.parseFloat(getComputedStyle(label).fontSize) < 14) {
            failures.push(`unreadable semantic diagram: ${describe(step)}`);
          }
          const stepRect = step.getBoundingClientRect();
          const labelRect = label?.getBoundingClientRect();
          if (
            labelRect &&
            (labelRect.left < stepRect.left - tolerance || labelRect.right > stepRect.right + tolerance)
          ) {
            failures.push(`clipped semantic diagram: ${describe(step)}`);
          }
        }
      }

      for (const image of document.querySelectorAll(".sl-markdown-content img")) {
        if (!image.complete || image.naturalWidth === 0 || image.naturalHeight === 0) {
          failures.push(`missing image: ${image.getAttribute("src")}`);
        }
      }
      return failures;
    },
    { diagramSteps, mobile, navigation, print },
  );
  throwFailures("responsive layout", failures);
}

async function assertExpectedSearchResult(page, expectedPath) {
  const result = page.locator(`a[href="${expectedPath}"]`).first();
  try {
    await result.waitFor({ state: "visible", timeout: 3_000 });
  } catch {
    throw new Error(`search selection: expected visible result ${expectedPath}`);
  }
  return result;
}

async function openSearch(page) {
  const button = page.getByRole("button", { name: "Search" });
  await expect(button).toBeEnabled();
  await button.focus();
  await assertFocusIndicator(button, "search button");
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Search" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByPlaceholder("Search")).toBeFocused();
  return dialog;
}

async function waitForImages(page) {
  const images = page.locator("img");
  for (let index = 0; index < (await images.count()); index += 1) {
    const image = images.nth(index);
    await image.scrollIntoViewIfNeeded();
    await image.evaluate((element) => {
      if (element.complete && element.naturalWidth > 0) return;
      return new Promise((resolveImage, rejectImage) => {
        element.addEventListener("load", resolveImage, { once: true });
        element.addEventListener("error", () => rejectImage(new Error(`image failed: ${element.src}`)), {
          once: true,
        });
      });
    });
  }
  await page.evaluate(() => scrollTo(0, 0));
  await page.evaluate(() => document.fonts.ready);
}

for (const deployment of deployments) {
  test.describe(`${deployment.name} deployment`, () => {
    test("keyboard, landmarks, sidebar, and on-page navigation", async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      const failures = await openPage(page, route(deployment.base, "developer/runtime-boundaries/"));
      await assertLandmarks(page);
      await assertKeyboardEntry(page);
      await assertLayout(page, { diagramSteps: 5 });
      await assertA11y(page);

      const approvals = page.locator(".sidebar-content").getByRole("link", {
        exact: true,
        name: "Approvals",
      });
      await approvals.focus();
      await assertFocusIndicator(approvals, "sidebar link");
      await page.keyboard.press("Enter");
      await page.waitForURL(route(deployment.base, "user/operations/approvals/"));
      await page.waitForLoadState("networkidle");
      await expect(page.locator("h1")).toHaveText("Approvals");

      await page.goto(route(deployment.base, "developer/runtime-boundaries/"), {
        waitUntil: "networkidle",
      });
      const onPage = page
        .locator(".right-sidebar-panel")
        .getByRole("link", { exact: true, name: "Confinement boundary" });
      await onPage.focus();
      await page.keyboard.press("Enter");
      await page.waitForFunction(() => location.hash === "#confinement-boundary");
      throwFailures(`${deployment.name} navigation`, failures);
    });

    test("search selection and theme persistence", async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      const failures = await openPage(page, route(deployment.base));
      const dialog = await openSearch(page);
      const input = dialog.getByPlaceholder("Search");
      await expect(input).toBeFocused();
      await input.fill("approval decisions");
      const result = await assertExpectedSearchResult(
        dialog,
        `${deployment.base}user/operations/approvals/`,
      );
      await result.focus();
      await assertFocusIndicator(result, "search result");
      await page.keyboard.press("Enter");
      await page.waitForURL(route(deployment.base, "user/operations/approvals/"));
      await expect(page.locator("h1")).toHaveText("Approvals");

      const theme = page.locator("starlight-theme-select select:visible").first();
      await theme.selectOption("dark");
      await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
      expect(await page.evaluate(() => localStorage.getItem("starlight-theme"))).toBe("dark");
      await page.reload({ waitUntil: "networkidle" });
      await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
      throwFailures(`${deployment.name} search/theme`, failures);
    });

    test("mobile navigation, reduced motion, and print", async ({ page }) => {
      await page.setViewportSize({ width: 390, height: 844 });
      await page.emulateMedia({ reducedMotion: "reduce" });
      const failures = await openPage(page, route(deployment.base, "user/operations/tui-and-cli/"));
      await assertLandmarks(page);
      await waitForImages(page);
      await assertLayout(page, { mobile: true });
      await assertA11y(page);
      expect(await page.evaluate(() => matchMedia("(prefers-reduced-motion: reduce)").matches)).toBe(
        true,
      );
      expect(await page.evaluate(() => getComputedStyle(document.documentElement).scrollBehavior)).not.toBe(
        "smooth",
      );

      const menu = page.getByRole("button", { name: "Menu" });
      await menu.focus();
      await assertFocusIndicator(menu, "mobile menu");
      await page.keyboard.press("Enter");
      await expect(page.locator("body")).toHaveAttribute("data-mobile-menu-expanded", "");
      await expect(page.locator(".sidebar-content").getByText("Developer docs", { exact: true })).toBeVisible();
      await page.keyboard.press("Escape");
      await expect(page.locator("body")).not.toHaveAttribute("data-mobile-menu-expanded", "true");

      await page.emulateMedia({ colorScheme: "light", media: "print", reducedMotion: "reduce" });
      await expect(page.locator("header").first()).toBeHidden();
      await expect(page.getByRole("navigation", { name: "Main" })).toBeHidden();
      await expect(page.locator("main")).toBeVisible();
      await assertLayout(page, { print: true });
      throwFailures(`${deployment.name} mobile/print`, failures);
    });

    test("real HTTP 404", async ({ page }) => {
      await page.setViewportSize({ width: 390, height: 844 });
      const url = route(deployment.base, "missing-browser-proof/");
      const failures = await openPage(page, url, 404);
      await expect(page.locator("h1")).toHaveText("404");
      await assertLandmarks(page, { navigation: false });
      await assertLayout(page, { mobile: true, navigation: false });
      await assertA11y(page);
      throwFailures(`${deployment.name} 404`, failures);
    });
  });
}

const negativeCases = [
  {
    file: "keyboard-focus.html",
    name: "keyboard/focus",
    diagnostic: "has no visible focus indicator",
    check: async (page) => await assertKeyboardEntry(page),
  },
  {
    file: "serious-accessibility.html",
    name: "serious accessibility",
    diagnostic: "accessibility: serious/critical",
    check: async (page) => await assertA11y(page),
  },
  {
    file: "mobile-overflow.html",
    name: "mobile overflow",
    diagnostic: "mobile overflow",
    check: async (page) => await assertLayout(page, { mobile: true }),
  },
  {
    file: "broken-search-selection.html",
    name: "broken search selection",
    diagnostic: "search selection: expected visible result",
    check: async (page) => await assertExpectedSearchResult(page, "/expected-result/"),
  },
  {
    file: "structural-visual.html",
    name: "structural visual regression",
    diagnostic: "structural visual: expected 5 visible steps, found 4",
    check: async (page) => await assertLayout(page, { diagramSteps: 5 }),
  },
];

for (const negative of negativeCases) {
  test(`controlled negative: ${negative.name}`, async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.setContent(await readFile(resolve(fixtureRoot, negative.file), "utf8"));
    let failure;
    try {
      await negative.check(page);
    } catch (error) {
      failure = error;
    }
    expect(failure, `${negative.name}: controlled negative unexpectedly passed`).toBeTruthy();
    expect(failure.message).toContain(negative.diagnostic);
  });
}

for (const baseline of manifest.baselines) {
  test(`visual baseline: ${baseline.name}`, async ({ page }) => {
    await page.setViewportSize(baseline.viewport);
    await page.addInitScript((theme) => localStorage.setItem("starlight-theme", theme), baseline.theme);
    await page.emulateMedia({
      colorScheme: baseline.theme,
      media: "screen",
      reducedMotion: "reduce",
    });
    const url = route("/", baseline.route.replace(/^\//, ""));
    const failures = await openPage(page, url, baseline.status ?? 200);
    await assertLandmarks(page, { navigation: baseline.name !== "404" });
    await waitForImages(page);
    await assertLayout(page, {
      diagramSteps: baseline.name === "homepage" ? 5 : baseline.name === "developer" ? 5 : null,
      mobile: baseline.viewport.width === 390,
      navigation: baseline.name !== "404",
    });
    await assertA11y(page);

    if (baseline.name === "search") {
      const dialog = await openSearch(page);
      await dialog.getByPlaceholder("Search").fill("approval decisions");
      await assertExpectedSearchResult(dialog, "/user/operations/approvals/");
    }

    await expect(page).toHaveScreenshot(baseline.file, { fullPage: baseline.fullPage });
    throwFailures(`visual ${baseline.name}`, failures);
  });
}

test("five-baseline manifest and pinned toolchain", async () => {
  const expectedNames = ["404", "developer", "homepage", "search", "user"];
  const files = (await readdir(baselineRoot)).filter((file) => file.endsWith(".png")).sort();
  const manifestFiles = manifest.baselines.map((baseline) => baseline.file).sort();
  expect(manifest.schema).toBe(1);
  expect(manifest.baselines.map((baseline) => baseline.name).sort()).toEqual(expectedNames);
  expect(files).toEqual(manifestFiles);
  expect(files).toHaveLength(5);

  for (const baseline of manifest.baselines) {
    expect([390, 1440]).toContain(baseline.viewport.width);
    expect(["light", "dark"]).toContain(baseline.theme);
    expect(baseline.rationale.length).toBeGreaterThan(20);
  }

  const playwright = JSON.parse(
    await readFile(resolve(dirname(require.resolve("@playwright/test")), "package.json"), "utf8"),
  );
  const axe = JSON.parse(
    await readFile(resolve(dirname(require.resolve("@axe-core/playwright")), "../package.json"), "utf8"),
  );
  const browsers = JSON.parse(
    await readFile(resolve(dirname(require.resolve("playwright-core")), "browsers.json"), "utf8"),
  );
  const chromium = browsers.browsers.find((browser) => browser.name === "chromium");
  expect(process.versions.node.split(".")[0]).toBe(manifest.toolchain.node);
  expect(playwright.version).toBe(manifest.toolchain.playwright);
  expect(axe.version).toBe(manifest.toolchain.axe);
  expect(chromium.revision).toBe(manifest.toolchain.chromiumRevision);
  expect(chromium.browserVersion).toBe(manifest.toolchain.chromiumVersion);
  expect(manifest.toolchain.systemDependencies).toBe(
    "ubuntu-24.04 + playwright install --with-deps chromium",
  );

  if (process.env.DOCS_UPDATE_BASELINES !== "reviewed") {
    for (const baseline of manifest.baselines) {
      const digest = createHash("sha256")
        .update(await readFile(resolve(baselineRoot, baseline.file)))
        .digest("hex");
      expect(digest, `${baseline.file}: update manifest sha256 and rationale`).toBe(baseline.sha256);
    }
  }
});
