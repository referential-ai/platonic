import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import AxeBuilder from "@axe-core/playwright";
import { chromium } from "playwright";

import { readRedirectManifest, redirectsFromManifest } from "./docs-redirects.mjs";

const defaultBaseUrl = "http://127.0.0.1:8080/";
const baseUrl = new URL(
  process.argv[2] ?? process.env.BASE_URL ?? process.env.SITE_URL ?? defaultBaseUrl,
);
const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const artifactRoot = resolve(process.env.SITE_PROOF_DIR ?? resolve(siteRoot, "artifacts/browser"));
const redirects = redirectsFromManifest(await readRedirectManifest());

if (baseUrl.pathname !== "/" || baseUrl.search || baseUrl.hash) {
  throw new Error(`BASE_URL or SITE_URL must be an origin with a trailing slash, received ${baseUrl.href}`);
}

const routes = ["/", "/blog/", "/blog/introducing-platonic/"];
const profiles = [
  { name: "desktop-wide", viewport: { width: 1440, height: 900 }, captureAllRoutes: true },
  { name: "desktop", viewport: { width: 1180, height: 760 }, captureAllRoutes: true },
  { name: "tablet", viewport: { width: 768, height: 1024 } },
  { name: "mobile", viewport: { width: 390, height: 844 }, isMobile: true, captureAllRoutes: true },
  { name: "mobile-small", viewport: { width: 320, height: 568 }, isMobile: true, captureAllRoutes: true },
];

const resourceRoutes = [
  { path: "/assets/site.css", status: 200, contentType: "text/css" },
  { path: "/assets/plato-agent-desktop.png", status: 200, contentType: "image/png" },
  { path: "/feed.xml", status: 200, contentType: "application/atom+xml" },
  { path: "/sitemap.xml", status: 200, contentType: "application/xml" },
  { path: "/robots.txt", status: 200, contentType: "text/plain" },
  { path: "/missing-browser-proof", status: 404, contentType: "text/html" },
];

const securityHeaders = new Map([
  ["content-security-policy", "default-src 'self'; img-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'none'"],
  ["referrer-policy", "strict-origin-when-cross-origin"],
  ["x-content-type-options", "nosniff"],
  ["x-frame-options", "DENY"],
]);

const failures = [];

function record(condition, message) {
  if (!condition) {
    failures.push(message);
  }
}

function proveSecurityHeaders(headers, label) {
  for (const [name, expected] of securityHeaders) {
    record(headers[name] === expected, `${label}: expected ${name}: ${expected}, received ${headers[name] ?? "no header"}`);
  }
}

function screenshotName(route, profileName) {
  const routeName = route === "/" ? "home" : route.split("/").filter(Boolean).at(-1);
  return `${routeName}-${profileName}.png`;
}

async function proveResources(request) {
  for (const resource of resourceRoutes) {
    const response = await request.get(new URL(resource.path, baseUrl).href);
    record(response.status() === resource.status, `${resource.path}: expected HTTP ${resource.status}, received ${response.status()}`);
    const contentType = response.headers()["content-type"] ?? "";
    record(contentType.startsWith(resource.contentType), `${resource.path}: expected ${resource.contentType}, received ${contentType || "no content type"}`);
    proveSecurityHeaders(response.headers(), resource.path);
  }
}

async function proveRedirects(request) {
  const requestUrl = (path) => `${baseUrl.origin}${path}`;

  for (const redirect of redirects) {
    const response = await request.get(requestUrl(redirect.source), { maxRedirects: 0 });
    record(response.status() === 308, `${redirect.source}: expected HTTP 308, received ${response.status()}`);
    record(response.headers().location === redirect.destination, `${redirect.source}: expected Location ${redirect.destination}, received ${response.headers().location ?? "no Location"}`);
    proveSecurityHeaders(response.headers(), redirect.source);

    const query = "proof=redirect&next=%2Fdocs";
    const queried = await request.get(requestUrl(`${redirect.source}?${query}`), { maxRedirects: 0 });
    record(queried.status() === 308, `${redirect.source}?${query}: expected HTTP 308, received ${queried.status()}`);
    record(queried.headers().location === `${redirect.destination}?${query}`, `${redirect.source}: query string was not preserved exactly`);
  }

  const existing404 = await request.get(requestUrl("/missing-browser-proof"));
  const existing404Body = await existing404.text();
  const rejectedPaths = [
    "/docs/not-in-manifest",
    "/docs/docs/quickstart.html",
    "/docs/docs/QUICKSTART.html/",
    "/docs//docs/QUICKSTART.html",
    "/%64ocs/docs/QUICKSTART.html",
  ];

  for (const path of rejectedPaths) {
    const response = await request.get(requestUrl(path), { maxRedirects: 0 });
    record(response.status() === 404, `${path}: expected the existing HTTP 404, received ${response.status()}`);
    record(await response.text() === existing404Body, `${path}: response did not match the existing real 404`);
    proveSecurityHeaders(response.headers(), path);
  }
}

async function inspectLayout(page) {
  return page.evaluate(() => {
    const tolerance = 1;
    const exemptSelector = [
      ".hero-media",
      ".hero-media *",
      ".hero-scrim",
      ".hero-scrim *",
      ".skip-link",
      ".visually-hidden",
      "[aria-hidden='true']",
      "[aria-hidden='true'] *",
      "[data-allow-scroll]",
      "[data-allow-scroll] *",
    ].join(", ");
    const textSelector = "h1, h2, h3, p, li, a, button, code, figcaption, time, small, span";

    function describe(element) {
      const id = element.id ? `#${element.id}` : "";
      const classes = [...element.classList].slice(0, 2).map((name) => `.${name}`).join("");
      const text = element.textContent?.replace(/\s+/g, " ").trim().slice(0, 48);
      return `${element.localName}${id}${classes}${text ? ` (${JSON.stringify(text)})` : ""}`;
    }

    function isVisible(element) {
      const style = getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.display !== "none"
        && style.visibility !== "hidden"
        && Number(style.opacity) > 0
        && rect.width > tolerance
        && rect.height > tolerance;
    }

    function isExempt(element) {
      return element.matches(exemptSelector) || Boolean(element.closest("[data-allow-scroll]"));
    }

    function clips(axis, style) {
      const overflow = axis === "x" ? style.overflowX : style.overflowY;
      return overflow === "hidden" || overflow === "clip";
    }

    const allElements = [...document.body.querySelectorAll("*")];
    const clipped = new Set();

    for (const element of allElements) {
      if (!isVisible(element) || isExempt(element)) {
        continue;
      }

      const style = getComputedStyle(element);
      const clipsWidth = !element.matches(".hero")
        && clips("x", style)
        && element.scrollWidth > element.clientWidth + tolerance;
      const clipsHeight = clips("y", style) && element.scrollHeight > element.clientHeight + tolerance;
      if (clipsWidth || clipsHeight) {
        clipped.add(`${describe(element)} clips ${clipsWidth ? "width" : "height"}`);
      }
    }

    for (const element of document.querySelectorAll(textSelector)) {
      if (!isVisible(element) || isExempt(element)) {
        continue;
      }

      const rect = element.getBoundingClientRect();
      for (let ancestor = element.parentElement; ancestor; ancestor = ancestor.parentElement) {
        if (isExempt(ancestor)) {
          break;
        }
        const ancestorStyle = getComputedStyle(ancestor);
        const ancestorRect = ancestor.getBoundingClientRect();
        const clippedHorizontally = clips("x", ancestorStyle)
          && (rect.left < ancestorRect.left - tolerance || rect.right > ancestorRect.right + tolerance);
        const clippedVertically = clips("y", ancestorStyle)
          && (rect.top < ancestorRect.top - tolerance || rect.bottom > ancestorRect.bottom + tolerance);
        if (clippedHorizontally || clippedVertically) {
          clipped.add(`${describe(element)} extends outside clipping ancestor ${describe(ancestor)}`);
          break;
        }
      }
    }

    const overlaps = [];
    for (const parent of allElements) {
      if (!isVisible(parent) || isExempt(parent)) {
        continue;
      }

      const children = [...parent.children].filter((element) => {
        if (!isVisible(element) || isExempt(element)) {
          return false;
        }
        const style = getComputedStyle(element);
        return !["absolute", "fixed", "sticky"].includes(style.position) && style.display !== "inline";
      });

      for (let firstIndex = 0; firstIndex < children.length; firstIndex += 1) {
        const first = children[firstIndex];
        const firstRect = first.getBoundingClientRect();
        for (let secondIndex = firstIndex + 1; secondIndex < children.length; secondIndex += 1) {
          const second = children[secondIndex];
          const secondRect = second.getBoundingClientRect();
          const overlapWidth = Math.min(firstRect.right, secondRect.right) - Math.max(firstRect.left, secondRect.left);
          const overlapHeight = Math.min(firstRect.bottom, secondRect.bottom) - Math.max(firstRect.top, secondRect.top);
          if (overlapWidth > tolerance && overlapHeight > tolerance) {
            overlaps.push(`${describe(first)} overlaps ${describe(second)} inside ${describe(parent)}`);
          }
        }
      }
    }

    return { clipped: [...clipped], overlaps };
  });
}

async function inspectVisibleHeroCrop(page) {
  return page.locator(".hero-media").evaluate((media) => {
    const image = media.querySelector("img");
    if (!image || !image.complete || image.naturalWidth === 0 || image.naturalHeight === 0) {
      return { failure: "hero image is not loaded" };
    }

    const mediaRect = media.getBoundingClientRect();
    const imageRect = image.getBoundingClientRect();
    const style = getComputedStyle(image);
    const scale = style.objectFit === "cover"
      ? Math.max(imageRect.width / image.naturalWidth, imageRect.height / image.naturalHeight)
      : Math.min(imageRect.width / image.naturalWidth, imageRect.height / image.naturalHeight);

    function positionFraction(value, start, end) {
      if (value === start) {
        return 0;
      }
      if (value === end) {
        return 1;
      }
      if (value === "center") {
        return 0.5;
      }
      return value.endsWith("%") ? Number.parseFloat(value) / 100 : 0.5;
    }

    const [positionX = "50%", positionY = "50%"] = style.objectPosition.split(/\s+/);
    const paintedWidth = image.naturalWidth * scale;
    const paintedHeight = image.naturalHeight * scale;
    const paintedLeft = imageRect.left
      + (imageRect.width - paintedWidth) * positionFraction(positionX, "left", "right");
    const paintedTop = imageRect.top
      + (imageRect.height - paintedHeight) * positionFraction(positionY, "top", "bottom");
    const paintedRight = paintedLeft + paintedWidth;
    const paintedBottom = paintedTop + paintedHeight;

    const visibleLeft = Math.max(mediaRect.left, imageRect.left, paintedLeft);
    const visibleTop = Math.max(mediaRect.top, imageRect.top, paintedTop);
    const visibleRight = Math.min(mediaRect.right, imageRect.right, paintedRight);
    const visibleBottom = Math.min(mediaRect.bottom, imageRect.bottom, paintedBottom);
    const visibleWidth = Math.max(0, visibleRight - visibleLeft);
    const visibleHeight = Math.max(0, visibleBottom - visibleTop);

    if (visibleWidth === 0 || visibleHeight === 0 || !Number.isFinite(scale) || scale <= 0) {
      return { failure: "hero image has no visible painted intersection" };
    }

    const sourceLeft = Math.max(0, (visibleLeft - paintedLeft) / scale);
    const sourceTop = Math.max(0, (visibleTop - paintedTop) / scale);
    const sourceWidth = Math.min(image.naturalWidth - sourceLeft, visibleWidth / scale);
    const sourceHeight = Math.min(image.naturalHeight - sourceTop, visibleHeight / scale);
    const canvas = document.createElement("canvas");
    canvas.width = 64;
    canvas.height = 64;
    const context = canvas.getContext("2d", { willReadFrequently: true });
    if (!context) {
      return { failure: "could not create hero crop canvas" };
    }

    context.drawImage(
      image,
      sourceLeft,
      sourceTop,
      sourceWidth,
      sourceHeight,
      0,
      0,
      canvas.width,
      canvas.height,
    );
    const pixels = context.getImageData(0, 0, canvas.width, canvas.height).data;
    const colors = new Set();
    const topColors = new Set();
    let opaquePixels = 0;
    let nonWhitePixels = 0;
    let topNonWhitePixels = 0;
    for (let index = 0; index < pixels.length; index += 4) {
      if (pixels[index + 3] === 0) {
        continue;
      }
      opaquePixels += 1;
      const color = `${pixels[index] >> 4}:${pixels[index + 1] >> 4}:${pixels[index + 2] >> 4}`;
      colors.add(color);
      const nonWhite = pixels[index] < 240 || pixels[index + 1] < 240 || pixels[index + 2] < 240;
      if (nonWhite) {
        nonWhitePixels += 1;
      }
      const row = Math.floor(index / 4 / canvas.width);
      if (row < 10) {
        topColors.add(color);
        if (nonWhite) {
          topNonWhitePixels += 1;
        }
      }
    }

    return {
      mediaCoverage: (visibleWidth * visibleHeight) / (mediaRect.width * mediaRect.height),
      nonWhitePixels,
      objectFit: style.objectFit,
      objectPosition: style.objectPosition,
      opaquePixels,
      sourceBottom: sourceTop + sourceHeight,
      sourceHeight,
      sourceTop,
      topNonWhitePixels,
      topUniqueColors: topColors.size,
      uniqueColors: colors.size,
    };
  });
}

async function provePage(page, profile, route) {
  const pageFailures = [];
  const expectedOrigin = baseUrl.origin;

  page.on("console", (message) => {
    if (["warning", "error", "assert"].includes(message.type())) {
      pageFailures.push(`console ${message.type()}: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => pageFailures.push(`page error: ${error.message}`));
  page.on("requestfailed", (request) => {
    pageFailures.push(`request failed: ${request.method()} ${request.url()} (${request.failure()?.errorText ?? "unknown"})`);
  });
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.origin !== expectedOrigin) {
      pageFailures.push(`remote request: ${request.method()} ${url.href}`);
    }
  });
  page.on("response", (response) => {
    if (response.status() >= 400) {
      pageFailures.push(`HTTP ${response.status()}: ${response.url()}`);
    }
  });

  const response = await page.goto(new URL(route, baseUrl).href, { waitUntil: "networkidle" });
  record(Boolean(response?.ok()), `${profile.name} ${route}: navigation did not return 2xx`);
  record(response?.headers()["content-type"]?.startsWith("text/html") ?? false, `${profile.name} ${route}: page did not return text/html`);
  if (response) proveSecurityHeaders(response.headers(), `${profile.name} ${route}`);
  await page.locator("body").waitFor({ state: "visible" });

  const layout = await page.evaluate(() => {
    const root = document.documentElement;
    const navigation = document.querySelector(".primary-nav");
    const controls = [...document.querySelectorAll(".button")].map((element) => ({
      text: element.textContent?.trim() ?? "",
      widthFits: element.scrollWidth <= element.clientWidth + 1,
      heightFits: element.scrollHeight <= element.clientHeight + 1,
    }));
    return {
      clientWidth: root.clientWidth,
      scrollWidth: root.scrollWidth,
      navigationFits: !navigation || navigation.scrollWidth <= navigation.clientWidth + 1,
      reducedMotionScrollBehavior: getComputedStyle(root).scrollBehavior,
      controls,
    };
  });

  record(layout.scrollWidth <= layout.clientWidth + 1, `${profile.name} ${route}: horizontal overflow ${layout.scrollWidth}px > ${layout.clientWidth}px`);
  record(layout.navigationFits, `${profile.name} ${route}: primary navigation requires horizontal scrolling`);
  record(layout.reducedMotionScrollBehavior === "auto", `${profile.name} ${route}: reduced motion must disable smooth scrolling`);
  for (const control of layout.controls) {
    record(control.widthFits && control.heightFits, `${profile.name} ${route}: button text does not fit: ${control.text}`);
  }

  const layoutDefects = await inspectLayout(page);
  for (const clipped of layoutDefects.clipped) {
    record(false, `${profile.name} ${route}: clipped content: ${clipped}`);
  }
  for (const overlap of layoutDefects.overlaps) {
    record(false, `${profile.name} ${route}: accidental overlap: ${overlap}`);
  }

  const images = await page.locator("img").evaluateAll((nodes) =>
    nodes.map((node) => ({
      src: node.getAttribute("src"),
      complete: node.complete,
      naturalWidth: node.naturalWidth,
      naturalHeight: node.naturalHeight,
    })),
  );
  for (const image of images) {
    record(image.complete && image.naturalWidth > 0 && image.naturalHeight > 0, `${profile.name} ${route}: image did not render: ${image.src}`);
    if (image.src === "/assets/plato-agent-desktop.png") {
      record(image.naturalWidth === 1180 && image.naturalHeight === 760, `${profile.name} ${route}: product image must render at natural size 1180x760`);
    }
  }

  const accessibility = await new AxeBuilder({ page }).analyze();
  const severeViolations = accessibility.violations.filter((violation) =>
    ["serious", "critical"].includes(violation.impact ?? ""),
  );
  for (const violation of severeViolations) {
    const selectors = violation.nodes.flatMap((node) => node.target).join(", ");
    failures.push(`${profile.name} ${route}: axe ${violation.impact} ${violation.id}: ${selectors}`);
  }

  if (route === "/") {
    record((await page.locator("h1").allTextContents()).map((text) => text.trim()).join("|") === "Platonic", `${profile.name} /: expected one Platonic h1`);
    record(await page.getByText("by Referential.ai", { exact: true }).first().isVisible(), `${profile.name} /: exact server endorsement is not visible`);
    record(await page.getByText("Plato Agent", { exact: false }).first().isVisible(), `${profile.name} /: Plato Agent is not visible`);
    const primaryAction = page.getByRole("link", { name: "Start with Plato Agent", exact: true });
    record(await primaryAction.isVisible(), `${profile.name} /: primary quickstart action is not visible`);
    record(await primaryAction.getAttribute("href") === "#start", `${profile.name} /: primary quickstart action must target #start`);
    record(
      (await page.locator("#start code").allTextContents()).map((text) => text.trim()).join("|") === "platonic-v0.2.2",
      `${profile.name} /: expected exact platonic-v0.2.2 tag in #start`,
    );
    const taggedQuickstart = page.getByRole("link", { name: "Open the 0.2.2 quickstart", exact: true });
    record(await taggedQuickstart.isVisible(), `${profile.name} /: tagged 0.2.2 quickstart action is not visible`);
    record(
      await taggedQuickstart.getAttribute("href") === "https://github.com/referential-ai/platonic/blob/platonic-v0.2.2/docs/QUICKSTART.md",
      `${profile.name} /: tagged 0.2.2 quickstart action must target the immutable guide`,
    );

    const nextSectionTop = await page.locator("#why").evaluate((element) => element.getBoundingClientRect().top);
    record(nextSectionTop < profile.viewport.height, `${profile.name} /: first viewport must reveal the next section`);

    const visibleCrop = await inspectVisibleHeroCrop(page);
    if ("failure" in visibleCrop) {
      record(false, `${profile.name} /: ${visibleCrop.failure}`);
    } else {
      record(
        visibleCrop.opaquePixels > 3_800
          && visibleCrop.uniqueColors > 20
          && visibleCrop.nonWhitePixels > 300,
        `${profile.name} /: visible hero crop appears blank or monochrome (${visibleCrop.opaquePixels} opaque, ${visibleCrop.uniqueColors} colors, ${visibleCrop.nonWhitePixels} nonwhite)`,
      );

      if (profile.viewport.width <= 860) {
        record(visibleCrop.objectFit === "cover", `${profile.name} /: hero image must use object-fit: cover`);
        record(visibleCrop.mediaCoverage > 0.98, `${profile.name} /: hero image covers only ${(visibleCrop.mediaCoverage * 100).toFixed(1)}% of its media viewport`);
        record(
          visibleCrop.sourceTop <= 1 && visibleCrop.sourceBottom <= 380,
          `${profile.name} /: hero crop must frame the top of the app (source y ${visibleCrop.sourceTop.toFixed(1)}-${visibleCrop.sourceBottom.toFixed(1)}, ${visibleCrop.objectPosition})`,
        );
        record(
          visibleCrop.topUniqueColors >= 3 && visibleCrop.topNonWhitePixels >= 16,
          `${profile.name} /: top of product capture is not meaningfully visible (${visibleCrop.topUniqueColors} colors, ${visibleCrop.topNonWhitePixels} nonwhite pixels)`,
        );
      }
    }

    if (profile.captureAllRoutes || route === "/") {
      await page.screenshot({
        path: resolve(artifactRoot, screenshotName(route, profile.name)),
        fullPage: true,
      });
    }

    await page.keyboard.press("Tab");
    record(await page.locator(".skip-link").evaluate((element) => element === document.activeElement), `${profile.name} /: skip link is not the first keyboard target`);
    record(await page.locator(".skip-link").isVisible(), `${profile.name} /: focused skip link is not visible`);
    await page.evaluate(() => new Promise((resolveFrame) => {
      requestAnimationFrame(() => requestAnimationFrame(resolveFrame));
    }));
    const focusStyle = await page.locator(".skip-link").evaluate((element) => {
      const style = getComputedStyle(element);
      return {
        boxShadow: style.boxShadow,
        outlineColor: style.outlineColor,
        outlineStyle: style.outlineStyle,
        outlineWidth: style.outlineWidth,
      };
    });
    record(
      focusStyle.outlineColor === "rgb(255, 255, 255)"
        && focusStyle.outlineStyle === "solid"
        && focusStyle.outlineWidth === "3px"
        && focusStyle.boxShadow.includes("rgb(17, 21, 28)")
        && focusStyle.boxShadow.includes("0px 0px 0px 6px"),
      `${profile.name} /: focus must render a white inner ring and #11151c outer ring (${focusStyle.outlineColor} ${focusStyle.outlineStyle} ${focusStyle.outlineWidth}; ${focusStyle.boxShadow})`,
    );

    await primaryAction.click();
    await page.waitForFunction(() => window.location.hash === "#start");
    const fragmentPosition = await page.evaluate(() => {
      const header = document.querySelector(".site-header");
      const start = document.querySelector("#start");
      const heading = document.querySelector("#start-title");
      return {
        hash: window.location.hash,
        headerBottom: header?.getBoundingClientRect().bottom ?? Number.POSITIVE_INFINITY,
        headingTop: heading?.getBoundingClientRect().top ?? Number.NEGATIVE_INFINITY,
        startTop: start?.getBoundingClientRect().top ?? Number.NEGATIVE_INFINITY,
      };
    });
    record(fragmentPosition.hash === "#start", `${profile.name} /: primary quickstart action did not reach #start`);
    record(fragmentPosition.startTop >= fragmentPosition.headerBottom - 1, `${profile.name} /: #start is hidden behind the sticky header`);
    record(fragmentPosition.headingTop >= fragmentPosition.headerBottom - 1, `${profile.name} /: #start heading is hidden behind the sticky header`);
  } else if (profile.captureAllRoutes) {
    await page.screenshot({
      path: resolve(artifactRoot, screenshotName(route, profile.name)),
      fullPage: true,
    });
  }

  for (const failure of pageFailures) {
    failures.push(`${profile.name} ${route}: ${failure}`);
  }
}

await mkdir(artifactRoot, { recursive: true });
const browser = await chromium.launch({ headless: true });

try {
  for (const [profileIndex, profile] of profiles.entries()) {
    const context = await browser.newContext({
      viewport: profile.viewport,
      isMobile: profile.isMobile ?? false,
      reducedMotion: "reduce",
      colorScheme: "light",
    });
    try {
      if (profileIndex === 0) {
        await proveResources(context.request);
        await proveRedirects(context.request);
      }
      for (const route of routes) {
        const page = await context.newPage();
        try {
          await provePage(page, profile, route);
        } catch (error) {
          const reason = error instanceof Error ? error.stack ?? error.message : String(error);
          failures.push(`${profile.name} ${route}: ${reason}`);
        } finally {
          await page.close();
        }
      }
    } finally {
      await context.close();
    }
  }
} finally {
  await browser.close();
}

if (failures.length > 0) {
  console.error(`Browser proof failed (${failures.length}):`);
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exitCode = 1;
} else {
  console.log(`Browser proof passed for ${routes.length} routes at ${profiles.length} viewports.`);
  console.log(`Screenshots: ${artifactRoot}`);
}
