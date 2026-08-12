// Production host selection remains pending in platonic-workspace#108.
const siteUrl = new URL(process.env.DOCS_SITE ?? "https://docs.example.invalid");

if (siteUrl.pathname !== "/" || siteUrl.search || siteUrl.hash) {
  throw new Error("DOCS_SITE must be an origin; configure its path with DOCS_BASE");
}

const baseUrl = new URL(process.env.DOCS_BASE ?? "/", siteUrl);

if (baseUrl.origin !== siteUrl.origin || baseUrl.search || baseUrl.hash) {
  throw new Error("DOCS_BASE must be a path on DOCS_SITE");
}

export const site = siteUrl.origin;
export const base = baseUrl.pathname.endsWith("/") ? baseUrl.pathname : `${baseUrl.pathname}/`;
