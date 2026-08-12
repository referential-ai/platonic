import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";

import { base, site } from "./config.mjs";

export default defineConfig({
  site,
  base,
  output: "static",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "Platonic docs",
      description: "User, developer, and reference documentation for Platonic and Plato Agent.",
      favicon: "favicon.svg",
      logo: {
        src: "./src/assets/platonic-mark.svg",
        alt: "",
      },
      customCss: ["./src/styles/custom.css"],
      social: [
        {
          icon: "github",
          label: "Platonic on GitHub",
          href: "https://github.com/referential-ai/platonic",
        },
      ],
      sidebar: [
        {
          label: "User docs",
          items: [{ autogenerate: { directory: "user", attrs: { "data-section": "user" } } }],
        },
        {
          label: "Developer docs",
          items: [
            { autogenerate: { directory: "developer", attrs: { "data-section": "developer" } } },
          ],
        },
        {
          label: "Reference",
          items: [
            { autogenerate: { directory: "reference", attrs: { "data-section": "reference" } } },
          ],
        },
      ],
    }),
  ],
});
