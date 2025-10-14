import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  sassOptions: {
    quietDeps: true,
    silenceDeprecations: [
      /* Until bootstrap migrates _variables.scss to @use.
         https://github.com/twbs/bootstrap/issues/40962 */
      "import",
      /* Until next.js adds experimental support for new SASS API
         or stable support no sooner than NextJS 16
         https://github.com/vercel/next.js/issues/71638 */
      "legacy-js-api",
    ],
  },
};

export default nextConfig;
