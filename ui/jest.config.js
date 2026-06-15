/** @type {import("jest").Config} **/
module.exports = {
  setupFiles: ["<rootDir>/.jest/setEnvVars.js"],
  testEnvironment: "node",
  transform: {
    "^.+\\.tsx?$": ["ts-jest", {
      tsconfig: {
        rootDir: ".",
        moduleResolution: "nodenext",
        module: "nodenext",
      },
    }],
    "^.+/node_modules/kysely/.+\\.js$": ["ts-jest", {
      diagnostics: false,
      tsconfig: {
        allowJs: true,
        esModuleInterop: true,
        module: "CommonJS",
        moduleResolution: "node",
        ignoreDeprecations: "6.0",
      },
    }],
  },
  transformIgnorePatterns: ["/node_modules/(?!(kysely)/)"],
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/$1",
  },
};
