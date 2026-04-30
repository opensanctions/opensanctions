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
  },
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/$1",
  },
};
