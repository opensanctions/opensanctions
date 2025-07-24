const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const gitHash = process.env.ZAVOD_UI_GIT_REVISION || execSync('git rev-parse --short HEAD').toString().trim();
const buildTime = new Date().toISOString();

const versionInfo = {
  git: gitHash,
  buildTime,
};

const outPath = path.join(__dirname, '..', 'version.json');
fs.writeFileSync(outPath, JSON.stringify(versionInfo, null, 2));
console.log('Wrote version info:', versionInfo);
