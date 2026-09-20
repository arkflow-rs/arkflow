#!/usr/bin/env node
// Translation freshness report for the zh-Hans locale.
// Report-only: always exits 0, regardless of staleness findings (see
// docs/DOCUMENTATION.md — content staleness never fails a gate).
//
// Usage:
//   node scripts/i18n-freshness-report.mjs            # stale candidates + coverage
//   node scripts/i18n-freshness-report.mjs --verbose  # also list fresh pairs

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(new URL('..', import.meta.url).pathname);
const docsRoot = path.join(root, 'docs');
const zhRoot = path.join(
  root,
  'i18n',
  'zh-Hans',
  'docusaurus-plugin-content-docs',
  'current',
);
const verbose = process.argv.includes('--verbose');

const gitTime = (file) => {
  try {
    return Number(
      execFileSync('git', ['log', '-1', '--format=%ct', '--', file], {
        cwd: root,
        encoding: 'utf8',
      }).trim(),
    );
  } catch {
    return 0;
  }
};

const walk = (dir) => {
  const out = [];
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...walk(p));
    else if (/\.(md|mdx)$/.test(e.name)) out.push(p);
  }
  return out;
};

const enFiles = walk(docsRoot);
const fresh = [];
const stale = [];
for (const en of enFiles) {
  const rel = path.relative(docsRoot, en);
  const zh = path.join(zhRoot, rel);
  if (!fs.existsSync(zh)) continue;
  const enTime = gitTime(path.relative(root, en));
  const zhTime = gitTime(path.relative(root, zh));
  // A file without git history (timestamp 0) is newly added and cannot be
  // stale — it was written against the current English page.
  if (enTime > zhTime && zhTime > 0) stale.push({ rel, enTime, zhTime });
  else fresh.push(rel);
}

const total = enFiles.length;
const translated = total - (enFiles.filter((en) => !fs.existsSync(path.join(zhRoot, path.relative(docsRoot, en)))).length);
const fmt = (ts) => new Date(ts * 1000).toISOString().slice(0, 10);

console.log(`# zh-Hans translation freshness report`);
console.log('');
console.log(`coverage: ${translated}/${total} pages (${Math.round((translated / total) * 100)}%)`);
console.log(`stale candidates: ${stale.length}`);
console.log('');
if (stale.length > 0) {
  console.log('| page | english last commit | translation last commit |');
  console.log('|------|--------------------|-------------------------|');
  for (const s of stale) {
    console.log(`| ${s.rel} | ${fmt(s.enTime)} | ${fmt(s.zhTime)} |`);
  }
  console.log('');
}
if (verbose) {
  console.log(`fresh pairs (${fresh.length}):`);
  for (const f of fresh) console.log(`  - ${f}`);
}
console.log('report-only: staleness never fails a gate (see docs/DOCUMENTATION.md).');
process.exit(0);
