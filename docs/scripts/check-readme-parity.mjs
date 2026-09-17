import fs from 'node:fs';
import path from 'node:path';

// README component-list parity gate: both READMEs carry generated-style
// marker regions (`README_COMPONENTS:<kind> START/END`) around the component
// lists. Each bullet must state its registry type-name(s) right after the
// label: `- **Kafka** (`kafka`): ...`. This script enforces that the names
// per kind exactly match the generated component inventory
// (docs/reference/component-inventory.json) and that README.md and
// README_zh.md enumerate the same set — the requirement in
// openspec/specs/documentation-accuracy that was previously unenforced.

const docsRoot = path.resolve(new URL('..', import.meta.url).pathname);
const repoRoot = path.dirname(docsRoot);
const inventoryPath = path.join(docsRoot, 'reference', 'component-inventory.json');
const errors = [];

let inventory;
try {
  inventory = JSON.parse(fs.readFileSync(inventoryPath, 'utf8'));
} catch (error) {
  console.error(`check-readme-parity: cannot read component inventory: ${error.message}`);
  process.exit(1);
}

const registryByKind = new Map();
for (const component of inventory.components ?? []) {
  if (!registryByKind.has(component.kind)) registryByKind.set(component.kind, new Map());
  const names = registryByKind.get(component.kind);
  names.set(component.name, (names.get(component.name) ?? 0) + 1);
}

const REGION = /<!--\s*README_COMPONENTS:([a-z]+)\s+START\s*-->([\s\S]*?)<!--\s*README_COMPONENTS:\1\s+END\s*-->/g;
const ANY_OPEN = /<!--\s*README_COMPONENTS:[a-z]+\s+START\s*-->/g;
const ANY_CLOSE = /<!--\s*README_COMPONENTS:[a-z]+\s+END\s*-->/g;
// Bullet shape: `- **Label** (`name` / `other`): description` (en or zh colon).
const BULLET = /^-\s+\*\*[^*]+\*\*\s*\(([^)]*)\)\s*[:\uFF1A]/;
const NAME = /`([a-z0-9_]+)`/g;

function countMap(names) {
  const counts = new Map();
  for (const name of names) counts.set(name, (counts.get(name) ?? 0) + 1);
  return counts;
}

function diffCounts(actual, expected) {
  const problems = [];
  for (const [name, count] of expected) {
    const have = actual.get(name) ?? 0;
    if (have === 0) problems.push(`missing type-name \`${name}\``);
    else if (have > count) problems.push(`\`${name}\` listed ${have - count} time(s) too often`);
  }
  for (const [name, count] of actual) {
    if (!expected.has(name)) problems.push(`unknown type-name \`${name}\` (not registered for this kind)`);
    else if (count > expected.get(name)) problems.push(`\`${name}\` listed ${count - expected.get(name)} time(s) too often`);
  }
  return problems;
}

function parseReadme(file) {
  const where = path.relative(repoRoot, file);
  const text = fs.readFileSync(file, 'utf8');
  const opens = (text.match(ANY_OPEN) ?? []).length;
  const closes = (text.match(ANY_CLOSE) ?? []).length;
  if (opens !== closes) {
    errors.push(`${where}: unbalanced README_COMPONENTS markers (${opens} START / ${closes} END)`);
  }

  const byKind = new Map();
  for (const match of text.matchAll(REGION)) {
    const kind = match[1];
    if (!registryByKind.has(kind)) {
      errors.push(`${where}: README_COMPONENTS region uses unknown kind "${kind}"`);
      continue;
    }
    if (!byKind.has(kind)) byKind.set(kind, []);
    for (const line of match[2].split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('-')) continue;
      const bullet = BULLET.exec(trimmed);
      if (!bullet) {
        errors.push(
          `${where}: ${kind} bullet lacks a "(\`type_name\`)" registry name right after its label: ${trimmed.slice(0, 90)}`,
        );
        continue;
      }
      const names = [...bullet[1].matchAll(NAME)].map((m) => m[1]);
      if (names.length === 0) {
        errors.push(`${where}: ${kind} bullet name group contains no backticked type-name: ${trimmed.slice(0, 90)}`);
        continue;
      }
      byKind.get(kind).push(...names);
    }
  }
  return {where, byKind};
}

const readmes = ['README.md', 'README_zh.md'].map((file) => parseReadme(path.join(repoRoot, file)));

for (const {where, byKind} of readmes) {
  for (const [kind, expected] of registryByKind) {
    if (!byKind.has(kind)) {
      errors.push(`${where}: no README_COMPONENTS:${kind} region, but the inventory registers ${expected.size}`);
      continue;
    }
    for (const problem of diffCounts(countMap(byKind.get(kind)), expected)) {
      errors.push(`${where}: README_COMPONENTS:${kind} ${problem}`);
    }
  }
}

const [en, zh] = readmes;
for (const kind of registryByKind.keys()) {
  const enNames = (en.byKind.get(kind) ?? []).sort().join(',');
  const zhNames = (zh.byKind.get(kind) ?? []).sort().join(',');
  if (enNames !== zhNames) {
    errors.push(`README.md and README_zh.md enumerate different ${kind} components`);
  }
}

if (errors.length) {
  console.error(`readme parity check failed (${errors.length} issue${errors.length === 1 ? '' : 's'}):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  const total = [...registryByKind.values()].reduce((sum, names) => sum + names.size, 0);
  console.log(`readme parity check passed: ${readmes.length} READMEs, ${total} components across ${registryByKind.size} kinds`);
}
