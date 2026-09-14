import fs from 'node:fs';
import path from 'node:path';
import {parseComponentOwnership, routeFor, walk} from './lib.mjs';

const root = path.resolve(new URL('..', import.meta.url).pathname);
const inventoryPath = path.join(root, 'reference', 'component-inventory.json');
const docsRoot = path.join(root, 'docs');
const componentsDir = path.join(docsRoot, 'components');
const pagePath = path.join(root, 'docs', 'reference', 'component-inventory.md');
const start = '<!-- COMPONENT_INVENTORY_START -->';
const end = '<!-- COMPONENT_INVENTORY_END -->';

const inventory = JSON.parse(fs.readFileSync(inventoryPath, 'utf8'));
if (inventory.version !== 2) {
  throw new Error(
    `${path.relative(root, inventoryPath)}: expected format version 2, got ${inventory.version}; regenerate it with ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`,
  );
}

// Join the registry export with page ownership declarations: every
// (kind, name) maps to the routes of the pages documenting it.
const owners = new Map();
for (const file of walk(componentsDir)) {
  if (!file.endsWith('.md') || file.endsWith('_category_.md')) continue;
  const text = fs.readFileSync(file, 'utf8');
  for (const {kind, name} of parseComponentOwnership(docsRoot, file, text)) {
    const key = `${kind}:${name}`;
    if (!owners.has(key)) owners.set(key, []);
    owners.set(key, owners.get(key).concat(routeFor(docsRoot, file)));
  }
}

const rows = inventory.components
  .slice()
  .sort((a, b) => `${a.kind}:${a.name}`.localeCompare(`${b.kind}:${b.name}`))
  .map((component) => {
    const routes = owners.get(`${component.kind}:${component.name}`) ?? [];
    const links = routes
      .map((route) => `[reference](../${route})`)
      .join(', ');
    const documentation = links || '(no page yet)';
    const description = String(component.description || '').replace(/\|/g, '\\|');
    return `| ${component.kind} | \`${component.name}\` | ${description} | ${documentation} |`;
  })
  .join('\n');

const generated = `${start}\n\n| Kind | Component | Description | Documentation |\n| --- | --- | --- | --- |\n${rows}\n\n${end}`;
const current = fs.readFileSync(pagePath, 'utf8');
const pattern = new RegExp(`${start}[\\s\\S]*?${end}`);
if (!pattern.test(current)) throw new Error(`missing generated markers in ${pagePath}`);
const next = current.replace(pattern, generated);
if (process.argv.includes('--check')) {
  if (next !== current) {
    console.error(`${path.relative(root, pagePath)} is stale; run pnpm components:generate`);
    process.exitCode = 1;
  } else {
    console.log('component inventory is up to date');
  }
} else {
  fs.writeFileSync(pagePath, next);
  console.log(`generated ${path.relative(root, pagePath)} from ${inventory.components.length} entries`);
}
