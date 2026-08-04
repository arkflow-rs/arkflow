import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(new URL('..', import.meta.url).pathname);
const inventoryPath = path.join(root, 'reference', 'component-inventory.json');
const pagePath = path.join(root, 'docs', 'reference', 'component-inventory.md');
const inventory = JSON.parse(fs.readFileSync(inventoryPath, 'utf8'));
const start = '<!-- COMPONENT_INVENTORY_START -->';
const end = '<!-- COMPONENT_INVENTORY_END -->';
const routeFor = (doc) => doc.replace(/\/(\d+)-/g, '/');
const rows = inventory.components
  .slice()
  .sort((a, b) => `${a.kind}:${a.name}`.localeCompare(`${b.kind}:${b.name}`))
  .map((component) => `| ${component.kind} | \`${component.name}\` | [reference](../${routeFor(component.doc)}) |`)
  .join('\n');
const generated = `${start}\n\n| Kind | Component | Documentation |\n| --- | --- | --- |\n${rows}\n\n${end}`;
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
