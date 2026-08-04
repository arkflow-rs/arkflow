import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(new URL('..', import.meta.url).pathname);
const docsRoot = path.join(root, 'docs');
const inventoryPath = path.join(root, 'reference', 'component-inventory.json');
const examplesPath = path.join(root, 'reference', 'example-manifest.json');
const errors = [];

function walk(dir) {
  return fs.readdirSync(dir, {withFileTypes: true}).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? walk(file) : [file];
  });
}

const markdown = walk(docsRoot).filter((file) => file.endsWith('.md') || file.endsWith('.mdx'));
const routeFiles = new Set(markdown.map((file) => {
  const relative = path.relative(docsRoot, file).replace(/\.(md|mdx)$/, '');
  return relative.split(path.sep).map((segment) => segment.replace(/^\d+-/, '')).join('/');
}));
for (const file of markdown) {
  const text = fs.readFileSync(file, 'utf8');
  if (!text.startsWith('---\n') && !file.includes(`${path.sep}versioned_docs${path.sep}`)) {
    errors.push(`${path.relative(root, file)}: missing front matter`);
  }
  if (!/^#\s+\S+/m.test(text)) {
    errors.push(`${path.relative(root, file)}: missing level-one heading`);
  }
  for (const match of text.matchAll(/\]\(([^)]+)\)/g)) {
    const target = match[1].split('#')[0].trim().replace(/^<|>$/g, '');
    if (!target || target.includes('/category/') || target.startsWith('http://') || target.startsWith('https://') || target.startsWith('mailto:') || target.startsWith('/')) continue;
    const base = path.resolve(path.dirname(file), target);
    const candidates = [base, `${base}.md`, path.join(base, 'index.md')];
    const route = path.relative(docsRoot, base).replace(/\.(md|mdx)$/, '').split(path.sep).map((segment) => segment.replace(/^\d+-/, '')).join('/');
    if (!candidates.some((candidate) => fs.existsSync(candidate)) && !routeFiles.has(route)) {
      errors.push(`${path.relative(root, file)}: unresolved internal link ${target}`);
    }
  }
}

let inventory;
try {
  inventory = JSON.parse(fs.readFileSync(inventoryPath, 'utf8'));
} catch (error) {
  errors.push(`reference/component-inventory.json: ${error.message}`);
  inventory = {components: []};
}

const inventoryPage = path.join(docsRoot, 'reference', 'component-inventory.md');
const inventoryText = fs.existsSync(inventoryPage) ? fs.readFileSync(inventoryPage, 'utf8') : '';
for (const component of inventory.components ?? []) {
  const page = path.join(docsRoot, `${component.doc}.md`);
  if (!fs.existsSync(page)) errors.push(`${component.name}: missing page ${component.doc}`);
}
const componentFiles = walk(path.join(docsRoot, 'components'))
  .filter((file) => file.endsWith('.md') && !file.endsWith('_category_.md'))
  .map((file) => path.relative(docsRoot, file).replace(/\.md$/, ''));
const inventoryDocs = new Set((inventory.components ?? []).map((component) => component.doc));
for (const componentDoc of componentFiles) {
  if (!inventoryDocs.has(componentDoc)) errors.push(`reference/component-inventory.json: undocumented component page ${componentDoc}`);
}
if (!inventoryText.includes('COMPONENT_INVENTORY_START') || !inventoryText.includes('COMPONENT_INVENTORY_END')) {
  errors.push('docs/reference/component-inventory.md: missing generated section markers');
}

try {
  const manifest = JSON.parse(fs.readFileSync(examplesPath, 'utf8'));
  for (const example of manifest.examples ?? []) {
    const exampleFile = path.resolve(root, example.path);
    if (!fs.existsSync(exampleFile)) errors.push(`reference/example-manifest.json: missing example ${example.path}`);
    else if (fs.statSync(exampleFile).size === 0) errors.push(`reference/example-manifest.json: empty example ${example.path}`);
  }
} catch (error) {
  errors.push(`reference/example-manifest.json: ${error.message}`);
}

if (errors.length) {
  console.error(`docs check failed (${errors.length} issue${errors.length === 1 ? '' : 's'}):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  console.log(`docs check passed: ${markdown.length} markdown pages, ${inventory.components?.length ?? 0} inventory entries`);
}
