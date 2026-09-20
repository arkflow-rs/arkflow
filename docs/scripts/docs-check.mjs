import fs from 'node:fs';
import path from 'node:path';
import {parseComponentOwnership, routeFor, walk} from './lib.mjs';

const root = path.resolve(new URL('..', import.meta.url).pathname);
const docsRoot = path.join(root, 'docs');
const repoRoot = path.dirname(root);
const inventoryPath = path.join(root, 'reference', 'component-inventory.json');
const examplesPath = path.join(root, 'reference', 'example-manifest.json');
const errors = [];

function walkFlat(dir) {
  return fs.readdirSync(dir, {withFileTypes: true}).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? walkFlat(file) : [file];
  });
}

const markdown = walkFlat(docsRoot).filter(
  (file) => file.endsWith('.md') || file.endsWith('.mdx'),
);
const routeFiles = new Set(
  markdown.map((file) => routeFor(docsRoot, file)),
);

// ---------------------------------------------------------------------------
// Headings and anchors. GitHub-style slugs (Docusaurus uses the same rules):
// lowercase, strip punctuation, whitespace -> "-", deduplicated with -N.
// ---------------------------------------------------------------------------

function slugify(raw) {
  // Strip markdown formatting but keep underscores: Docusaurus derives
  // heading ids with github-slugger, which preserves `snake_case` text.
  let text = raw
    .replace(/`([^`]*)`/g, '$1')
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/\*([^*]+)\*/g, '$1');
  text = text.toLowerCase().trim();
  let out = '';
  for (const ch of text) {
    if (/[\p{L}\p{N}\p{M}_-]/u.test(ch)) out += ch;
    else if (/\s/.test(ch)) out += '-';
  }
  return out;
}

function headingSlugs(text) {
  const slugs = new Set();
  const counts = new Map();
  let inFence = false;
  for (const line of text.split('\n')) {
    if (/^\s*(```|~~~)/.test(line)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    const heading = /^(#{1,6})\s+(.*?)\s*#*\s*$/.exec(line);
    if (!heading) continue;
    const explicit = /\{#([\w-]+)\}\s*$/.exec(heading[2]);
    const base = explicit ? explicit[1] : slugify(heading[2]);
    const count = counts.get(base) ?? 0;
    counts.set(base, count + 1);
    slugs.add(count === 0 ? base : `${base}-${count}`);
  }
  return slugs;
}

const slugCache = new Map();
function slugsFor(file) {
  if (!slugCache.has(file)) {
    slugCache.set(file, headingSlugs(fs.readFileSync(file, 'utf8')));
  }
  return slugCache.get(file);
}

// ---------------------------------------------------------------------------
// Per-page checks: front matter, H1, internal links, anchors.
// ---------------------------------------------------------------------------

for (const file of markdown) {
  const text = fs.readFileSync(file, 'utf8');
  if (!text.startsWith('---\n') && !file.includes(`${path.sep}versioned_docs${path.sep}`)) {
    errors.push(`${path.relative(root, file)}: missing front matter`);
  }
  if (!/^#\s+\S+/m.test(text)) {
    errors.push(`${path.relative(root, file)}: missing level-one heading`);
  }
  for (const match of text.matchAll(/\]\(([^)]+)\)/g)) {
    const raw = match[1].replace(/^<|>$/g, '').trim();
    const hashAt = raw.indexOf('#');
    const target = hashAt === -1 ? raw : raw.slice(0, hashAt);
    const anchor = hashAt === -1 ? null : raw.slice(hashAt + 1);
    if (!target && !anchor) continue;
    if (
      target &&
      (target.includes('/category/') ||
        target.startsWith('http://') ||
        target.startsWith('https://') ||
        target.startsWith('mailto:') ||
        target.startsWith('pathname:') ||
        target.startsWith('/'))
    ) {
      continue;
    }

    // Resolve the linked file: an empty target means this page itself.
    let resolved = null;
    if (!target) {
      resolved = file;
    } else {
      const base = path.resolve(path.dirname(file), target);
      const candidates = [base, `${base}.md`, path.join(base, 'index.md')];
      resolved = candidates.find((candidate) => fs.existsSync(candidate)) ?? null;
      const route = path
        .relative(docsRoot, base)
        .replace(/\.(md|mdx)$/, '')
        .split(path.sep)
        .map((segment) => segment.replace(/^\d+-/, ''))
        .join('/');
      if (!resolved && !routeFiles.has(route)) {
        errors.push(`${path.relative(root, file)}: unresolved internal link ${target}`);
        continue;
      }
    }

    if (anchor && resolved && /\.(md|mdx)$/.test(resolved)) {
      if (!slugsFor(resolved).has(anchor)) {
        errors.push(`${path.relative(root, file)}: broken anchor #${anchor} in link to ${target || path.basename(file)}`);
      }
    }
  }
}

// Component coverage: the committed inventory is a generated export of the
// Rust registry (see crates/arkflow-plugin/tests/docs_inventory_snapshot.rs);
// pages declare ownership in front matter and this check validates the two
// directions against each other.
let inventory;
try {
  inventory = JSON.parse(fs.readFileSync(inventoryPath, 'utf8'));
  if (inventory.version !== 2) {
    errors.push(
      `reference/component-inventory.json: expected format version 2, got ${inventory.version}`,
    );
  }
} catch (error) {
  errors.push(`reference/component-inventory.json: ${error.message}`);
  inventory = {components: []};
}

const inventoryKeys = new Set(
  (inventory.components ?? []).map((component) => `${component.kind}:${component.name}`),
);
const declaredKeys = new Set();

const componentsDir = path.join(docsRoot, 'components');
for (const file of walkFlat(componentsDir)) {
  if (!file.endsWith('.md') || file.endsWith('_category_.md')) continue;
  const text = fs.readFileSync(file, 'utf8');
  const where = path.relative(root, file);
  try {
    const owned = parseComponentOwnership(docsRoot, file, text);
    for (const {kind, name} of owned) {
      const key = `${kind}:${name}`;
      declaredKeys.add(key);
      if (!inventoryKeys.has(key)) {
        errors.push(`${where}: declares ${key} but the generated inventory has no such component`);
      }
    }
  } catch (error) {
    errors.push(error.message);
  }
}
for (const key of inventoryKeys) {
  if (!declaredKeys.has(key)) {
    errors.push(
      `reference/component-inventory.json: component ${key} has no page declaring it under docs/components`,
    );
  }
}

const inventoryPage = path.join(docsRoot, 'reference', 'component-inventory.md');
const inventoryText = fs.existsSync(inventoryPage)
  ? fs.readFileSync(inventoryPage, 'utf8')
  : '';
if (!inventoryText.includes('COMPONENT_INVENTORY_START') || !inventoryText.includes('COMPONENT_INVENTORY_END')) {
  errors.push('docs/reference/component-inventory.md: missing generated section markers');
}

// ---------------------------------------------------------------------------
// Example manifest: registered entries must exist, and every YAML example on
// disk must be registered — silent skipping is not permitted.
// ---------------------------------------------------------------------------

try {
  const manifest = JSON.parse(fs.readFileSync(examplesPath, 'utf8'));
  const manifestNames = new Map();
  const declaredExamplePaths = new Set();
  for (const example of manifest.examples ?? []) {
    if (!manifestNames.has(example.name)) manifestNames.set(example.name, 0);
    manifestNames.set(example.name, manifestNames.get(example.name) + 1);
    const exampleFile = path.resolve(root, example.path);
    declaredExamplePaths.add(exampleFile);
    if (!fs.existsSync(exampleFile)) errors.push(`reference/example-manifest.json: missing example ${example.path}`);
    else if (fs.statSync(exampleFile).size === 0) errors.push(`reference/example-manifest.json: empty example ${example.path}`);
  }
  for (const [name, count] of manifestNames) {
    if (count > 1) errors.push(`reference/example-manifest.json: duplicate example name ${name}`);
  }
  const examplesDir = path.join(repoRoot, 'examples');
  if (fs.existsSync(examplesDir)) {
    for (const entry of fs.readdirSync(examplesDir, {withFileTypes: true})) {
      if (!entry.isFile() || !/\.(ya?ml)$/.test(entry.name)) continue;
      const full = path.join(examplesDir, entry.name);
      if (!declaredExamplePaths.has(full)) {
        errors.push(`examples/${entry.name} is not registered in reference/example-manifest.json`);
      }
    }
  }
} catch (error) {
  errors.push(`reference/example-manifest.json: ${error.message}`);
}

// ---------------------------------------------------------------------------
// Inline YAML classification. Every ```yaml block must carry a classification
// marker in its fence metastring (see docs/DOCUMENTATION.md). The vocabulary
// here MUST stay in sync with crates/arkflow/tests/docs_snippets_validate.rs;
// drift fails both gates loudly, and both run in CI.
// ---------------------------------------------------------------------------

const YAML_CLASSIFICATIONS = new Set(['full', 'fragment', 'foreign']);
const YAML_WRAP_KINDS = new Set(['input', 'output', 'processors', 'durability', 'buffer', 'stream', 'codec', 'engine']);

function classifyYamlMeta(meta) {
  // Returns null when classified, or an error message string.
  const validate = /(?:^|\s)validate=([\w-]+)/.exec(meta);
  if (!validate) {
    return 'missing classification marker (add validate=full, validate=fragment wrap=<input|output|processors|durability|buffer|stream|codec|engine>, or validate=foreign reason="..." — see docs/DOCUMENTATION.md)';
  }
  const kind = validate[1];
  if (kind === 'full') return null;
  if (kind === 'foreign') {
    const reason = /(?:^|\s)reason="([^"]+)"|(?:^|\s)reason=([^\s"]+)/.exec(meta);
    if (!reason) {
      return 'validate=foreign requires a reason="..." (why this block is not an ArkFlow config)';
    }
    return null;
  }
  if (kind === 'fragment') {
    const wrap = /(?:^|\s)wrap=([\w-]+)/.exec(meta);
    if (!wrap) {
      return 'validate=fragment requires wrap=<input|output|processors|durability|buffer|stream|codec|engine>';
    }
    if (!YAML_WRAP_KINDS.has(wrap[1])) {
      return `unknown wrap kind 'wrap=${wrap[1]}' (expected one of: ${[...YAML_WRAP_KINDS].join(', ')})`;
    }
    return null;
  }
  return `unknown validate kind 'validate=${kind}' (expected full, fragment, or foreign)`;
}

function checkYamlClassifications() {
  for (const file of markdown) {
    const text = fs.readFileSync(file, 'utf8');
    const lines = text.split('\n');
    let inFence = false;
    let fenceStart = -1;
    let meta = '';
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (!inFence && /^\s*(```|~~~)/.test(line)) {
        inFence = true;
        fenceStart = i + 1;
        meta = line.replace(/^\s*(```|~~~)/, '').trim();
        continue;
      }
      if (inFence && /^\s*(```|~~~)\s*$/.test(line)) {
        inFence = false;
        if (/^yaml(\s|$)/.test(meta)) {
          const problem = classifyYamlMeta(meta.replace(/^yaml\b/, ''));
          if (problem) {
            errors.push(
              `${path.relative(root, file)}:${fenceStart}: yaml code block ${problem}`,
            );
          }
        }
        continue;
      }
    }
  }
}

checkYamlClassifications();

// ---------------------------------------------------------------------------
// Sidebar reachability: every content page must be reachable from
// docs/sidebars.ts (explicit id or autogenerated directory) or belong to the
// legacy compatibility stubs, which are intentionally absent from the sidebar
// until the next release removes them (see docs/docs/migration/routes.md).
// ---------------------------------------------------------------------------

const LEGACY_STUB_ROUTES = new Set([
  // Root-level compatibility stubs.
  'start-here',
  'streaming-jobs',
  'build-pipelines',
  'operate',
  'intro',
]);

const LEGACY_STUB_PREFIXES = [
  'getting-started/',
  'tutorials/',
  'concepts/',
  'configuration/',
  'control-plane/',
  'deploy/',
  'how-to/',
  'cases/',
];

function sidebarsReachableRoutes() {
  const source = fs.readFileSync(path.join(root, 'sidebars.ts'), 'utf8');
  const routes = new Set();
  // Explicit doc ids: array-element strings plus link {type: 'doc', id: '...'}.
  // Skip strings that are labels, types, or autogenerated dirNames.
  for (const match of source.matchAll(/(?<!label:\s)(?<!type:\s)(?<!dirName:\s)['"]([a-z0-9_][a-z0-9_/-]*)['"]/g)) {
    routes.add(match[1]);
  }
  // Autogenerated categories expand to every page below the directory.
  for (const match of source.matchAll(/dirName:\s*'([^']+)'/g)) {
    const dir = path.join(docsRoot, match[1]);
    if (!fs.existsSync(dir)) {
      errors.push(`sidebars.ts: autogenerated dirName '${match[1]}' does not exist under docs/`);
      continue;
    }
    for (const file of walk(dir)) {
      if (!file.endsWith('.md') || file.endsWith('_category_.md')) continue;
      routes.add(routeFor(docsRoot, file));
    }
  }
  return routes;
}

try {
  const reachableRoutes = sidebarsReachableRoutes();
  for (const file of markdown) {
    const base = path.basename(file);
    if (base === '_category_.md') continue;
    const route = routeFor(docsRoot, file);
    if (reachableRoutes.has(route)) continue;
    if (LEGACY_STUB_ROUTES.has(route)) continue;
    if (LEGACY_STUB_PREFIXES.some((prefix) => route.startsWith(prefix))) continue;
    errors.push(`${path.relative(root, file)}: page is not reachable from the sidebar`);
  }
} catch (error) {
  errors.push(`sidebars.ts: ${error.message}`);
}

// ---------------------------------------------------------------------------
// Localized trees (docs/i18n/<locale>/docusaurus-plugin-content-docs/current).
// English is canonical; a translated page must structurally mirror its
// English counterpart: the counterpart must exist, front-matter identity and
// component ownership must be preserved, file-relative links/anchors must
// resolve inside the localized tree (untranslated targets are linked via
// locale-prefixed absolute routes, which render English fallback content),
// and YAML code fences must be byte-identical so translated pages are
// snippet-valid by construction (the Rust-side validator keeps reading
// docs/docs/ only).
// ---------------------------------------------------------------------------

const FRONT_MATTER_PARITY_KEYS = ['id', 'slug', 'sidebar_position', 'sidebar_label'];

function frontMatterBlock(text) {
  const match = /^---\n([\s\S]*?)\n---/.exec(text);
  return match ? match[1] : null;
}

function scalarFrontMatter(text) {
  const block = frontMatterBlock(text);
  const scalars = {};
  if (block === null) return scalars;
  for (const line of block.split('\n')) {
    const match = /^([A-Za-z_][\w-]*):\s*(.*)$/.exec(line);
    if (match && !['components', 'tags', 'keywords'].includes(match[1])) {
      scalars[match[1]] = match[2].trim().replace(/^['"]|['"]$/g, '');
    }
  }
  return scalars;
}

function componentsDeclaration(text) {
  const block = frontMatterBlock(text);
  if (block === null) return null;
  const lines = block.split('\n');
  const at = lines.findIndex((line) => /^components:/.test(line));
  if (at === -1) return null;
  const inline = lines[at].slice('components:'.length).trim();
  if (inline) return inline;
  const items = [];
  for (const line of lines.slice(at + 1)) {
    if (/^\s*-\s*\S+\s*$/.test(line)) items.push(line.replace(/^\s*-\s*/, '').trim());
    else if (line.trim() === '') continue;
    else break;
  }
  return items.join(', ');
}

function yamlFences(text) {
  const fences = [];
  let inFence = false;
  let meta = '';
  let content = [];
  for (const line of text.split('\n')) {
    if (!inFence && /^\s*(```|~~~)/.test(line)) {
      inFence = true;
      meta = line.replace(/^\s*(```|~~~)/, '').trim();
      content = [];
      continue;
    }
    if (inFence && /^\s*(```|~~~)\s*$/.test(line)) {
      inFence = false;
      if (/^yaml(\s|$)/.test(meta)) fences.push({meta, content: content.join('\n')});
      continue;
    }
    if (inFence) content.push(line);
  }
  return fences;
}

for (const locale of ['zh-Hans']) {
  const localeDocsRoot = path.join(root, 'i18n', locale, 'docusaurus-plugin-content-docs', 'current');
  if (!fs.existsSync(localeDocsRoot)) continue;
  const localePrefix = `i18n/${locale}/docusaurus-plugin-content-docs/current`;

  // Pre-detection for the localized-build resolution rule: a file-relative
  // link from an untranslated English page to a page that HAS a localized
  // counterpart fails the localized build (the target is registered under
  // its localized source path). Convert such links to their absolute
  // /docs/... route, or translate the English page (see docs/DOCUMENTATION.md).
  for (const file of markdown) {
    const rel = path.relative(docsRoot, file);
    if (fs.existsSync(path.join(localeDocsRoot, rel))) continue;
    const text = fs.readFileSync(file, 'utf8');
    for (const match of text.matchAll(/\]\(([^)]+)\)/g)) {
      const raw = match[1].replace(/^<|>$/g, '').trim();
      const hashAt = raw.indexOf('#');
      const target = hashAt === -1 ? raw : raw.slice(0, hashAt);
      if (
        !target ||
        !/\.(md|mdx)$/.test(target) ||
        target.startsWith(('http')) ||
        target.startsWith('/')
      ) {
        continue;
      }
      const base = path.resolve(path.dirname(file), target);
      for (const candidate of [base, `${base}.md`, path.join(base, 'index.md')]) {
        if (fs.existsSync(candidate) && fs.existsSync(path.join(localeDocsRoot, path.relative(docsRoot, candidate)))) {
          errors.push(
            `${path.relative(root, file)}: links to localized page ${target} — convert the link to its absolute /docs/... route, or translate this page, or the localized build fails to resolve the link (see docs/DOCUMENTATION.md)`,
          );
          break;
        }
      }
    }
  }

  for (const file of walkFlat(localeDocsRoot)) {
    if (!file.endsWith('.md') && !file.endsWith('.mdx')) continue;
    const rel = path.relative(localeDocsRoot, file);
    const where = `${localePrefix}${path.sep}${rel}`;
    const enFile = path.join(docsRoot, rel);

    if (!fs.existsSync(enFile)) {
      errors.push(`${where}: no English counterpart at docs${path.sep}${rel} — the page moved or was deleted upstream; mirror the move or delete the translation`);
      continue;
    }

    const text = fs.readFileSync(file, 'utf8');
    const enText = fs.readFileSync(enFile, 'utf8');

    if (!/^#\s+\S+/m.test(text)) {
      errors.push(`${where}: missing level-one heading`);
    }

    const scalars = scalarFrontMatter(text);
    const enScalars = scalarFrontMatter(enText);
    for (const key of FRONT_MATTER_PARITY_KEYS) {
      if ((scalars[key] ?? '') !== (enScalars[key] ?? '')) {
        errors.push(
          `${where}: front matter "${key}" must equal the English counterpart's (${enScalars[key] || 'unset'} != ${scalars[key] || 'unset'})`,
        );
      }
    }

    const components = componentsDeclaration(text);
    const enComponents = componentsDeclaration(enText);
    if (components !== enComponents) {
      errors.push(
        `${where}: "components:" ownership list must equal the English counterpart's (${enComponents ?? 'unset'} != ${components ?? 'unset'})`,
      );
    }

    for (const match of text.matchAll(/\]\(([^)]+)\)/g)) {
      const raw = match[1].replace(/^<|>$/g, '').trim();
      const hashAt = raw.indexOf('#');
      const target = hashAt === -1 ? raw : raw.slice(0, hashAt);
      const anchor = hashAt === -1 ? null : raw.slice(hashAt + 1);
      if (!target && !anchor) continue;
      if (
        target &&
        (target.includes('/category/') ||
          target.startsWith('http://') ||
          target.startsWith('https://') ||
          target.startsWith('mailto:') ||
          target.startsWith('pathname:') ||
          target.startsWith('/'))
      ) {
        continue;
      }

      // File-relative links must resolve inside the localized tree: the
      // production build (onBrokenMarkdownLinks: throw) resolves them the
      // same way and cannot fall back to the English tree. Links to
      // untranslated pages use locale-prefixed absolute routes instead
      // (skipped here, same policy as the English tree).
      let resolved = null;
      if (!target) {
        resolved = file;
      } else {
        const base = path.resolve(path.dirname(file), target);
        const candidates = [base, `${base}.md`, path.join(base, 'index.md')];
        resolved = candidates.find((candidate) => fs.existsSync(candidate)) ?? null;
        if (!resolved) {
          errors.push(
            `${where}: unresolved internal link ${target} — file-relative links must target a localized page; for untranslated pages link the /${locale}/... route absolutely`,
          );
          continue;
        }
      }

      if (anchor && resolved && /\.(md|mdx)$/.test(resolved)) {
        if (!slugsFor(resolved).has(anchor)) {
          errors.push(`${where}: broken anchor #${anchor} in link to ${target || path.basename(file)}`);
        }
      }
    }

    const enFences = yamlFences(enText);
    const localeFences = yamlFences(text);
    if (localeFences.length !== enFences.length) {
      errors.push(
        `${where}: yaml code block count (${localeFences.length}) differs from the English counterpart (${enFences.length}) — code fences are not translated`,
      );
    } else {
      for (let i = 0; i < enFences.length; i++) {
        if (
          localeFences[i].meta !== enFences[i].meta ||
          localeFences[i].content !== enFences[i].content
        ) {
          errors.push(
            `${where}: yaml code block #${i + 1} differs from the English counterpart — fence classification and content must be byte-identical`,
          );
        }
      }
    }
  }
}

if (errors.length) {
  console.error(`docs check failed (${errors.length} issue${errors.length === 1 ? '' : 's'}):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  console.log(`docs check passed: ${markdown.length} markdown pages, ${inventoryKeys.size} inventory entries`);
}
