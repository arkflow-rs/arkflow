import fs from 'node:fs';
import path from 'node:path';

const KINDS = ['input', 'output', 'processor', 'buffer', 'codec', 'temporary'];

// Component kind inferred from the numbered directories under docs/components.
const KIND_BY_DIR = {
  '0-inputs': 'input',
  '1-buffers': 'buffer',
  '2-processors': 'processor',
  '3-outputs': 'output',
  '4-temporary': 'temporary',
  '5-codecs': 'codec',
};

export {KINDS, KIND_BY_DIR};

export function walk(dir) {
  return fs.readdirSync(dir, {withFileTypes: true}).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? walk(file) : [file];
  });
}

export function routeFor(docsRoot, file) {
  return path
    .relative(docsRoot, file)
    .replace(/\.mdx?$/, '')
    .split(path.sep)
    .map((segment) => segment.replace(/^\d+-/, ''))
    .join('/');
}

/**
 * Extract the `components:` ownership declaration from a page's front
 * matter. Supports exactly two shapes and fails loudly on anything else:
 *
 *   components: [name, kind/name]
 *   components:
 *     - name
 *     - kind/name
 *
 * Returns a list of `{kind, name}` entries. A bare name takes its kind from
 * the page's directory; a `kind/name` entry states the kind explicitly.
 */
export function parseComponentOwnership(docsRoot, file, text) {
  const frontMatter = /^---\n([\s\S]*?)\n---/.exec(text);
  if (!frontMatter) {
    throw new Error(`${path.relative(docsRoot, file)}: missing front matter`);
  }
  const lines = frontMatter[1].split('\n');
  const declarationAt = lines.findIndex((line) => /^components:/.test(line));
  if (declarationAt === -1) {
    throw new Error(
      `${path.relative(docsRoot, file)}: missing "components:" front matter declaration`,
    );
  }

  const inline = lines[declarationAt].slice('components:'.length).trim();
  let items;
  if (inline) {
    if (!inline.startsWith('[') || !inline.endsWith(']')) {
      throw new Error(
        `${path.relative(docsRoot, file)}: "components:" inline value must be [name, ...]`,
      );
    }
    items = inline
      .slice(1, -1)
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
  } else {
    items = [];
    for (const line of lines.slice(declarationAt + 1)) {
      if (/^\s*-\s*\S+\s*$/.test(line)) {
        items.push(line.replace(/^\s*-\s*/, '').trim());
      } else if (line.trim() === '') {
        continue;
      } else if (/^\s+\S/.test(line)) {
        throw new Error(
          `${path.relative(docsRoot, file)}: unsupported "components:" list entry ${line.trim()}`,
        );
      } else {
        break;
      }
    }
  }

  if (items.length === 0) {
    throw new Error(
      `${path.relative(docsRoot, file)}: "components:" declaration is empty`,
    );
  }

  const pageKind = pageKindFor(docsRoot, file);
  return items.map((item) => {
    if (!/^[a-z0-9_-]+(\/[a-z0-9_-]+)?$/.test(item)) {
      throw new Error(
        `${path.relative(docsRoot, file)}: malformed component reference "${item}" (expected name or kind/name)`,
      );
    }
    if (item.includes('/')) {
      const [kind, name] = item.split('/');
      if (!KINDS.includes(kind)) {
        throw new Error(
          `${path.relative(docsRoot, file)}: unknown component kind "${kind}" in "${item}"`,
        );
      }
      return {kind, name};
    }
    if (!pageKind) {
      throw new Error(
        `${path.relative(docsRoot, file)}: cannot infer the kind for "${item}"; use a kind/name reference such as "temporary/${item}"`,
      );
    }
    return {kind: pageKind, name: item};
  });
}

function pageKindFor(docsRoot, file) {
  const relative = path.relative(docsRoot, file).split(path.sep);
  if (relative[0] !== 'components' || relative.length < 3) {
    return null;
  }
  return KIND_BY_DIR[relative[1]] ?? null;
}
