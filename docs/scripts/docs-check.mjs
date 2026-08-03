#!/usr/bin/env node

import {execFileSync} from 'node:child_process';
import {existsSync, readdirSync, readFileSync} from 'node:fs';
import {join, relative} from 'node:path';

const repo = join(new URL('.', import.meta.url).pathname, '../..');
const docsRoot = join(repo, 'docs');
const contentRoot = join(docsRoot, 'docs');
const binary = process.env.ARKFLOW_BIN ?? join(repo, 'target/debug/arkflow');
const sections = ['introduction', 'getting-started', 'concepts', 'configuration', 'components', 'sql', 'processing-guides', 'cookbook', 'observability', 'operations', 'troubleshooting', 'migration'];
const errors = [];

for (const section of sections) {
  if (!existsSync(join(contentRoot, section, '_category_.json'))) errors.push(`missing category metadata: ${section}`);
}

function markdownFiles(dir) {
  return readdirSync(dir, {withFileTypes: true}).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return markdownFiles(path);
    return entry.name.endsWith('.md') ? [path] : [];
  });
}

for (const file of markdownFiles(contentRoot)) {
  const rel = relative(contentRoot, file);
  const section = rel.split('/')[0];
  if (!sections.includes(section)) errors.push(`orphan page outside canonical sections: ${rel}`);
}

const aliases = {'processor/json': 'json_to_arrow', 'processor/protobuf': 'protobuf_to_arrow', 'codec/debezium': 'debezium_json', 'codec/schema-registry': 'schema_registry'};
const directories = {'0-inputs': 'input', '1-buffers': 'buffer', '2-processors': 'processor', '3-outputs': 'output', '5-codecs': 'codec'};
const list = execFileSync(binary, ['components', 'list'], {encoding: 'utf8'});
const registered = new Set();
let kind = null;
for (const line of list.split('\n')) {
  const heading = line.match(/^(input|output|processor|buffer|codec):$/);
  if (heading) { kind = heading[1]; continue; }
  const item = line.match(/^\s{2}([^\s]+)\s{2}/);
  if (kind && item) registered.add(`${kind}/${item[1]}`);
}
for (const [directory, fileKind] of Object.entries(directories)) {
  for (const file of readdirSync(join(contentRoot, 'components', directory)).filter((name) => name.endsWith('.md'))) {
    const filename = file.slice(0, -3);
    const component = aliases[`${fileKind}/${filename}`] ?? filename;
    if (!registered.has(`${fileKind}/${component}`)) errors.push(`phantom component page: ${fileKind}/${filename}`);
    const body = readFileSync(join(contentRoot, 'components', directory, file), 'utf8');
    if (!body.includes('<!-- BEGIN AUTO:') || !body.includes('<!-- END AUTO -->')) errors.push(`missing generated fence: components/${directory}/${file}`);
    if (!body.match(/^\| Field \| Type \| Required \| Default \| common\? \|/m)) errors.push(`non-canonical table: components/${directory}/${file}`);
    const metadata = JSON.parse(execFileSync(binary, ['components', 'show', fileKind, component, '--format', 'json'], {encoding: 'utf8'}));
    for (const field of Object.keys(metadata.config_schema?.properties ?? {})) {
      if (!body.match(new RegExp(`^\\| ${field.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')} \\|`, 'm'))) errors.push(`missing schema field ${field}: components/${directory}/${file}`);
    }
  }
}
if (errors.length) { console.error(errors.join('\n')); process.exit(1); }
console.log(`docs check passed: ${sections.length} sections, ${registered.size} registered components`);
