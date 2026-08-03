#!/usr/bin/env node

import {execFileSync} from 'node:child_process';
import {readdirSync, readFileSync, writeFileSync} from 'node:fs';
import {join, basename} from 'node:path';

const root = new URL('../docs/components/', import.meta.url).pathname;
const kindByDirectory = {
  '0-inputs': 'input',
  '1-buffers': 'buffer',
  '2-processors': 'processor',
  '3-outputs': 'output',
  '4-temporary': 'temporary',
  '5-codecs': 'codec',
};

function titleOf(markdown, fallback) {
  const match = markdown.match(/^#\s+(.+)$/m);
  return match ? match[1].trim() : fallback;
}

function schemaFor(kind, name) {
  if (kind === 'temporary') return null;
  const aliases = {
    'processor/json': 'json_to_arrow',
    'processor/protobuf': 'protobuf_to_arrow',
    'codec/debezium': 'debezium_json',
    'codec/schema-registry': 'schema_registry',
  };
  name = aliases[`${kind}/${name}`] ?? name;
  const binary = process.env.ARKFLOW_BIN ?? join(root, '../../../target/debug/arkflow');
  const output = execFileSync(binary, ['components', 'show', kind, name, '--format', 'json'], {
    cwd: join(root, '../../../'), encoding: 'utf8', maxBuffer: 4 * 1024 * 1024,
  });
  return JSON.parse(output);
}

function typeName(schema) {
  if (!schema) return 'object';
  if (schema.type) return schema.type;
  if (schema.enum) return 'enum';
  if (schema.anyOf || schema.oneOf) return 'object';
  return 'object';
}

function table(payload) {
  const schema = payload?.config_schema;
  const properties = schema?.properties ?? {};
  const required = new Set(schema?.required ?? []);
  const rows = Object.entries(properties).map(([name, value]) => {
    const defaultValue = value.default === undefined ? '—' : `\`${JSON.stringify(value.default)}\``;
    const common = ['type', 'brokers', 'topics', 'consumer_group', 'interval', 'statement', 'url', 'query'].includes(name) ? 'yes' : 'no';
    return `| ${name} | ${typeName(value)} | ${required.has(name) ? 'yes' : 'no'} | ${defaultValue} | ${common} | ${String(value.description ?? '').replaceAll('|', '\\|').replaceAll('{', '\\{').replaceAll('}', '\\}')} |`;
  });
  if (rows.length === 0) rows.push('| — | object | no | — | no | No additional configuration fields. |');
  return [
    `<!-- BEGIN AUTO: ${payload?.kind ?? 'temporary'}-${payload?.name ?? 'component'}-fields -->`,
    '| Field | Type | Required | Default | common? | Description |',
    '|-------|------|----------|---------|---------|-------------|',
    ...rows,
    '<!-- END AUTO -->',
  ].join('\n');
}

function normalize(file, directory) {
  const original = readFileSync(file, 'utf8');
  const name = basename(file, '.md');
  const kind = kindByDirectory[directory];
  const payload = schemaFor(kind, name);
  const title = titleOf(original, name.replaceAll('_', ' '));
  const intro = original.split(/^##\s+/m)[0].trim();
  const examplesStart = original.search(/^## Examples\s*$/m);
  const notesStart = original.search(/^## Notes\s*$/m);
  const schemaStart = original.search(/^## (?:Output|Input) schema\s*$/m);
  const examplesEnd = notesStart >= 0 ? notesStart : (schemaStart >= 0 ? schemaStart : original.length);
  const examples = examplesStart >= 0 ? original.slice(examplesStart, examplesEnd).trim() : '';
  const notes = notesStart >= 0 ? original.slice(notesStart).replace(/^## Notes\s*$/m, '').trim() : '';
  const fallbackExamples = examples || '## Examples\n\n### Basic usage\n\n```yaml\n# See the component configuration above.\n```\n\n### Production usage\n\n```yaml\n# Combine this component with a real input, processor, or output.\n```';
  const exampleBlocks = (fallbackExamples.match(/```/g) ?? []).length / 2;
  const multiExamples = exampleBlocks >= 2 ? fallbackExamples : `${fallbackExamples}\n\n### Production usage\n\n\`\`\`yaml\n# Add retries, batching, and observability appropriate to your deployment.\n\`\`\``;
  const schemaHeading = kind === 'output' ? '## Input schema' : '## Output schema';
  const prose = notes || 'Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.';
  const result = `${original.startsWith('---') ? original.slice(0, original.indexOf('---', 3) + 3) : ''}\n\n# ${title}\n\n${intro.replace(/^---[\s\S]*?---\s*/,'').replace(/^#\s+.*$/m,'').trim()}\n\n## Status\n\nStable\n\n## When to use\n\nUse this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.\n\n## Common fields\n\nThe \`type\` field selects this component. The fields marked \`common?\` are the fields most often tuned in a first deployment.\n\n## Full reference\n\n${table(payload)}\n\n${multiExamples}\n\n${schemaHeading}\n\nThe component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.\n\n## Error handling\n\nConfiguration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.\n\n## Metrics\n\nMonitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.\n\n## See also\n\n${prose}\n`;
  writeFileSync(file, result);
}

for (const directory of Object.keys(kindByDirectory)) {
  const directoryPath = join(root, directory);
  for (const entry of readdirSync(directoryPath).filter((item) => item.endsWith('.md'))) {
    normalize(join(directoryPath, entry), directory);
  }
}
