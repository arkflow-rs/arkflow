# Stdout

The Stdout output writes each message payload to standard output. It is handy for debugging, demos, and small local pipelines.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"stdout"` |
| append_newline | boolean | no | `true` | Whether to append a line break after each message. |

## Examples

```yaml
output:
  type: "stdout"
  append_newline: true
```
