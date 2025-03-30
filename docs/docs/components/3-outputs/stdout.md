# Stdout

The Stdout output component writes messages to the standard output stream.

## Configuration

### **append_newline**

Whether to add a line break after each message (optional).

type: `bool`

default: `true`

## Examples

```yaml
- output:
    type: "stdout"
    append_newline: true
```