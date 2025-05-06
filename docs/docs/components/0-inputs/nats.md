# NATS

The NATS input component receives data from a NATS server, supporting both regular NATS and JetStream modes.

## Configuration

### **url**

NATS server URL.

type: `string`

### **mode**

NATS operation mode.

type: `object`

#### Regular Mode

```yaml
mode:
  type: "regular"
  subject: "my.subject"
  queue_group: "my_group" # optional
```

##### **subject**

NATS subject to subscribe to.

type: `string`

##### **queue_group**

NATS queue group (optional).

type: `string`

#### JetStream Mode

```yaml
mode:
  type: "jet_stream"
  stream: "my_stream"
  consumer_name: "my_consumer"
  durable_name: "my_durable" # optional
```

##### **stream**

Stream name.

type: `string`

##### **consumer_name**

Consumer name.

type: `string`

##### **durable_name**

Durable name (optional).

type: `string`

### **auth**

Authentication credentials (optional).

type: `object`

#### **username**

Username for authentication (optional).

type: `string`

#### **password**

Password for authentication (optional).

type: `string`

#### **token**

Token for authentication (optional).

type: `string`

## Examples

### Regular NATS

```yaml
- input:
    type: "nats"
    url: "nats://localhost:4222"
    mode:
      type: "regular"
      subject: "my.subject"
      queue_group: "my_group"
    auth:
      username: "user"
      password: "pass"
```

### NATS JetStream

```yaml
- input:
    type: "nats"
    url: "nats://localhost:4222"
    mode:
      type: "jet_stream"
      stream: "my_stream"
      consumer_name: "my_consumer"
      durable_name: "my_durable"
    auth:
      token: "my_token"
```