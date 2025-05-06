# NATS

The NATS input component receives data from a NATS server, supporting both regular NATS and JetStream modes.

## Configuration

#### **url** (required)

NATS server URL in the format `nats://host:port` or `tls://host:port`. For clustering/failover, multiple servers can be specified as a comma-separated list (e.g., `nats://server1:4222,nats://server2:4222`).

type: `string`

#### **mode** (required)

NATS operation mode. Must be specified with a `type` of either `"regular"` or `"jet_stream"`.

type: `object`

##### Regular Mode

```yaml
mode:
  type: "regular"
  subject: "my.subject"
  queue_group: "my_group"
```

###### **subject** (required)

NATS subject to subscribe to.

type: `string`

###### **queue_group**

NATS queue group. If omitted, no queue group will be used.

type: `string`

##### JetStream Mode

```yaml
mode:
  type: "jet_stream"
  stream: "my_stream"
  consumer_name: "my_consumer"
  durable_name: "my_durable"
```

###### **stream** (required)

Stream name.

type: `string`

###### **consumer_name** (required)

Consumer name.

type: `string`

###### **durable_name**

Durable name. If omitted, the system will not use a durable consumer. For most production scenarios, specifying a durable name is recommended to ensure message delivery across restarts.

type: `string`

#### **auth**

Authentication credentials. If omitted, no authentication will be used.

type: `object`

##### **username**

Username for authentication.

type: `string`

##### **password**

Password for authentication. Only used when `username` is specified.

type: `string`

##### **token**

Token for authentication. If both token and username/password are specified, token authentication takes precedence.

type: `string`

## Examples

### Regular Mode Example (Minimal)

```yaml
- input:
    type: "nats"
    url: "nats://localhost:4222"
    mode:
      type: "regular"
      subject: "my.subject"
```

### Regular Mode Example (Complete)

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

### JetStream Mode Example (Minimal)

```yaml
- input:
    type: "nats"
    url: "nats://localhost:4222"
    mode:
      type: "jet_stream"
      stream: "my_stream"
      consumer_name: "my_consumer"
```

### JetStream Mode Example (Complete)

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