# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator. The documentation content workflow (generated component reference, ownership checks, example validation) is described in [DOCUMENTATION.md](./DOCUMENTATION.md).

### Installation

```
$ pnpm install --frozen-lockfile
```

### Local Development

```
$ pnpm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```
$ pnpm build
```

This command generates static content into the `build` directory and can be served by any static contents hosting service.

### Validation

```
$ pnpm docs:check
```

Runs the same documentation checks used by CI (front matter, links, generated inventory coverage, example manifests).
