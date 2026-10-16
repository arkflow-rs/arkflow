---
sidebar_position: 8
description: Contribute code and documentation to ArkFlow.
---

# Contribute

ArkFlow documentation is maintained alongside the implementation. A page should help a reader complete a task, understand a behavior, or look up a precise reference.

## Page contract

- Add front matter with a stable title/description and an intentional sidebar position when appropriate.
- Identify the audience and prerequisites near the top of task-oriented pages.
- Use complete commands and configuration snippets; state the expected result.
- Link to the relevant concept and reference pages rather than duplicating rules.
- Add a compatibility or deprecation notice when behavior differs by release.
- Update the component inventory when adding or changing a supported component.

Run `pnpm docs:check` and `pnpm build` from `docs/` before opening a pull request. Runtime behavior changes should also update the relevant OpenSpec change and tests.

## Review ownership

- Component pages: component maintainer and documentation reviewer.
- Concepts/configuration: runtime or API maintainer.
- Deployment/control plane: operations or control-plane maintainer.
- Release/version pages: release maintainer.

If ownership is unclear, request review from the maintainers of the changed implementation area and explain the intended reader journey in the pull request.
