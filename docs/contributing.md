---
title: Contributing
description: >-
  Set up taskiq-sqs locally with uv, run lint and tests against the ministack AWS emulator, and
  build the docs with zensical.
---

taskiq-sqs follows the same contribution philosophy as the rest of the taskiq ecosystem — see the
[taskiq contribution guide](https://taskiq-python.github.io/contrib.html) for the general rules (found a bug?
open an issue; not sure about something? open a draft PR and ask in the description; and so on).

The commands below are specific to this repository — it uses [uv](https://docs.astral.sh/uv/) and
[zensical](https://zensical.org/) rather than the tox/VuePress setup described on that page.

## Setting up the environment

```bash
git clone https://github.com/taskiq-python/taskiq-sqs.git
cd taskiq-sqs
make init
```

Tests need a local AWS emulator ([ministack](https://github.com/ministackorg/ministack)):

```bash
make run_infra
```

## Linting and testing

```bash
make lint   # ruff + mypy
make test   # pytest, against the ministack container started above
```

`make help` lists every available target.

## Working with documentation

This site is built with [zensical](https://zensical.org/). To preview changes locally:

```bash
make run_docs
```
