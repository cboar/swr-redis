# Contributing

## Development

Run these from the repository root with Node.js 24:

```sh
npm run check                    # Format check, typecheck, unit tests, and package test
npm run format                   # Apply Prettier to src, tests, and scripts
npm test                         # Unit tests against FakeRedis
npm run typecheck                # Typecheck, including the public API type tests
npm run build                    # ESM JavaScript and declarations in dist/
npm run test:package             # Pack the tarball, install it in a temp dir, check exports and types
npm run test:integration         # Real Redis at REDIS_HOST and REDIS_PORT
npm run test:integration:docker  # Real Redis in a temporary redis:alpine container
```

Read [DECISIONS.md](DECISIONS.md) before changing the envelope format, the lock
protocol, or the memory cache rules.

## Documentation

The docs are an Astro + Starlight project in `docs/`, with their own npm package
and lockfile. Public pages live in `docs/src/content/docs/`.

From the repository root:

```sh
npm ci --prefix docs
npm run docs:dev
```

Open `http://localhost:4321/swr-redis/`. Changes to Markdown pages refresh
automatically. To build and preview the production site, including search:

```sh
npm run docs:build
npm run docs:preview
```

Output goes to `docs/dist/`. The Docs workflow builds the site on pull requests
and deploys it to GitHub Pages from `main`.

## Edit or add a page

Add a Markdown file under `docs/src/content/docs/` with a title and a unique
description in YAML front matter:

```markdown
---
title: My guide
description: What this guide helps readers do.
---

Start with a short introduction.

## First step

Add explanations and fenced code examples here.
```

Starlight renders `title` as the page heading, so start body headings at `##`. A
file named `my-guide.md` becomes `/swr-redis/my-guide/`, and `index.md` is
the homepage. Add sidebar entries in `docs/astro.config.mjs`.

Link to other pages with the repository prefix:

```markdown
[Configuration](/swr-redis/configuration/)
```

Before submitting, run `npm run docs:build`, check links and page titles, and
verify TypeScript examples against the public API. When a change affects the
README, keep it in step with the getting started page.

## Releases

To release a new version, bump it in the PR and commit `package.json` and
`package-lock.json`.

```sh
npm version patch --no-git-tag-version
```

After merge to `main`, the Release workflow runs CI, publishes to npm, and
creates a tagged GitHub Release. Prereleases publish under `next`.
