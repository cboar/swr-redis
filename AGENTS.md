# AGENTS.md

## Design guidance

- Before changing cache behavior or stored formats, review the established decisions and trade-offs in [DECISIONS.md](DECISIONS.md).

## Runtime validation

- Rely on TypeScript's compile-time guarantees.
- At runtime boundaries, validate only constraints TypeScript cannot verify, such as numeric ranges and configuration limits.
- Avoid repeating checks already handled elsewhere, especially on hot paths.
- Trust internal structures and Redis cache envelopes without defensive shape checks.

## Comments

- Omit banner comments and section dividers such as `// Helpers` or `// Read path`.
- Keep inline comments short and reserve them for non-obvious details such as races, protocol constraints, or surprising decisions.
