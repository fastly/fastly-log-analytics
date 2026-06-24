# Contributing

Contributions are welcome — bug reports, feature requests, and pull requests alike.

## Development setup

```bash
make install   # uv sync + frontend npm ci
make dev       # backend + frontend with hot reload
```

See the [README "Development" section](README.md#development) for the full command list (typecheck, lint, format, type generation) and [Manual Installation](README.md#manual-installation) for the non-Docker path.

## Running the tests

Run the full gate before opening a PR:

```bash
make ci        # lint + format + typecheck + tests + osv scan
```

…or the individual suites:

```bash
make test            # backend pytest
make test-frontend   # frontend vitest
```

Backend dependencies live in `.venv` via `uv.lock`, so always invoke pytest through uv — `uv run pytest` — rather than a system `pytest`, which drifts off the locked deps. The frontend suite is `cd frontend && npm test`.

## Before you open a PR

- For significant changes, open an issue first to discuss the approach.
- Keep pull requests focused. One feature or fix per PR.
- Make sure the project builds and runs before submitting.

### PR checklist

A few habits the project leans on to keep complexity from accumulating.

- [ ] **Mutable operational config + drift detection in the same PR.** Anything that can drift over time (IP allowlists, public-key pins, third-party API surfaces, version locks) should ship with either a CI check or a scheduled refresh job in the same commit. Don't defer the automation — see [.github/workflows/cidr-refresh.yml](.github/workflows/cidr-refresh.yml) for the pattern.
- [ ] **Placeholder fields have a delete-by date.** If you add a field, stub, or scaffolding "for future use," leave a `# delete by <YYYY-MM-DD>` comment. Unused scaffolding gets deleted on a 7-day clock unless a real consumer lands first.
- [ ] **Compound abstractions justify themselves.** Adding a hook, context, mixin, or wrapper that bundles multiple independent concerns? Confirm every consumer actually needs every part. If any consumer reads fewer than half the properties, split it (or don't compound in the first place).
- [ ] **New endpoints state their budget.** Each new analytics/query endpoint declares its target p95 latency, storage growth, and scale boundary in its docstring or route comment. Catches debt-inducing designs at PR time instead of at incident time. Policy + format in [docs/adr/07-feature-budgets.md](docs/adr/07-feature-budgets.md).

## Rust scorer prerequisites

The session scoring Compute@Edge service (`compute/scorer/`) requires:

- Rust 1.90+ (pinned in `compute/scorer/rust-toolchain.toml`)
- `wasm32-wasip1` target: `rustup target add wasm32-wasip1`
- [viceroy](https://github.com/fastly/Viceroy) (Fastly's local Compute runtime) — optional, only needed for running the scorer locally
- The scorer is rebuilt and deployed via:
  ```
  scripts/scoring/deploy_wasm.sh --service-id <compute-svc> --token <fastly-token>
  ```

## License

This project is licensed under the [Apache License 2.0](LICENSE). By submitting a pull request, you agree that your contribution will be licensed under the same terms.

You retain copyright over your contributions. By contributing, you grant the project maintainers a perpetual, worldwide, non-exclusive, royalty-free license to use, reproduce, modify, and distribute your contributions as part of this project under the Apache License 2.0.
