# Contributing

Contributions are welcome — bug reports, feature requests, and pull requests alike.

## Before you open a PR

- For significant changes, open an issue first to discuss the approach.
- Keep pull requests focused. One feature or fix per PR.
- Make sure the project builds and runs before submitting.

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
