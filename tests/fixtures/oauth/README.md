# OAuth test fixtures

Committed RSA test keypair + static JWKS for the analyst OAuth suite.
`private_key.json` signs fixture id_tokens; `jwks.json` (public only) is what
the mock IdP transport serves so joserfc verifies against the real key set.
These are TEST-ONLY keys — never used by any real provider.
