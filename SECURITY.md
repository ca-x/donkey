# Security

## Reporting

请通过 GitHub Security Advisory 私下报告漏洞。报告中不要包含真实 Registry 密码、OIDC token、Cookie 或带签名的 Blob URL。

## Authentication boundaries

- 管理界面使用本地 Argon2id 或 OIDC 登录以及服务端会话。
- Registry 入口使用独立的 Docker Basic 凭据。
- CONNECT 使用独立的 Proxy Basic 凭据和目标 allowlist。
- 上游与目标 Registry secret 使用 `DONKEY_CREDENTIAL_KEY` 加密，API 不回传明文。

## Dependency advisory review

### RUSTSEC-2023-0071 — accepted with constrained reachability

- Dependency: `rsa 0.9.10`, through `openidconnect 4.0.1`.
- Severity: medium; no fixed release was available on 2026-08-24.
- Advisory: Marvin Attack, potential RSA private-key recovery through timing side channels.
- Donkey usage: OIDC Discovery and ID Token signature verification with the provider's public JWK. Donkey does not load an RSA private key and does not call RSA decryption or private signing operations.
- Decision: the vulnerable private-key operation is not reachable in Donkey. Keep the advisory visible rather than applying a blanket audit ignore.
- Review by: 2026-11-24, or immediately when `openidconnect`/`rsa` publishes a fixed compatible release.

Verification command:

```bash
cargo audit --no-fetch
```

The command is expected to report this documented medium advisory until an upstream fix is available. Any additional critical/high advisory blocks release unless its production path is proven unreachable and documented here.
