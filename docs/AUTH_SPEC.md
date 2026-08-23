# Spec: Donkey Identity

## Objective

为管理控制台提供可部署的本地账户和 OIDC 登录。浏览器使用独立登录页和服务端会话；Registry 与 CONNECT 继续使用各自的 Docker/代理认证。首个账户必须确定且只能有一个管理员，后续身份按角色授权。

## Assumptions

1. 环境变量初始化账户是一次性 bootstrap，不是每次启动的期望状态同步。
2. OIDC 提供方支持标准 Discovery、Authorization Code、PKCE S256 和 ID Token。
3. OIDC `sub` 是身份主键；email/name 只作展示，不用于合并本地用户。
4. v0.1 不提供公开注册，本地账户只由 bootstrap 创建；OIDC 可按成功登录自动建用户。
5. 现有 `DONKEY_ADMIN_AUTH=username:password` 保留为 API Basic 兼容入口一个版本，浏览器默认使用会话。

## Tech Stack

- Rust 1.94、Axum、SeaORM/SQLite。
- Argon2id 密码哈希；OpenID Connect 客户端执行 discovery、PKCE 和 ID Token 校验。
- React 19、Mantine 9、TanStack Query、React Router。

## Commands

- Rust gate: `cargo fmt --all -- --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked --all-targets`
- Frontend gate: `pnpm --dir frontend lint && pnpm --dir frontend build`
- Browser gate: `agent-browser` 验证 `/login`、本地登录、登出、未授权跳转、375px、双主题与 reduced motion。

## Project Structure

- `src/auth.rs`: bootstrap、密码校验、会话、OIDC、授权中间件与 API。
- `src/db.rs`: `users`、`sessions`、`oidc_login_states` SeaORM entities 和迁移。
- `src/config.rs`: bootstrap、会话与 OIDC 环境变量。
- `src/server.rs`: 公共/受保护路由边界与旧 Basic 兼容。
- `frontend/src/pages/LoginPage.tsx`: 独立登录页面。
- `frontend/src/auth.tsx`: 登录态查询、路由保护和登出。

## Code Style

```rust
let session = auth.authenticate_cookie(request.headers()).await?;
auth.require_role(&session, Role::Admin)?;
```

- 外部字符串先验证长度和格式；数据库查询参数化。
- API DTO 不序列化密码哈希、会话哈希、OIDC token、client secret 或 transient verifier。
- 认证失败对客户端使用统一消息；日志只记录用户 id、事件类型和结果，不记录 secret/token。

## Testing Strategy

- 单元：Argon2 验证、session token hash、Cookie flags、输入边界、OIDC 配置全有或全无。
- SQLite 集成：bootstrap 幂等、已有用户不被覆盖、并发首用户只产生一个管理员、过期/撤销会话拒绝。
- Router 集成：未登录 API 401、本地登录设置 Cookie、成员 GET 成功/写操作 403、旧 Basic 兼容、登出撤销。
- OIDC：本地 mock issuer 覆盖 state/nonce/PKCE、错误 state、错误 issuer/audience/nonce 与签名失败；真实提供方不进入自动测试。
- 浏览器：独立 `/login`、错误就地反馈、密码管理器/粘贴、登录后回到原路径、键盘/屏幕阅读器标签。

## Boundaries

- Always: HTTPS 传输 secret；Argon2id；随机会话；OIDC state/nonce/PKCE；角色检查；统一失败消息；限速；过期清理。
- Ask first: 外部 IdP 新 claim 映射、公开注册、管理员 UI 管理用户、跨站 Cookie、refresh token 持久化。
- Never: 明文密码落库；localStorage 保存登录 token；按 email 自动合并 OIDC 身份；记录 Authorization/Cookie/token；把 Registry 登录当管理登录。

## Success Criteria

- 空数据库配 bootstrap 环境变量后恰好创建一个管理员，重启不会改写其密码。
- 空数据库首个 OIDC 用户成为管理员；并发首次回调至多一个管理员。
- 本地登录成功设置服务端会话 Cookie，失败不泄露用户名是否存在；登出后 Cookie 立即无效。
- OIDC 回调校验 PKCE/state/nonce/签名/issuer/audience；一次性 state 不可重放。
- 未登录访问管理页跳转 `/login`；管理 API 返回 401；成员写 API 返回 403。
- 登录页中英文、浅色/暗色、375px、键盘、密码管理器和 reduced motion 均可用。
- Registry Basic 和 CONNECT Proxy-Authorization 行为无回归。

## Open Questions

- v0.1 成员只有只读权限；用户/角色管理 UI 留到后续版本。
