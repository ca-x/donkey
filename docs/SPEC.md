# Donkey Capability Map

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| `proxy-core` | OCI Registry 转发、CONNECT 隧道、Range 分片调度、故障转移与完整性校验 | — |
| `control-plane` | SeaORM/SQLite 节点、测速、缓存索引与 DomainFold 映射 | `proxy-core` |
| `identity` | 本地管理员、OIDC、角色、服务端会话与管理 API 授权 | `control-plane` |
| `web-console` | 嵌入二进制的 React 管理台，覆盖桌面和移动端 | `control-plane`, `identity` |
| `image-tools` | 镜像内容浏览、离线导出、Registry 复制、定时同步与任务恢复 | `control-plane`, `identity` |
| `delivery` | 多架构二进制、Docker 镜像、Compose 与 GitHub Actions | `web-console`, `image-tools` |

Build order: `proxy-core` → `control-plane` → `identity` → `image-tools`, `web-console` → `delivery`

# Spec: Donkey

## Objective

Donkey 是一个可自托管、可审计的 Docker/OCI 镜像与通用下载加速器。它从 KSpeeder 的公开功能推导产品能力，但不复制其闭源实现或视觉设计。

主要用户是网络条件不稳定、需要在 NAS、家庭实验室或 CI 中加速镜像拉取的运维人员。成功意味着：Docker Registry 请求可以在多个兼容上游之间转发和故障切换；大 Blob 在安全条件下可以并发 Range 下载并缓存；节点质量和缓存可以在响应式 Web 管理台中查看和管理。

## Functional Requirements

### Proxy core

- 提供 Docker Registry V2 兼容入口，保留认证挑战、Bearer token、manifest/blob 内容类型、ETag、Range 与摘要语义。
- 对大 Blob 先探测 `Content-Length`、`Accept-Ranges` 与编码。只有所有条件满足时才按可配置分片并发下载。
- 分片由健康节点动态领取；失败分片立即换源，节点分数由延迟、近期吞吐、成功率和优先级组成。
- 调度策略支持 `balanced` 与 `speed-first`。速度优先使用真实分片吞吐 EWMA，并按活跃分片数修正可用容量。
- 调度算法默认使用 `current-balanced`；可在运行时设置中切换到 `projected-completion`。前者适合节点稳定且希望平滑分布流量的环境，后者综合吞吐、延迟、成功率、在途负载和并发上限，适合节点差异或网络波动明显的环境。
- 合并前检查分片范围和总长度；路径含 `sha256:<digest>` 时必须校验 SHA-256。
- 不支持 Range、返回压缩内容、未知长度或兼容性不足时回退到最佳单源获取。
- 缓存采用内容寻址文件，命中时支持 GET/HEAD 和客户端 Range。并发相同 miss 只执行一次下载。
- 管理端口支持带 Basic Auth 的 CONNECT；目标必须命中配置的域名规则，支持把 `host:443` 重映射到本地 Registry TLS 端口。
- Registry 路由把客户端命名空间映射到规范上游 Registry：默认路由占用根命名空间，其他路由使用唯一的路径前缀。Docker Hub 内置路由使用根路径，GHCR 内置路由使用 `/ghcr`。
- Docker Hub 示例为 `registry.example.com/library/alpine:latest`；GHCR 示例为 `registry.example.com/ghcr/owner/image:tag`。Docker daemon 的 `registry-mirrors` 通常只影响 Docker Hub，不自动接管 GHCR 等其他 Registry。

### Control plane

- SeaORM 管理 SQLite；默认数据库为 `${DONKEY_DATA_DIR}/donkey.db`。
- 持久化 Registry 路由、节点、节点测量、缓存索引和域名下载映射。每个节点必须绑定一个 Registry 路由；同一路由可有多个镜像端点参与调度。
- Registry 路由包含唯一键、规范 Registry、可选访问路径、仓库路径模式、默认状态和启用状态。内置 Docker Hub/GHCR 路由不可删除；被节点引用的自定义路由不可删除。
- v0.2.0 要求干净的 SQLite 数据库，不支持从 v0.1 数据库原地升级。升级前必须备份 `/data`，再用空数据库启动并重新配置用户、Registry 路由和节点。
- 后台健康检查可配置间隔；失败不会阻塞请求路径。
- API 提供 dashboard、Registry 路由 CRUD、节点 CRUD/即时测速、缓存列表/清理、域名下载映射 CRUD/链接转换和运行时配置读取。
- 所有输入有长度、枚举、URL、路径和数值边界；错误响应不暴露堆栈。

### Web console

- React 19 + TypeScript + Vite，使用 Mantine、TanStack Query、React Router、Recharts 与 Tabler Icons。
- 页面：概览、节点、缓存、域名加速、设置/部署说明。Registry 路由管理保留在节点页弹层中，不增加导航项。
- 节点页按 Registry 路由筛选节点，并分别显示逻辑 Registry 与镜像端点；节点表单必须选择 Registry 命名空间。
- 视觉语言为深色、高对比、紧凑的运维控制台；信息层级优先，不复刻参考截图。
- 375/768/1024/1440 px 均无横向滚动。桌面侧栏在窄屏变为底部导航；表格在移动端变为信息卡。
- 触控目标至少 44×44 px；可键盘导航；图标按钮有可访问名称；焦点可见；颜色不作为唯一状态编码。
- 高频导航不动画；按钮按压 120–160ms，抽屉/弹层 180–240ms ease-out；只动画 transform/opacity，并支持 `prefers-reduced-motion`。
- 前端 `dist` 在 Rust 编译期嵌入，生产不依赖外部静态文件。

### Identity

- 管理界面使用独立 `/login` 页面，不依赖浏览器原生 Basic 登录弹窗。
- `DONKEY_INITIAL_ADMIN_USERNAME` 与 `DONKEY_INITIAL_ADMIN_PASSWORD` 只在用户表为空时初始化本地管理员；已有用户时不得覆盖密码或角色。
- 本地密码使用 Argon2id 带随机盐哈希，明文只存在于启动环境和单次登录请求中，不写入数据库、响应或日志。
- OIDC 使用 Authorization Code、PKCE、`state` 与 `nonce`；服务端完成发现、令牌交换、签名/issuer/audience/nonce 校验，浏览器不接收上游 refresh token。
- 用户表为空时，第一个成功落库的本地或 OIDC 用户成为管理员。该判断和插入必须在 SQLite 事务中串行化，避免并发回调创建两个首管理员。
- 后续 OIDC 用户默认为成员。成员可读取控制台状态；节点、缓存、映射、凭据、镜像复制和同步规则等写操作只允许管理员。
- 会话使用至少 256 bit 随机 bearer token；SQLite 仅保存 SHA-256 token hash。Cookie 为 `HttpOnly`、`SameSite=Lax`、`Path=/`，在内置/外置 HTTPS 下设置 `Secure`，默认 7 天过期。
- 登录失败返回统一错误并限速；登出立即撤销服务端会话。禁用或删除用户时其会话不得继续授权。
- `DONKEY_ADMIN_AUTH` 在 v0.1 保留为受 TLS 约束的 API Basic 兼容入口并标记弃用；它不创建浏览器会话，也不参与 OIDC 身份合并。
- Registry `docker login` 和 CONNECT 认证属于独立信任域，不复用管理会话或 OIDC token。

### Image tools

- 镜像工具是独立页面和后台模块，不改变公共 `/v2` 拉取代理的只读边界。
- 内容浏览按 OS / architecture / variant 解析平台 manifest，安全合并 layer whiteout，展示文件树并允许下载单个普通文件。
- 离线导出支持 Docker archive 和 OCI archive；产物写入 `${DONKEY_DATA_DIR}/image-tools/jobs`，有总容量、TTL 和清理路径。
- Registry 复制支持任何 OCI Distribution 兼容目标。复制前检查目标 Blob，只上传缺失内容，最后写 manifest。
- 源与目标 Registry 凭据独立管理，支持 anonymous、Basic 和 Bearer。secret 使用 AES-256-GCM 加密落库，API 永不回传。
- 同 tag 每次执行前解析 index digest 和平台 manifest digest；digest 未变化时跳过复制或复用导出产物。
- 同步规则支持 Cron、时区、暂停、手动运行和 API 触发。API 接受 `Idempotency-Key`，同目标引用和源 digest 不重复执行。
- 任务状态为 pending / running / completed / failed / cancelled / skipped。服务重启后恢复未完成任务，运行租约过期后可重试。
- 管理端新增 Registry 凭据时必须是管理员会话，配置 `DONKEY_CREDENTIAL_KEY`，并通过 loopback 或 HTTPS 访问（`DONKEY_ADMIN_EXTERNAL_TLS=true` 表示反向代理已终止 TLS）。

### Delivery

- Docker 多阶段构建前端和 Rust，运行镜像使用非 root 用户并包含 CA 证书。
- Compose 暴露管理/CONNECT 端口 5003 和 Registry 端口 5443，持久化 `/data`。
- CI 在 PR/分支执行前端 lint/build、Rust fmt/clippy/test/build。
- Docker Buildx 独立构建 linux/amd64 与 linux/arm64；main 和版本标签推送 GHCR，PR 只构建验证。
- 版本标签生成 Linux amd64/arm64 和 Windows amd64 二进制压缩包及 SHA256SUMS。

## Threat Model

- Trust boundaries: 登录表单、OIDC 浏览器重定向与提供方响应、会话 Cookie、管理 API 输入、Registry 客户端请求、上游 Registry 响应、CONNECT 目标、节点 URL、缓存文件系统。
- Assets: 本地密码哈希、OIDC client secret、会话 token、上游凭据/Bearer token、缓存完整性、宿主网络、磁盘容量、管理配置。
- Controls: Argon2id、OIDC PKCE/state/nonce、服务端哈希会话、角色检查、登录限速；默认拒绝私网节点和不安全 HTTP；URL DNS 解析后拒绝特殊地址；CONNECT 仅允许规则目标；上游请求超时和大小上限；敏感请求头不写日志；SeaORM 参数化查询；CSP 等安全头；缓存路径由哈希生成。
- Abuse cases: 凭据填充、会话固定/窃取、OIDC 回调 CSRF/nonce 重放、并发首用户提权、成员调用写 API、节点 URL 指向云元数据或 LAN、CONNECT 作为开放代理、恶意 Range 造成资源耗尽、伪造 digest 污染缓存、超长镜像路径/表单输入耗尽内存。

## Tech Stack

- Rust 1.94+; Tokio, Axum, Reqwest/rustls, SeaORM/sqlx-sqlite, RustEmbed, tower-http, tracing.
- React 19, TypeScript 6, Vite 8, Mantine 9, TanStack Query 5, React Router 7, Recharts 3.
- SQLite + filesystem content cache.

## Commands

- Development: `pnpm --dir frontend dev` and `cargo run`
- Frontend build: `pnpm --dir frontend install --frozen-lockfile && pnpm --dir frontend build`
- Rust build: `cargo build --locked`
- Test: `cargo test --locked --all-targets`
- Lint: `cargo fmt --all -- --check && cargo clippy --locked --all-targets -- -D warnings && pnpm --dir frontend lint`
- Container: `docker build -t donkey:dev .`

## Project Structure

- `src/`: Rust application, proxy, scheduler, persistence, API and static embedding.
- `frontend/src/`: React console grouped by pages/components/API.
- `src/image_tools/`: Registry 凭据、镜像解析、文件树、归档、复制、规则和 worker。
- `frontend/dist/`: generated and embedded production assets.
- `tests/`: Rust integration tests and fixture upstreams.
- `docs/`: specification and implementation plan.
- `.github/workflows/`: validation, binary release and container workflows.

## Code Style

```rust
pub async fn get_node(State(state): State<AppState>, Path(id): Path<Uuid>) -> ApiResult<Json<NodeDto>> {
    let node = state.nodes.find(id).await?.ok_or(ApiError::not_found("node"))?;
    Ok(Json(node.into()))
}
```

- Rust modules expose narrow domain interfaces; API DTOs never expose ORM active models.
- Errors use typed variants and stable machine codes.
- React uses controlled forms, stable entity IDs as keys, semantic tokens, and no `dangerouslySetInnerHTML`.

## Testing Strategy

- Unit tests cover scoring, URL validation, Range parsing, cache keys, digest verification and DomainFold conversion.
- Integration tests start local mock registries for passthrough, failover, Range merge and cache-hit paths.
- Image-tool integration tests cover archive layout, whiteout/path traversal, Blob reuse, digest change, copy idempotency and task recovery.
- API smoke tests cover health, node CRUD and embedded SPA fallback.
- Frontend type-check/build is the minimum UI gate; runtime browser checks cover 375 and 1440 px.

## Boundaries

- Always: validate network targets and request sizes; use atomic cache rename; verify digest; run formatting, tests and builds.
- Ask first: make the proxy open to arbitrary CONNECT targets; enable private upstreams by default; add credential storage; change cache deletion semantics.
- Never: log Authorization/Proxy-Authorization; concatenate SQL; use client paths as filesystem paths; silently accept corrupt content; require a CDN-hosted frontend asset at runtime.

## Success Criteria

- A local mock Registry manifest and Blob can be fetched through Donkey, a repeated Blob request hits disk cache, and a multi-node Range test reconstructs the exact digest.
- A local test Registry image can be exported, browsed, copied into a second Registry, skipped when digest is unchanged, and recopied after the mutable tag changes.
- Unallowed CONNECT and private upstream inputs are rejected.
- Node/config/cache APIs persist across restart.
- `frontend/dist` is served from the compiled binary, including SPA deep links.
- UI builds and remains usable at 375/768/1024/1440 px with keyboard focus and reduced motion.
- CI workflow files validate branch work and publish tagged binaries/images without embedded secrets.
