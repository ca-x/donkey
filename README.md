<p align="center">
  <img src="frontend/public/donkey-logo.webp" width="180" alt="Donkey logo" />
</p>

# Donkey

Donkey 是一个 Rust 编写的 OCI / Docker Registry 拉取代理。它支持多上游分片下载、Bearer 认证交换、内容缓存、CONNECT 隧道和 Web 管理界面。前端编译后嵌入二进制，运行时不需要 Node.js。

> 本项目参考 KSpeeder 的公开功能说明重新设计，与 KSpeeder、LinkEase 和 Docker Inc. 无隶属关系，也不包含其闭源代码。

![暗色概览](docs/images/dashboard-dark.png)

<table>
  <tr>
    <td><img src="docs/images/nodes-light-zh.png" alt="浅色节点页面" /></td>
    <td width="300"><img src="docs/images/mobile-dark-zh.png" alt="移动端概览" /></td>
  </tr>
</table>

<table>
  <tr>
    <td><img src="docs/images/login-light-en.png" alt="独立登录页面" /></td>
    <td><img src="docs/images/image-tools-dark-zh.png" alt="暗色镜像工具页面" /></td>
  </tr>
</table>

<table>
  <tr>
    <td><img src="docs/images/mobile-login-light-en.png" alt="移动端登录页面" /></td>
    <td><img src="docs/images/mobile-image-tools-dark-zh.png" alt="移动端镜像工具" /></td>
    <td><img src="docs/images/mobile-nodes-dark-zh.png" alt="移动端加速节点" /></td>
  </tr>
</table>

## 功能

- Docker Registry V2 拉取代理，保留 manifest、Blob、ETag、Range 和 OCI digest 语义。
- 多源 Range 分片下载。节点失败后，未完成分片会转移到其他节点。
- `balanced` 与 `speed-first` 两种节点策略。速度优先按真实分片吞吐和当前连接数分配任务。
- 对不支持 Range、长度未知或编码不兼容的响应自动改用单源下载。
- SHA-256 完整性检查、同一 Blob 单次回源、文件流落盘和原子缓存写入。
- `balanced`、`lru`、`lfu` 三种缓存回收策略，以及高低水位和 TTL 参数。
- 自动处理上游 Bearer challenge。节点支持 Basic、Bearer 和自定义 Header 认证。
- 节点凭据使用 AES-256-GCM 加密后存入 SQLite。
- 可选 Registry Basic 认证，兼容 `docker login`。CONNECT 使用独立 Basic 认证和目标白名单。
- 独立登录页、本地 Argon2id 管理员、服务端会话和 OIDC Authorization Code + PKCE。
- 镜像内容浏览、单文件下载、Docker/OCI 离线包、跨 Registry 复制和 Cron 同步。
- React + Mantine 管理界面，中英文、浅色 / 暗色主题和移动端布局。
- Linux amd64 / arm64、Windows amd64、macOS arm64 二进制和多架构容器构建。

## Docker Compose

> **v0.2.0 数据要求：** 必须使用干净的 SQLite 数据库，不能原地复用 v0.1 的数据库。升级前先停止 Donkey 并备份 `/data`，然后使用新的空数据卷（或清空数据库文件）启动，再在管理界面重新配置用户、Registry 命名空间和节点。缓存文件可另行保留，但不要把旧 `donkey.db` 带入 v0.2.0。

```bash
git clone https://github.com/ca-x/donkey.git
cd donkey
cp .env.example .env
```

至少修改以下值：

```dotenv
DONKEY_INITIAL_ADMIN_USERNAME=admin
DONKEY_INITIAL_ADMIN_PASSWORD=replace-with-at-least-12-characters
DONKEY_CREDENTIAL_KEY=<64-character-hex-key>
```

Registry 客户端认证和 CONNECT 代理默认均未启用。只有明确需要时，才分别设置 `DONKEY_REGISTRY_AUTH` 或 `DONKEY_PROXY_AUTH`；未配置的 Registry 保持匿名访问，未配置认证的 CONNECT 则保持禁用，避免意外开放代理。

| 入口 | 未配置认证 | 明确配置认证后 |
| --- | --- | --- |
| 管理界面 | 非 loopback 监听必须配置本地管理员、OIDC 或旧 API Basic 之一 | 浏览器使用 `/login` 的本地会话或 OIDC；旧 API Basic 仅接受客户端主动凭据，不发送浏览器 challenge |
| Registry | 匿名访问，不返回 Basic challenge | 使用 `DONKEY_REGISTRY_AUTH`，按 Docker Registry 规范返回 Basic challenge |
| CONNECT | 默认禁用，返回 403 且不返回代理 challenge | 使用独立的 `DONKEY_PROXY_AUTH`，无效凭据返回 407 |

生成凭据加密主密钥：

```bash
openssl rand -hex 32
```

启动：

```bash
docker compose up -d
```

GHCR 首次创建的 package 默认可能是 private。组织所有者可以在 GitHub Package settings 将 `ca-x/donkey` 改为 Public；修改前先执行 `docker login ghcr.io`，或保留 Compose 的本地 `build: .` 从源码构建。

管理界面默认位于 `http://127.0.0.1:5003/login`，Registry 默认监听 `5443`。初始化变量只在用户表为空时创建管理员；重启不会覆盖已有密码或角色。Compose 用 `DONKEY_ADMIN_EXTERNAL_LOOPBACK=true` 声明容器端口只映射到宿主 loopback；如果修改端口映射使管理端离开本机，必须删除该声明、通过 HTTPS 反向代理提供，并设置 `DONKEY_ADMIN_EXTERNAL_TLS=true`。

### 管理登录与 OIDC

本地密码使用 Argon2id 哈希，浏览器只保存 `HttpOnly` 会话 Cookie。`DONKEY_ADMIN_AUTH=user:password` 仅作为旧 API Basic 兼容入口保留，不再触发浏览器登录弹窗。

接入 OIDC 时同时设置：

```dotenv
DONKEY_OIDC_ISSUER=https://id.example.com/realms/donkey
DONKEY_OIDC_CLIENT_ID=donkey
DONKEY_OIDC_CLIENT_SECRET=replace-me
DONKEY_OIDC_REDIRECT_URL=https://donkey.example.com/api/auth/oidc/callback
DONKEY_OIDC_DISPLAY_NAME=Company SSO
DONKEY_ADMIN_EXTERNAL_TLS=true
```

OIDC 使用 Discovery、Authorization Code、PKCE、`state` 和 `nonce`。用户表为空时，第一个成功创建的本地或 OIDC 用户成为管理员；后续 OIDC 用户默认只有读取权限。生产部署最好先用本地初始化管理员完成所有权确认，再开放 OIDC 登录入口。

管理前端支持反向代理子路径。代理把外部前缀（例如 `/console`）剥离后转发到管理端口即可；前端会从脚本地址自动推导前缀，并同步用于静态资源、API、OIDC、文件下载和客户端路由。根路径部署仍保持原有 URL。

### TLS 与 `docker login`

Docker 客户端通常要求镜像源使用 HTTPS。把证书放到 `./certs`，然后设置：

```dotenv
DONKEY_TLS_CERT=/certs/fullchain.pem
DONKEY_TLS_KEY=/certs/privkey.pem
DONKEY_REGISTRY_AUTH=registry-user:registry-password
```

如果 TLS 在 Caddy、Traefik 或 Nginx 终止，设置 `DONKEY_REGISTRY_EXTERNAL_TLS=true`。不要在明文 HTTP 上启用 Registry Basic 认证。

```bash
docker login registry.example.com
```

Donkey 验证客户端凭据后会移除该 Authorization，再按节点配置向上游换取 Bearer token。Donkey 的登录密码不会发送给上游。

## 配置 Registry 命名空间与节点

「加速节点」页面把逻辑 Registry 与镜像端点分开管理。干净数据库会创建两个内置命名空间：Docker Hub 是根命名空间，GHCR 使用 `ghcr` 路径前缀。先选择「管理命名空间」，再为对应命名空间添加一个或多个镜像端点，例如：

```text
https://docker.1ms.run
https://mirror.czyt.eu.org
https://registry-1.docker.io
```

1ms 付费节点使用标准 Docker Basic 认证：认证方式选择 `Docker / HTTP Basic`，用户名填写 `1ms`，密钥填写 1ms 生成的 Docker secret。需要先设置 `DONKEY_CREDENTIAL_KEY`。

默认只接受解析到公开 IP 的 HTTPS 上游。内网 Registry 需要显式设置 `DONKEY_ALLOW_PRIVATE_UPSTREAMS=true`；HTTP 上游还需设置 `DONKEY_ALLOW_INSECURE_UPSTREAMS=true`。

客户端拉取示例（将域名替换为你的 Donkey Registry）：

```bash
# Docker Hub 默认命名空间
docker pull registry.example.com/library/alpine:latest

# GHCR 内置命名空间；对应上游 ghcr.io/owner/image:tag
docker pull registry.example.com/ghcr/owner/image:tag
```

自定义 Registry 也使用独立且唯一的路径前缀。内置命名空间可以编辑、启用或停用，但不能删除；仍被节点引用的自定义命名空间也不能删除。

## 镜像工具

「镜像工具」是单独页面，公共 `/v2` 拉取代理仍然只读。辅助 worker 可以：

- 按平台解析镜像并浏览合并后的 rootfs，下载单个普通文件。
- 生成 Docker archive 或 OCI archive 离线包。
- 把镜像复制到 Aliyun ACR、CNB、GHCR、Harbor 等 OCI Registry。
- 用 Cron 和 IANA 时区定期检查 mutable tag；源 digest 未变化时跳过重复复制。
- 接受 `Idempotency-Key` 的 API 任务，支持取消、重试和重启恢复。

源 Registry、可选加速节点和目标 Registry 使用三套独立身份。源站不能直连时，在「拉取方式」选择 `docker.1ms.run`、`mirror.czyt.eu.org` 或自建节点；该节点的 Basic、Bearer 或自定义 Header 只由服务端使用，不会发送到目标 Registry，也不会在 API 回显。目标凭据必须与目标 Registry 主机完全匹配。

登录后可用同一会话调用任务 API：

```bash
read -rsp 'Donkey password: ' DONKEY_PASSWORD; printf '\n'
jq -n --arg username admin --arg password "$DONKEY_PASSWORD" \
  '{username: $username, password: $password}' \
  | curl -fsS -c cookie.txt -H 'Content-Type: application/json' \
      --data-binary @- http://127.0.0.1:5003/api/auth/login
unset DONKEY_PASSWORD

curl -b cookie.txt -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: alpine-export-1' \
  -d '{"kind":"export","source_ref":"docker.io/library/alpine:latest","source_node_id":null,"source_credential_id":null,"destination_ref":null,"destination_credential_id":null,"platform_os":"linux","platform_arch":"amd64","output_format":"oci"}' \
  http://127.0.0.1:5003/api/image-tools/jobs
```

镜像工具数据使用 `DONKEY_MAX_EXPORT_BYTES` 总配额和 `DONKEY_EXPORT_TTL` 保留期。共享 layer 按 digest 去重；过期任务目录和不再引用的共享 Blob 会回收。

## 配置 Docker

### 一键切换

Donkey 会按当前访问域名动态生成脚本：

```bash
curl -fsSL https://donkey.example.com/helper | sudo sh -s -- configure
```

```powershell
irm https://donkey.example.com/helper.win | iex
```

管理界面的「设置与部署」会显示替换成实际域名后的 Linux、macOS、Windows 和临时拉取命令。动态脚本不包含用户名、密码或 Token。

一键脚本只配置 Docker Hub 的 `registry-mirrors`，不会把 `ghcr.io`、Quay 等任意 Registry 地址自动改写，也不会安装 Docker 或保存 Registry 凭据。Linux 需要 root，macOS / Windows 需要 Docker Desktop。非 Docker Hub 镜像请使用 Donkey 命名空间：

```bash
docker pull donkey.example.com/ghcr/owner/image:tag
```

也可以直接使用 GitHub 上的通用脚本并显式传入地址：

Linux：

```bash
curl -fsSL https://raw.githubusercontent.com/ca-x/donkey/main/scripts/helper.sh \
  | sudo sh -s -- configure --url https://registry.example.com --username registry-user
```

macOS：

```bash
curl -fsSL https://raw.githubusercontent.com/ca-x/donkey/main/scripts/helper.sh \
  | sh -s -- configure --url https://registry.example.com --username registry-user
```

Windows PowerShell：

```powershell
$env:DONKEY_URL = 'https://registry.example.com'
$env:DONKEY_USERNAME = 'registry-user'
irm https://raw.githubusercontent.com/ca-x/donkey/main/scripts/helper.ps1 | iex
```

Helper 会先备份已有配置，只合并镜像源字段。可以先加 `--dry-run` 查看改动。Linux / macOS 使用以下命令恢复备份：

```bash
sh scripts/helper.sh restore --backup /path/to/daemon.json.donkey.TIMESTAMP.bak
```

直接执行网络脚本有供应链风险。更稳妥的方式是先下载脚本，核对仓库内容后再运行。

### 临时使用

不修改 Docker daemon 时，可以直接把 Donkey 当 Registry 使用：

```bash
docker login registry.example.com
docker pull registry.example.com/library/alpine:latest
```

Helper 也可以只输出临时命令：

```bash
curl -fsSL https://raw.githubusercontent.com/ca-x/donkey/main/scripts/helper.sh \
  | sh -s -- temporary --url https://registry.example.com
```

### 手工设置镜像源

Linux 的 `/etc/docker/daemon.json`：

```json
{
  "registry-mirrors": ["https://registry.example.com"]
}
```

Docker daemon 的 `registry-mirrors` 通常只接管 Docker Hub 拉取，不会自动代理 `ghcr.io` 等任意 Registry。Docker Hub 可以继续使用原镜像名（例如 `docker pull alpine`）；GHCR 请显式使用 Donkey 的命名空间地址，例如 `docker pull registry.example.com/ghcr/owner/image:tag`。

然后重启 Docker：

```bash
sudo systemctl restart docker
```

## CONNECT 模式

当宿主机不能直接映射 443 时，可以让 Docker daemon 连接管理端口的 CONNECT 代理，再把指定域名重映射到 Registry 端口：

```dotenv
DONKEY_PROXY_AUTH=proxy-user:proxy-password
DONKEY_CONNECT_REMAP=registry.example.com:443=127.0.0.1:5443
```

Docker daemon 代理：

```text
http://proxy-user:proxy-password@donkey-host:5003
```

非重映射 CONNECT 请求必须匹配 `DONKEY_CONNECT_ALLOW`。未配置认证时 CONNECT 默认禁用。

## 缓存参数

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DONKEY_MAX_CACHE_BYTES` | `53687091200` | 最大缓存 50 GiB |
| `DONKEY_CACHE_POLICY` | `balanced` | `balanced`、`lru` 或 `lfu` |
| `DONKEY_CACHE_HIGH_WATERMARK` | `0.90` | 达到该比例时开始回收 |
| `DONKEY_CACHE_LOW_WATERMARK` | `0.80` | 回收到该比例后停止 |
| `DONKEY_CACHE_TTL` | 空 | 可选，例如 `30d`、`12h` |
| `DONKEY_PUBLIC_BEARER_CACHE_ENABLED` | `false` | 显式允许未配置上游认证的公共命名空间跨 Bearer 令牌轮换复用 Blob；认证节点和不同命名空间始终隔离 |
| `DONKEY_CHUNK_SIZE` | `2097152` | 上游 Range 分片大小 |
| `DONKEY_ADAPTIVE_CHUNKING_ENABLED` | `true` | 根据 Blob 大小和节点容量在 2–8 MiB 内自动选择分片 |
| `DONKEY_CHUNK_CONCURRENCY` | `32` | 关闭自动并发计算后使用的单个 Blob 手动上限 |
| `DONKEY_AUTOMATIC_CONCURRENCY_ENABLED` | `true` | 自动汇总所有启用节点的并发容量，最高 64 |
| `DONKEY_PARALLEL_THRESHOLD` | `8388608` | 大于该值才启用分片 |
| `DONKEY_RESUMABLE_THRESHOLD` | `8388608` | 大于该值且上游支持 Range 时启用断点续传 |
| `DONKEY_SCHEDULER_POLICY` | `balanced` | `balanced` 或 `speed-first` |
| `DONKEY_SCHEDULER_ALGORITHM` | `current-balanced` | `current-balanced` 适合稳定节点；`projected-completion` 综合吞吐、延迟、成功率和在途负载，适合速度差异或波动明显的节点 |
| `DONKEY_UPSTREAM_TIMEOUT` | `30s` | 单个上游请求超时 |
| `DONKEY_STREAM_FALLBACK_TIMEOUT` | `10s` | 完整缓存下载超过该时间后切换流式响应 |
| `DONKEY_PARTIAL_TTL` | `1h` | 断点临时文件最长保留时间 |
| `DONKEY_MAX_EXPORT_BYTES` | `21474836480` | 镜像工具产物和共享 Blob 总配额 20 GiB |
| `DONKEY_EXPORT_TTL` | `7d` | 已完成镜像任务产物保留时间 |
| `DONKEY_PULL_LOGGING_ENABLED` | `true` | 是否记录并显示镜像拉取历史 |
| `DONKEY_PULL_LOG_RETENTION_DAYS` | `30` | 拉取历史保留天数 |
| `DONKEY_PULL_LOG_MAX_ENTRIES` | `10000` | 拉取历史最大条数 |

`speed-first` 使用实际 Blob 分片吞吐的指数移动平均值。选择节点时会除以该节点当前活跃分片数，因此快节点会接收更多任务，但不会独占所有分片。未知节点保留最低探索权重，仍会获得少量请求用于建立速度样本。

匿名公共 Blob 按 digest 去重。带上游 Authorization 的请求会加入不可逆授权作用域，避免私有内容跨凭据命中。

缓存、调度和续传参数也可以在管理界面的“运行时参数”中修改，保存到 SQLite，并在服务重启后恢复；环境变量仅作为首次启动默认值。

完整数据目录备份、SQLite 在线备份、恢复校验和 NAS 注意事项见 [备份与恢复](docs/BACKUP_AND_RECOVERY.md)。

## 本地开发

```bash
pnpm --dir frontend install --frozen-lockfile --ignore-scripts
pnpm --dir frontend build
pnpm --dir frontend dev
cargo run
```

生产构建必须先生成前端资源：

```bash
pnpm --dir frontend build
cargo build --release --locked
```

验证：

```bash
pnpm --dir frontend lint
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

## English summary

Donkey is a Rust OCI Registry pull-through proxy with multi-source Range downloads, digest verification, filesystem caching, encrypted Registry credentials, local/OIDC administration sessions, image extraction/export/copy tools, CONNECT tunneling, and an embedded React console. v0.2.0 requires a clean SQLite database. Docker Hub uses the root namespace (`registry.example.com/library/alpine`), while GHCR uses the built-in `/ghcr` namespace (`registry.example.com/ghcr/owner/image`); Docker daemon `registry-mirrors` normally applies only to Docker Hub. See `.env.example` for deployment settings.

## License

[MIT](LICENSE)

安全问题和已知依赖 advisory 处理见 [SECURITY.md](SECURITY.md)。
