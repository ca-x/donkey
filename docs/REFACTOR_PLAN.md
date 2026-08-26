# Donkey 全量重构计划

> 状态：规划阶段。所有阶段确定并获得确认后再开始实现。

## 总目标

在不改变 Docker Registry 公共路径和认证信任域的前提下，提升任务执行正确性、缓存命中率、配置一致性、可测试性和前端可理解性。

## 阶段一：正确性与数据安全基础

目标：先消除会造成重复任务、半配置、缓存损坏或权限越界的问题。

### 1. ImageTools 任务所有权

- 原子领取 pending 任务；
- 增加 worker identity、attempt/fencing token；
- 只回收已过期 lease；
- 运行中周期续租；
- 进度和完成状态必须匹配当前 owner + attempt；
- 覆盖双 worker、重启、超时、取消和陈旧完成写入测试。

### 2. 定时同步 lineage

- image job 记录来源 sync rule 和 scheduled time；
- 创建 job 与推进 next_run_at 使用同一事务；
- 手动任务不更新同步规则；
- 任务成功只更新触发它的规则；
- 增加重复调度和崩溃恢复测试。

### 3. 首个管理员事务

- 首用户判断和插入在 SQLite 写事务中完成；
- 覆盖两个独立数据库连接并发 OIDC/local 首次登录；
- 保证最多一个管理员由“首个用户”规则产生。

### 4. 数据库约束与迁移

- 为 node metric、session、job、sync rule、credential、node limit 增加关系约束；
- 为状态、角色、认证方式、大小和进度增加 CHECK；
- 迁移按版本执行并记录 name/checksum；
- 补充索引和 EXPLAIN QUERY PLAN 验证；
- 每个迁移支持失败回滚和旧数据库 fixture。

### 5. 配置快照原子导入

- 导入先解析、校验和生成变更预览；
- Registry 路由、镜像源节点、运行参数在一个事务中写入；
- 认证密钥不导出；
- 带认证节点导入后默认停用并提示重新填写凭据；
- 任一步失败整体回滚；
- 导入格式带 version 和 schema identifier。

### 6. 缓存安全清理与对账

- 清空操作同时处理 SQLite 索引和磁盘对象；
- 保护进行中的下载和准入；
- 启动时清理 stale partial、缺文件索引和未索引孤儿文件；
- 缓存路径不依赖可迁移的绝对路径；
- 增加 clear 与 download 并发测试。

### 7. 镜像归档正确性

- Docker archive 和 OCI archive 使用不同的构建适配器；
- Docker archive 必须通过 `docker load` 或等价布局校验；
- 提取目录不能误显示为归档下载；
- 增加真实 tar layout 和文件下载回归测试。

## 阶段二：性能与模块深度

目标：在阶段一稳定后，收敛重复逻辑并提高高命中率、大 Blob 和多源场景性能。

### 8. RuntimeConfigStore

- 环境变量作为默认值；
- SQLite 作为持久覆盖；
- 所有长生命周期模块按操作读取一致快照；
- 明确哪些参数立即生效、哪些需要重启；
- 保存后行为测试不能只验证 API 响应。

### 9. NodeSelection 模块

- 集中管理吞吐 EWMA、延迟、成功率、失败冷却和并发许可；
- 节点达到并发上限时等待或选择其他节点，不误报 all nodes failed；
- balanced 保留探索流量，speed-first 优先容量；
- 只使用实际网络表现，不引入地域、IP、ASN 或地理定位；
- 增加确定性调度和冷却退避测试。

### 10. 自适应 Blob 分片

- 默认范围 2–8 MiB；
- 小 Blob 自动完整下载；
- 大 Blob 使用 Range 并发和跨节点续传；
- 根据成功率、吞吐和错误反馈在上下限内调整；
- 失败时回退固定分片或完整下载；
- Dashboard 展示实际分片大小、续传次数和重试流量。

### 11. CacheRepository / ObjectStore

- 将索引、磁盘对象、并发 flight、淘汰和 HTTP 响应策略拆成明确模块；
- 命中统计使用原子增量或内存批量刷新；
- 淘汰只读取有界候选集，不全表搬入内存排序；
- 缓存键默认保持私有认证隔离；
- 公共 Bearer 共享必须是显式、可审计的信任选项。

### 12. UpstreamTransport 适配器

- 统一 URL/SSRF 校验、超时、重定向、Header 过滤、Bearer 流程和可重试状态；
- Registry、DomainFold、ImageTools 复用同一适配器；
- 区分响应头前失败与响应体中断；
- 对可恢复 Range 请求支持跨节点续传；
- 加入断流、429、5xx、重定向和认证测试。

### 13. ImageTools 深化

- Job orchestration；
- JobStore/lease coordinator；
- SourceRegistryAdapter；
- DestinationRegistryAdapter；
- ArtifactStore；
- ArchiveBuilder；
- FileBrowser；
- 每个模块保留窄接口和可替换测试适配器。

### 14. 持久化适配器

- ORM model 只在持久化模块内部使用；
- API 使用稳定 DTO；
- 路由、节点、设置、缓存和任务分别拥有存储适配器；
- 所有跨聚合操作明确事务边界；
- API contract test 防止 schema 泄漏。

## 阶段三：用户体验、可运维性与交付

目标：让默认路径开箱即用，同时让高级用户可以理解、迁移和排查系统。

### 15. 设置向导与默认策略

- 推荐模式默认可直接工作；
- 常用参数和高级参数分层；
- 单位选择支持 KB/MB/GB；
- 清理阈值使用“开始清理阈值/停止清理阈值”；
- 显示当前实际策略摘要；
- 支持恢复推荐设置；
- 明确立即生效和重启生效参数。

### 16. 配置导出/导入向导

- 选择导出运行参数、Registry 路由、镜像源节点和同步规则；
- 导入前显示数量、冲突和缺失凭据；
- 导出不含任何 secret；
- 支持下载、上传、预览、确认、成功/失败反馈；
- 支持 schema version 和回滚提示。

### 17. 账户与认证体验

- 昵称立即更新到导航和会话缓存；
- 本地用户可修改登录名和密码；
- 修改密码要求当前密码并撤销其他会话；
- OIDC 用户只修改显示名称，登录凭据交给 IdP；
- 所有账户文案中英文完整覆盖。

### 18. 前端设计与无障碍

- 按 Emil Design：响应式按压反馈、可中断的短过渡、明确焦点、减少无意义动画；
- 375/768/1024/1440 px 无横向滚动；
- 所有表单字段有稳定 label/helper/error 区域；
- 所有图标按钮有 aria-label；
- i18n key 通过类型或构建检查；
- 后端 enum 不直接作为用户可见文案。

### 19. Worker 生命周期与可观测性

- 健康检查和 ImageTools worker 使用 shutdown token；
- 服务停止时等待任务安全结束；
- Dashboard 展示请求、命中、重试、续传、节点冷却和任务状态；
- 日志不包含凭据，错误包含稳定 machine code；
- 增加健康检查、指标和恢复演练文档。

### 20. 备份、恢复与交付

- SQLite 与 Blob/产物目录分离配置；
- 提供在线备份、恢复校验和 WAL checkpoint 文档；
- 明确 NAS 不适合作为 SQLite WAL 主存储；
- CI 增加 Docker load、迁移、导入回滚和多架构验证；
- 所有阶段完成后再创建版本 tag 和发布镜像/二进制。

## 阶段依赖

```text
阶段一 1–7
  ├─ 阶段二 8–14
  └─ 阶段三 15–20
```

阶段二的 RuntimeConfigStore、NodeSelection、CacheRepository 依赖阶段一的数据约束和事务边界。阶段三的导入向导、状态反馈和发布验证依赖阶段二稳定的模块接口。

## 全局验收标准

- Rust fmt、Clippy、单元测试、集成测试全部通过；
- 前端 lint、TypeScript、生产构建和 375/1440 px 浏览器检查通过；
- 两个独立 worker 不会重复领取同一个任务；
- 配置导入失败不会留下半配置状态；
- 清空缓存不会留下索引/磁盘不一致；
- Docker archive 可被 Docker load；
- 同一公共 Blob 在 token 轮换后仍能安全命中，私有凭据不会跨作用域共享；
- 所有公开 API 不暴露 ORM model；
- 主仓库、镜像、二进制和 LazyCat 发布前都有可追溯版本信息。

## 明确不做

- 不引入地域、IP、ASN 或客户端地理定位；
- 不引入 P2P/BitTorrent；
- 不把 OIDC、管理员密码、Registry secret 放入导出文件；
- 不在本轮将 SQLite 替换为分布式数据库；
- 不改变公共 `/v2` 路径和认证信任域；
- 不为了抽象而抽象，所有模块拆分必须由真实测试 seam 和变化原因驱动。
