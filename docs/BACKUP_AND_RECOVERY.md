# Donkey 备份与恢复

Donkey 的持久数据默认位于 `DONKEY_DATA_DIR`（容器内为 `/data`）：

- `donkey.db`、`donkey.db-wal`、`donkey.db-shm`：控制面 SQLite；
- `cache/`：可重新拉取的 Registry Blob 缓存；
- `image-tools/`：镜像提取目录、离线归档和共享 Blob；
- Registry 凭据的密文保存在 SQLite，解密所需的 `DONKEY_CREDENTIAL_KEY` 不在数据目录中。

## 推荐：停机一致性备份

停机备份可同时固定数据库和磁盘产物，是完整恢复的首选方式。

```bash
docker compose stop donkey
docker run --rm \
  -v donkey-data:/source:ro \
  -v "$PWD/backups":/backup \
  alpine:3.22 \
  tar -C /source -czf "/backup/donkey-$(date +%Y%m%d-%H%M%S).tar.gz" .
docker compose start donkey
```

同时在密码管理器或其他受控密钥系统中备份：

- `DONKEY_CREDENTIAL_KEY`；
- OIDC Client Secret；
- 初始管理员和 Registry 客户端认证配置；
- TLS 私钥与证书（如果由 Donkey 容器挂载）。

不要把这些密钥写进与数据归档相同的公开存储。

## 在线数据库备份

只备份配置、用户、任务和历史记录时，可使用 SQLite Online Backup，而不要直接复制正在使用的 `donkey.db`：

```bash
sqlite3 /data/donkey.db ".timeout 5000" ".backup '/backup/donkey.db'"
sqlite3 /backup/donkey.db "PRAGMA integrity_check;"
```

`integrity_check` 必须输出 `ok`。在线数据库备份不保证与同时变化的 `cache/` 和 `image-tools/` 文件严格对应：

- `cache/` 可以不备份，Donkey 会按需重新拉取；
- 需要保留离线包和提取结果时，应使用停机一致性备份；
- 不要分别复制 `donkey.db`、`-wal` 和 `-shm` 并假设它们天然构成一致快照。

## 恢复

1. 停止 Donkey，确认没有容器或二进制仍在访问目标数据目录。
2. 将现有数据目录移动到隔离位置，不要直接覆盖，保留快速回滚副本。
3. 解压备份到一个空目录。
4. 恢复原来的文件属主和权限。官方镜像默认 UID 为 `10001`；LazyCat 等打包环境按应用 manifest 的运行用户处理。
5. 恢复与备份匹配的 `DONKEY_CREDENTIAL_KEY` 和认证环境变量。
6. 启动前执行数据库校验：

   ```bash
   sqlite3 /data/donkey.db "PRAGMA integrity_check;"
   ```

7. 启动 Donkey，检查 `/api/health`、节点列表、Registry `/v2/` 和最近任务。
8. 用一个小镜像执行拉取，再验证缓存写入和拉取历史。

如果新版本已经执行数据库迁移，不要把旧数据库文件覆盖到正在运行的新进程中；先停止服务，再恢复完整快照并由目标版本启动迁移。

## NAS 注意事项

- SQLite WAL 主文件应放在本机磁盘或保证 POSIX 锁语义的块存储上。
- 不建议把活动中的 `donkey.db` 放到 NFS、SMB、对象存储挂载或不明确支持文件锁的 NAS 共享目录。
- NAS 适合保存备份归档和只读导出产物，不适合作为不受验证的 WAL 主存储。
- 如果必须使用网络存储，先用故障断连、并发写入和恢复演练验证锁、`fsync` 与原子重命名语义。

## 恢复演练清单

- [ ] 备份文件有 SHA-256 校验值；
- [ ] SQLite `integrity_check` 为 `ok`；
- [ ] 可以使用原凭据解密并访问受认证 Registry；
- [ ] `/v2/` 返回 Registry v2 响应；
- [ ] 小镜像拉取、缓存命中和 Range 请求正常；
- [ ] ImageTools 任务和离线归档可访问；
- [ ] 恢复失败时可以切回隔离的原数据目录。
