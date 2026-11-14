# 04-changecast（变投影）

Changecast — 变更日志投影：源变更流 → 按序投影 → 乱序/晚到策略钩子 → 快照可查互证。

与 Feedstitch（多源 Feed 归并）、Batchgate（作业依赖编排）、Syncledger（双端对账）主链路不同。夹具全本地，不连活库。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5400 |
| API | http://localhost:10400 |

## 主链

配置投影流 → 读取 CDC 夹具并按 seq 应用到投影表 → 批次计数与快照互证

## seq 重放（目标快照，隔离存放）

排查「当时长什么样」时，不能改小主水位再跑增量（会弄脏主水位、也回答不了截断点问题）。
本切口提供按流重放到目标 seq 的独立链路：

- `POST /api/replays`：从**事件源**（完整夹具）截断 `seq <= target_seq`，按批重放生成目标快照，
  全部写入隔离表 `replay_runs / replay_batches / replay_projection_rows / replay_applied_events`，
  主投影三表（`projection_rows / project_batches / applied_events`）与主水位在重放路径上零写入。
- **互证**：源截断点（事件数、max seq）、重放批次计数求和、目标快照行集合与快照 max seq、
  全量重算计数四方交叉校验，任一不符置 `failed`，失败重放不可提升。
- `GET /api/replays/{id}/diff`：目标快照与当前主投影逐行对照（仅重放有 / 仅主投影有 / 同行变更）。
- `POST /api/replays/{id}/promote`：**唯一**能把重放快照提升为主投影的独立接口；
  提升前复核持久化快照与截断点，通过后才替换主投影并移动主水位。重放成功本身绝不顺手改水位。
- **互斥**：按流锁 `stream_locks`，常规开跑、重放、提升同流互斥（冲突返回 409 `stream_locked`），
  不同流互不阻塞；锁带 TTL，持有者正常结束即释放。任何写主投影的切面都必须先取得同一把锁，
  不得与重放并行写主投影。
- 重放失败只落 `replay_*` 失败记录，主投影与主水位保持不变。
- 页面入口：「seq 重放」页可指定目标 seq 与批次大小、查看互证与差异、发起提升，
  并用持锁秒数演示与「投影台」常规开跑的双向互斥。

## 种子投影流

- **member_profile**：会员档案变更（`member_id`，字段 `display_name` / `tier` / `region`）
- **store_archive**：门店档案变更（`store_id`，字段 `name` / `status` / `city`）
- **channel_attr**：渠道属性变更（`channel_code`，字段 `label` / `active`）

底座默认按 **事件 seq 升序** 投影；乱序缓冲表空挂，深逻辑留给后题。对外文案不用「工单」「CRM」。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见出题规划）

`arrival_vs_event_order` / `ooo_window` / `event_idempotency` / `tombstone_policy` / `watermark_rewind` / …

> 已落地：**13-projection-replay-to-seq**（按目标 seq 的隔离重放、互证、独立提升、与开跑互斥），见上节。
> 注意 `watermark_rewind`（回拨主水位跑增量）仍是未实现项，本切口明确不用它冒充目标 seq 重放。
