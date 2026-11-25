# 训练作业事件的跨参与方提交与取消

交接链路涉及采集站、质量复核、样本接收与排班。资料使用合成清单引用，外部参与方以各自回执表达真实结果。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **manifest**　采集端提交 site/run、revision、collection 和 use。该清单的身份是 site/run/revision，collection 表示固定内容的指纹样式字符串。
- **signoff**　质量复核引用同一清单与 use，decision 为 accepted 或 rejected。
- **prepare_ack**　样本方回应预留，对应 reservation 与 revision，outcome 为 reserved 或 refused。
- **receipt**　样本方报告实际状态，outcome 为 committed、not_committed 或 unknown。not_committed 表示该预留今后不能提交。
- **withdraw_ack**　清单曾接收时，样本方确认已撤回一条特定 reservation。
- **void**　运维申请作废指定 site/run。
- **receive**　排班或样本确认端领取待处理通知。client 表示消费方，delivery 是通知持有标识，样例中 delivery:N 指同一消费方自开头收到的第 N 次通知；重复领取可返回相同标识。
- **ack**　消费方确认通知。reservation:N 表示协调器为同一轮发出的第 N 项预留请求。
- **start_next**　排班请求用一项交接的完成资格开始指定 next_run。
- **inspect**　运营按 run 查看交接、投递与排班结果。

## 资料文件

- void-meets-late-commit.jsonl
- superseded-review.jsonl
- started-before-withdrawal.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。

---

## 事务消息协调服务 `coordinator/`

协调器处在作业来源（采集/复核）、样本接收方（`sample-index`）与后续排班消费方（`schedule`）之间，以 `(site, run)` 作为一轮交接的业务身份。仅依赖标准库，状态全部落在一个 SQLite 文件中。

### 设计要点

- **事件溯源**：每次状态变化写成不可变领域事件（`coordinator/events.py`），重启后重放事件即可重建任意交接的完整进度，已对接收方作出的决定不会丢失。
- **每消费方独立事务发件箱**：决定与通知在同一个数据库事务内提交（`coordinator/store.py`）。两个消费方各自领取、各自确认，一方已处理不代表另一方可以跳过；同一消费方按入箱序号投递，`received` 严格先于 `withdrawn`，不会倒序。
- **串行写事务**：所有命令经 `BEGIN IMMEDIATE` 加进程锁串行化，重发命令幂等，重复处理不会让样本变成两份或取得两次后继资格。
- **外部状态不明不猜测**：`receipt=unknown` 只保留待确认；作废意图先登记为 `void_pending` 并通知排班端暂停，直到能确定预留可放弃（作废闭环）或确认已接收（转撤回流程）才给最终结果。

### 关键业务规则的落地

| 需求 | 实现 |
| --- | --- |
| 签认只对所引用修订与用途有效，旧签认不能使新清单通过 | 签认挂在具体 revision/use 上，仅当前修订且用途匹配的 accepted 才触发预留 |
| 相同修订号换内容 | 记录 `manifest_conflict` 事件并在 inspect 中留痕，不覆盖正在处理的清单 |
| 预留位置不是已接收样本 | `reserved` 仅表示留位；只有 `committed` 回执才产生 `received` 决定 |
| 拒收后释放位置，未释放不能再占用同一位置 | 拒收/未接收即 `reservation_released`；位置忙时新修订不会产生第二项预留 |
| 重发按身份恢复进度 | 未 ack 的在途通知重复 receive 返回同一 delivery 标识；ack 按消费方序号兼容回退 |
| 作废与接收互相追上 | 未接收→`void_completed` 并释放预留；已接收→保留接收事实，等待 `withdraw_ack` 后发引用原交接的 `withdrawn` |
| 后继资格 | 仅排班端首认 `received` 后开启一次；撤回时未开始则取消、已开始则转人工，历史不回改 |
| 作废待确认期间 | `start_next` 被拒绝，排班端暂停发放新开始许可 |

### 使用方式

回放一段演练并查看逐步结果：

```sh
python3 -m coordinator --db state.db replay data/void-meets-late-commit.jsonl
python3 -m coordinator --db state.db handovers
python3 -m coordinator --db state.db inspect east r41
```

`inspect` 返回阶段、各修订签认与冲突、预留占用与回执、作废/撤回状态、两个消费方各自缺失的确认（`missing_acks`）、撤回是否传播到每一方（`withdrawal_propagation`）以及后继作业能否开始（`can_start_next`）。

启动 HTTP 服务（标准库实现，`POST /event` 提交契约事件，`GET /state` 查询，`GET /handovers` 列出交接）：

```sh
python3 -m coordinator --db state.db serve --host 127.0.0.1 --port 8080
curl -X POST http://127.0.0.1:8080/event -H 'Content-Type: application/json' \
  -d '{"type":"manifest","data":{"site":"east","run":"r41","revision":1,"collection":"sha-a","use":"training"}}'
curl 'http://127.0.0.1:8080/state?site=east&run=r41'
```

### 代码结构

```
coordinator/
├── errors.py     # 领域错误（含同修订内容冲突）
├── events.py     # 不可变领域事件定义
├── messages.py   # 外发通知类型、消费方与路由
├── state.py      # 事件重放与交接状态机
├── store.py      # SQLite 事件日志 + 每消费方事务发件箱
├── service.py    # 协调器命令处理（业务规则入口）
├── runner.py     # JSONL 契约事件到协调器命令的映射
├── server.py     # 标准库 HTTP 入口
└── cli.py        # replay / inspect / handovers / serve
```

领域行为由 `test_coordinator.py` 的 31 个用例覆盖，含三份随附演练的端到端回放、重启恢复与并发写压测。
