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
