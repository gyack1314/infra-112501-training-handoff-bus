"""协调服务领域规则测试。"""

import tempfile
import unittest
from pathlib import Path

from coordinator import Coordinator, CoordinationError, Store
from coordinator.messages import (
    RECEIVED,
    REFUSED,
    RESERVED,
    SAMPLE_INDEX,
    SCHEDULE,
    VOID_COMPLETED,
    VOID_PENDING,
    WITHDRAWN,
)
from coordinator.runner import TraceRunner
from coordinator.state import (
    ELIG_CANCELLED,
    ELIG_MANUAL,
    ELIG_OPEN,
    ELIG_STARTED,
    PHASE_RECEIVED,
    PHASE_VOID_PENDING,
    PHASE_WITHDRAW_PENDING,
    PHASE_WITHDRAWN,
    SLOT_RELEASED,
)


class CoordinatorTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "state.db"
        self.store = Store(self.db_path)
        self.coordinator = Coordinator(self.store)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def drain(self, client: str, site: str, run: str):
        """领取并确认某消费方在该交接上的全部现有通知，返回消息类型序列。"""
        seen = []
        while True:
            delivery = self.coordinator.receive(client, site, run)
            if delivery is None:
                return seen
            seen.append(delivery["message"])
            self.coordinator.ack(client, delivery["delivery"])

    def receive_and_ack(self, client: str, site: str, run: str, message_type: str):
        delivery = self.coordinator.receive(client, site, run)
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["message"], message_type)
        result = self.coordinator.ack(client, delivery["delivery"])
        return delivery, result


class ManifestAndSignoffTests(CoordinatorTestBase):
    def test_stale_signoff_cannot_approve_new_revision(self):
        c = self.coordinator
        c.submit_manifest("west", "r7", 1, "sha-b", "audit")
        c.submit_manifest("west", "r7", 2, "sha-c", "training")
        # 旧修订的 accepted 签认晚到，只挂在修订 1 上
        c.record_signoff("west", "r7", 1, "audit", "accepted")
        state = c.inspect("west", "r7")
        self.assertEqual(state["phase"], "pending_review")
        self.assertEqual(len(state["reservations"]), 0)
        # 用途不匹配的签认也不能通过
        c.record_signoff("west", "r7", 2, "audit", "accepted")
        state = c.inspect("west", "r7")
        self.assertFalse(state["manifests"][1]["accepted_for_use"])
        self.assertEqual(state["phase"], "pending_review")
        # 正确修订+用途的签认到达后才请求预留
        c.record_signoff("west", "r7", 2, "training", "accepted")
        state = c.inspect("west", "r7")
        self.assertEqual(state["phase"], "awaiting_slot")
        self.assertEqual(state["reservations"][0]["reservation"], "reservation:1")

    def test_same_revision_different_content_is_conflict_not_overwrite(self):
        c = self.coordinator
        c.submit_manifest("east", "r1", 1, "sha-a", "training")
        c.record_signoff("east", "r1", 1, "training", "accepted")
        result = c.submit_manifest("east", "r1", 1, "sha-CHANGED", "training")
        self.assertEqual(result["status"], "conflict")
        state = c.inspect("east", "r1")
        # 正在处理的清单指纹未被覆盖
        self.assertEqual(state["manifests"][0]["collection"], "sha-a")
        self.assertEqual(state["manifests"][0]["conflicts"], ["sha-CHANGED"])
        # 原修订的预留流程继续有效
        self.assertEqual(len(state["reservations"]), 1)
        # 完全相同的重送是幂等
        again = c.submit_manifest("east", "r1", 1, "sha-a", "training")
        self.assertEqual(again["status"], "idempotent")

    def test_commands_are_idempotent_on_resend(self):
        c = self.coordinator
        c.submit_manifest("east", "r2", 1, "sha-a", "training")
        c.record_signoff("east", "r2", 1, "training", "accepted")
        c.record_signoff("east", "r2", 1, "training", "accepted")
        self.assertEqual(len(c.inspect("east", "r2")["reservations"]), 1)


class ReservationTests(CoordinatorTestBase):
    def _prepare_reserved(self, site="east", run="r3"):
        c = self.coordinator
        c.submit_manifest(site, run, 1, "sha-a", "training")
        c.record_signoff(site, run, 1, "training", "accepted")
        c.record_prepare_ack(site, run, 1, "reservation:1", "reserved")
        return c.inspect(site, run)

    def test_reserved_is_not_received(self):
        state = self._prepare_reserved()
        self.assertEqual(state["phase"], "reserved")
        self.assertTrue(state["slot_busy"])
        self.assertFalse(state["received"])
        # 两个消费方都拿到 reserved 决定，但没有 received
        for client in (SAMPLE_INDEX, SCHEDULE):
            messages = self.drain(client, "east", "r3")
            self.assertEqual(messages, [RESERVED])
        state = self.coordinator.inspect("east", "r3")
        self.assertFalse(state["received"])
        self.assertIsNone(state["successor"])

    def test_refused_releases_slot_and_allows_new_revision(self):
        c = self.coordinator
        c.submit_manifest("east", "r4", 1, "sha-a", "training")
        c.record_signoff("east", "r4", 1, "training", "accepted")
        c.record_prepare_ack("east", "r4", 1, "reservation:1", "refused")
        state = c.inspect("east", "r4")
        self.assertEqual(state["reservations"][0]["slot"], SLOT_RELEASED)
        self.assertFalse(state["slot_busy"])
        for client in (SAMPLE_INDEX, SCHEDULE):
            self.assertEqual(self.drain(client, "east", "r4"), [REFUSED])
        # 同修订不会自动再次占用位置
        c.record_signoff("east", "r4", 1, "training", "accepted")
        self.assertEqual(len(c.inspect("east", "r4")["reservations"]), 1)
        # 新修订取得签认后才能占用下一个位置
        c.submit_manifest("east", "r4", 2, "sha-b", "training")
        c.record_signoff("east", "r4", 2, "training", "accepted")
        state = c.inspect("east", "r4")
        self.assertEqual(len(state["reservations"]), 2)
        self.assertEqual(state["reservations"][1]["reservation"], "reservation:2")

    def test_slot_cannot_be_occupied_twice_while_busy(self):
        c = self.coordinator
        c.submit_manifest("east", "r5", 1, "sha-a", "training")
        c.record_signoff("east", "r5", 1, "training", "accepted")
        # 预留请求在途，再来一个新修订也不能产生第二个预留
        c.submit_manifest("east", "r5", 2, "sha-b", "training")
        c.record_signoff("east", "r5", 2, "training", "accepted")
        state = c.inspect("east", "r5")
        self.assertEqual(len(state["reservations"]), 1)
        self.assertTrue(state["slot_busy"])

    def test_mismatched_prepare_ack_is_rejected(self):
        c = self.coordinator
        c.submit_manifest("east", "r6", 1, "sha-a", "training")
        c.record_signoff("east", "r6", 1, "training", "accepted")
        with self.assertRaises(CoordinationError):
            c.record_prepare_ack("east", "r6", 2, "reservation:1", "reserved")
        with self.assertRaises(CoordinationError):
            c.record_prepare_ack("east", "r6", 1, "reservation:9", "reserved")


class ReceiptTests(CoordinatorTestBase):
    def _committed(self, site="east", run="r8"):
        c = self.coordinator
        c.submit_manifest(site, run, 1, "sha-a", "training")
        c.record_signoff(site, run, 1, "training", "accepted")
        c.record_prepare_ack(site, run, 1, "reservation:1", "reserved")
        c.record_receipt(site, run, "reservation:1", "committed")

    def test_unknown_receipt_keeps_pending(self):
        c = self.coordinator
        c.submit_manifest("east", "r9", 1, "sha-a", "training")
        c.record_signoff("east", "r9", 1, "training", "accepted")
        c.record_prepare_ack("east", "r9", 1, "reservation:1", "reserved")
        c.record_receipt("east", "r9", "reservation:1", "unknown")
        state = c.inspect("east", "r9")
        self.assertEqual(state["phase"], "reserved")
        self.assertFalse(state["received"])
        # unknown 不产生任何可执行决定
        self.assertEqual(self.drain(SCHEDULE, "east", "r9"), [RESERVED])

    def test_committed_fact_is_immutable(self):
        c = self.coordinator
        self._committed()
        # 重送 committed 幂等
        c.record_receipt("east", "r8", "reservation:1", "committed")
        # unknown 不能抹掉接收事实
        c.record_receipt("east", "r8", "reservation:1", "unknown")
        self.assertTrue(c.inspect("east", "r8")["received"])
        # not_committed 与已接收事实冲突
        with self.assertRaises(CoordinationError):
            c.record_receipt("east", "r8", "reservation:1", "not_committed")

    def test_not_committed_releases_slot(self):
        c = self.coordinator
        c.submit_manifest("east", "r10", 1, "sha-a", "training")
        c.record_signoff("east", "r10", 1, "training", "accepted")
        c.record_prepare_ack("east", "r10", 1, "reservation:1", "reserved")
        c.record_receipt("east", "r10", "reservation:1", "not_committed")
        state = c.inspect("east", "r10")
        self.assertFalse(state["slot_busy"])
        self.assertEqual(state["reservations"][0]["slot"], SLOT_RELEASED)
        # 之后再报 committed 必须被拒绝
        with self.assertRaises(CoordinationError):
            c.record_receipt("east", "r10", "reservation:1", "committed")


class VoidRaceTests(CoordinatorTestBase):
    def _reserved_handover(self, site="east", run="r11"):
        c = self.coordinator
        c.submit_manifest(site, run, 1, "sha-a", "training")
        c.record_signoff(site, run, 1, "training", "accepted")
        c.record_prepare_ack(site, run, 1, "reservation:1", "reserved")

    def test_void_while_receipt_unknown_then_not_committed_completes_void(self):
        c = self.coordinator
        self._reserved_handover()
        c.record_receipt("east", "r11", "reservation:1", "unknown")
        result = c.request_void("east", "r11")
        self.assertEqual(result["phase"], PHASE_VOID_PENDING)
        state = c.inspect("east", "r11")
        self.assertTrue(state["void_registered"])
        self.assertFalse(state["void_terminal"])
        # 排班端收到暂停通知
        self.assertEqual(self.drain(SCHEDULE, "east", "r11"), [RESERVED, VOID_PENDING])
        # 外部确认未接收 -> 释放预留 + 作废闭环
        c.record_receipt("east", "r11", "reservation:1", "not_committed")
        state = c.inspect("east", "r11")
        self.assertTrue(state["void_terminal"])
        self.assertFalse(state["received"])
        for client in (SAMPLE_INDEX, SCHEDULE):
            messages = self.drain(client, "east", "r11")
            self.assertIn(VOID_COMPLETED, messages)

    def test_void_then_late_commit_switches_to_withdraw_flow(self):
        c = self.coordinator
        self._reserved_handover(run="r12")
        c.record_receipt("east", "r12", "reservation:1", "unknown")
        c.request_void("east", "r12")
        # 迟到的实际接收：作废不能抹掉接收事实，转等待撤回
        c.record_receipt("east", "r12", "reservation:1", "committed")
        state = c.inspect("east", "r12")
        self.assertEqual(state["phase"], PHASE_WITHDRAW_PENDING)
        self.assertTrue(state["received"])
        self.assertTrue(state["void_registered"])
        self.assertFalse(state["void_terminal"])
        # 未撤回前不能开始后继
        self.assertFalse(state["can_start_next"]["allowed"])
        with self.assertRaises(CoordinationError):
            c.start_next("east", "r12", "r13")
        # 样本接收方确认撤回后才终结
        c.confirm_withdraw("east", "r12", "reservation:1")
        state = c.inspect("east", "r12")
        self.assertEqual(state["phase"], PHASE_WITHDRAWN)
        self.assertTrue(state["withdrawn"])

    def test_void_after_commit_waits_for_withdraw_ack(self):
        c = self.coordinator
        self._reserved_handover(run="r13")
        c.record_receipt("east", "r13", "reservation:1", "committed")
        result = c.request_void("east", "r13")
        self.assertEqual(result["phase"], PHASE_WITHDRAW_PENDING)
        # 没有撤回确认前，作废不终结
        self.assertFalse(c.inspect("east", "r13")["void_terminal"])
        c.confirm_withdraw("east", "r13", "reservation:1")
        self.assertTrue(c.inspect("east", "r13")["withdrawn"])

    def test_void_before_any_receipt_resolves_when_slot_abandonable(self):
        c = self.coordinator
        c.submit_manifest("east", "r14", 1, "sha-a", "training")
        c.record_signoff("east", "r14", 1, "training", "accepted")
        # 预留被拒收（位置已释放）后登记作废：立即闭环
        c.record_prepare_ack("east", "r14", 1, "reservation:1", "refused")
        c.request_void("east", "r14")
        state = c.inspect("east", "r14")
        self.assertTrue(state["void_terminal"])
        self.assertEqual(
            self.drain(SCHEDULE, "east", "r14"), [REFUSED, VOID_COMPLETED]
        )

    def test_withdraw_without_void_is_rejected(self):
        c = self.coordinator
        self._reserved_handover(run="r15")
        c.record_receipt("east", "r15", "reservation:1", "committed")
        with self.assertRaises(CoordinationError):
            c.confirm_withdraw("east", "r15", "reservation:1")


class ConsumerAndSuccessorTests(CoordinatorTestBase):
    def _received_handover(self, site="east", run="r20"):
        c = self.coordinator
        c.submit_manifest(site, run, 1, "sha-a", "training")
        c.record_signoff(site, run, 1, "training", "accepted")
        c.record_prepare_ack(site, run, 1, "reservation:1", "reserved")
        c.record_receipt(site, run, "reservation:1", "committed")

    def test_consumers_ack_independently(self):
        c = self.coordinator
        self._received_handover()
        # 排班端处理
        delivery = c.receive(SCHEDULE, "east", "r20")
        self.assertEqual(delivery["message"], RESERVED)
        c.ack(SCHEDULE, delivery["delivery"])
        delivery = c.receive(SCHEDULE, "east", "r20")
        self.assertEqual(delivery["message"], RECEIVED)
        c.ack(SCHEDULE, delivery["delivery"])
        # 样本索引方一条都还没处理：不因排班端已处理而跳过
        state = c.inspect("east", "r20")
        missing = state["missing_acks"][SAMPLE_INDEX]
        self.assertEqual([item["message"] for item in missing], [RESERVED, RECEIVED])
        self.assertEqual(state["missing_acks"][SCHEDULE], [])
        # 样本索引方按自己的节奏处理，同样是 reserved 再 received
        first = c.receive(SAMPLE_INDEX, "east", "r20")
        self.assertEqual(first["message"], RESERVED)
        c.ack(SAMPLE_INDEX, first["delivery"])
        second = c.receive(SAMPLE_INDEX, "east", "r20")
        self.assertEqual(second["message"], RECEIVED)
        c.ack(SAMPLE_INDEX, second["delivery"])
        state = c.inspect("east", "r20")
        self.assertEqual(state["missing_acks"][SAMPLE_INDEX], [])

    def test_receive_redelivery_returns_same_tag_until_acked(self):
        c = self.coordinator
        self._received_handover()
        first = c.receive(SCHEDULE, "east", "r20")
        again = c.receive(SCHEDULE, "east", "r20")
        self.assertEqual(first["delivery"], again["delivery"])
        c.ack(SCHEDULE, first["delivery"])
        next_one = c.receive(SCHEDULE, "east", "r20")
        self.assertNotEqual(first["delivery"], next_one["delivery"])
        self.assertEqual(next_one["message"], RECEIVED)

    def test_duplicate_ack_does_not_double_successor(self):
        c = self.coordinator
        self._received_handover()
        d1 = c.receive(SCHEDULE, "east", "r20")
        c.ack(SCHEDULE, d1["delivery"])
        d2 = c.receive(SCHEDULE, "east", "r20")
        result1 = c.ack(SCHEDULE, d2["delivery"])
        self.assertTrue(result1["first_ack"])
        result2 = c.ack(SCHEDULE, d2["delivery"])
        self.assertFalse(result2["first_ack"])
        state = c.inspect("east", "r20")
        self.assertEqual(state["successor"]["state"], ELIG_OPEN)
        # 只有一次资格开启事件
        events = self.store.load_events("east", "r20")
        self.assertEqual(sum(e.type == "successor_opened" for e in events), 1)

    def test_successor_lifecycle_and_start_permission(self):
        c = self.coordinator
        self._received_handover()
        # 排班端未处理 received 前不能开始
        with self.assertRaises(CoordinationError):
            c.start_next("east", "r20", "r21")
        self.drain(SCHEDULE, "east", "r20")
        state = c.inspect("east", "r20")
        self.assertTrue(state["can_start_next"]["allowed"])
        c.start_next("east", "r20", "r21")
        self.assertEqual(c.inspect("east", "r20")["successor"]["state"], ELIG_STARTED)
        # 重复开始同一后继是幂等，开始别的后继被拒绝
        c.start_next("east", "r20", "r21")
        with self.assertRaises(CoordinationError):
            c.start_next("east", "r20", "r22")

    def test_withdrawal_cancels_unstarted_successor(self):
        c = self.coordinator
        self._received_handover()
        self.drain(SCHEDULE, "east", "r20")
        c.request_void("east", "r20")
        c.confirm_withdraw("east", "r20", "reservation:1")
        # 排班端处理撤回消息
        delivery = c.receive(SCHEDULE, "east", "r20")
        messages = []
        while delivery is not None:
            messages.append(delivery["message"])
            c.ack(SCHEDULE, delivery["delivery"])
            delivery = c.receive(SCHEDULE, "east", "r20")
        self.assertIn(WITHDRAWN, messages)
        state = c.inspect("east", "r20")
        self.assertEqual(state["successor"]["state"], ELIG_CANCELLED)
        self.assertFalse(state["can_start_next"]["allowed"])
        # 样本索引方尚未处理撤回 -> 未传播到每一方
        self.assertFalse(
            c.inspect("east", "r20")["withdrawal_propagation"]["propagated_to_all"]
        )
        self.drain(SAMPLE_INDEX, "east", "r20")
        self.assertTrue(
            c.inspect("east", "r20")["withdrawal_propagation"]["propagated_to_all"]
        )

    def test_withdrawal_after_start_requires_manual_handling(self):
        c = self.coordinator
        self._received_handover(run="r21")
        self.drain(SCHEDULE, "east", "r21")
        c.start_next("east", "r21", "r22")
        c.request_void("east", "r21")
        c.confirm_withdraw("east", "r21", "reservation:1")
        self.drain(SCHEDULE, "east", "r21")
        state = c.inspect("east", "r21")
        # 历史不能改写成从未获得资格：保留 started 事实并转人工
        self.assertEqual(state["successor"]["state"], ELIG_MANUAL)
        self.assertEqual(state["successor"]["next_run"], "r22")
        events = self.store.load_events("east", "r21")
        self.assertTrue(any(e.type == "successor_started" for e in events))

    def test_void_before_schedule_processes_received_blocks_successor(self):
        """作废意图先于排班端处理 received 登记：资格不再开启，暂停生效。"""
        c = self.coordinator
        self._received_handover(run="r24")
        c.request_void("east", "r24")
        c.confirm_withdraw("east", "r24", "reservation:1")
        self.drain(SCHEDULE, "east", "r24")
        state = c.inspect("east", "r24")
        self.assertTrue(state["withdrawn"])
        self.assertIsNone(state["successor"])
        self.assertFalse(state["can_start_next"]["allowed"])

    def test_received_before_withdrawn_order_guaranteed(self):
        c = self.coordinator
        self._received_handover(run="r23")
        c.request_void("east", "r23")
        c.confirm_withdraw("east", "r23", "reservation:1")
        for client in (SAMPLE_INDEX, SCHEDULE):
            types = [d.message_type for d in self.store.list_deliveries(client, "east", "r23")]
            self.assertLess(types.index(RECEIVED), types.index(WITHDRAWN))


class RestartTests(CoordinatorTestBase):
    def test_restart_recovers_state_and_pending_deliveries(self):
        c = self.coordinator
        c.submit_manifest("east", "r30", 1, "sha-a", "training")
        c.record_signoff("east", "r30", 1, "training", "accepted")
        c.record_prepare_ack("east", "r30", 1, "reservation:1", "reserved")
        c.record_receipt("east", "r30", "reservation:1", "committed")
        # 排班端只领取不确认，模拟崩溃
        pending = c.receive(SCHEDULE, "east", "r30")
        self.assertEqual(pending["message"], RESERVED)
        # 重启：用同一个库文件新建协调器
        self.store.close()
        store2 = Store(self.db_path)
        c2 = Coordinator(store2)
        state = c2.inspect("east", "r30")
        self.assertEqual(state["phase"], PHASE_RECEIVED)
        self.assertTrue(state["received"])
        # 在途通知按原投递标识恢复，不会丢也不会重号
        redelivered = c2.receive(SCHEDULE, "east", "r30")
        self.assertEqual(redelivered["delivery"], pending["delivery"])
        c2.ack(SCHEDULE, redelivered["delivery"])
        store2.close()

    def test_replay_three_shipped_scenarios(self):
        data_dir = Path(__file__).with_name("data")
        scenarios = {
            "void-meets-late-commit.jsonl": ("east", "r41"),
            "superseded-review.jsonl": ("west", "r7"),
            "started-before-withdrawal.jsonl": ("east", "r55"),
        }
        for filename, identity in scenarios.items():
            with self.subTest(file=filename):
                with tempfile.TemporaryDirectory() as tmp:
                    store = Store(Path(tmp) / "scenario.db")
                    runner = TraceRunner(Coordinator(store))
                    outputs = runner.run_file(data_dir / filename)
                    # 除“作废待确认期间请求开始后继”这类被状态机拒绝的命令外，
                    # 其余输入都应正常处理
                    errors = [
                        row
                        for row in outputs
                        if row["result"].get("status") == "error"
                    ]
                    for row in errors:
                        self.assertEqual(row["type"], "start_next")
                    site, run = identity
                    state = Coordinator(store).inspect(site, run)
                    self.assertTrue(state["deliveries"])
                    store.close()

    def test_void_meets_late_commit_full_behavior(self):
        data_dir = Path(__file__).with_name("data")
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "s.db")
            runner = TraceRunner(Coordinator(store))
            outputs = runner.run_file(data_dir / "void-meets-late-commit.jsonl")
            # timestamp=5 的 inspect 处于 void_pending
            mid = next(row for row in outputs if row["timestamp"] == 5)
            self.assertEqual(mid["result"]["inspection"]["phase"], PHASE_VOID_PENDING)
            # timestamp=9 start_next 必须被拒绝（作废待确认暂停）
            start = next(row for row in outputs if row["type"] == "start_next")
            self.assertEqual(start["result"]["status"], "error")
            # 最终：撤回完成；撤回通知已入样本方发件箱但尚未确认，因此未全传播
            final = outputs[-1]["result"]["inspection"]
            self.assertTrue(final["withdrawn"])
            self.assertFalse(final["withdrawal_propagation"]["propagated_to_all"])
            pending_sample = final["withdrawal_propagation"]["deliveries"][SAMPLE_INDEX]
            self.assertEqual(pending_sample["state"], "pending")
            store.close()

    def test_started_before_withdrawal_full_behavior(self):
        data_dir = Path(__file__).with_name("data")
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "s.db")
            runner = TraceRunner(Coordinator(store))
            outputs = runner.run_file(data_dir / "started-before-withdrawal.jsonl")
            # ts6 的 start_next 发生在 received 被排班端确认之前，被正确拒绝
            start = next(row for row in outputs if row["type"] == "start_next")
            self.assertEqual(start["result"]["status"], "error")
            # 作废意图先于排班端处理 received：后继资格不再开启（暂停语义）
            final = outputs[-1]["result"]["inspection"]
            self.assertIsNone(final["successor"])
            self.assertTrue(final["withdrawn"])
            store.close()

    def test_manual_handling_when_start_really_happened_first(self):
        """同名需求的另一面：资格确实已开始后撤回，必须转人工、保留历史。"""
        c = self.coordinator
        c.submit_manifest("east", "r24", 1, "sha-a", "training")
        c.record_signoff("east", "r24", 1, "training", "accepted")
        c.record_prepare_ack("east", "r24", 1, "reservation:1", "reserved")
        c.record_receipt("east", "r24", "reservation:1", "committed")
        self.drain(SCHEDULE, "east", "r24")
        c.start_next("east", "r24", "r25")
        c.request_void("east", "r24")
        c.confirm_withdraw("east", "r24", "reservation:1")
        self.drain(SCHEDULE, "east", "r24")
        state = c.inspect("east", "r24")
        self.assertEqual(state["successor"]["state"], ELIG_MANUAL)
        self.assertEqual(state["successor"]["next_run"], "r25")
        events = self.store.load_events("east", "r24")
        self.assertTrue(any(e.type == "successor_started" for e in events))

    def test_superseded_review_full_behavior(self):
        data_dir = Path(__file__).with_name("data")
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "s.db")
            runner = TraceRunner(Coordinator(store))
            outputs = runner.run_file(data_dir / "superseded-review.jsonl")
            final = outputs[-1]["result"]["inspection"]
            # 修订 2 被拒收并释放；修订 3 尚无签认
            self.assertEqual(final["current_revision"], 3)
            self.assertEqual(final["reservations"][0]["slot"], SLOT_RELEASED)
            self.assertEqual(final["phase"], "pending_review")
            store.close()


if __name__ == "__main__":
    unittest.main()
