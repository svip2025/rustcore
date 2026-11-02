"""Real paper V2 entry/close -> V1 events -> Python outbox -> batch schema.

Rust owns the isolated authenticated server and a fresh, production-recorded
mark of 100 for SYMBOL. No event/report is constructed or repaired here.
Usage: paper_protection_reporting_client.py TARGET CREDENTIAL_ID
The schema-only endpoint deliberately does not certify MySQL or risk reporting.
"""
from __future__ import annotations

import contextlib
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time

import grpc
from google.protobuf.json_format import MessageToDict

ROOT = Path(__file__).resolve().parents[4]
SOURCE_ROOTS = {
    "execution_orchestrator": "services/execution-orchestrator/src",
    "agent_runtime": "services/agent-runtime/src",
    "ai_trade_baseline_strategy": "packages/baseline-strategy/python",
    "ai_trade_data_plane": "packages/data-plane/python",
    "ai_trade_indicators": "packages/indicators/python",
    "ai_trade_contracts": "packages/contracts/python",
}
sys.path[:0] = [str(ROOT / p) for p in SOURCE_ROOTS.values()] + [
    str(ROOT / "services/execution-orchestrator/tests")
]
from execution_orchestrator import event_consumer
from execution_orchestrator.grpc.rustcore_client import RustCoreGrpcClient, RustCoreTarget
from execution_orchestrator.proto_gen import execution_engine_v2_pb2 as pb2
from execution_orchestrator.reporting import event_mapper as mapper_module
from execution_orchestrator.reporting.runtime import flush_outbox_once
from execution_orchestrator.storage.sqlite_store import SqliteStateStore
from fixtures.fill_reporting_bridge import consumer_runtime, contract_server

ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
BOT = "bot_pd_87b016316af06a3cbcdfa5ba1"
RUN = f"run_{ULID}"
CYCLE = "cyc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
SYMBOL = "binance|futures|BTC|USDT|USDT"
PROTECTION_ROLES = {"protection_sl", "protection_tp"}


def wire(message):
    return MessageToDict(message, preserving_proto_field_name=True)


def drain(client, credential, since_seq):
    """Read the actual stream through a bounded quiet-tail deadline."""
    events = []
    stream = client.stream_events(
        credential_id=credential, since_seq=since_seq, timeout_s=2.0
    )
    try:
        events.extend(stream)
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.DEADLINE_EXCEEDED:
            raise
    finally:
        stream.close()
    assert events, "production stream returned no phase events"
    return events


def deliver(events, store, rt, report_client, observed):
    before = observed()
    for event in events:
        for _ in range(2):
            event_consumer._persist_event(
                event=event, credential_id=event.credential_id,
                mapper=mapper_module.EngineEventMapper(), state_db=store,
            )
    reports = [item.payload for item in store.fetch_due_outbox(kind="trade", limit=100)]
    assert reports, "actual consumer queued no trade reports"
    assert flush_outbox_once(rt=rt, store=store, client=report_client, batch_size=100)
    after = observed()
    with sqlite3.connect(store.db_path) as con:
        pending = con.execute(
            "SELECT kind,payload_json FROM orch_outbox WHERE sent_at_ms IS NULL ORDER BY id"
        ).fetchall()
    return {
        "events": [{"event": wire(event), "protobuf_hex": event.SerializeToString().hex()}
                   for event in events],
        "outboxReports": reports,
        "acceptedReports": [report for batch in after["accepted"][len(before["accepted"]):]
                            for report in batch.get("trades", [])],
        "batchRejected": after["rejected"] - before["rejected"],
        "tradeReportsUnsent": [json.loads(raw) for kind, raw in pending if kind == "trade"],
        "riskReportsUnsent": [json.loads(raw) for kind, raw in pending if kind == "risk_verdict"],
        "unresolved": store.unattributed_fill_status(),
        "schemaBinding": after["schemaBinding"],
    }


def main():
    target, credential = sys.argv[1:]
    for module in (event_consumer, mapper_module):
        assert Path(module.__file__).resolve().is_relative_to(ROOT), module.__file__
    assert re.fullmatch(r"127\.0\.0\.1:[1-9][0-9]{0,4}", target), "loopback only"
    assert re.fullmatch(r"paper:[a-f0-9]{32}", credential), "isolated simulation credential required"
    started = time.monotonic()
    client = RustCoreGrpcClient(target=RustCoreTarget(target), timeout_s=8)
    grpc.channel_ready_future(client._channel).result(timeout=8)
    token = os.environ.pop("RUSTCORE_GRPC_AUTH_TOKEN")
    anonymous = RustCoreGrpcClient(target=RustCoreTarget(target), timeout_s=3)
    try:
        anonymous.execute_intents(credential_id=credential, run_mode="paper", intents=[])
    except grpc.RpcError as error:
        assert error.code() == grpc.StatusCode.UNAUTHENTICATED, error.code()
    else:
        raise AssertionError("unauthenticated V2 ExecuteIntents was accepted")
    finally:
        anonymous._channel.close()
        os.environ["RUSTCORE_GRPC_AUTH_TOKEN"] = token
    failures = []
    try:
        funding = client.fund_simulation_account(
            credential_id=credential, amount_usdt="10000",
            idempotency_identity=f"paper-protection-wire:{credential}",
        )
        assert funding.applied, "fresh isolated funding did not apply"
        budget = client.put_risk_budget(
            credential_id=credential, as_of_ms=int(time.time() * 1000),
            equity_usdt="10000", available_usdt="10000", today_realized_pnl_usdt="0",
            today_realized_pnl_pct=0, drawdown_pct=0, total_exposure_pct=0,
            open_positions_count=0, group_exposure_json='{"majors":0,"l1_alts":0}',
            kill_switch_level="none", remaining_credential_notional_usdt="5000",
        )
        assert budget.applied, "fresh risk budget did not apply"
        intent = pb2.OrderIntent(
            bot_uid=BOT, run_id=RUN, cycle_id=CYCLE, idempotency_key="idem_" + "a" * 64,
            client_order_id_short="paper-protection-wire", exchange="binance",
            market_kind="futures", symbol_key=SYMBOL, action="open_long",
            order_type="limit", limit_price="100", intent_id=f"int_{ULID}",
            max_slippage_bps=15, protection=pb2.Protection(
                required=True, stop_loss_price="95", take_profit_price="110",
                confirm_timeout_ms=3000, schema_version=1,
            ), policy_version="risk_policy_v3", valid_until_ms=int(time.time() * 1000) + 30000,
            config_hash="c" * 64, prompt_hash="sha256:" + "b" * 64,
            code_commit="a" * 40, schema_version=1,
        )
        opened = client.execute_intents(
            credential_id=credential, run_mode="paper", intents=[intent],
            data_snapshot=pb2.DataSnapshotDecision(
                snapshot_id="snap_" + "a" * 32, quality_gate="pass", allow_open=True,
                allow_close=True, created_at_ms=int(time.time() * 1000), schema_version=1,
            ),
        )
        assert len(opened.acks) == 1 and opened.acks[0].status == pb2.STATUS_ACCEPTED, wire(opened)
        held_native = client.get_position_states(credential_id=credential, bot_uid=BOT)
        assert len(held_native.positions) == 1, wire(held_native)
        position = held_native.positions[0]
        assert position.state == "protected" and Decimal(position.qty) > 0, wire(position)
        held_orders = client.get_account_order_state(credential_id=credential)
        assert len(held_orders.local.orders.rows) == 2, wire(held_orders)
        assert all(row.classified_state == "working" for row in held_orders.local.orders.rows)
        held_events = drain(client, credential, 0)
        with tempfile.TemporaryDirectory(prefix="paper-protection-reporting-") as directory:
            store = SqliteStateStore(db_path=Path(directory) / "outbox.sqlite")
            store.replace_bot_state(bot_uid=BOT, state={
                "status": "running", "credentialId": credential, "config": {},
            })
            with consumer_runtime(store) as rt, contract_server() as (report_client, observed):
                held = deliver(held_events, store, rt, report_client, observed)
                target = client.get_position_close_target(
                    credential_id=credential, position_id=position.position_id,
                )
                assert target.available, wire(target)
                closed = client.close_position(
                    credential_id=credential, position_id=position.position_id,
                    expected_revision=target.target.revision, expected_boot_id=target.engine_boot_id,
                    request_ref="01ARZ3NDEKTSV4RRFFQ69G5FAX", actor_member_id=73,
                )
                assert closed.request_status == "accepted" and closed.reason_code == "PAPER_FILLED", wire(closed)
                closed_native = client.get_position_states(
                    credential_id=credential, bot_uid=BOT, include_closed=True,
                )
                assert len(closed_native.positions) == 1, wire(closed_native)
                assert closed_native.positions[0].state == "closed", wire(closed_native)
                assert Decimal(closed_native.positions[0].qty) == 0, wire(closed_native)
                closed_orders = client.get_account_order_state(credential_id=credential)
                assert not closed_orders.local.orders.rows, wire(closed_orders)
                assert not closed_orders.local.positions.rows, wire(closed_orders)
                assert not closed_orders.local.protection_orders.rows, wire(closed_orders)
                closed_events = drain(client, credential, max(event.seq for event in held_events))
                exit_phase = deliver(closed_events, store, rt, report_client, observed)
        held_payloads = [(event, json.loads(event.payload_json or b"{}")) for event in held_events]
        entry_fills = [p for e, p in held_payloads if e.event_type == "fill" and p.get("orderRole") == "entry"]
        arms = [p for e, p in held_payloads if e.event_type == "order_ack" and p.get("orderRole") in PROTECTION_ROLES]
        if len(entry_fills) != 1 or {p.get("orderRole") for p in arms} != PROTECTION_ROLES or len(arms) != 2:
            failures.append("held must contain exactly one entry fill and two protection acknowledgements")
        if any(e.event_type == "fill" and p.get("orderRole") in PROTECTION_ROLES for e, p in held_payloads):
            failures.append("arming protection emitted a false fill")
        if any(p.get("fillQty") is not None or p.get("orderStatus") == "FILLED" for p in arms):
            failures.append("protection acknowledgement claims executed quantity or FILLED")
        exit_fills = [json.loads(e.payload_json) for e in closed_events if e.event_type == "fill"]
        if len(exit_fills) != 1 or exit_fills[0].get("orderRole") != "exit" or exit_fills[0].get("action") != "close_long":
            failures.append("manual close must emit exactly one actual exit fill")
        cancels = [json.loads(e.payload_json) for e in closed_events if e.event_type == "cancel"]
        if len(cancels) != 2 or {p.get("orderRole") for p in cancels} != PROTECTION_ROLES:
            failures.append("manual close must retire both armed protection orders via original cancel events")
        for name, phase in (("held", held), ("closed", exit_phase)):
            if phase["batchRejected"] or phase["tradeReportsUnsent"] or not phase["acceptedReports"]:
                failures.append(name + " actual reports failed the production batch schema")
            if phase["unresolved"]["unresolvedCount"]:
                failures.append(name + " has unattributed fills")
        return {
            "credentialId": credential, "botUid": BOT, "runId": RUN, "cycleId": CYCLE,
            "unauthenticatedRejected": True, "transport": "authenticated_loopback_grpc",
            "opened": wire(opened), "heldNative": wire(held_native), "closeTarget": wire(target),
            "closeReceipt": wire(closed), "closedNative": wire(closed_native),
            "held": held, "closed": exit_phase,
            "heldOrderState": wire(held_orders), "closedOrderState": wire(closed_orders),
            "heldReports": held["outboxReports"], "closedReports": exit_phase["outboxReports"],
            "elapsedSeconds": round(time.monotonic() - started, 3), "failures": failures,
            "pythonModules": {m.__name__: str(Path(m.__file__).resolve())
                              for m in (event_consumer, mapper_module)},
            "scope": "native simulation and trade batch schema; MySQL not exercised; risk outbox retained",
        }
    finally:
        client._channel.close()


if __name__ == "__main__":
    with contextlib.redirect_stdout(sys.stderr):
        result = main()
    print(json.dumps(result, sort_keys=True))
    if result["failures"]:
        raise AssertionError("; ".join(result["failures"]))
