"""Consume actual Rust REST-fill events through authenticated gRPC and reporting.

The input side belongs to Rust. This consumer must not construct or repair any
EngineEvent payload. This is a reporting bridge, not an exchange recovery test.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
from pathlib import Path
import sys
import tempfile
import sqlite3

import grpc

ROOT = Path(__file__).resolve().parents[4]
# Rust deliberately clears the subprocess environment, so pytest.ini cannot
# supply the package roots needed by the actual consumer's server runtime.
SOURCE_ROOTS = {
    "execution_orchestrator": "services/execution-orchestrator/src",
    "agent_runtime": "services/agent-runtime/src",
    "ai_trade_baseline_strategy": "packages/baseline-strategy/python",
    "ai_trade_data_plane": "packages/data-plane/python",
    "ai_trade_indicators": "packages/indicators/python",
    "ai_trade_contracts": "packages/contracts/python",
}
sys.path[:0] = [str(ROOT / relative) for relative in SOURCE_ROOTS.values()] + [
    str(ROOT / "services/execution-orchestrator/tests")
]
from execution_orchestrator import event_consumer
from execution_orchestrator.reporting import event_mapper as mapper_module
from execution_orchestrator.storage import sqlite_store as store_module
from execution_orchestrator.grpc.rustcore_client import (
    RustCoreGrpcClient,
    RustCoreTarget,
)
from execution_orchestrator.reporting.event_mapper import EngineEventMapper
from execution_orchestrator.reporting.runtime import flush_outbox_once
from execution_orchestrator.storage.sqlite_store import SqliteStateStore
from fixtures.fill_reporting_bridge import consumer_runtime, contract_server


def main() -> None:
    for module in (event_consumer, mapper_module, store_module):
        assert Path(module.__file__).resolve().is_relative_to(ROOT), module.__file__
    target, vector_path = sys.argv[1:]
    vector_bytes = Path(vector_path).read_bytes()
    vector = json.loads(vector_bytes)
    credential = vector["credential_id"]
    client = RustCoreGrpcClient(target=RustCoreTarget(target), timeout_s=8)
    grpc.channel_ready_future(client._channel).result(timeout=8)
    token = os.environ.pop("RUSTCORE_GRPC_AUTH_TOKEN")
    anonymous = RustCoreGrpcClient(target=RustCoreTarget(target), timeout_s=8)
    try:
        next(anonymous.stream_events(credential_id=credential, since_seq=0))
    except grpc.RpcError as error:
        assert error.code() == grpc.StatusCode.UNAUTHENTICATED, error.code()
    else:
        raise AssertionError("unauthenticated client reached StreamEvents")
    finally:
        anonymous._channel.close()
        os.environ["RUSTCORE_GRPC_AUTH_TOKEN"] = token

    expected = {row["trade_id"]: row for row in vector["cases"]}
    stream = client.stream_events(credential_id=credential, since_seq=0)
    events = list(itertools.islice(stream, len(expected)))
    stream.close()
    assert len(events) == len(expected)
    observations = []
    mapper = EngineEventMapper()
    with tempfile.TemporaryDirectory(prefix="rest-fill-reporting-") as directory:
        store = SqliteStateStore(db_path=Path(directory) / "outbox.sqlite")
        for event in events:
            assert event.event_type == "fill"
            payload = json.loads(event.payload_json)
            case = expected[payload["tradeId"]]
            assert payload["source"] == "binance_rest_backfill"
            if case.get("external"):
                # Preserve the factual event. Its missing bot identity is an
                # existing reporting gap, not permission to invent ownership.
                assert event.bot_uid == ""
                assert event.idempotency_key == ""
                assert "action" not in payload and "side" not in payload
            else:
                assert event.bot_uid == vector["bot_uid"]
                assert payload.get("action") == case["action"], (
                    case,
                    payload.get("action"),
                )
                assert payload.get("side") == case["side"], (case, payload.get("side"))
                assert payload["trade"]["side"] == case["exchange_side"]
            with consumer_runtime(store):
                for _ in range(2):
                    event_consumer._persist_event(
                        event=event,
                        credential_id=credential,
                        mapper=mapper,
                        state_db=store,
                    )
            observations.append(
                {
                    "external": bool(case.get("external")),
                    "action": payload.get("action"),
                    "side": payload.get("side"),
                    "protobuf_hex": event.SerializeToString().hex(),
                }
            )
        queued = store.fetch_due_outbox(kind="trade", limit=20)
        assert len(queued) == len(expected) - 1
        assert store.load_checkpoint(credential_id=credential) == len(expected)
        for item in queued:
            case = expected[item.payload["executionTradeId"]]
            if not case.get("external"):
                assert (item.payload["action"], item.payload["side"]) == (
                    case["action"],
                    case["side"],
                )
        reports = [item.payload for item in queued]
        restarted = SqliteStateStore(db_path=store.db_path)
        with consumer_runtime(restarted) as runtime, contract_server() as (
            report_client,
            observed,
        ):
            assert flush_outbox_once(
                rt=runtime, store=restarted, client=report_client, batch_size=20
            )
            accepted = observed()
        assert accepted["rejected"] == 0
        assert Path(accepted["schemaBinding"]["path"]).resolve() == (
            ROOT / "packages/shared/src/executionContract.ts"
        )
        accepted_reports = accepted["accepted"][0]["trades"]
        assert {item["executionTradeId"] for item in accepted_reports} == {
            tid for tid, case in expected.items() if not case.get("external")
        }
        for item in accepted_reports:
            case = expected[item["executionTradeId"]]
            assert (item["action"], item["side"]) == (case["action"], case["side"])
        unresolved = restarted.unattributed_fill_status()
        assert unresolved["status"] == "degraded" and unresolved["unresolvedCount"] == 1
        with sqlite3.connect(store.db_path) as con:
            assert con.execute("SELECT COUNT(*) FROM orch_engine_events").fetchone()[
                0
            ] == len(expected)
            assert (
                con.execute(
                    "SELECT COUNT(*) FROM orch_outbox WHERE sent_at_ms IS NOT NULL"
                ).fetchone()[0]
                == len(expected) - 1
            )
            row = con.execute(
                "SELECT payload_json,sent_at_ms,attempts FROM orch_outbox WHERE kind='unattributed_fill'"
            ).fetchone()
            assert row is not None and row[1] is None and row[2] == 0
            retained = json.loads(row[0])
            assert retained["sourceEvent"]["bot_uid"] == ""
            assert "missing_bot_uid" in retained["reasonCodes"]
    client._channel.close()
    source_modules = {}
    for name, relative in SOURCE_ROOTS.items():
        module_path = Path(sys.modules[name].__file__).resolve()
        assert module_path.is_relative_to(ROOT / relative), str(module_path)
        source_modules[name] = str(module_path)
    print(
        json.dumps(
            {
                "fixture_sha256": hashlib.sha256(vector_bytes).hexdigest(),
                "unauthenticated_rejected": True,
                "transport": "authenticated_loopback_grpc",
                "events": observations,
                "outbox_reports": reports,
                "accepted_reports": accepted_reports,
                "python_modules": {
                    "consumer": event_consumer.__file__,
                    "mapper": mapper_module.__file__,
                    "store": store_module.__file__,
                },
                "python_source_modules": source_modules,
                "schema_binding": accepted["schemaBinding"],
                "actual_batch_schema_rejected": accepted["rejected"],
                "unattributed_fill_retained_unsent": True,
                "unresolved": unresolved,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
