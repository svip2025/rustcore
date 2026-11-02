"""Real Python client consumer for the Rust-owned authenticated loopback server."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import grpc

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "services/execution-orchestrator/src"))
from execution_orchestrator.grpc.rustcore_client import RustCoreGrpcClient, RustCoreTarget


def main() -> None:
    target, vector_path = sys.argv[1:]
    vector_bytes = Path(vector_path).read_bytes()
    vector = json.loads(vector_bytes)
    client = RustCoreGrpcClient(target=RustCoreTarget(target), timeout_s=8)
    grpc.channel_ready_future(client._channel).result(timeout=8)
    # Exercise the production auth interceptor in both directions. No private token is printed.
    token = os.environ.pop("RUSTCORE_GRPC_AUTH_TOKEN")
    unauthenticated = RustCoreGrpcClient(target=RustCoreTarget(target), timeout_s=8)
    try:
        unauthenticated.get_open_orders(credential_id=vector["credential_id"])
    except grpc.RpcError as error:
        assert error.code() == grpc.StatusCode.UNAUTHENTICATED, error.code()
    else:
        raise AssertionError("missing auth reached GetOpenOrders")
    finally:
        unauthenticated._channel.close()
        os.environ["RUSTCORE_GRPC_AUTH_TOKEN"] = token

    response = client.get_open_orders(credential_id=vector["credential_id"])
    observed = {order.engine_order_id: order.status for order in response.orders}
    assert set(observed) == set(vector["expected_visible_ids"]), observed
    expected_statuses = {row["engine_order_id"]: row["status"] for row in vector["orders"]}
    assert all(status == expected_statuses[identity] for identity, status in observed.items())
    before_hex = response.SerializeToString().hex()
    refused = client.cancel_order(credential_id="foreign-without-order",
                                   engine_order_id=vector["expected_cancel_ids"][0],
                                   reason="cross-credential-negative")
    assert not refused.accepted and refused.message == "not_found"
    # Resolve the requested cancel targets from the actual Rust-produced response.
    selected = [order for order in response.orders if order.engine_order_id in vector["expected_cancel_ids"]]
    assert len(selected) == len(vector["expected_cancel_ids"])
    canceled = []
    for order in sorted(selected, key=lambda row: row.engine_order_id):
        ack = client.cancel_order(credential_id=order.credential_id,
                                  engine_order_id=order.engine_order_id,
                                  reason="owned-loopback-cleanup")
        assert ack.accepted, ack.message
        truth = client.get_order(credential_id=order.credential_id,
                                 engine_order_id=order.engine_order_id)
        assert truth.found and truth.order.status == "canceled"
        canceled.append(order.engine_order_id)
    after = client.get_open_orders(credential_id=vector["credential_id"])
    assert {row.engine_order_id for row in after.orders} == set(observed) - set(canceled)
    client._channel.close()
    print(json.dumps({"version": 1, "transport": "real_authenticated_loopback_grpc_vnext",
                      "fixture_sha256": hashlib.sha256(vector_bytes).hexdigest(),
                      "get_open_orders_protobuf_hex": before_hex,
                      "observed_statuses": observed, "canceled_engine_order_ids": canceled,
                      "remaining_protobuf_hex": after.SerializeToString().hex(),
                      "unauthenticated_rejected": True,
                      "cross_credential_cancel_refused": True}, sort_keys=True))


if __name__ == "__main__":
    main()
