//! Filesystem RPC transport for the process-level L4 harness.
//!
//! This module is compiled only with `l4-fault-injection`. It lets the L4
//! process harness exercise the production service implementations in
//! sandboxes that prohibit every socket family. Requests and responses are
//! protobuf bytes; a request is claimed by rename before execution so a
//! SIGKILL cannot make a restarted process replay an abandoned RPC.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use prost::Message;
use tonic::Request;

use crate::proto;
use crate::proto::engine_execution_server::EngineExecution as EngineExecutionV1;
use crate::proto_v2;
use crate::proto_v2::engine_execution_server::EngineExecution as EngineExecutionV2;
use crate::svc::EngineExecutionService;
use crate::svc_v2::EngineExecutionV2Service;

const RPC_DIR: &str = "L4_FILE_RPC_DIR";

fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let temporary =
        path.with_extension(format!("tmp-{}-{}", std::process::id(), ulid::Ulid::new()));
    std::fs::write(&temporary, bytes)
        .with_context(|| format!("write temporary L4 RPC file {}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("publish L4 RPC file {}", path.display()))?;
    Ok(())
}

fn response_path(dir: &Path, request_id: &str, suffix: &str) -> PathBuf {
    dir.join(format!("{request_id}.{suffix}"))
}

async fn call<Req, Resp, F, Fut>(bytes: &[u8], invoke: F) -> Result<Vec<u8>, tonic::Status>
where
    Req: Message + Default,
    Resp: Message,
    F: FnOnce(Req) -> Fut,
    Fut: std::future::Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
{
    let request = Req::decode(bytes)
        .map_err(|error| tonic::Status::invalid_argument(format!("decode request: {error}")))?;
    let response = invoke(request).await?.into_inner();
    Ok(response.encode_to_vec())
}

async fn dispatch(
    method: &str,
    bytes: &[u8],
    v1: &EngineExecutionService,
    v2: &EngineExecutionV2Service,
) -> Result<Vec<u8>, tonic::Status> {
    match method {
        "v1_get_open_orders" => {
            call::<proto::GetOpenOrdersRequest, proto::GetOpenOrdersResponse, _, _>(
                bytes,
                |request| v1.get_open_orders(Request::new(request)),
            )
            .await
        }
        "v2_execute_intents" => {
            call::<proto_v2::ExecuteIntentsRequest, proto_v2::ExecuteIntentsResponse, _, _>(
                bytes,
                |request| v2.execute_intents(Request::new(request)),
            )
            .await
        }
        "v2_reconcile_now" => {
            call::<proto_v2::ReconcileNowRequest, proto_v2::ReconcileNowResponse, _, _>(
                bytes,
                |request| v2.reconcile_now(Request::new(request)),
            )
            .await
        }
        "v2_set_kill_switch" => {
            call::<proto_v2::SetKillSwitchRequest, proto_v2::SetKillSwitchResponse, _, _>(
                bytes,
                |request| v2.set_kill_switch(Request::new(request)),
            )
            .await
        }
        "v2_reset_kill_switch" => {
            call::<proto_v2::ResetKillSwitchRequest, proto_v2::ResetKillSwitchResponse, _, _>(
                bytes,
                |request| v2.reset_kill_switch(Request::new(request)),
            )
            .await
        }
        "v2_get_position_states" => {
            call::<proto_v2::GetPositionStatesRequest, proto_v2::GetPositionStatesResponse, _, _>(
                bytes,
                |request| v2.get_position_states(Request::new(request)),
            )
            .await
        }
        "v2_get_order_by_client_order_id" => {
            call::<
                proto_v2::GetOrderByClientOrderIdRequest,
                proto_v2::GetOrderByClientOrderIdResponse,
                _,
                _,
            >(bytes, |request| {
                v2.get_order_by_client_order_id(Request::new(request))
            })
            .await
        }
        "v2_release_pending" => {
            call::<proto_v2::ReleasePendingRequest, proto_v2::ReleasePendingResponse, _, _>(
                bytes,
                |request| v2.release_pending(Request::new(request)),
            )
            .await
        }
        "v2_fund_simulation_account" => {
            call::<
                proto_v2::FundSimulationAccountRequest,
                proto_v2::FundSimulationAccountResponse,
                _,
                _,
            >(bytes, |request| {
                v2.fund_simulation_account(Request::new(request))
            })
            .await
        }
        "v2_get_account_state" => {
            call::<proto_v2::GetAccountStateRequest, proto_v2::GetAccountStateResponse, _, _>(
                bytes,
                |request| v2.get_account_state(Request::new(request)),
            )
            .await
        }
        "v2_put_risk_budget" => {
            call::<proto_v2::PutRiskBudgetRequest, proto_v2::PutRiskBudgetResponse, _, _>(
                bytes,
                |request| v2.put_risk_budget(Request::new(request)),
            )
            .await
        }
        "v2_put_promotion_certification" => {
            call::<
                proto_v2::PutPromotionCertificationRequest,
                proto_v2::PutPromotionCertificationResponse,
                _,
                _,
            >(bytes, |request| {
                v2.put_promotion_certification(Request::new(request))
            })
            .await
        }
        _ => Err(tonic::Status::unimplemented(format!(
            "unknown L4 file RPC method {method}"
        ))),
    }
}

pub(crate) struct PreparedFileRpc {
    dir: PathBuf,
}

pub(crate) fn prepare_if_configured() -> anyhow::Result<Option<PreparedFileRpc>> {
    let raw = match std::env::var(RPC_DIR) {
        Ok(raw) if !raw.trim().is_empty() => raw,
        Ok(_) | Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => bail!("L4_FILE_RPC_DIR must be valid Unicode"),
    };
    crate::fault_injection::validate_file_transport()?;
    let dir = PathBuf::from(raw.trim());
    // validate_file_transport has already checked both required absolute paths.
    std::fs::create_dir_all(&dir).context("prepare L4 file RPC directory failed")?;
    std::fs::read_dir(&dir).context("read prepared L4 file RPC directory failed")?;
    Ok(Some(PreparedFileRpc { dir }))
}

pub(crate) async fn serve_prepared(
    prepared: &PreparedFileRpc,
    v1: EngineExecutionService,
    v2: EngineExecutionV2Service,
) -> anyhow::Result<()> {
    let dir = &prepared.dir;
    atomic_write(
        &dir.join("ready"),
        format!("pid={}\n", std::process::id()).as_bytes(),
    )?;

    loop {
        let mut requests = std::fs::read_dir(&dir)
            .with_context(|| format!("read L4 file RPC directory {}", dir.display()))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "req"))
            .collect::<Vec<_>>();
        requests.sort();

        for request_path in requests {
            let Some(name) = request_path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let mut parts = name.split('.');
            let Some(request_id) = parts.next() else {
                continue;
            };
            let Some(method) = parts.next() else {
                continue;
            };
            if parts.next() != Some("req") || parts.next().is_some() {
                continue;
            }
            let claimed = dir.join(format!(
                "{request_id}.{method}.working-{}",
                std::process::id()
            ));
            if std::fs::rename(&request_path, &claimed).is_err() {
                continue;
            }
            let bytes = match std::fs::read(&claimed) {
                Ok(bytes) => bytes,
                Err(error) => {
                    atomic_write(
                        &response_path(&dir, request_id, "err"),
                        format!("INTERNAL: read claimed request failed: {error}").as_bytes(),
                    )?;
                    continue;
                }
            };
            match dispatch(method, &bytes, &v1, &v2).await {
                Ok(response) => {
                    atomic_write(&response_path(&dir, request_id, "ok"), &response)?;
                }
                Err(status) => {
                    atomic_write(
                        &response_path(&dir, request_id, "err"),
                        format!("{:?}: {}", status.code(), status.message()).as_bytes(),
                    )?;
                }
            }
            let _ = std::fs::remove_file(claimed);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
