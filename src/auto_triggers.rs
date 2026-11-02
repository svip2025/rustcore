//! 自动安全触发源（行情中断 / 订单拒绝率）。
//!
//! engine safety model §4.2 的触发源表里，四类自动源里已经接了
//! `daily_loss` / `drawdown` / `reconcile_diff` / `risk_budget` 陈旧，缺的是
//! **行情中断 → 等效 L1**、**LLM 连续失败 → 等效 L1**、**订单拒绝率 → L2**
//! （the roadmap 的 `T4.C1` 与 the architecture
//! 都如实登记着"仍未实现"）。
//!
//! 本模块实现其中**两条判据活在引擎侧**的：行情中断与订单拒绝率。
//! LLM 连续失败的信号只有编排器看得到（rustcore 不调用任何 LLM），
//! 由 `execution-orchestrator` 侧的 streak latch 经 `SetKillSwitch` RPC 接进来。
//!
//! ## 三条设计纪律
//!
//! 1. **判据与取数分离**：下面两个 `*_decision` 是纯函数，不读时钟、不碰数据库，
//!    因此可以被逐条变异打红；取数在 `collect_*` 里，只做查询不做判断。
//! 2. **不新增拦截面**：本模块只决定"什么时候按已有的按钮"。三层语义、
//!    reduce-only / 保护单 / 对账永远可用这条不变量，仍由
//!    `risk-policy` 的 `!open → not_open` 保证（`crates/risk-policy/src/lib.rs`）。
//! 3. **锁存交给 `arm_in_tx`**：它本身是**只升不降 + 幂等**的（当前档位严重度
//!    ≥ 请求档位时直接返回，不重复写 `kill_switch_events`），所以这里不需要
//!    另造一份 latch 状态；恢复必须人工 reset（SAFETY-MODEL §4.4）。

use std::time::Duration;

use sqlx::Row;

use crate::kill_switch::{self, ArmRequest, Level};
use crate::store::Store;
use crate::venue::CredentialKind;

/// 行情中断阈值的**下界**。真正的阈值是 `max(3 根 bar, 本下界)`，**逐流**计算。
/// 3 分钟 = 1m 周期连丢 3 根；比这更短会把网络抖动与交易所偶发延迟当成中断。
/// 固定 180s 套在所有周期上是 设计要求 明令禁止的（段级评审 regression）。
pub const DEFAULT_MARKET_DATA_INTERRUPTION_FLOOR_MS: i64 = 180_000;
/// 订单拒绝率的统计窗口。
pub const DEFAULT_ORDER_REJECT_WINDOW_MS: i64 = 600_000;
/// 窗口内样本下限。低于它不判定——一次失败不等于 100% 拒绝率。
pub const DEFAULT_ORDER_REJECT_MIN_SAMPLES: i64 = 5;
/// 触发档位的失败率（百分数）。
pub const DEFAULT_ORDER_REJECT_RATE_PCT: i64 = 50;

pub const TRIGGER_SOURCE_MARKET_DATA: &str = "auto_market_data_interruption";
pub const TRIGGER_SOURCE_ORDER_REJECT_RATE: &str = "auto_order_reject_rate";
/// 编排器侧的 LLM 连续失败 latch 用这个 source 经 RPC 接进来。
pub const TRIGGER_SOURCE_LLM_FAILURES: &str = "auto_llm_failures";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoTriggerDecision {
    pub level: Level,
    pub reason: String,
}

/// 一条 market stream 的身份与最近心跳。
///
/// ****：
/// 首版把"该 credential 下**任意** `bar_closed` 的 `MAX(ts_ms)`"当成行情新鲜度，
/// 于是 BTC/1m 健康就能**永久掩盖** ETH/1m 中断；阈值又是固定 180s，
/// 一条纯 1h 周期的流会在 180 秒后被误判中断。
/// 裁决要求按**真实 stream identity（至少 credential + symbol + timeframe）**判定，
/// 且阈值必须跟着周期走。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketStreamHeartbeat {
    pub symbol_key: String,
    pub timeframe: String,
    pub last_bar_ms: i64,
}

/// 把 `1m` / `5m` / `1h` / `4h` / `1d` 之类的周期解析成毫秒。
/// 认不出来的周期返回 `None`——**不猜**，由调用方按"无法判定"处理。
pub fn timeframe_to_ms(timeframe: &str) -> Option<i64> {
    let raw = timeframe.trim().to_lowercase();
    let (value, unit) = raw.split_at(raw.len().checked_sub(1)?);
    let value: i64 = value.parse().ok()?;
    if value <= 0 {
        return None;
    }
    let unit_ms = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return None,
    };
    value.checked_mul(unit_ms)
}

/// 某条流的中断阈值：**3 根 bar**，但不低于下界。
/// 3 根是"连丢三根才算断"，下界防 1m 以下周期的抖动误报。
pub fn interruption_threshold_ms(timeframe: &str, floor_ms: i64) -> Option<i64> {
    let bar_ms = timeframe_to_ms(timeframe)?;
    Some(bar_ms.saturating_mul(3).max(floor_ms))
}

/// 一条**被要求存在**的行情流，以及它是从什么时候起被要求的。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredStream {
    pub symbol_key: String,
    pub timeframe: String,
    /// **首次**被要求的时刻。重复 ensure 不刷新——刷新会让一条从未起来的流
    /// 每次重试都把计时清零，永远够不到阈值。
    pub requested_at_ms: i64,
}

/// 行情中断判定所需的事实。全部来自权威表，**没有一项是自报的**。
#[derive(Clone, Debug)]
pub struct MarketDataFacts {
    pub credential_id: String,
    pub kind: CredentialKind,
    /// 该 credential **逐条 stream** 的最近心跳（只含真的收到过 bar 的流）。
    pub streams: Vec<MarketStreamHeartbeat>,
    /// 该 credential **被要求存在**的流（订阅真值集合）。
    ///
    /// 只有观测面时，
    /// "应该存在但从未出现过的流"根本不在判据的输入集合里——
    /// `streams=[]` 遍历空集返回 None，触发器对"从一开始就死"的流失明。
    pub required: Vec<RequiredStream>,
    /// 该 credential 当前是否有敞口（未平仓位或未终态订单）。
    pub has_exposure: bool,
}

/// 行情中断 → `pause_new`（SAFETY-MODEL §4.2「行情中断 > T → 等效 L1」）。
///
/// 三条**误触发边界**，每条都能被单独变异打红：
/// 1. **模拟凭据永不触发**——按安全边界模拟凭据没有 venue，kline 流本来就起不来
///    ，拿它当"中断"会在模拟环境里全量误报。
/// 2. **从未收到过 bar ≠ 中断**——必须先观察到过至少一条 `bar_closed` 才开始计时。
/// 3. **无敞口不触发**——没有仓位也没有在途订单时没有要保护的东西，
///    arm 一个需要人工 reset 的开关只会制造噪声。
pub fn market_data_interruption_decision(
    facts: &MarketDataFacts,
    now_ms: i64,
    floor_ms: i64,
) -> Option<AutoTriggerDecision> {
    if facts.kind != CredentialKind::ExchangeBacked {
        return None;
    }
    // **结构性死流必须可处置。**
    //
    // 一条不通过当前闭世界的 required 行（历史 `7m`、`okx|spot|…`、币本位
    // settle、认不出周期的行）**永远**不会被起成 worker，也就永远不会产出
    // 一根 bar。R10 之后启动重建会拒绝它，可判据这边只 `warn!` + `skip`——
    // 于是它既不产 bar、也不 arm 任何东西，正好落进两条路径中间的静默区。
    //
    // 三点刻意的处置：
    // 1. **不等阈值**。等待是为了区分"暂时没来"与"断了"；一条结构上不可能
    //    产 bar 的流，等多久答案都一样。
    // 2. **不受 `has_exposure` 约束**，因此这一段放在敞口门**之前**：
    //    没有敞口恰恰是最该拦住新开仓的时候——这条流一直不来行情，
    //    bot 会拿着陈旧/缺失的数据开新仓（engine safety contract「行情陈旧仍开仓」）。
    // 3. 仍受模拟凭据门约束：模拟凭据本来就不起 kline 流，对它判"死流"
    //    会在模拟环境里全量误报。
    let mut dead: Option<(String, String)> = None;
    for required in &facts.required {
        let Some(reason) = crate::market_data::persisted_subscription_rejection(
            &required.symbol_key,
            &required.timeframe,
        ) else {
            continue;
        };
        let identity = format!("{}|{}", required.symbol_key, required.timeframe);
        // 取字典序最小的一条，保证同一份事实得到同一条裁决（可重放）。
        match &dead {
            Some((worst_identity, _)) if worst_identity.as_str() <= identity.as_str() => {}
            _ => dead = Some((identity, reason)),
        }
    }
    if let Some((identity, reason)) = dead {
        return Some(AutoTriggerDecision {
            level: Level::PauseNew,
            reason: format!(
                "market data subscription {identity} can never start under the current \
                 closed world: {reason}"
            ),
        });
    }
    if !facts.has_exposure {
        return None;
    }
    // **逐流判定**：任意一条已知流断了就算中断——健康的流不得掩盖已死的流。
    // 报出的是**具体哪一条**，运维不必再去猜是哪个 symbol/周期。
    let mut worst: Option<(String, i64, i64)> = None;
    let mut never_seen = false;
    for stream in &facts.streams {
        let Some(threshold_ms) = interruption_threshold_ms(&stream.timeframe, floor_ms) else {
            // 认不出的周期**不判**（不猜阈值），但要留痕。
            tracing::warn!(
                credential_id = %facts.credential_id,
                symbol_key = %stream.symbol_key,
                timeframe = %stream.timeframe,
                "unrecognised timeframe; market-data interruption not evaluated for this stream"
            );
            continue;
        };
        let gap_ms = now_ms.saturating_sub(stream.last_bar_ms);
        if gap_ms <= threshold_ms {
            continue;
        }
        let over = gap_ms.saturating_sub(threshold_ms);
        let identity = format!("{}|{}", stream.symbol_key, stream.timeframe);
        match &worst {
            Some((_, _, worst_over)) if *worst_over >= over => {}
            _ => worst = Some((identity, gap_ms, threshold_ms)),
        }
        let _ = over;
    }
    // **required − observed**：被要求存在、却从未产出过 bar 的流
    // （段级评审 regression / regression，High）。
    //
    // 这一段就是"从一开始就死"的那类流唯一能被发现的地方。判据与上面逐流陈旧
    // 判定**同一条阈值**，只是起点不同：陈旧从"最近一根 bar"起算，
    // never-seen 从"首次被要求"起算。两者都不猜阈值，认不出的周期一律不判。
    //
    // 误触发边界仍然守住：模拟凭据、无敞口在函数开头就返回了；
    // 刚下发的订阅在阈值内也不算——起流本来就需要时间。
    for required in &facts.required {
        let observed = facts.streams.iter().any(|stream| {
            stream.symbol_key == required.symbol_key && stream.timeframe == required.timeframe
        });
        if observed {
            continue;
        }
        let Some(threshold_ms) = interruption_threshold_ms(&required.timeframe, floor_ms) else {
            tracing::warn!(
                credential_id = %facts.credential_id,
                symbol_key = %required.symbol_key,
                timeframe = %required.timeframe,
                "unrecognised timeframe; required-but-never-seen stream not evaluated"
            );
            continue;
        };
        let waited_ms = now_ms.saturating_sub(required.requested_at_ms);
        if waited_ms <= threshold_ms {
            continue;
        }
        let over = waited_ms.saturating_sub(threshold_ms);
        let identity = format!("{}|{}", required.symbol_key, required.timeframe);
        match &worst {
            Some((_, _, worst_over)) if *worst_over >= over => {}
            _ => {
                worst = Some((identity, waited_ms, threshold_ms));
                never_seen = true;
            }
        }
    }

    let (identity, gap_ms, threshold_ms) = worst?;
    Some(AutoTriggerDecision {
        level: Level::PauseNew,
        reason: if never_seen {
            // 措辞必须能把两类分开：运维看到"从未收到过任何 bar"会去查订阅/
            // 网络起没起来，看到"陈旧"会去查流为什么停了。混成一句会误导排查。
            format!(
                "market data never started on {identity}: required for {gap_ms}ms without a single bar_closed (> {threshold_ms}ms)"
            )
        } else {
            format!(
                "market data interrupted on {identity}: no bar_closed for {gap_ms}ms (> {threshold_ms}ms)"
            )
        },
    })
}

/// 一条事件相对于"订单提交结果"的归属。
///
/// **返修（独立评审 regression，High）**：首版按
/// `event_type IN ('order_ack','reject','error')` 无差别计数，而
/// `event_type='reject'` **同时承载风控裁决拒绝**（`svc.rs` 的
/// `verdict_event_payload`，三处），`event_type='error'` 还承载
/// **本地 position_state 孤儿告警**（`svc.rs` 的 `position_state_orphan_recovery`，
/// `severity=warning`）。于是一个被风控持续正确拒绝的 bot，配上同 credential 上
/// 正常 ack 的单，就能把 credential 级 `manage_only` 按下去，撤掉**别的 bot**
/// 的未成交入场单——直接击穿 `T4.C1`「订单拒绝率必须区分策略性正常拒绝与
/// 交易所/系统异常拒绝」。
///
/// 归属判据按**闭世界正向证明**写：必须能证明它是提交结果，才计入；
/// 证明不了的一律 `NotSubmission`（不进分子也不进分母）。
/// 这与 `venue::is_wellformed_exchange_credential_id`「证明它是交易所凭据」
/// 而不是「排除已知的 paper 拼法」是同一条纪律——黑名单式判别在本仓已被判定为反模式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionOutcome {
    /// 交易所受理。
    Accepted,
    /// 提交路径上的拒绝（交易所拒绝、venue 路由拒绝）。
    Rejected,
    /// 提交路径上的错误（交易所或系统错误）。
    Errored,
    /// **不是**提交结果：风控裁决拒绝、本地告警、bar_closed 等。
    NotSubmission,
}

/// 判定一条事件是否属于订单提交结果，以及属于哪一类。
///
/// - `order_ack`：全部三个 emit 点都在提交路径上（`svc.rs` 的两处 ack 与 algo ack），
///   直接算受理。
/// - `reject`：必须**带 `rejectReasonCode`** 且**不带 `riskVerdict`**。
///   风控裁决拒绝的载荷带 `riskVerdict`（`verdict_event_payload`），据此排除。
/// - `error`：必须**带顶层 `error` 字段**且**不带 `severity`**。
///   孤儿告警的载荷带 `severity`/`source`，据此排除。
/// - 载荷缺失或结构不认识 → `NotSubmission`。
///   **不计**比**误计**保守：本触发源 arm 的是需要人工 reset 的 `manage_only`，
///   把不认识的事件算成失败会让运维为一次未知载荷去手工复位整条 credential。
///   该边界已登记，新增提交结果事件时必须同批扩这里。
pub fn classify_submission_outcome(
    event_type: &str,
    payload: Option<&serde_json::Value>,
) -> SubmissionOutcome {
    match event_type {
        "order_ack" => SubmissionOutcome::Accepted,
        "reject" => {
            let Some(payload) = payload.and_then(|value| value.as_object()) else {
                return SubmissionOutcome::NotSubmission;
            };
            if payload.contains_key("riskVerdict") {
                return SubmissionOutcome::NotSubmission;
            }
            if payload
                .get("rejectReasonCode")
                .and_then(|value| value.as_str())
                .is_some_and(|code| !code.trim().is_empty())
            {
                SubmissionOutcome::Rejected
            } else {
                SubmissionOutcome::NotSubmission
            }
        }
        "error" => {
            let Some(payload) = payload.and_then(|value| value.as_object()) else {
                return SubmissionOutcome::NotSubmission;
            };
            if payload.contains_key("severity") {
                return SubmissionOutcome::NotSubmission;
            }
            if payload.contains_key("error") {
                SubmissionOutcome::Errored
            } else {
                SubmissionOutcome::NotSubmission
            }
        }
        _ => SubmissionOutcome::NotSubmission,
    }
}

/// 订单拒绝率判定所需的事实：窗口内**提交结果事件**的三种计数。
///
/// 计数只收 `classify_submission_outcome` 认定的提交结果——
/// **风控裁决拒绝与本地告警都不在其中**（`T4.C1` 明令要区分的"策略性正常拒绝"）。
#[derive(Clone, Debug)]
pub struct OrderRejectFacts {
    #[allow(
        dead_code,
        reason = "Retained internal fact field for caller attribution."
    )]
    pub credential_id: String,
    pub acks: i64,
    pub rejects: i64,
    pub errors: i64,
    /// 本轮扫描是否触到行上限。**必须是数据面的事实而不只是一行日志**——
    /// 静默截断会让"这个比率覆盖了全窗口"的结论悄悄失真，而日志没人能拿来断言。
    #[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
    pub truncated: bool,
}

/// 订单拒绝率 → `manage_only`（SAFETY-MODEL §4.2「订单拒绝率 > X% / 窗口 → L2」）。
pub fn order_reject_rate_decision(
    facts: &OrderRejectFacts,
    min_samples: i64,
    rate_pct: i64,
) -> Option<AutoTriggerDecision> {
    let failures = facts.rejects.saturating_add(facts.errors);
    let total = facts.acks.saturating_add(failures);
    if total < min_samples {
        return None;
    }
    // 整数比较，避免浮点在边界上抖动：failures/total >= rate_pct/100。
    if failures.saturating_mul(100) < rate_pct.saturating_mul(total) {
        return None;
    }
    Some(AutoTriggerDecision {
        level: Level::ManageOnly,
        reason: format!(
            "order reject rate {failures}/{total} >= {rate_pct}% in window (rejects={}, errors={})",
            facts.rejects, facts.errors
        ),
    })
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

async fn candidate_credentials(store: &Store) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT credential_id FROM risk_budget UNION SELECT credential_id FROM engine_orders UNION SELECT credential_id FROM position_state",
    )
    .fetch_all(store.pool())
    .await?;
    let mut credentials = rows
        .into_iter()
        .map(|row| row.get::<String, _>("credential_id"))
        .collect::<Vec<_>>();
    credentials.sort();
    Ok(credentials)
}

async fn has_exposure(store: &Store, credential_id: &str) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "SELECT (SELECT COUNT(*) FROM position_state WHERE credential_id=? AND state <> 'closed') + (SELECT COUNT(*) FROM engine_orders WHERE credential_id=? AND status IN ('pending_submit','submitted','acked','partial_filled','unknown')) AS n",
    )
    .bind(credential_id)
    .bind(credential_id)
    .fetch_one(store.pool())
    .await?;
    Ok(row.get::<i64, _>("n") > 0)
}

/// 登记一条**被要求存在**的行情流（订阅真值集合的唯一写入点）。
///
/// 。
/// **首次要求的时刻不刷新**：重复 ensure 若刷新它，一条从未起来的流每次重试
/// 都把计时清零，永远够不到阈值——那等于这条判据不存在。
/// **整批**登记被要求存在的行情流——一个事务，全成或全不成。
///
/// R7 是逐条登记，任一条失败
/// 立即返回、已写前缀不回滚，而编排器 catch 掉 RPC 失败后仍把 bot 置 running。
/// 于是未登记的后缀**既没有 stream task、也不在 required 集合里**——
/// 行情缺失无法 fail-closed，正好是这条判据要防的事。
///
/// 宁可整批拒绝，也不留"半个订阅集合"：半个集合是**判据看不见的状态**，
/// 比明确失败危险得多。
pub(crate) async fn record_required_streams_atomically(
    store: &Store,
    credential_id: &str,
    streams: &[(String, String)],
    now_ms: i64,
) -> Result<(), sqlx::Error> {
    if streams.is_empty() {
        return Ok(());
    }
    let mut tx = store.pool().begin().await?;
    for (symbol_key, timeframe) in streams {
        sqlx::query(required_stream_upsert_sql(store))
            .bind(credential_id.trim())
            .bind(symbol_key.trim())
            .bind(timeframe.trim().to_lowercase())
            .bind(now_ms)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

/// 登记语句的**唯一一份**（生产与整批路径共用，避免两处各写各的 upsert）。
fn required_stream_upsert_sql(store: &Store) -> &'static str {
    match store.kind() {
        crate::store::DbKind::MySql => {
            "INSERT INTO market_stream_subscriptions (credential_id, symbol_key, timeframe, requested_at_ms) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE requested_at_ms=requested_at_ms"
        }
        _ => {
            "INSERT INTO market_stream_subscriptions (credential_id, symbol_key, timeframe, requested_at_ms) VALUES (?,?,?,?) ON CONFLICT(credential_id, symbol_key, timeframe) DO NOTHING"
        }
    }
}

#[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
pub(crate) async fn record_required_stream(
    store: &Store,
    credential_id: &str,
    symbol_key: &str,
    timeframe: &str,
    now_ms: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(required_stream_upsert_sql(store))
        .bind(credential_id.trim())
        .bind(symbol_key.trim())
        .bind(timeframe.trim().to_lowercase())
        .bind(now_ms)
        .execute(store.pool())
        .await?;
    Ok(())
}

/// 一条跨 credential 的 required 订阅（重启重建执行体时用）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PersistedRequiredStream {
    pub credential_id: String,
    pub symbol_key: String,
    pub timeframe: String,
    pub requested_at_ms: i64,
}

/// 取**全部** credential 的 required 订阅。
///
/// rustcore 重启后 required 真值
/// 还在库里，执行态却没了。启动路径按这份集合重建 kline worker
/// （`MarketDataManager::restore_required_streams`）。**只读**——
/// `requested_at_ms` 是首次被要求的时刻，重启不得把它刷新。
pub(crate) async fn all_required_streams(
    store: &Store,
) -> Result<Vec<PersistedRequiredStream>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT credential_id, symbol_key, timeframe, \
         CAST(requested_at_ms AS CHAR) AS requested_at_ms_text \
         FROM market_stream_subscriptions ORDER BY credential_id, symbol_key, timeframe",
    )
    .fetch_all(store.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(PersistedRequiredStream {
                credential_id: row.get::<String, _>("credential_id"),
                symbol_key: row.get::<String, _>("symbol_key"),
                timeframe: row.get::<String, _>("timeframe"),
                // **sqlx-i64 核查批**：见 `store::try_get_i64_text`。
                requested_at_ms: crate::store::try_get_i64_text(&row, "requested_at_ms_text")?,
            })
        })
        .collect()
}

/// 取该 credential **被要求存在**的所有流。
pub(crate) async fn required_streams(
    store: &Store,
    credential_id: &str,
) -> Result<Vec<RequiredStream>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT symbol_key, timeframe, CAST(requested_at_ms AS CHAR) AS requested_at_ms_text FROM market_stream_subscriptions WHERE credential_id=? ORDER BY symbol_key, timeframe",
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(RequiredStream {
                symbol_key: row.get::<String, _>("symbol_key"),
                timeframe: row.get::<String, _>("timeframe"),
                // **sqlx-i64 核查批**：见 `store::try_get_i64_text`。
                requested_at_ms: crate::store::try_get_i64_text(&row, "requested_at_ms_text")?,
            })
        })
        .collect()
}

/// 取该 credential **每条已知 stream** 的最近心跳。
///
/// 数据来自 `market_stream_health`（由 kline 收盘路径 upsert），
/// 而不是对事件表取 `MAX(ts_ms)`——后者会让健康的流掩盖已死的流
/// （段级评审 regression）。表里没有的流 = 从未收到过 bar，
/// 不参与判定（"从未有过 bar ≠ 中断"）。
async fn market_stream_heartbeats(
    store: &Store,
    credential_id: &str,
) -> Result<Vec<MarketStreamHeartbeat>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT symbol_key, timeframe, CAST(last_bar_ms AS CHAR) AS last_bar_ms_text FROM market_stream_health WHERE credential_id=? ORDER BY symbol_key, timeframe",
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(MarketStreamHeartbeat {
                symbol_key: row.get::<String, _>("symbol_key"),
                timeframe: row.get::<String, _>("timeframe"),
                // **sqlx-i64 核查批· 行情中断判据的输入。**
                //
                // 旧 sqlx 0.7.4 `Any` + SQLite 会把 `last_bar_ms` 截成低 32 位；
                // 0.8.6 已修复。若将来回归，`gap_ms = now_ms - last_bar_ms`
                // 恒在 1.7e12 量级、恒超任何周期阈值——**每一条流在每一轮都被
                // 判成"行情中断"**，而这条判据是 auto-trigger 的输入。
                last_bar_ms: crate::store::try_get_i64_text(&row, "last_bar_ms_text")?,
            })
        })
        .collect()
}

/// 单轮扫描最多读多少条候选事件。触到上限时**必须记日志**——
/// 静默截断会让"覆盖了全窗口"的结论悄悄失真（无声上限是本仓明令禁止的）。
const REJECT_SCAN_ROW_LIMIT: i64 = 5_000;

async fn reject_facts(
    store: &Store,
    credential_id: &str,
    since_ms: i64,
) -> Result<OrderRejectFacts, sqlx::Error> {
    reject_facts_with_limit(store, credential_id, since_ms, REJECT_SCAN_ROW_LIMIT).await
}

async fn reject_facts_with_limit(
    store: &Store,
    credential_id: &str,
    since_ms: i64,
    row_limit: i64,
) -> Result<OrderRejectFacts, sqlx::Error> {
    // 归属判据活在**载荷**里（见 `classify_submission_outcome`），
    // 因此不能在 SQL 里 GROUP BY 了事：取回窗口内的候选事件，逐条分类。
    let rows = sqlx::query(
        "SELECT event_type, payload_json FROM engine_order_events WHERE credential_id=? AND ts_ms >= ? AND event_type IN ('order_ack','reject','error') ORDER BY ts_ms DESC LIMIT ?",
    )
    .bind(credential_id)
    .bind(since_ms)
    .bind(row_limit)
    .fetch_all(store.pool())
    .await?;
    let truncated = rows.len() as i64 >= row_limit;
    if truncated {
        tracing::warn!(
            credential_id = %credential_id,
            limit = row_limit,
            "order reject-rate scan hit the row limit; rate is computed on a truncated window"
        );
    }
    let mut facts = OrderRejectFacts {
        credential_id: credential_id.to_string(),
        acks: 0,
        rejects: 0,
        errors: 0,
        truncated,
    };
    for row in rows {
        let event_type = row.get::<String, _>("event_type");
        let payload = crate::store::try_get_nullable_bytes(&row, "payload_json")
            .ok()
            .flatten()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
        match classify_submission_outcome(&event_type, payload.as_ref()) {
            SubmissionOutcome::Accepted => facts.acks += 1,
            SubmissionOutcome::Rejected => facts.rejects += 1,
            SubmissionOutcome::Errored => facts.errors += 1,
            SubmissionOutcome::NotSubmission => {}
        }
    }
    Ok(facts)
}

/// **启动重建拒绝了持久订阅行 → 结构化 not-ready**（第七轮
/// regression，High）。
///
/// R10 之后 `restore_required_streams` 会返回 `rejected` 计数，但 `main`
/// 拿到之后既不停机也不降级，直接继续 serving——一个没人消费的计数等于
/// 只写了日志。
///
/// 这里选的是**结构化 not-ready 而不是 fail startup**，理由写在报告里，
/// 一句话版本：fail startup 会连带停掉 Protection / Close / Kill Switch /
/// Reconcile（engine safety contract 明令这些不得因非交易风险被停），
/// 而一条起不来的行情流只应该拦住**新风险**。`cred:<id>` 作用域的
/// `pause_new` 正是这个语义：持久化、fail-closed、需人工 reset。
///
/// 判据复用 `market_data_interruption_decision`——`streams` 传空、
/// `has_exposure` 传 false，因此**只有**结构性死流那一条分支可能触发，
/// 不会在启动时把"暂时还没来 bar"误判成中断。
pub async fn arm_dead_market_data_subscriptions(
    store: &Store,
    credential_ids: &std::collections::BTreeSet<String>,
    now_ms: i64,
) -> anyhow::Result<Vec<String>> {
    let mut armed = Vec::new();
    for credential_id in credential_ids {
        let facts = MarketDataFacts {
            credential_id: credential_id.clone(),
            kind: crate::venue::credential_kind(credential_id),
            streams: Vec::new(),
            required: required_streams(store, credential_id).await?,
            has_exposure: false,
        };
        let Some(decision) = market_data_interruption_decision(
            &facts,
            now_ms,
            DEFAULT_MARKET_DATA_INTERRUPTION_FLOOR_MS,
        ) else {
            continue;
        };
        let scope_key = format!("cred:{credential_id}");
        if arm_auto_trigger_or_preserve_invalid(
            store,
            ArmRequest {
                scope_key: &scope_key,
                level: decision.level,
                trigger_source: TRIGGER_SOURCE_MARKET_DATA,
                reason: &decision.reason,
                now_ms,
            },
        )
        .await?
        {
            armed.push(scope_key);
        }
    }
    Ok(armed)
}

/// Arm one automatic trigger without letting a malformed authority row stop
/// unrelated credentials from receiving their own safety latch.
///
/// `kill_switch::arm` returns a typed [`kill_switch::InvalidPersistedLevel`]
/// before writing when the target row is noncanonical. That row must remain
/// byte-for-byte intact for operator diagnosis, while the independent action
/// retry task treats it as conservative `flatten`. Only that typed data error
/// is isolated here; storage and transaction failures still abort the scan.
async fn arm_auto_trigger_or_preserve_invalid(
    store: &Store,
    request: ArmRequest<'_>,
) -> anyhow::Result<bool> {
    let requested_scope_key = request.scope_key.to_string();
    let trigger_source = request.trigger_source.to_string();
    match kill_switch::arm(store, request).await {
        Ok(_) => Ok(true),
        Err(error) => {
            let Some(invalid) = error.downcast_ref::<kill_switch::InvalidPersistedLevel>() else {
                return Err(error);
            };
            tracing::error!(
                requested_scope_key = %requested_scope_key,
                scope_key = %invalid.scope_key,
                raw_level = ?invalid.raw_level,
                trigger_source = %trigger_source,
                conservative_level = Level::Flatten.as_str(),
                error_code = "KILL_SWITCH_INVALID_PERSISTED_LEVEL",
                "automatic trigger found an invalid durable kill-switch level; preserving it and continuing sibling credentials"
            );
            Ok(false)
        }
    }
}

/// 跑一轮自动触发扫描。**返回本轮真正 arm 了的 (scope_key, trigger_source)**，
/// 供测试断言；生产侧只关心副作用。
pub async fn drive_auto_triggers_once(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<Vec<(String, &'static str)>> {
    // 语义已改：这是**下界**，真正的阈值是 max(3 根 bar, 下界)，逐流计算。
    let interruption_floor_ms = env_i64(
        "KILL_SWITCH_MARKET_DATA_INTERRUPTION_FLOOR_MS",
        DEFAULT_MARKET_DATA_INTERRUPTION_FLOOR_MS,
    );
    let window_ms = env_i64(
        "KILL_SWITCH_ORDER_REJECT_WINDOW_MS",
        DEFAULT_ORDER_REJECT_WINDOW_MS,
    );
    let min_samples = env_i64(
        "KILL_SWITCH_ORDER_REJECT_MIN_SAMPLES",
        DEFAULT_ORDER_REJECT_MIN_SAMPLES,
    );
    let rate_pct = env_i64(
        "KILL_SWITCH_ORDER_REJECT_RATE_PCT",
        DEFAULT_ORDER_REJECT_RATE_PCT,
    );

    let mut armed = Vec::new();
    for credential_id in candidate_credentials(store).await? {
        let kind = crate::venue::credential_kind(&credential_id);
        let scope_key = format!("cred:{credential_id}");

        // ---- 行情中断 ----
        let facts = MarketDataFacts {
            credential_id: credential_id.clone(),
            kind,
            streams: market_stream_heartbeats(store, &credential_id).await?,
            required: required_streams(store, &credential_id).await?,
            has_exposure: has_exposure(store, &credential_id).await?,
        };
        if let Some(decision) =
            market_data_interruption_decision(&facts, now_ms, interruption_floor_ms)
        {
            if arm_auto_trigger_or_preserve_invalid(
                store,
                ArmRequest {
                    scope_key: &scope_key,
                    level: decision.level,
                    trigger_source: TRIGGER_SOURCE_MARKET_DATA,
                    reason: &decision.reason,
                    now_ms,
                },
            )
            .await?
            {
                armed.push((scope_key.clone(), TRIGGER_SOURCE_MARKET_DATA));
            }
        }

        // ---- 订单拒绝率 ----
        // 模拟凭据不产生 order_ack / reject / error（这三类事件只在
        // exchange-backed 提交路径上写），因此天然不参与；这里仍显式跳过，
        // 免得未来有人给模拟路径补事件时静默改变判定面。
        if kind != CredentialKind::ExchangeBacked {
            continue;
        }
        let reject = reject_facts(store, &credential_id, now_ms - window_ms).await?;
        if let Some(decision) = order_reject_rate_decision(&reject, min_samples, rate_pct) {
            if arm_auto_trigger_or_preserve_invalid(
                store,
                ArmRequest {
                    scope_key: &scope_key,
                    level: decision.level,
                    trigger_source: TRIGGER_SOURCE_ORDER_REJECT_RATE,
                    reason: &decision.reason,
                    now_ms,
                },
            )
            .await?
            {
                armed.push((scope_key, TRIGGER_SOURCE_ORDER_REJECT_RATE));
            }
        }
    }
    Ok(armed)
}

/// 周期性扫描任务。与 `spawn_action_retry_task` 同形：失败只记日志并重试，
/// **绝不**因为一次查询失败就退出循环（退出等于自动触发面静默消失）。
pub fn spawn_auto_trigger_task(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let tick_ms = env_i64("KILL_SWITCH_AUTO_TRIGGER_TICK_MS", 5_000).max(200) as u64;
        let mut ticker = tokio::time::interval(Duration::from_millis(tick_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now_ms = crate::svc::now_ms();
            match drive_auto_triggers_once(&store, now_ms).await {
                Ok(armed) => {
                    for (scope_key, source) in armed {
                        tracing::warn!(%scope_key, trigger_source = source, "auto kill-switch trigger armed");
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "auto kill-switch trigger scan failed; retrying");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use super::*;

    static INSTALL_DRIVERS: Once = Once::new();

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-auto-triggers-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect auto-trigger sqlite store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn seed_position(store: &Store, credential_id: &str) {
        sqlx::query(
            "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, state, qty, updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(format!("pos_{credential_id}"))
        .bind(credential_id)
        .bind("bot_auto_trigger")
        .bind("binance|futures|BTC|USDT|USDT")
        .bind("long")
        .bind(1_i64)
        .bind("protected")
        .bind("1")
        .bind(1_i64)
        .execute(store.pool())
        .await
        .expect("seed position_state");
    }

    async fn seed_event(
        store: &Store,
        credential_id: &str,
        seq: i64,
        event_type: &str,
        ts_ms: i64,
    ) {
        seed_event_with_payload(
            store,
            credential_id,
            seq,
            event_type,
            ts_ms,
            default_payload(event_type),
        )
        .await;
        if event_type == "bar_closed" {
            // 走**生产写入方**，而不是自己往心跳表里塞——这样读侧与写侧
            // 必须真的对上；任一侧改了 identity（symbol/timeframe）都会红。
            crate::market_data::record_market_stream_heartbeat(
                store,
                credential_id,
                "binance|futures|BTC|USDT|USDT",
                "1m",
                ts_ms,
            )
            .await
            .expect("record market stream heartbeat");
        }
    }

    async fn seed_stream_heartbeat(
        store: &Store,
        credential_id: &str,
        symbol_key: &str,
        timeframe: &str,
        last_bar_ms: i64,
    ) {
        crate::market_data::record_market_stream_heartbeat(
            store,
            credential_id,
            symbol_key,
            timeframe,
            last_bar_ms,
        )
        .await
        .expect("record market stream heartbeat");
    }

    /// 夹具载荷必须**长得像生产事件**——归属判据活在载荷里（评审 regression），
    /// 用空载荷造夹具会让判据在测试里根本不被行使。
    fn default_payload(event_type: &str) -> Option<serde_json::Value> {
        match event_type {
            // 提交路径的交易所拒绝（形如 svc.rs 的 demo venue reject）。
            "reject" => Some(serde_json::json!({
                "runMode": "live",
                "paper": false,
                "rejectReasonCode": "EXCHANGE_REJECTED",
                "message": "exchange rejected (live): -2019 margin insufficient",
                "orderRole": "entry",
            })),
            // 提交路径的交易所/系统错误（形如 svc.rs 的 submit 失败分支）。
            "error" => Some(serde_json::json!({
                "runMode": "live",
                "paper": false,
                "error": "connection reset by peer",
                "degraded": false,
            })),
            "order_ack" => Some(serde_json::json!({
                "runMode": "live",
                "paper": false,
                "exchangeOrderId": "12345",
            })),
            _ => None,
        }
    }

    /// 风控裁决拒绝的载荷（形如 svc.rs 的 `verdict_event_payload`）。
    fn risk_verdict_reject_payload() -> serde_json::Value {
        serde_json::json!({
            "runMode": "live",
            "action": "open_long",
            "intentId": "int_x",
            "riskVerdictId": "rv_x",
            "orderRole": "entry",
            "riskVerdict": { "decision": "reject", "reject_reason_code": "MAX_LEVERAGE" },
        })
    }

    /// 本地 position_state 孤儿告警（形如 svc.rs 的 `position_state_orphan_recovery`）。
    fn local_warning_payload() -> serde_json::Value {
        serde_json::json!({
            "severity": "warning",
            "code": "POSITION_STATE_ORPHAN",
            "message": "close request targets a position with no local position_state record",
            "source": "position_state_orphan_recovery",
        })
    }

    async fn seed_event_with_payload(
        store: &Store,
        credential_id: &str,
        seq: i64,
        event_type: &str,
        ts_ms: i64,
        payload: Option<serde_json::Value>,
    ) {
        sqlx::query(
            "INSERT INTO engine_order_events (credential_id, seq, event_id, ts_ms, event_type, exchange, market_kind, symbol_key, payload_json) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(credential_id)
        .bind(seq)
        .bind(format!("evt_{credential_id}_{seq}"))
        .bind(ts_ms)
        .bind(event_type)
        .bind("binance")
        .bind("futures")
        .bind("binance|futures|BTC|USDT|USDT")
        .bind(payload.map(|value| value.to_string().into_bytes()))
        .execute(store.pool())
        .await
        .expect("seed engine_order_events");
    }

    async fn armed_state(store: &Store, scope_key: &str) -> Option<(String, String, String)> {
        sqlx::query("SELECT level, trigger_source, reason FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_optional(store.pool())
            .await
            .expect("load kill switch state")
            .map(|row| {
                (
                    row.get::<String, _>("level"),
                    row.get::<String, _>("trigger_source"),
                    crate::store::try_get_text(&row, "reason")
                        .expect("decode the kill switch reason")
                        .unwrap_or_default(),
                )
            })
    }

    async fn seed_invalid_kill_switch_level(store: &Store, credential_id: &str, raw_level: &str) {
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, last_reconcile_clean, last_reconcile_at_ms, \
             updated_at_ms, flatten_generation) VALUES (?, ?, 'invalid fixture', \
             'test_fixture', 10, NULL, 0, NULL, 10, 3)",
        )
        .bind(format!("cred:{credential_id}"))
        .bind(raw_level)
        .execute(store.pool())
        .await
        .expect("seed an invalid persisted kill-switch level");
    }

    #[tokio::test]
    async fn startup_invalid_authority_does_not_block_healthy_sibling_arm() {
        let (store, path) = test_store("startup-invalid-sibling").await;
        let invalid_credential = "cred_00_invalid_startup";
        let healthy_credential = "cred_99_healthy_startup";
        seed_invalid_kill_switch_level(&store, invalid_credential, "PAUSE_NEW").await;

        for credential_id in [invalid_credential, healthy_credential] {
            record_required_streams_atomically(
                &store,
                credential_id,
                &[(
                    "binance|futures|BTC|USDT|USDT".to_string(),
                    "7m".to_string(),
                )],
                1_000,
            )
            .await
            .expect("seed a structurally dead persisted subscription");
        }
        let credential_ids = std::collections::BTreeSet::from([
            invalid_credential.to_string(),
            healthy_credential.to_string(),
        ]);

        let armed = arm_dead_market_data_subscriptions(&store, &credential_ids, 2_000)
            .await
            .expect("an invalid first credential must be isolated, not abort startup recovery");

        assert_eq!(
            armed,
            vec![format!("cred:{healthy_credential}")],
            "the healthy sibling after the invalid credential must still be armed"
        );
        assert_eq!(
            armed_state(&store, &format!("cred:{invalid_credential}"))
                .await
                .expect("invalid authority row remains present")
                .0,
            "PAUSE_NEW",
            "startup recovery must preserve the invalid raw value byte-for-byte"
        );
        let healthy = armed_state(&store, &format!("cred:{healthy_credential}"))
            .await
            .expect("healthy sibling state is written");
        assert_eq!(healthy.0, "pause_new");
        assert_eq!(healthy.1, TRIGGER_SOURCE_MARKET_DATA);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn periodic_invalid_authority_does_not_block_healthy_sibling_arm() {
        let (store, path) = test_store("periodic-invalid-sibling").await;
        let invalid_credential = "cred_00_invalid_periodic";
        let healthy_credential = "cred_99_healthy_periodic";
        let now_ms = 10_000_000;
        for credential_id in [invalid_credential, healthy_credential] {
            seed_position(&store, credential_id).await;
            seed_event(
                &store,
                credential_id,
                1,
                "bar_closed",
                now_ms - DEFAULT_MARKET_DATA_INTERRUPTION_FLOOR_MS - 1,
            )
            .await;
        }
        seed_invalid_kill_switch_level(&store, invalid_credential, " PAUSE_NEW ").await;
        assert_eq!(
            candidate_credentials(&store)
                .await
                .expect("load periodic candidates"),
            vec![
                invalid_credential.to_string(),
                healthy_credential.to_string()
            ],
            "the regression requires the invalid credential to be visited first"
        );

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("an invalid first credential must be isolated, not abort the periodic scan");

        assert_eq!(
            armed,
            vec![(
                format!("cred:{healthy_credential}"),
                TRIGGER_SOURCE_MARKET_DATA
            )],
            "the healthy sibling after the invalid credential must still be armed"
        );
        assert_eq!(
            armed_state(&store, &format!("cred:{invalid_credential}"))
                .await
                .expect("invalid authority row remains present")
                .0,
            " PAUSE_NEW ",
            "periodic scanning must preserve the invalid raw value byte-for-byte"
        );
        let healthy = armed_state(&store, &format!("cred:{healthy_credential}"))
            .await
            .expect("healthy sibling state is written");
        assert_eq!(healthy.0, "pause_new");
        assert_eq!(healthy.1, TRIGGER_SOURCE_MARKET_DATA);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn reject_rate_invalid_authority_does_not_block_healthy_sibling_arm() {
        let (store, path) = test_store("reject-invalid-sibling").await;
        let invalid_credential = "cred_00_invalid_reject";
        let healthy_credential = "cred_99_healthy_reject";
        let now_ms = 10_000_000;
        seed_invalid_kill_switch_level(&store, invalid_credential, "MANAGE_ONLY").await;

        for credential_id in [invalid_credential, healthy_credential] {
            seed_position(&store, credential_id).await;
            // Keep market data healthy so only the reject-rate arm point is exercised.
            seed_event(&store, credential_id, 1, "bar_closed", now_ms - 1_000).await;
            for seq in 2..=6 {
                seed_event(&store, credential_id, seq, "reject", now_ms - 60_000).await;
            }
            for seq in 7..=8 {
                seed_event(&store, credential_id, seq, "order_ack", now_ms - 60_000).await;
            }
        }

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("a corrupt first reject-rate authority must not abort sibling scanning");
        assert_eq!(
            armed,
            vec![(
                format!("cred:{healthy_credential}"),
                TRIGGER_SOURCE_ORDER_REJECT_RATE
            )]
        );
        assert_eq!(
            armed_state(&store, &format!("cred:{invalid_credential}"))
                .await
                .expect("invalid authority remains present")
                .0,
            "MANAGE_ONLY"
        );
        let healthy = armed_state(&store, &format!("cred:{healthy_credential}"))
            .await
            .expect("healthy reject-rate sibling is armed");
        assert_eq!(healthy.0, "manage_only");
        assert_eq!(healthy.1, TRIGGER_SOURCE_ORDER_REJECT_RATE);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn auto_trigger_helper_propagates_non_typed_errors() {
        let (store, path) = test_store("non-typed-arm-error").await;
        let error = arm_auto_trigger_or_preserve_invalid(
            &store,
            ArmRequest {
                scope_key: "not-a-valid-scope",
                level: Level::PauseNew,
                trigger_source: TRIGGER_SOURCE_MARKET_DATA,
                reason: "fixture",
                now_ms: 1,
            },
        )
        .await
        .expect_err("only InvalidPersistedLevel may be isolated");
        assert!(
            error
                .to_string()
                .contains("scope_key must be global, cred:<id>, or bot:<uid>"),
            "unexpected non-typed error: {error}"
        );
        cleanup(store, path).await;
    }

    /// **接线证明**：判据不是只在纯函数层成立，扫描任务真的会把它写进
    /// `kill_switch_state` 与 `kill_switch_events`。变异「扫描里不调用判定」
    /// 或「判定结果不 arm」都会在这几条上变红。
    #[tokio::test]
    async fn wiring_market_data_interruption_arms_pause_new_in_db() {
        let (store, path) = test_store("market-data").await;
        let credential_id = "cred_auto_md";
        seed_position(&store, credential_id).await;
        seed_event(&store, credential_id, 1, "bar_closed", 1_000).await;

        let armed = drive_auto_triggers_once(&store, 1_000 + 180_001)
            .await
            .expect("scan runs");
        assert!(armed
            .iter()
            .any(|(scope, source)| scope == "cred:cred_auto_md"
                && *source == TRIGGER_SOURCE_MARKET_DATA));

        let state = armed_state(&store, "cred:cred_auto_md")
            .await
            .expect("state row written");
        assert_eq!(state.0, "pause_new");
        assert_eq!(state.1, TRIGGER_SOURCE_MARKET_DATA);
        // 审计要求：reason 必须写明判据数值，不得写成"自动触发"这种无法复核的措辞。
        assert!(
            state.2.contains("180001ms"),
            "reason must carry the measured gap: {}",
            state.2
        );

        // 审计事件同批落库。
        let events = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=? AND trigger_source=?",
        )
        .bind("cred:cred_auto_md")
        .bind(TRIGGER_SOURCE_MARKET_DATA)
        .fetch_one(store.pool())
        .await
        .expect("count kill switch events");
        assert_eq!(events.get::<i64, _>("n"), 1);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn wiring_no_exposure_does_not_arm_in_db() {
        // 反向：同样的行情缺口，但没有任何敞口 → 不得 arm。
        let (store, path) = test_store("no-exposure").await;
        let credential_id = "cred_auto_idle";
        seed_event(&store, credential_id, 1, "bar_closed", 1_000).await;

        let armed = drive_auto_triggers_once(&store, 1_000 + 999_999)
            .await
            .expect("scan runs");
        assert!(armed.is_empty(), "idle credential must not arm: {armed:?}");
        assert!(armed_state(&store, "cred:cred_auto_idle").await.is_none());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn wiring_simulation_credential_is_skipped_in_db() {
        let (store, path) = test_store("simulation").await;
        let credential_id = "paper:00112233445566778899aabbccddeeff";
        seed_position(&store, credential_id).await;
        seed_event(&store, credential_id, 1, "bar_closed", 1_000).await;

        let armed = drive_auto_triggers_once(&store, 1_000 + 999_999)
            .await
            .expect("scan runs");
        assert!(
            armed.is_empty(),
            "simulation credential must never arm: {armed:?}"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn wiring_order_reject_rate_arms_manage_only_in_db() {
        let (store, path) = test_store("reject-rate").await;
        let credential_id = "cred_auto_rej";
        seed_position(&store, credential_id).await;
        // 行情正常（刚收到 bar），只有拒绝率超标。
        let now_ms = 10_000_000;
        seed_event(&store, credential_id, 1, "bar_closed", now_ms - 1_000).await;
        for seq in 2..=6 {
            seed_event(&store, credential_id, seq, "reject", now_ms - 60_000).await;
        }
        for seq in 7..=8 {
            seed_event(&store, credential_id, seq, "order_ack", now_ms - 60_000).await;
        }

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(armed
            .iter()
            .any(|(scope, source)| scope == "cred:cred_auto_rej"
                && *source == TRIGGER_SOURCE_ORDER_REJECT_RATE));
        let state = armed_state(&store, "cred:cred_auto_rej")
            .await
            .expect("state row written");
        assert_eq!(state.0, "manage_only");
        assert_eq!(state.1, TRIGGER_SOURCE_ORDER_REJECT_RATE);
        assert!(
            state.2.contains("5/7"),
            "reason must carry the counts: {}",
            state.2
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn wiring_stale_rejects_outside_window_do_not_arm() {
        // 窗口纪律：更早的一批失败滚出窗口后不得继续把开关按住。
        let (store, path) = test_store("stale-window").await;
        let credential_id = "cred_auto_old";
        seed_position(&store, credential_id).await;
        let now_ms = 10_000_000;
        seed_event(&store, credential_id, 1, "bar_closed", now_ms - 1_000).await;
        for seq in 2..=8 {
            seed_event(&store, credential_id, seq, "reject", now_ms - 900_000).await;
        }

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(
            armed.is_empty(),
            "rejects outside the window must not arm: {armed:?}"
        );
        cleanup(store, path).await;
    }

    /// V2-E Exit 8「分层 pause/kill 在**所需 scope** 生效」的反向面：
    /// 一个 bot 被 arm 不得把同一 credential 上的**另一个 bot** 一起按停。
    /// 既有的 `effective_scope_uses_strictest_level_and_global_tiebreak` 证明的是
    /// "取最严"，这条证明的是"不外溢"——两者缺一不可。
    #[tokio::test]
    async fn bot_scoped_arm_does_not_leak_to_a_sibling_bot() {
        let (store, path) = test_store("scope-isolation").await;
        kill_switch::arm(
            &store,
            ArmRequest {
                scope_key: "bot:bot_llm_down",
                level: Level::PauseNew,
                trigger_source: TRIGGER_SOURCE_LLM_FAILURES,
                reason: "llm provider failures 3/3",
                now_ms: 1_000,
            },
        )
        .await
        .expect("arm bot scope");

        let mut tx = store.pool().begin().await.expect("begin");
        let same_bot = kill_switch::effective_in_tx(&mut tx, "cred_shared", "bot_llm_down")
            .await
            .expect("read effective for armed bot");
        let sibling = kill_switch::effective_in_tx(&mut tx, "cred_shared", "bot_healthy")
            .await
            .expect("read effective for sibling bot");
        tx.rollback().await.expect("rollback");

        assert_eq!(
            same_bot
                .effective
                .expect("armed bot has a determinate state")
                .level,
            Level::PauseNew
        );
        assert!(same_bot.invalid_levels.is_empty());
        assert!(
            sibling.effective.is_none() || sibling.effective.as_ref().unwrap().level == Level::None,
            "a sibling bot on the same credential must stay unaffected"
        );
        assert!(sibling.invalid_levels.is_empty());
        cleanup(store, path).await;
    }

    // ---- 提交结果归属（评审 regression，High）----

    #[test]
    fn risk_verdict_rejects_are_not_submission_outcomes() {
        // T4.C1：策略性正常拒绝必须与交易所/系统异常拒绝分开。
        // 风控裁决拒绝在插单**之前**就发生，从不进提交路径。
        assert_eq!(
            classify_submission_outcome("reject", Some(&risk_verdict_reject_payload())),
            SubmissionOutcome::NotSubmission
        );
    }

    #[test]
    fn exchange_rejects_are_submission_outcomes() {
        assert_eq!(
            classify_submission_outcome("reject", default_payload("reject").as_ref()),
            SubmissionOutcome::Rejected
        );
        assert_eq!(
            classify_submission_outcome("order_ack", default_payload("order_ack").as_ref()),
            SubmissionOutcome::Accepted
        );
        assert_eq!(
            classify_submission_outcome("error", default_payload("error").as_ref()),
            SubmissionOutcome::Errored
        );
    }

    #[test]
    fn risk_verdict_exclusion_is_load_bearing_on_its_own() {
        // **分层独立**：正向判据（带 rejectReasonCode）与排除判据（带 riskVerdict）
        // 必须各自能被单独打红，否则两层互相遮蔽——B2 的 J7/J8 就是这样双双存活的。
        // 这个载荷两者都有：裁决本身带 reject_reason_code，未来有人把它提到顶层
        // 就会命中这条路径。
        let both = serde_json::json!({
            "runMode": "live",
            "riskVerdictId": "rv_x",
            "riskVerdict": { "decision": "reject" },
            "rejectReasonCode": "MAX_LEVERAGE",
        });
        assert_eq!(
            classify_submission_outcome("reject", Some(&both)),
            SubmissionOutcome::NotSubmission,
            "a risk-verdict payload must stay excluded even if it also carries rejectReasonCode"
        );
    }

    #[test]
    fn local_warning_exclusion_is_load_bearing_on_its_own() {
        // 同上：带 severity 的告警即使也带 error 字段，也不得计入提交结果。
        let both = serde_json::json!({
            "severity": "warning",
            "source": "position_state_orphan_recovery",
            "error": "close request targets a position with no local position_state record",
        });
        assert_eq!(
            classify_submission_outcome("error", Some(&both)),
            SubmissionOutcome::NotSubmission,
            "a local warning must stay excluded even if it also carries an error field"
        );
    }

    #[test]
    fn local_warnings_carried_on_the_error_event_are_not_submission_outcomes() {
        // `event_type='error'` 还承载本地 position_state 孤儿告警（severity=warning）。
        assert_eq!(
            classify_submission_outcome("error", Some(&local_warning_payload())),
            SubmissionOutcome::NotSubmission
        );
    }

    #[test]
    fn unrecognised_or_missing_payloads_are_not_counted() {
        // 认不出来就不计——本触发源 arm 的是需要人工 reset 的 manage_only。
        assert_eq!(
            classify_submission_outcome("reject", None),
            SubmissionOutcome::NotSubmission
        );
        assert_eq!(
            classify_submission_outcome("error", None),
            SubmissionOutcome::NotSubmission
        );
        assert_eq!(
            classify_submission_outcome("bar_closed", None),
            SubmissionOutcome::NotSubmission
        );
        assert_eq!(
            classify_submission_outcome(
                "reject",
                Some(&serde_json::json!({ "rejectReasonCode": "   " }))
            ),
            SubmissionOutcome::NotSubmission
        );
    }

    #[tokio::test]
    async fn scan_reports_truncation_as_a_fact_not_just_a_log() {
        // 「无声上限」是本仓明令禁止的：截断必须是**可断言的数据**。
        let (store, path) = test_store("truncation").await;
        let credential_id = "cred_auto_trunc";
        for seq in 1..=4 {
            seed_event(&store, credential_id, seq, "order_ack", 1_000).await;
        }
        let full = reject_facts_with_limit(&store, credential_id, 0, 10)
            .await
            .expect("scan");
        assert!(!full.truncated);
        assert_eq!(full.acks, 4);

        let capped = reject_facts_with_limit(&store, credential_id, 0, 2)
            .await
            .expect("scan");
        assert!(
            capped.truncated,
            "hitting the row limit must be visible in the facts"
        );
        assert_eq!(capped.acks, 2);
        cleanup(store, path).await;
    }

    /// **接线级**：纯风控拒绝的账户不得被 arm。
    /// 这是 regression 的收紧面——首版实现在这里会 arm。
    #[tokio::test]
    async fn wiring_risk_verdict_rejects_alone_never_arm_the_reject_rate_trigger() {
        let (store, path) = test_store("risk-rejects").await;
        let credential_id = "cred_auto_risk";
        seed_position(&store, credential_id).await;
        let now_ms = 10_000_000;
        seed_event(&store, credential_id, 1, "bar_closed", now_ms - 1_000).await;
        // 十条风控拒绝 + 两条正常受理：按事件类型无差别计数会得到 10/12 ≈ 83%。
        for seq in 2..=11 {
            seed_event_with_payload(
                &store,
                credential_id,
                seq,
                "reject",
                now_ms - 60_000,
                Some(risk_verdict_reject_payload()),
            )
            .await;
        }
        for seq in 12..=13 {
            seed_event(&store, credential_id, seq, "order_ack", now_ms - 60_000).await;
        }

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(
            !armed
                .iter()
                .any(|(_, source)| *source == TRIGGER_SOURCE_ORDER_REJECT_RATE),
            "strategy rejects adjudicated by risk policy must never arm the reject-rate trigger: {armed:?}"
        );
        cleanup(store, path).await;
    }

    /// **接线级**：风控拒绝也不得进**分母**——否则会稀释真实拒绝率，
    /// 让本该 arm 的账户逃掉（漏触发方向）。
    #[tokio::test]
    async fn wiring_risk_verdict_rejects_do_not_dilute_the_denominator() {
        let (store, path) = test_store("risk-dilute").await;
        let credential_id = "cred_auto_dilute";
        seed_position(&store, credential_id).await;
        let now_ms = 10_000_000;
        seed_event(&store, credential_id, 1, "bar_closed", now_ms - 1_000).await;
        // 提交路径：3 拒 / 2 受理 = 60% ≥ 50%，样本 5 ⇒ 应当 arm。
        for seq in 2..=4 {
            seed_event(&store, credential_id, seq, "reject", now_ms - 60_000).await;
        }
        for seq in 5..=6 {
            seed_event(&store, credential_id, seq, "order_ack", now_ms - 60_000).await;
        }
        // 再掺 20 条风控拒绝与 1 条本地告警：若它们进了分母，比率会被稀释到 <50%。
        for seq in 7..=26 {
            seed_event_with_payload(
                &store,
                credential_id,
                seq,
                "reject",
                now_ms - 60_000,
                Some(risk_verdict_reject_payload()),
            )
            .await;
        }
        seed_event_with_payload(
            &store,
            credential_id,
            27,
            "error",
            now_ms - 60_000,
            Some(local_warning_payload()),
        )
        .await;

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(
            armed
                .iter()
                .any(|(_, source)| *source == TRIGGER_SOURCE_ORDER_REJECT_RATE),
            "submission-path rate must be computed on submission events only: {armed:?}"
        );
        cleanup(store, path).await;
    }

    /// 评审自设计变异体 X2 暴露的缺口：拒绝率一侧的**模拟凭据豁免**此前无测试。
    #[tokio::test]
    async fn wiring_simulation_credential_never_arms_the_reject_rate_trigger() {
        let (store, path) = test_store("sim-rejects").await;
        let credential_id = "paper:aabbccddeeff00112233445566778899";
        seed_position(&store, credential_id).await;
        let now_ms = 10_000_000;
        // 即使真的写进了提交结果事件（未来某次改动可能这么做），
        // 模拟凭据也不得被 arm——它根本不该有交易所提交面。
        for seq in 1..=6 {
            seed_event(&store, credential_id, seq, "reject", now_ms - 60_000).await;
        }
        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(
            armed.is_empty(),
            "simulation credential must never arm: {armed:?}"
        );
        cleanup(store, path).await;
    }

    /// **B3 组合验收**：trigger → kill-switch state → 本地订单语义，
    /// 一整条链路走通，且**保护单与平仓单不被误伤**。
    ///
    /// 这条不是把已有用例抄一遍：既有用例都是**手工 arm** 之后验证语义，
    /// 本条从**自动触发**这一端进去，证明 B3b 新接的判据真的能驱动既有的
    /// pause/kill 动作面——「有急停按钮但没人按」与「按了但没接到动作」
    /// 是两种不同的半成品，这里堵的是第二种。
    #[tokio::test]
    async fn combined_chain_trigger_to_state_to_order_semantics() {
        let (store, path) = test_store("combined").await;
        let credential_id = "cred_chain";
        seed_position(&store, credential_id).await;

        // 三张单：入场单 / 保护单（SL）/ 平仓单。
        for (id, action, order_type, reduce_only, order_role) in [
            ("entry", "open_long", "limit", 0, "entry"),
            ("sl", "close_long", "stop_market", 1, "protection_sl"),
            ("exit", "close_long", "limit", 1, "exit"),
        ] {
            sqlx::query(
                r#"
                INSERT INTO engine_orders (
                  engine_order_id, credential_id, bot_uid, idempotency_key,
                  exchange, market_kind, symbol_key, action, side, order_type,
                  quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
                ) VALUES (?, ?, 'bot_auto_trigger', ?, 'binance', 'futures',
                  'binance|futures|BTC|USDT|USDT', ?, 'long', ?, '1', ?, ?, 'acked', 1, 1)
                "#,
            )
            .bind(id)
            .bind(credential_id)
            .bind(format!("idem-chain-{id}"))
            .bind(action)
            .bind(order_type)
            .bind(reduce_only)
            .bind(order_role)
            .execute(store.pool())
            .await
            .expect("insert chain order fixture");
        }

        // ① 行情中断触发 → pause_new
        seed_event(&store, credential_id, 1, "bar_closed", 1_000).await;
        let now_ms = 1_000 + 180_001;
        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(armed
            .iter()
            .any(|(_, source)| *source == TRIGGER_SOURCE_MARKET_DATA));

        // ② 状态面：该 credential 的有效档位是 pause_new
        let mut tx = store.pool().begin().await.expect("begin");
        let effective = kill_switch::effective_in_tx(&mut tx, credential_id, "bot_auto_trigger")
            .await
            .expect("read effective");
        assert!(effective.invalid_levels.is_empty());
        let effective = effective.effective.expect("determinate state");
        tx.rollback().await.expect("rollback");
        assert_eq!(effective.level, Level::PauseNew);

        // ③ 动作面：L1 不撤任何单（保护单与平仓单当然也在）
        let cancelled = kill_switch::apply_local_order_semantics(
            &store,
            &format!("cred:{credential_id}"),
            effective.level,
            now_ms,
        )
        .await
        .expect("apply L1");
        assert_eq!(cancelled, 0, "L1 must not cancel any order");

        // ④ 升级：拒绝率触发 → manage_only，只撤未成交**入场**单
        for seq in 2..=8 {
            seed_event(&store, credential_id, seq, "reject", now_ms - 60_000).await;
        }
        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("second scan runs");
        assert!(armed
            .iter()
            .any(|(_, source)| *source == TRIGGER_SOURCE_ORDER_REJECT_RATE));
        let cancelled = kill_switch::apply_local_order_semantics(
            &store,
            &format!("cred:{credential_id}"),
            Level::ManageOnly,
            now_ms + 1,
        )
        .await
        .expect("apply L2");
        assert_eq!(cancelled, 1, "L2 cancels exactly the entry order");

        let statuses = sqlx::query(
            "SELECT engine_order_id, status FROM engine_orders WHERE credential_id=? ORDER BY engine_order_id",
        )
        .bind(credential_id)
        .fetch_all(store.pool())
        .await
        .expect("read statuses");
        let status_of = |id: &str| {
            statuses
                .iter()
                .find(|row| row.get::<String, _>("engine_order_id") == id)
                .map(|row| row.get::<String, _>("status"))
                .unwrap_or_default()
        };
        // **保护单与平仓单必须活着**——任何层级下 reduce-only 与保护单永远可用
        // （SAFETY-MODEL §4.1 的不变量）。这条是本用例存在的理由。
        assert_eq!(status_of("entry"), "canceled");
        assert_eq!(status_of("sl"), "acked");
        assert_eq!(status_of("exit"), "acked");

        cleanup(store, path).await;
    }

    /// trigger → scope/level 映射表（SAFETY-MODEL §4.2 的三行）。
    /// 改任一格都会在这里变红；映射漂移是"急停按钮接错线"的典型形态。
    #[test]
    fn trigger_to_scope_and_level_mapping_is_pinned() {
        // 行情中断 → credential 级 → pause_new
        assert_eq!(
            market_data_interruption_decision(&exchange_facts(Some(0)), 1_000_000, 180_000)
                .expect("armed")
                .level,
            Level::PauseNew
        );
        // 订单拒绝率 → credential 级 → manage_only
        assert_eq!(
            order_reject_rate_decision(&reject_facts_of(0, 5, 0), 5, 50)
                .expect("armed")
                .level,
            Level::ManageOnly
        );
        // LLM 连续失败 → bot 级 → pause_new（判定在编排器侧，这里钉 source 常量，
        // 避免两侧字面量各写各的而漂移）。
        assert_eq!(TRIGGER_SOURCE_LLM_FAILURES, "auto_llm_failures");
        assert_eq!(TRIGGER_SOURCE_MARKET_DATA, "auto_market_data_interruption");
        assert_eq!(TRIGGER_SOURCE_ORDER_REJECT_RATE, "auto_order_reject_rate");
    }

    #[tokio::test]
    async fn wiring_repeat_scan_does_not_duplicate_audit_events() {
        // latch 交给 arm_in_tx 的"只升不降 + 幂等"：连续扫描不得刷屏审计表。
        let (store, path) = test_store("latch").await;
        let credential_id = "cred_auto_latch";
        seed_position(&store, credential_id).await;
        seed_event(&store, credential_id, 1, "bar_closed", 1_000).await;

        for _ in 0..3 {
            drive_auto_triggers_once(&store, 1_000 + 180_001)
                .await
                .expect("scan runs");
        }
        let events = sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=?")
            .bind("cred:cred_auto_latch")
            .fetch_one(store.pool())
            .await
            .expect("count events");
        assert_eq!(events.get::<i64, _>("n"), 1);
        cleanup(store, path).await;
    }

    /// 一条 1m 流的夹具（不传 last_bar_ms 表示"从未收到过 bar"，即无流登记）。
    ///     /// **「应该存在但从未出现过的流」必须可发现。**
    ///
    /// 复评的反例：有敞口、配置要求 BTC/1m，但该订阅从第一根 bar 起就没起来，
    /// 心跳表里没有这一行，`streams=[]` 于是遍历空集返回 None——
    /// 安全触发器对"从一开始就死"的流完全失明。
    ///     /// **认不出周期的订阅必须在入口就被拒，不得登记后被判据静默跳过。**
    ///
    /// R7 的 RPC 只校验非空，登记之后中断判据又因为映射不出阈值而 warn 后
    /// `continue`：这条流永远不产 bar、也永远不触发 pause_new，
    /// 等于一条**对判据隐形的死流**。闭世界校验必须发生在登记之前。
    #[test]
    fn unrecognised_timeframes_have_no_threshold_and_must_not_be_accepted() {
        // 判据侧：认不出就没有阈值——这是入口必须拒绝它的理由。
        // 注意选样本时要真的取**解析不出来**的：`7m` 是能解析的
        // （解析器接受任意 `<正整数><s|m|h|d|w>`），拿它当"未知周期"
        // 会写出一条自己就不成立的用例。
        for unmappable in ["banana", "1x", "0m", "-5m", "m", ""] {
            assert!(
                timeframe_to_ms(unmappable).is_none(),
                "expected {unmappable:?} to be unmappable"
            );
            assert!(interruption_threshold_ms(unmappable, 180_000).is_none());
        }
        // 放行面自证：认得出的周期照常有阈值，收紧不得误伤。
        assert!(timeframe_to_ms("1m").is_some());
        assert!(interruption_threshold_ms("1m", 180_000).is_some());

        // **这条语义被改写了。**
        //
        // R8 在这里断言的是"若这样一条流真被登记了，判据只能跳过它"——
        // 那正是评审反例的形状：入口收紧只挡新的，一条**已经在库里**的
        // 非法行于是既不产 bar、也永远不 arm，落进两条路径中间的静默区。
        // 现在这类行进入**可处置健康判据**：立刻 pause_new，不等阈值。
        let facts = MarketDataFacts {
            credential_id: "cred-x".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: Vec::new(),
            required: vec![RequiredStream {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "banana".to_string(),
                requested_at_ms: 0,
            }],
            has_exposure: true,
        };
        let decision = market_data_interruption_decision(&facts, 9_999_999, 180_000)
            .expect("a persisted row that can never produce a bar must be actionable");
        assert_eq!(decision.level, Level::PauseNew);
        assert!(
            decision.reason.contains("can never start"),
            "the reason must separate a structurally dead stream from a stale one: {}",
            decision.reason
        );
    }

    /// **regression（High）· 结构性死流的四个形状，以及它的放行面。**
    ///
    /// 评审反例只给了历史 `7m`；这里把"重建会拒、判据看不见"的**每一类**
    /// 都摆出来（周期、市场、settle、语法），并逐条钉住放行面不被误伤。
    #[test]
    fn every_closed_world_violation_in_a_persisted_row_is_actionable() {
        let dead = |symbol_key: &str, timeframe: &str| {
            let facts = MarketDataFacts {
                credential_id: "cred-dead".to_string(),
                kind: CredentialKind::ExchangeBacked,
                streams: Vec::new(),
                required: vec![RequiredStream {
                    symbol_key: symbol_key.to_string(),
                    timeframe: timeframe.to_string(),
                    // **刚刚**被要求：证明这条判据不靠等待
                    requested_at_ms: 9_999_999,
                }],
                // **没有敞口**：结构性死流的处置不受敞口门约束
                has_exposure: false,
            };
            market_data_interruption_decision(&facts, 9_999_999, 180_000)
        };
        for (symbol_key, timeframe, what) in [
            (
                "binance|futures|BTC|USDT|USDT",
                "7m",
                "an interval the engine never serves",
            ),
            ("okx|spot|BTC|USDT|USDT", "1m", "another venue and market"),
            (
                "binance|futures|BTC|USDT|BTC",
                "1m",
                "a coin-margined settle asset",
            ),
            (
                "binance|futures|BTC|USDT",
                "1m",
                "a four-segment key with no settle",
            ),
            ("not-a-symbol-key", "1m", "a key that is not a key"),
        ] {
            let decision = dead(symbol_key, timeframe).unwrap_or_else(|| {
                panic!("{symbol_key} / {timeframe} ({what}) must be actionable")
            });
            assert_eq!(decision.level, Level::PauseNew);
        }
        // **放行面**：合法的一行，刚被要求、还没到阈值、也没有敞口 → 不判。
        assert!(
            dead("binance|futures|BTC|USDT|USDT", "1m").is_none(),
            "a legal subscription that simply has not waited long enough must not fire"
        );
        // 模拟凭据仍然不判（既有误触发边界不得被本条放宽）。
        let simulated = MarketDataFacts {
            credential_id: "cred-sim".to_string(),
            kind: CredentialKind::Simulation,
            streams: Vec::new(),
            required: vec![RequiredStream {
                symbol_key: "okx|spot|BTC|USDT|USDT".to_string(),
                timeframe: "1m".to_string(),
                requested_at_ms: 0,
            }],
            has_exposure: true,
        };
        assert!(
            market_data_interruption_decision(&simulated, 9_999_999, 180_000).is_none(),
            "simulated credentials never run a kline worker; judging them would be a \
             blanket false positive"
        );
    }

    /// **整批登记要么全成、要么全不成。**
    #[tokio::test]
    async fn required_stream_registration_is_all_or_nothing() {
        let (store, path) = test_store("required-atomic").await;

        // 正常整批：三条都在。
        let batch: Vec<(String, String)> = ["1m", "5m", "15m"]
            .iter()
            .map(|tf| ("binance|futures|BTC|USDT|USDT".to_string(), tf.to_string()))
            .collect();
        super::record_required_streams_atomically(&store, "cred-x", &batch, 1)
            .await
            .expect("batch registration");
        assert_eq!(
            super::required_streams(&store, "cred-x")
                .await
                .unwrap()
                .len(),
            3
        );

        // 重复 ensure **不刷新** first-requested：刷新会让一条从未起来的流
        // 每次重试都把计时清零，永远够不到阈值——判据等于不存在。
        super::record_required_streams_atomically(&store, "cred-x", &batch, 9_999)
            .await
            .expect("idempotent batch registration");
        let rows = super::required_streams(&store, "cred-x").await.unwrap();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(
                row.requested_at_ms, 1,
                "repeat ensure must not reset the first-requested clock"
            );
        }

        // 连接池关掉 → 整批失败，且**一条都不写**（这里已有 3 条，
        // 断言的是失败会上抛而不是留下部分写入）。
        let pool = store.pool().clone();
        pool.close().await;
        let more: Vec<(String, String)> = vec![(
            "binance|futures|ETH|USDT|USDT".to_string(),
            "1m".to_string(),
        )];
        assert!(
            super::record_required_streams_atomically(&store, "cred-x", &more, 2)
                .await
                .is_err(),
            "a failed batch must surface as an error, not a silent partial write"
        );
        let _ = std::fs::remove_file(path);
    }

    /// **sqlx-i64 核查批· 真实毫秒量级的活流不得被判中断。**
    ///
    /// 这两条用例的判别力**全在夹具的数量级上**。本模块原有的每一条判据用例
    /// 用的都是 `1_000_000` 一类小值（< `i32::MAX`），旧 sqlx 0.7.4
    /// `Any` + SQLite 的读侧截断在它们身上不可见。这里换成真实毫秒 epoch；
    /// 若驱动回归，`last_bar_ms` 会掉到 7.2e8，
    /// `gap_ms = now - 7.2e8 ≈ 1.79e12` 必然超过任何周期阈值，
    /// **每一条流在每一轮都会被判成"行情中断"并 arm `pause_new`**。
    #[tokio::test]
    async fn a_live_stream_at_a_real_millisecond_epoch_must_not_be_judged_interrupted() {
        let (store, path) = test_store("i64-heartbeat").await;
        let credential_id = "cred_auto_i64_hb";
        seed_position(&store, credential_id).await;
        // 2026-08-25 附近；低 32 位是 723097806，两者差三个数量级。
        let now_ms = 1_787_429_492_942_i64;
        seed_stream_heartbeat(
            &store,
            credential_id,
            "binance|futures|BTC|USDT|USDT",
            "1m",
            now_ms - 1_000,
        )
        .await;

        // 读侧真值自证：先钉住"库里那一行是对的"，免得夹具自己写坏了
        // 还被当成判据修好了。
        let streams = super::market_stream_heartbeats(&store, credential_id)
            .await
            .expect("read heartbeats");
        assert_eq!(streams.len(), 1);
        assert_eq!(
            streams[0].last_bar_ms,
            now_ms - 1_000,
            "the heartbeat must read back as the stored millisecond epoch, not its low 32 bits"
        );

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(
            armed.is_empty(),
            "a stream whose last bar is 1s old must not arm anything: {armed:?}"
        );
        cleanup(store, path).await;
    }

    /// 同一条历史缺陷在 `required − observed` 那半边：若 `requested_at_ms` 回归为截断值，
    /// `waited_ms = now - 7.2e8 ≈ 1.79e12`，一条**刚刚登记**的订阅立刻被判成
    /// 「从未收到过任何 bar」。
    #[tokio::test]
    async fn a_just_registered_stream_at_a_real_millisecond_epoch_must_not_be_judged_never_started()
    {
        let (store, path) = test_store("i64-required").await;
        let credential_id = "cred_auto_i64_req";
        seed_position(&store, credential_id).await;
        let now_ms = 1_787_429_492_942_i64;
        let batch = vec![(
            "binance|futures|BTC|USDT|USDT".to_string(),
            "1m".to_string(),
        )];
        super::record_required_streams_atomically(&store, credential_id, &batch, now_ms)
            .await
            .expect("register the required stream");

        let required = super::required_streams(&store, credential_id)
            .await
            .expect("read required streams");
        assert_eq!(required.len(), 1);
        assert_eq!(
            required[0].requested_at_ms, now_ms,
            "requested_at_ms must read back as the stored millisecond epoch"
        );
        let all = super::all_required_streams(&store)
            .await
            .expect("read all required streams");
        assert_eq!(all.len(), 1);
        assert_eq!(
            all[0].requested_at_ms, now_ms,
            "the restart-path reader must see the same truth"
        );

        let armed = drive_auto_triggers_once(&store, now_ms + 1_000)
            .await
            .expect("scan runs");
        assert!(
            armed.is_empty(),
            "a subscription registered 1s ago must not be judged never-started: {armed:?}"
        );
        cleanup(store, path).await;
    }

    #[test]
    fn a_required_stream_that_never_produced_a_bar_must_trigger() {
        let required = |requested_at_ms: i64| MarketDataFacts {
            credential_id: "cred-x".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: Vec::new(),
            required: vec![RequiredStream {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "1m".to_string(),
                requested_at_ms,
            }],
            has_exposure: true,
        };

        // 阈值内：起流本来就需要时间，不得误触发。
        assert!(market_data_interruption_decision(
            &required(1_000_000),
            1_000_000 + 60_000,
            180_000
        )
        .is_none());

        // 超过阈值仍无一根 bar：必须触发，且措辞与"陈旧"分得开——
        // 运维看到 never started 会去查订阅/网络起没起来。
        let decision =
            market_data_interruption_decision(&required(1_000_000), 1_000_000 + 400_000, 180_000)
                .expect("a required stream that never started must arm the trigger");
        assert_eq!(decision.level, Level::PauseNew);
        assert!(
            decision.reason.contains("never started"),
            "reason must distinguish never-started from stale: {}",
            decision.reason
        );
        assert!(decision.reason.contains("BTC"));
    }

    #[test]
    fn a_required_stream_that_is_alive_must_not_trigger() {
        // 放行面自证：同一条流一旦有心跳且不陈旧，就不得因为"在 required 里"而触发。
        let facts = MarketDataFacts {
            credential_id: "cred-x".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: vec![MarketStreamHeartbeat {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "1m".to_string(),
                last_bar_ms: 1_000_000 + 399_000,
            }],
            required: vec![RequiredStream {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "1m".to_string(),
                requested_at_ms: 1_000_000,
            }],
            has_exposure: true,
        };
        assert!(market_data_interruption_decision(&facts, 1_000_000 + 400_000, 180_000).is_none());
    }

    #[test]
    fn never_seen_streams_still_respect_the_two_false_positive_boundaries() {
        let base = |kind: CredentialKind, has_exposure: bool| MarketDataFacts {
            credential_id: "cred-x".to_string(),
            kind,
            streams: Vec::new(),
            required: vec![RequiredStream {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "1m".to_string(),
                requested_at_ms: 0,
            }],
            has_exposure,
        };
        // 模拟凭据没有 venue，kline 流本来就起不来——不得全量误报。
        assert!(market_data_interruption_decision(
            &base(CredentialKind::Simulation, true),
            9_999_999,
            180_000
        )
        .is_none());
        // 无敞口时没有要保护的东西，不得 arm 一个需要人工 reset 的开关。
        assert!(market_data_interruption_decision(
            &base(CredentialKind::ExchangeBacked, false),
            9_999_999,
            180_000
        )
        .is_none());
    }

    fn exchange_facts(last_bar_ms: Option<i64>) -> MarketDataFacts {
        MarketDataFacts {
            credential_id: "cred_live".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: last_bar_ms
                .map(|ms| {
                    vec![MarketStreamHeartbeat {
                        symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                        timeframe: "1m".to_string(),
                        last_bar_ms: ms,
                    }]
                })
                .unwrap_or_default(),
            required: Vec::new(),
            has_exposure: true,
        }
    }

    #[test]
    fn market_data_gap_beyond_threshold_arms_pause_new() {
        let decision = market_data_interruption_decision(
            &exchange_facts(Some(1_000)),
            1_000 + 180_001,
            180_000,
        )
        .expect("gap beyond threshold must arm");
        assert_eq!(decision.level, Level::PauseNew);
        assert!(decision.reason.contains("market data interrupted"));
    }

    #[test]
    fn healthy_stream_must_not_mask_a_dead_one() {
        // 段级评审 regression 的核心：首版对该 credential 下**任意**
        // bar_closed 取 MAX(ts_ms)，BTC 一直有 bar 就能永久掩盖 ETH 已经断了。
        let now_ms = 10_000_000;
        let facts = MarketDataFacts {
            credential_id: "cred_live".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: vec![
                MarketStreamHeartbeat {
                    symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                    timeframe: "1m".to_string(),
                    last_bar_ms: now_ms - 1_000, // 健康
                },
                MarketStreamHeartbeat {
                    symbol_key: "binance|futures|ETH|USDT|USDT".to_string(),
                    timeframe: "1m".to_string(),
                    last_bar_ms: now_ms - 3_600_000, // 断了一小时
                },
            ],
            required: Vec::new(),
            has_exposure: true,
        };
        let decision = market_data_interruption_decision(&facts, now_ms, 180_000)
            .expect("a dead stream must arm even when another stream is healthy");
        assert_eq!(decision.level, Level::PauseNew);
        // 审计必须报出**是哪一条流**，运维不必再去猜 symbol/周期。
        assert!(decision.reason.contains("ETH"), "{}", decision.reason);
        assert!(decision.reason.contains("1m"), "{}", decision.reason);
    }

    #[test]
    fn threshold_follows_the_timeframe_instead_of_a_fixed_180s() {
        // 固定 180s 套在所有周期上是 设计要求 明令禁止的：1h 周期的流在 180 秒后
        // 被判"中断"是误触发；真正断了 3 根（>3h）才算。
        let now_ms = 100_000_000;
        let hourly = |last_bar_ms: i64| MarketDataFacts {
            credential_id: "cred_live".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: vec![MarketStreamHeartbeat {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "1h".to_string(),
                last_bar_ms,
            }],
            required: Vec::new(),
            has_exposure: true,
        };
        // 距上一根 1h bar 才 10 分钟：远没到 3 根，不得 arm。
        assert!(
            market_data_interruption_decision(&hourly(now_ms - 600_000), now_ms, 180_000).is_none()
        );
        // 恰好 3 小时：等于阈值，不 arm（边界与相邻间隔判据同侧）。
        assert!(
            market_data_interruption_decision(&hourly(now_ms - 10_800_000), now_ms, 180_000)
                .is_none()
        );
        // 超过 3 小时一毫秒：arm。
        assert!(
            market_data_interruption_decision(&hourly(now_ms - 10_800_001), now_ms, 180_000)
                .is_some()
        );
    }

    #[test]
    fn threshold_never_goes_below_the_floor() {
        // 下界仍然有效：1s 周期的 3 根只有 3 秒，抖动就会误报。
        assert_eq!(interruption_threshold_ms("1s", 180_000), Some(180_000));
        assert_eq!(interruption_threshold_ms("1m", 180_000), Some(180_000));
        assert_eq!(interruption_threshold_ms("5m", 180_000), Some(900_000));
        assert_eq!(interruption_threshold_ms("1h", 180_000), Some(10_800_000));
        assert_eq!(interruption_threshold_ms("1d", 180_000), Some(259_200_000));
    }

    #[test]
    fn unrecognised_timeframe_is_not_evaluated_rather_than_guessed() {
        assert_eq!(timeframe_to_ms("15minutes"), None);
        assert_eq!(timeframe_to_ms(""), None);
        assert_eq!(timeframe_to_ms("0m"), None);
        assert_eq!(interruption_threshold_ms("wat", 180_000), None);

        let facts = MarketDataFacts {
            credential_id: "cred_live".to_string(),
            kind: CredentialKind::ExchangeBacked,
            streams: vec![MarketStreamHeartbeat {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                timeframe: "wat".to_string(),
                last_bar_ms: 0,
            }],
            required: Vec::new(),
            has_exposure: true,
        };
        // 认不出周期就不猜阈值——既不 arm 也不假装健康（日志留痕）。
        assert!(market_data_interruption_decision(&facts, 10_000_000_000, 180_000).is_none());
    }

    #[tokio::test]
    async fn wiring_dead_eth_stream_arms_even_while_btc_keeps_flowing() {
        // 接线级：两条流的心跳都由**生产写入方**落库，读侧必须逐条判。
        let (store, path) = test_store("stream-identity").await;
        let credential_id = "cred_streams";
        seed_position(&store, credential_id).await;
        let now_ms = 50_000_000;
        seed_stream_heartbeat(
            &store,
            credential_id,
            "binance|futures|BTC|USDT|USDT",
            "1m",
            now_ms - 30_000,
        )
        .await;
        seed_stream_heartbeat(
            &store,
            credential_id,
            "binance|futures|ETH|USDT|USDT",
            "1m",
            now_ms - 7_200_000,
        )
        .await;

        let armed = drive_auto_triggers_once(&store, now_ms)
            .await
            .expect("scan runs");
        assert!(armed
            .iter()
            .any(|(_, source)| *source == TRIGGER_SOURCE_MARKET_DATA));
        let state = armed_state(&store, "cred:cred_streams")
            .await
            .expect("state row written");
        assert!(
            state.2.contains("ETH"),
            "reason must name the dead stream: {}",
            state.2
        );
        cleanup(store, path).await;
    }

    #[test]
    fn market_data_gap_exactly_at_threshold_does_not_arm() {
        // 边界双侧：恰好等于阈值不算中断，超过一毫秒才算。
        assert!(market_data_interruption_decision(
            &exchange_facts(Some(1_000)),
            1_000 + 180_000,
            180_000
        )
        .is_none());
    }

    #[test]
    fn never_seen_a_bar_is_not_an_interruption() {
        // 模拟环境里 kline 流根本起不来，"从未有过 bar"是常态。
        assert!(
            market_data_interruption_decision(&exchange_facts(None), 10_000_000, 180_000).is_none()
        );
    }

    #[test]
    fn simulation_credential_never_arms_market_data_trigger() {
        let mut facts = exchange_facts(Some(1_000));
        facts.kind = CredentialKind::Simulation;
        assert!(market_data_interruption_decision(&facts, 10_000_000, 180_000).is_none());
    }

    #[test]
    fn unproven_identity_never_arms_market_data_trigger() {
        let mut facts = exchange_facts(Some(1_000));
        facts.kind = CredentialKind::UnprovenIdentity;
        assert!(market_data_interruption_decision(&facts, 10_000_000, 180_000).is_none());
    }

    #[test]
    fn no_exposure_does_not_arm() {
        let mut facts = exchange_facts(Some(1_000));
        facts.has_exposure = false;
        assert!(market_data_interruption_decision(&facts, 10_000_000, 180_000).is_none());
    }

    fn reject_facts_of(acks: i64, rejects: i64, errors: i64) -> OrderRejectFacts {
        OrderRejectFacts {
            credential_id: "cred_live".to_string(),
            acks,
            rejects,
            errors,
            truncated: false,
        }
    }

    #[test]
    fn reject_rate_at_threshold_arms_manage_only() {
        let decision = order_reject_rate_decision(&reject_facts_of(5, 5, 0), 5, 50)
            .expect("50% of 10 samples must arm");
        assert_eq!(decision.level, Level::ManageOnly);
        assert!(decision.reason.contains("order reject rate"));
    }

    #[test]
    fn reject_rate_below_threshold_does_not_arm() {
        assert!(order_reject_rate_decision(&reject_facts_of(6, 4, 0), 5, 50).is_none());
    }

    #[test]
    fn small_sample_never_arms() {
        // 一次失败不等于 100% 拒绝率：样本不足时既不 arm 也不清零。
        assert!(order_reject_rate_decision(&reject_facts_of(0, 4, 0), 5, 50).is_none());
    }

    #[test]
    fn exchange_errors_count_as_failures() {
        // 交易所/系统错误与显式拒绝同属"非策略性失败"，都要计入分子。
        let decision = order_reject_rate_decision(&reject_facts_of(4, 0, 6), 5, 50)
            .expect("errors alone must be able to arm");
        assert_eq!(decision.level, Level::ManageOnly);
    }

    #[test]
    fn levels_match_the_safety_model_table() {
        // SAFETY-MODEL §4.2：行情中断 → 等效 L1；订单拒绝率 → L2。
        // 把任一档位改掉（例如两条都用 pause_new）都会在这里变红。
        let market =
            market_data_interruption_decision(&exchange_facts(Some(0)), 1_000_000, 180_000)
                .expect("armed");
        let reject = order_reject_rate_decision(&reject_facts_of(0, 5, 0), 5, 50).expect("armed");
        assert_eq!(market.level, Level::PauseNew);
        assert_eq!(reject.level, Level::ManageOnly);
        assert_ne!(market.level, reject.level);
    }

    #[test]
    fn trigger_sources_are_not_manual() {
        // 人工源与自动源的降级/reset 前置不同，三个新 source 都不得混进人工集合。
        for source in [
            TRIGGER_SOURCE_MARKET_DATA,
            TRIGGER_SOURCE_ORDER_REJECT_RATE,
            TRIGGER_SOURCE_LLM_FAILURES,
            // R1-C03（#34 M2）：member_api 也不得进入人工集合——分类谓词
            // 的生产语义断言（与 v01 的入口拒绝+三表零写共同构成行为证明）。
            crate::kill_switch::TRIGGER_SOURCE_MEMBER_API,
        ] {
            assert!(
                !crate::kill_switch::source_is_manual_for_test(source),
                "{source} must not be treated as a manual trigger"
            );
        }
    }
}
