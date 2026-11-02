use anyhow::{anyhow, Context};
use sqlx::Row;

use crate::store::{DbKind, Store};

pub(crate) const LEGACY_UNKNOWN_SYMBOL: &str = "__legacy_unknown_symbol__";

pub(crate) const FILL_RECOVERY_RULE: &str = "fill_recovery_readiness";
pub(crate) const FILL_RECOVERY_REASON_CODE: &str = "FILL_RECOVERY_NOT_READY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryTrigger {
    Startup,
    Reconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryStep {
    ConnectWebSocket,
    BackfillRest,
    Reconcile,
    DeclareReady,
}

pub(crate) fn recovery_workflow(trigger: RecoveryTrigger) -> [RecoveryStep; 4] {
    match trigger {
        RecoveryTrigger::Startup | RecoveryTrigger::Reconnect => [
            RecoveryStep::ConnectWebSocket,
            RecoveryStep::BackfillRest,
            RecoveryStep::Reconcile,
            RecoveryStep::DeclareReady,
        ],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryPhaseResult {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadinessWrite {
    Ready,
    PreserveNotReady,
}

pub(crate) fn readiness_write(
    backfill: RecoveryPhaseResult,
    reconcile: RecoveryPhaseResult,
    stream: RecoveryPhaseResult,
) -> ReadinessWrite {
    match (backfill, reconcile, stream) {
        (
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
        ) => ReadinessWrite::Ready,
        _ => ReadinessWrite::PreserveNotReady,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillRecoveryScope {
    pub credential_id: String,
    pub symbol: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillStreamState {
    pub generation: i64,
    pub ready: bool,
    pub phase: String,
    pub current_symbol: Option<String>,
    pub cursor_last_trade_id: Option<i64>,
    pub failure_reason: Option<String>,
}

pub(crate) async fn register_fill_recovery_scope(
    store: &Store,
    credential_id: &str,
    symbol: &str,
    registered_at_ms: i64,
) -> anyhow::Result<bool> {
    let credential_id = require_nonempty(credential_id, "fill recovery credential")?;
    let symbol = normalize_symbol(symbol)?;
    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin scope registration")?;
    let query = match store.kind() {
        DbKind::MySql => {
            r#"
            INSERT IGNORE INTO fill_recovery_scopes (
              credential_id, symbol, registered_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?)
            "#
        }
        _ => {
            r#"
            INSERT OR IGNORE INTO fill_recovery_scopes (
              credential_id, symbol, registered_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?)
            "#
        }
    };
    let inserted = sqlx::query(query)
        .bind(credential_id)
        .bind(&symbol)
        .bind(registered_at_ms)
        .bind(registered_at_ms)
        .execute(&mut *tx)
        .await
        .context("register fill recovery scope")?
        .rows_affected()
        > 0;
    // **登记就是这一行的归属证明。**
    //
    // 迁移 0025 之后，`fill_recovery_scopes` 的每一行都要自己说明"凭什么
    // 它是一个 Binance USDⓈ-M 恢复域"：存量行取 `scope_proved_at_ms IS NULL`
    // （未复核 → 不建域），运行期登记的行则在**当场**证明。
    //
    // 证明分两段，缺一不可：
    //
    // * 归属：`ensure_running` / `start_discovered` 只会拿**已经解析成功的**
    //   USDⓈ-M 身份（`usdm_submission_identity` 的 `identity.symbol`，或
    //   一个已证明的发现结果）来登记——这一段由调用方保证；
    // * 闭世界形状：symbol 字符串本身必须过 `usdm_scope_symbol`，
    //   这一段在这里当场判。判不过就**不盖章**（行照旧登记，但不建域、
    //   不驱动回补），而不是静默当作已知。
    //
    // 存量行如果后来又被同一条生产入口登记一次，这里会把它**升级**成已证明
    // ——旧行不会因为一次复核降级就永久失去恢复域。
    match crate::binance_futures::usdm_scope_symbol(&symbol) {
        Some(_) => {
            sqlx::query(
                "UPDATE fill_recovery_scopes SET scope_known = 1, scope_proved_at_ms = ? \
                 WHERE credential_id = ? AND symbol = ?",
            )
            .bind(registered_at_ms)
            .bind(credential_id)
            .bind(&symbol)
            .execute(&mut *tx)
            .await
            .context("prove registered fill recovery scope")?;
        }
        None => tracing::error!(
            credential_id,
            %symbol,
            "fill recovery scope symbol is not a binance USDⓈ-M identity; it is registered \
             but proves no recovery scope"
        ),
    }
    if inserted {
        // ****：新登记一个恢复域同样
        // 作废在途的那一轮——这一轮的回补根本没扫过这个 symbol。不推代际
        // 的话，一次已经跑到一半的周期会在登记之后把 `ready` 写回来，
        // 而它给出的 readiness 覆盖不到这个新域。
        upsert_stream_state_in_tx(
            &mut tx,
            store.kind(),
            credential_id,
            false,
            "scope_registered",
            Some(&symbol),
            None,
            None,
            registered_at_ms,
            true,
        )
        .await?;
    }
    tx.commit().await.context("commit scope registration")?;
    Ok(inserted)
}

/// **paper 凭据隔离（2026-09-06 · 生产缺陷，High）· 交易所恢复只收
/// `ExchangeBacked` 凭据。**
///
/// `discover_fill_recovery_scopes` 的 SQL 判据是 `credential_id <> 'paper'`，
/// 只挡住了 legacy 字面量；Product Demo 的 canonical 模拟凭据是
/// `paper:<32位小写hex>`，从四个来源（scopes / cursors / fills / engine
/// orders）一路通过，直达 `binance_user_stream::start_discovered`。
/// `keys_from_env` 对模拟凭据必然返回 `Ok(None)`，于是**每次启动**都写一条
/// `startup_credentials` 失败并推进恢复代际——模拟数据在真实恢复域里制造
/// 持续的失败噪声与代际抖动（paper/live 混淆按 §3.1 属 High）。
///
/// 判别复用全仓唯一那份 `venue::credential_kind`，不在此另立文法：
///
/// * `Simulation`（`paper` 与 `paper:<32hex>`）——模拟凭据背后没有交易所
///   账户，永远不该进入交易所 fill recovery；
/// * `UnprovenIdentity`——保留 `paper` 命名空间下的畸形串（拼错一位的
///   Product Demo 凭据，如 `paper:` + 31 位 hex、大写 hex）。身份未被证明
///   的一律不得成为交易所 fallback；
/// * `ExchangeBacked`——唯一被放行的身份，真实交易所凭据照常从全部四个
///   来源产生恢复域（正域对照用例钉住，不因本判据变窄）。
fn exchange_recovery_admits_credential(credential_id: &str) -> bool {
    match crate::venue::credential_kind(credential_id) {
        crate::venue::CredentialKind::ExchangeBacked => true,
        crate::venue::CredentialKind::Simulation => {
            tracing::debug!(
                credential_id,
                "simulation credential seeds no exchange fill recovery scope"
            );
            false
        }
        crate::venue::CredentialKind::UnprovenIdentity => {
            tracing::warn!(
                credential_id,
                "credential id is not a proven exchange identity (reserved paper namespace or \
                 malformed); it seeds no exchange fill recovery scope"
            );
            false
        }
    }
}

pub(crate) async fn discover_fill_recovery_scopes(
    store: &Store,
) -> anyhow::Result<Vec<FillRecoveryScope>> {
    let rows = sqlx::query(
        r#"
        SELECT credential_id, symbol
        FROM fill_recovery_scopes
        WHERE credential_id <> 'paper'
          AND scope_known = 1
          AND scope_proved_at_ms IS NOT NULL
        UNION
        SELECT credential_id, symbol
        FROM fill_replay_cursors
        WHERE credential_id <> 'paper'
          AND scope_known = 1
          AND scope_proved_at_ms IS NOT NULL
        UNION
        SELECT credential_id, UPPER(TRIM(symbol)) AS symbol
        FROM engine_fills
        WHERE credential_id <> 'paper'
          AND symbol_scope_known = 1
          AND symbol_scope_proved_at_ms IS NOT NULL
          AND symbol IS NOT NULL
          AND TRIM(symbol) <> ''
        ORDER BY credential_id, symbol
        "#,
    )
    .fetch_all(store.pool())
    .await
    .context("discover fill recovery scopes")?;
    let mut scopes = rows
        .into_iter()
        .map(|row| FillRecoveryScope {
            credential_id: row.get::<String, _>("credential_id"),
            symbol: row.get::<String, _>("symbol").to_ascii_uppercase(),
        })
        .collect::<Vec<_>>();

    let order_rows = sqlx::query(
        "SELECT DISTINCT credential_id, exchange, market_kind, symbol_key FROM engine_orders \
         WHERE credential_id <> 'paper' ORDER BY credential_id, symbol_key",
    )
    .fetch_all(store.pool())
    .await
    .context("discover fill recovery scopes from engine orders")?;
    for row in order_rows {
        let symbol_key = row.get::<String, _>("symbol_key");
        // **恢复域也要核对两个身份来源。**
        //
        // R13 这里只把 `symbol_key` 送进 `usdm_symbol`。同一行的外层
        // `exchange` / `market_kind` 一列都没看：一条 `okx`/`spot` 的单只要
        // 内层 key 写着 `binance|futures|BTC|USDT|USDT`，就会播种一个
        // Binance USDⓈ-M 的恢复域，随后 fill recovery 拿着它去 Binance
        // 逐页回补别人的成交，并把回补结果记成本域真值。
        //
        // 矛盾行既不能建域也不能被当成"别的市场"静默跳过——它自己在撒谎，
        // 一律 `error!` 告警 + 不入恢复域。
        if let Err(reason) = crate::binance_futures::trading_identity_sources_agree(
            &symbol_key,
            &row.get::<String, _>("exchange"),
            &row.get::<String, _>("market_kind"),
        ) {
            tracing::error!(
                credential_id = %row.get::<String, _>("credential_id"),
                %symbol_key,
                exchange = %row.get::<String, _>("exchange"),
                market_kind = %row.get::<String, _>("market_kind"),
                %reason,
                "engine order carries two disagreeing trading identities; it registers no \
                 fill recovery scope"
            );
            continue;
        }
        // **恢复域的身份与交易所边界同源。**
        //
        // 这里此前是裸 `symbol_from_symbol_key`：它只取 `base`/`quote` 两段，
        // 把 `venue`/`market_kind`/`settle` 整段丢掉。于是一条
        // `okx|spot|BTC|USDT|BTC` 的本地单会被换算成 `BTCUSDT`，登记成一个
        // **Binance USDⓈ-M 的已知恢复域**——随后 fill recovery 就会拿着它
        // 去 Binance 逐页回补别人的成交，并把回补结果记成本域的真值。
        // 换成全仓唯一那份 `usdm_symbol`：闭世界之外没有 symbol，也就没有域。
        match crate::binance_futures::usdm_symbol(&symbol_key) {
            Some(symbol) => scopes.push(FillRecoveryScope {
                credential_id: row.get::<String, _>("credential_id"),
                symbol,
            }),
            None => tracing::warn!(
                credential_id = %row.get::<String, _>("credential_id"),
                %symbol_key,
                "engine order symbol_key is not a binance USDⓈ-M identity; it registers no \
                 fill recovery scope"
            ),
        }
    }
    // **paper 凭据隔离（2026-09-06，High）**：上面各条 SQL 里的 `<> 'paper'`
    // 只挡住 legacy 字面量；canonical `paper:<32hex>` 与保留命名空间下的
    // 畸形串在这里按同一份 `venue::credential_kind` 统一兜底。过滤放在
    // 四个来源汇合之后，任何一个来源都绕不过这一份分类。
    scopes.retain(|scope| exchange_recovery_admits_credential(&scope.credential_id));
    scopes.sort_by(|left, right| {
        (&left.credential_id, &left.symbol).cmp(&(&right.credential_id, &right.symbol))
    });
    scopes.dedup();
    Ok(scopes)
}

pub(crate) async fn scopes_for_credential(
    store: &Store,
    credential_id: &str,
) -> anyhow::Result<Vec<String>> {
    // **驱动回补的那一条也要按证明过滤。**
    //
    // 这不是 `discover_fill_recovery_scopes` 的副本：发现器决定"启动时给哪些
    // credential 起流"，而**这一条**才是 REST 逐页回补真正遍历的域集合
    // （`binance_user_stream::run_once`）。旧派生行如果只在发现器那边被挡住、
    // 在这里照旧返回，回补仍然会拿着一个没有任何证明的 symbol 去
    // Binance 拉别人的成交。判据与发现器同一份。
    let rows = sqlx::query(
        "SELECT symbol FROM fill_recovery_scopes \
         WHERE credential_id = ? AND scope_known = 1 AND scope_proved_at_ms IS NOT NULL \
         ORDER BY symbol",
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .context("load credential fill recovery scopes")?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("symbol"))
        .collect())
}

/// **一次恢复周期的代际号。**
///
/// 。`run_once` 在 spawn reader 之后
/// 还要跑 REST 回补与对账（好几个网络往返），而 reader 是**另一条任务**：
/// 它随时可能在这中间写下一条 `ready=0` 的 recovery failure。周期开始时拿到
/// 的这个号就是"我这一轮"的身份，`finalize_recovery_readiness` 靠它做 CAS
/// ——号被推走了（reader failure / 新登记的恢复域）就一个字节都不许写 ready。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveryCycle {
    generation: i64,
}

impl RecoveryCycle {
    #[cfg(test)]
    pub(crate) fn generation(self) -> i64 {
        self.generation
    }
}

pub(crate) async fn begin_recovery_cycle(
    store: &Store,
    credential_id: &str,
    trigger: RecoveryTrigger,
    now_ms: i64,
) -> anyhow::Result<RecoveryCycle> {
    let phase = match trigger {
        RecoveryTrigger::Startup => "startup",
        RecoveryTrigger::Reconnect => "reconnect",
    };
    let mut tx = store.pool().begin().await.context("begin recovery cycle")?;
    upsert_stream_state_in_tx(
        &mut tx,
        store.kind(),
        credential_id,
        false,
        phase,
        None,
        None,
        None,
        now_ms,
        true,
    )
    .await?;
    // 号在**同一个事务里**读回：这一行刚被本事务写过且仍持有排他锁
    // （SQLite 下连接唯一），读到的就是本周期真正拿到的那个号。
    let generation = current_generation_in_tx(&mut tx, credential_id).await?;
    tx.commit().await.context("commit recovery cycle start")?;
    Ok(RecoveryCycle { generation })
}

async fn current_generation_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
) -> anyhow::Result<i64> {
    sqlx::query("SELECT generation FROM fill_stream_state WHERE credential_id = ?")
        .bind(credential_id)
        .fetch_all(&mut **tx)
        .await
        .context("read fill stream recovery generation")?
        .into_iter()
        .next()
        .map(|row| row.get::<i64, _>("generation"))
        .ok_or_else(|| anyhow!("fill stream state row disappeared while a recovery cycle started"))
}

/// **进度写入也绑本周期的恢复代际。**
///
/// R16 这是一条无谓词的 upsert，`failure_reason` 传 `None` ⇒ **整行覆盖**。
/// 于是 `run_once` 主干每推进一步（`backfill` / `reconcile`），都会把 reader
/// 刚写下的 `phase='ws_parse'` 与那条解析失败理由**抹掉**：库里只剩
/// "一切正常，正在对账"，而这条流其实已经与交易所脱节了。真实二进制上实测
/// 到的 phase 序列就是 `startup → reconcile → cycle_failed`——`ws_parse`
/// 一次都留不住（engine safety contract：缺失/失败不得被写成正常）。
///
/// 绑代之后：reader 的失败推了代际，主干后续的进度写入一行都不命中，
/// 失败痕迹留在库里直到这一轮真的收尾。
pub(crate) async fn record_recovery_progress(
    store: &Store,
    credential_id: &str,
    cycle: RecoveryCycle,
    phase: &str,
    symbol: Option<&str>,
    cursor_last_trade_id: Option<i64>,
    now_ms: i64,
) -> anyhow::Result<()> {
    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin recovery progress")?;
    let rows = sqlx::query(
        r#"
        UPDATE fill_stream_state
        SET ready = 0, phase = ?, current_symbol = ?, cursor_last_trade_id = ?,
            failure_reason = NULL, updated_at_ms = ?
        WHERE credential_id = ? AND generation = ?
        "#,
    )
    .bind(require_nonempty(phase, "fill recovery phase")?)
    .bind(symbol)
    .bind(cursor_last_trade_id)
    .bind(now_ms)
    .bind(require_nonempty(credential_id, "fill recovery credential")?)
    .bind(cycle.generation)
    .execute(&mut *tx)
    .await
    .context("bind recovery progress to its cycle")?
    .rows_affected();
    if rows == 0 {
        tx.rollback()
            .await
            .context("rollback superseded recovery progress")?;
        tracing::warn!(
            credential_id,
            cycle_generation = cycle.generation,
            phase,
            "a recovery cycle was superseded while it was still reporting progress; the \
             progress write was discarded so the failure that superseded it stays visible"
        );
        return Ok(());
    }
    tx.commit().await.context("commit recovery progress")?;
    Ok(())
}

/// **一次失败就是一个新的代际。**
///
/// R16 这里 `increment_generation=false`：reader 任务写下 `ready=0` 之后，
/// `run_once` 主干那次**早于这一刻采样**的 `RecoveryPhaseResult::Succeeded`
/// 照样把 `ready` 写回 1（`tasks.reader_finished()` 是一次性采样，采样点与
/// ready 落库之间还隔着一次 `finalize_recovery_readiness` 的往返）。失败推代际
/// 之后，那次写入的 CAS 谓词一行都命中不了——**同一周期内失败过就不能再 ready**。
pub(crate) async fn record_recovery_failure(
    store: &Store,
    credential_id: &str,
    phase: &str,
    symbol: Option<&str>,
    cursor_last_trade_id: Option<i64>,
    reason: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    let existing = load_fill_stream_state(store, credential_id).await?;
    let current_symbol = symbol.map(str::to_string).or_else(|| {
        existing
            .as_ref()
            .and_then(|state| state.current_symbol.clone())
    });
    let cursor_last_trade_id = cursor_last_trade_id.or_else(|| {
        existing
            .as_ref()
            .and_then(|state| state.cursor_last_trade_id)
    });
    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin recovery failure")?;
    upsert_stream_state_in_tx(
        &mut tx,
        store.kind(),
        credential_id,
        false,
        phase,
        current_symbol.as_deref(),
        cursor_last_trade_id,
        Some(reason),
        now_ms,
        true,
    )
    .await?;
    tx.commit().await.context("commit recovery failure")?;
    Ok(())
}

/// **ready 写入绑恢复代际并查命中数。**
///
/// R16 这里是一条**无谓词的 upsert**：三段判据在调用方一次性算好，
/// 算完之后到落库之前发生的任何事都盖不住它。reader 是另一条任务，
/// 它在这个窗口里写下的 `ready=0` + `ws_parse` 失败理由会被这条 upsert
/// **整行覆盖**成 `ready=1 / phase='ready' / failure_reason=NULL`——
/// 一条已经与交易所脱节的流重新放行新开仓，而库里连失败痕迹都没了。
///
/// 现在写入带 `generation = <本周期的号>` 谓词：`record_recovery_failure`
/// 与 `register_fill_recovery_scope` 都推代际，命中 0 行就是"这一轮已经
/// 不是那一轮了"。0 行不是"没什么可写"——调用方拿到 `PreserveNotReady`，
/// 走的正是它原本的"没能 ready"分支（join reader、记 `disconnected_before_ready`、
/// 返回错误由外层重连）。
pub(crate) async fn finalize_recovery_readiness(
    store: &Store,
    credential_id: &str,
    backfill: RecoveryPhaseResult,
    reconcile: RecoveryPhaseResult,
    stream: RecoveryPhaseResult,
    cycle: RecoveryCycle,
    now_ms: i64,
) -> anyhow::Result<ReadinessWrite> {
    if readiness_write(backfill, reconcile, stream) != ReadinessWrite::Ready {
        return Ok(ReadinessWrite::PreserveNotReady);
    }
    let mut tx = store.pool().begin().await.context("begin ready write")?;
    let rows = sqlx::query(
        r#"
        UPDATE fill_stream_state
        SET ready = 1, phase = 'ready', current_symbol = NULL,
            cursor_last_trade_id = NULL, failure_reason = NULL, updated_at_ms = ?
        WHERE credential_id = ? AND generation = ?
        "#,
    )
    .bind(now_ms)
    .bind(require_nonempty(credential_id, "fill recovery credential")?)
    .bind(cycle.generation)
    .execute(&mut *tx)
    .await
    .context("bind the ready write to its recovery generation")?
    .rows_affected();
    if rows == 0 {
        tx.rollback()
            .await
            .context("rollback a stale ready write")?;
        tracing::error!(
            credential_id,
            cycle_generation = cycle.generation,
            "the fill recovery cycle was superseded between sampling its phases and writing \
             readiness; the handoff stays not-ready"
        );
        return Ok(ReadinessWrite::PreserveNotReady);
    }
    tx.commit().await.context("commit ready write")?;
    Ok(ReadinessWrite::Ready)
}

pub(crate) async fn load_fill_stream_state(
    store: &Store,
    credential_id: &str,
) -> anyhow::Result<Option<FillStreamState>> {
    let row = sqlx::query(
        r#"
        SELECT generation, ready, phase, current_symbol,
               CAST(cursor_last_trade_id AS CHAR) AS cursor_last_trade_id_text,
               failure_reason
        FROM fill_stream_state WHERE credential_id = ?
        "#,
    )
    .bind(credential_id)
    .fetch_optional(store.pool())
    .await
    .context("load fill stream state")?;
    row.map(|row| {
        Ok(FillStreamState {
            generation: row.get::<i64, _>("generation"),
            ready: row.get::<i64, _>("ready") != 0,
            phase: row.get::<String, _>("phase"),
            current_symbol: crate::store::try_get_nullable_string(&row, "current_symbol")?,
            // **sqlx-i64 核查批· 走文本取真值。**
            // 币安合约的 trade id 早已越过 `i32::MAX`（主流合约在 10^9 量级），
            // 旧 sqlx 0.7.4 `Any` + SQLite 会把它截成低 32 位；0.8.6 已修复。
            // 这个游标是**成交去重**的锚点：截断后的值比真值小得多，
            // 回补会从一个早得多的 trade id 重放 → 重复入账；
            // 若截断后为负（低 32 位最高位为 1，trade id 越过 2^31 后周期性发生）
            // 还会被下面的 `>= 0` 判据当成"没有游标"。两条都是账务错误。
            // NULL 仍然表示"没有游标"（原先用 `COALESCE(..., -1)` 表达）。
            cursor_last_trade_id: crate::store::try_get_optional_i64_text(
                &row,
                "cursor_last_trade_id_text",
            )?
            .filter(|value| *value >= 0),
            failure_reason: crate::store::try_get_nullable_string(&row, "failure_reason")?,
        })
    })
    .transpose()
}

#[allow(clippy::too_many_arguments)]
async fn upsert_stream_state_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    credential_id: &str,
    ready: bool,
    phase: &str,
    current_symbol: Option<&str>,
    cursor_last_trade_id: Option<i64>,
    failure_reason: Option<&str>,
    now_ms: i64,
    increment_generation: bool,
) -> anyhow::Result<()> {
    let query = match db_kind {
        DbKind::MySql => {
            r#"
            INSERT INTO fill_stream_state (
              credential_id, generation, ready, phase, current_symbol,
              cursor_last_trade_id, failure_reason, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              generation = generation + VALUES(generation),
              ready = VALUES(ready), phase = VALUES(phase),
              current_symbol = VALUES(current_symbol),
              cursor_last_trade_id = VALUES(cursor_last_trade_id),
              failure_reason = VALUES(failure_reason),
              updated_at_ms = VALUES(updated_at_ms)
            "#
        }
        _ => {
            r#"
            INSERT INTO fill_stream_state (
              credential_id, generation, ready, phase, current_symbol,
              cursor_last_trade_id, failure_reason, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(credential_id) DO UPDATE SET
              generation = fill_stream_state.generation + excluded.generation,
              ready = excluded.ready, phase = excluded.phase,
              current_symbol = excluded.current_symbol,
              cursor_last_trade_id = excluded.cursor_last_trade_id,
              failure_reason = excluded.failure_reason,
              updated_at_ms = excluded.updated_at_ms
            "#
        }
    };
    sqlx::query(query)
        .bind(require_nonempty(credential_id, "fill recovery credential")?)
        .bind(if increment_generation { 1_i64 } else { 0_i64 })
        .bind(if ready { 1_i64 } else { 0_i64 })
        .bind(require_nonempty(phase, "fill recovery phase")?)
        .bind(current_symbol)
        .bind(cursor_last_trade_id)
        .bind(failure_reason)
        .bind(now_ms)
        .execute(&mut **tx)
        .await
        .context("upsert fill stream state")?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillSymbolIdentity {
    pub value: String,
    pub scope_known: bool,
}

pub(crate) fn fill_symbol_identity(
    symbol: Option<&str>,
    symbol_key: Option<&str>,
) -> FillSymbolIdentity {
    let explicit = symbol.map(str::trim).filter(|value| !value.is_empty());
    // ****：派生身份走的是同一份 USDⓈ-M
    // 五段谓词。裸 `symbol_from_symbol_key` 会把 `okx|spot|BTC|USDT|BTC`
    // 折成 `BTCUSDT` 并把这条 fill 标成 `symbol_scope_known=1`——
    // 一个错的 venue/market/settle 就此变成一个"已知"的 Binance USDⓈ-M 域
    // （`discover_fill_recovery_scopes` 正是从 `symbol_scope_known=1` 里捞域的）。
    // 认不出闭世界身份就只能是 `__legacy_unknown_symbol__` + `scope_known=false`。
    let derived = symbol_key.and_then(crate::binance_futures::usdm_symbol);
    match explicit.map(str::to_string).or(derived) {
        Some(value) => FillSymbolIdentity {
            value: value.to_ascii_uppercase(),
            scope_known: true,
        },
        None => FillSymbolIdentity {
            value: LEGACY_UNKNOWN_SYMBOL.to_string(),
            scope_known: false,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackfillCompletion {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackfillAttempt {
    pub attempted_last_trade_id: i64,
    pub completed_at_ms: i64,
    pub completion: BackfillCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorWritePlan {
    Advance {
        last_trade_id: i64,
        completed_at_ms: i64,
    },
    PreserveAfterFailure,
}

pub(crate) fn cursor_write_plan(attempt: BackfillAttempt) -> CursorWritePlan {
    match attempt.completion {
        BackfillCompletion::Succeeded => CursorWritePlan::Advance {
            last_trade_id: attempt.attempted_last_trade_id,
            completed_at_ms: attempt.completed_at_ms,
        },
        BackfillCompletion::Failed => CursorWritePlan::PreserveAfterFailure,
    }
}

pub(crate) fn next_backfill_cursor(current: i64, observed_trade_id: i64) -> i64 {
    current.max(observed_trade_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorFinalizeOutcome {
    Stored { last_trade_id: i64 },
    PreservedAfterFailure,
}

pub(crate) async fn load_fill_replay_cursor(
    store: &Store,
    credential_id: &str,
    symbol: &str,
) -> anyhow::Result<Option<i64>> {
    // **sqlx-i64 核查批· 走文本取真值**（同 `load_fill_stream_state`：
    // 截断的 trade id 会让回补从错误的锚点重放 → 重复入账）。
    let row = sqlx::query(
        "SELECT CAST(last_trade_id AS CHAR) AS last_trade_id_text \
         FROM fill_replay_cursors WHERE credential_id = ? AND symbol = ?",
    )
    .bind(credential_id)
    .bind(normalize_symbol(symbol)?)
    .fetch_optional(store.pool())
    .await
    .context("load fill replay cursor")?;
    match row {
        None => Ok(None),
        Some(row) => Ok(Some(
            crate::store::try_get_i64_text(&row, "last_trade_id_text")
                .context("decode fill replay cursor")?,
        )),
    }
}

pub(crate) async fn finalize_fill_backfill(
    store: &Store,
    credential_id: &str,
    symbol: &str,
    attempt: BackfillAttempt,
) -> anyhow::Result<CursorFinalizeOutcome> {
    match cursor_write_plan(attempt) {
        CursorWritePlan::PreserveAfterFailure => Ok(CursorFinalizeOutcome::PreservedAfterFailure),
        CursorWritePlan::Advance {
            last_trade_id,
            completed_at_ms,
        } => {
            let symbol = normalize_symbol(symbol)?;
            let query = match store.kind() {
                DbKind::MySql => {
                    r#"
                    INSERT INTO fill_replay_cursors (
                      credential_id, symbol, last_trade_id,
                      last_success_at_ms, updated_at_ms
                    ) VALUES (?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                      last_trade_id = GREATEST(last_trade_id, VALUES(last_trade_id)),
                      last_success_at_ms = CASE
                        WHEN VALUES(last_trade_id) >= last_trade_id
                          THEN VALUES(last_success_at_ms)
                        ELSE last_success_at_ms
                      END,
                      updated_at_ms = CASE
                        WHEN VALUES(last_trade_id) >= last_trade_id
                          THEN VALUES(updated_at_ms)
                        ELSE updated_at_ms
                      END
                    "#
                }
                _ => {
                    r#"
                    INSERT INTO fill_replay_cursors (
                      credential_id, symbol, last_trade_id,
                      last_success_at_ms, updated_at_ms
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(credential_id, symbol) DO UPDATE SET
                      last_trade_id = MAX(fill_replay_cursors.last_trade_id, excluded.last_trade_id),
                      last_success_at_ms = CASE
                        WHEN excluded.last_trade_id >= fill_replay_cursors.last_trade_id
                          THEN excluded.last_success_at_ms
                        ELSE fill_replay_cursors.last_success_at_ms
                      END,
                      updated_at_ms = CASE
                        WHEN excluded.last_trade_id >= fill_replay_cursors.last_trade_id
                          THEN excluded.updated_at_ms
                        ELSE fill_replay_cursors.updated_at_ms
                      END
                    "#
                }
            };
            sqlx::query(query)
                .bind(credential_id)
                .bind(&symbol)
                .bind(last_trade_id)
                .bind(completed_at_ms)
                .bind(completed_at_ms)
                .execute(store.pool())
                .await
                .context("advance fill replay cursor")?;
            // ****：游标是恢复域发现的第二个旧
            // 派生来源。一次**成功的回补**推进的游标，其 symbol 来自
            // `scopes_for_credential` 返回的已证明域——这就是它自己的证明，
            // 当场盖章。运维手工 `reset_fill_replay_cursor` 写出来的新行**不**
            // 盖章：一次游标重置不构成"这个 credential 在这张合约上交易过"。
            if crate::binance_futures::usdm_scope_symbol(&symbol).is_some() {
                sqlx::query(
                    "UPDATE fill_replay_cursors SET scope_known = 1, scope_proved_at_ms = ? \
                     WHERE credential_id = ? AND symbol = ?",
                )
                .bind(completed_at_ms)
                .bind(credential_id)
                .bind(&symbol)
                .execute(store.pool())
                .await
                .context("prove advanced fill replay cursor")?;
            }
            let stored = load_fill_replay_cursor(store, credential_id, &symbol)
                .await?
                .ok_or_else(|| anyhow!("fill replay cursor disappeared after successful write"))?;
            Ok(CursorFinalizeOutcome::Stored {
                last_trade_id: stored,
            })
        }
    }
}

pub(crate) async fn reset_fill_replay_cursor(
    store: &Store,
    credential_id: &str,
    symbol: &str,
    new_last_trade_id: i64,
    reason: &str,
    actor: &str,
    reset_at_ms: i64,
) -> anyhow::Result<String> {
    let symbol = normalize_symbol(symbol)?;
    let reason = require_nonempty(reason, "cursor reset reason")?;
    let actor = require_nonempty(actor, "cursor reset actor")?;
    let mut tx = store.pool().begin().await.context("begin cursor reset")?;
    // **sqlx-i64 核查批· 走文本取真值**——这条被原样写进
    // `fill_replay_cursor_resets.old_last_trade_id` 审计行，截断会让审计
    // 记录下一个从未存在过的游标值。
    let old_last_trade_id = match sqlx::query(
        "SELECT CAST(last_trade_id AS CHAR) AS last_trade_id_text \
         FROM fill_replay_cursors WHERE credential_id = ? AND symbol = ?",
    )
    .bind(credential_id)
    .bind(&symbol)
    .fetch_all(&mut *tx)
    .await
    .context("load cursor before reset")?
    .into_iter()
    .next()
    {
        None => None,
        Some(row) => Some(
            crate::store::try_get_i64_text(&row, "last_trade_id_text")
                .context("decode cursor before reset")?,
        ),
    };
    let reset_id = ulid::Ulid::new().to_string();
    sqlx::query(
        r#"
        INSERT INTO fill_replay_cursor_resets (
          reset_id, credential_id, symbol, old_last_trade_id,
          new_last_trade_id, reason, actor, reset_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&reset_id)
    .bind(credential_id)
    .bind(&symbol)
    .bind(old_last_trade_id)
    .bind(new_last_trade_id)
    .bind(reason)
    .bind(actor)
    .bind(reset_at_ms)
    .execute(&mut *tx)
    .await
    .context("append fill replay cursor reset audit")?;
    let upsert = match store.kind() {
        DbKind::MySql => {
            r#"
            INSERT INTO fill_replay_cursors (
              credential_id, symbol, last_trade_id,
              last_success_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              last_trade_id = VALUES(last_trade_id),
              last_success_at_ms = VALUES(last_success_at_ms),
              updated_at_ms = VALUES(updated_at_ms)
            "#
        }
        _ => {
            r#"
            INSERT INTO fill_replay_cursors (
              credential_id, symbol, last_trade_id,
              last_success_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(credential_id, symbol) DO UPDATE SET
              last_trade_id = excluded.last_trade_id,
              last_success_at_ms = excluded.last_success_at_ms,
              updated_at_ms = excluded.updated_at_ms
            "#
        }
    };
    sqlx::query(upsert)
        .bind(credential_id)
        .bind(&symbol)
        .bind(new_last_trade_id)
        .bind(reset_at_ms)
        .bind(reset_at_ms)
        .execute(&mut *tx)
        .await
        .context("store explicitly reset fill replay cursor")?;
    tx.commit()
        .await
        .context("commit fill replay cursor reset")?;
    Ok(reset_id)
}

/// **一条 fill 行的域身份是否可被证明**。
///
/// 0017 的 backfill 只问了一句"`symbol` 非空吗"，非空就标
/// `symbol_scope_known=1`。恢复域正是从这个标记里长出来的，所以这句话等价于
/// "任何一条写着 symbol 的旧行都可以变成一个 Binance USDⓈ-M 恢复域"。
///
/// 可证明的定义与全仓其它交易所边界完全同源，一条都不放宽：
///
/// 1. 这一行有 `symbol_key`（0004 时代的行没有——**证不出来就是证不出来**，
///    没有"看着像 BTCUSDT 就算"的降级）；
/// 2. 同一行的外层 `exchange` / `market_kind` 与 key 内部逐段一致
///    （`trading_identity_sources_agree`）；
/// 3. 整条 key 落在 USDⓈ-M 闭世界内（`usdm_identity`）；
/// 4. key 推出的 symbol 与这一行实际用作域身份的 `symbol_identity` 一致
///    ——否则这条 fill 会被记进另一个域的游标。
///
/// 返回 `Err(reason)`：reason 是逐行审计证据，原样落 `evidence` 列。
pub(crate) fn fill_symbol_scope_proof(
    symbol_identity: &str,
    symbol_key: Option<&str>,
    exchange: &str,
    market_kind: &str,
) -> Result<(), String> {
    let Some(symbol_key) = symbol_key.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(
            "legacy fill row has no symbol_key; its venue/market_kind/settle identity cannot \
             be proven and it must not seed a recovery scope"
                .to_string(),
        );
    };
    crate::binance_futures::trading_identity_sources_agree(symbol_key, exchange, market_kind)?;
    let identity = crate::binance_futures::usdm_identity(symbol_key).ok_or_else(|| {
        format!("symbol_key {symbol_key:?} is not a binance futures USDⓈ-M identity")
    })?;
    if !identity.symbol.eq_ignore_ascii_case(symbol_identity.trim()) {
        return Err(format!(
            "symbol_key {symbol_key:?} resolves to {:?} but the row is scoped under \
             {symbol_identity:?}",
            identity.symbol
        ));
    }
    Ok(())
}

pub(crate) const FILL_SCOPE_VERDICT_PROVEN: &str = "usdm_identity_proven";
pub(crate) const FILL_SCOPE_VERDICT_UNPROVABLE: &str = "unprovable_identity";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct FillScopeRevalidation {
    pub examined: u64,
    pub proven: u64,
    pub demoted: u64,
}

/// **对 0017 遗留标记的可审计复核**（迁移 0024 的执行侧）。
///
/// 迁移本身零 DML：它只加了一列 `symbol_scope_proved_at_ms`，
/// 于是**所有**存量行都是"未复核"，`discover_fill_recovery_scopes`
/// 立刻对它们 fail-closed。这个函数在 `Store::connect` 里、迁移之后、
/// 任何恢复域发现之前，用**生产谓词本身**逐行复核并写下证据：
///
/// * 证明得了 → 保持 `symbol_scope_known` 原值（1），记 `proven`；
/// * 证明不了 → `symbol_scope_known=0`，不再播种恢复域，记 `unprovable`。
///
/// 幂等：只取 `symbol_scope_proved_at_ms IS NULL` 的行；跑过一次之后
/// 再跑是空集。审计行 PK 与 fill 行同形，重跑不会重复计数。
pub(crate) async fn revalidate_legacy_fill_symbol_scopes(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<FillScopeRevalidation> {
    let rows = sqlx::query(
        r#"
        SELECT credential_id, symbol_identity, exchange_trade_id, symbol_scope_known,
               exchange, market_kind, symbol_key
        FROM engine_fills
        WHERE symbol_scope_proved_at_ms IS NULL
        ORDER BY credential_id, symbol_identity, exchange_trade_id
        "#,
    )
    .fetch_all(store.pool())
    .await
    .context("load fill rows whose symbol scope was never revalidated")?;
    if rows.is_empty() {
        return Ok(FillScopeRevalidation::default());
    }
    let mut outcome = FillScopeRevalidation::default();
    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin fill symbol scope revalidation")?;
    for row in rows {
        let credential_id = row.get::<String, _>("credential_id");
        let symbol_identity = row.get::<String, _>("symbol_identity");
        let exchange_trade_id = row.get::<String, _>("exchange_trade_id");
        let previous_scope_known = row.get::<i64, _>("symbol_scope_known");
        let exchange = crate::store::try_get_nullable_string(&row, "exchange")?.unwrap_or_default();
        let market_kind =
            crate::store::try_get_nullable_string(&row, "market_kind")?.unwrap_or_default();
        let symbol_key = crate::store::try_get_nullable_string(&row, "symbol_key")?;
        let proof = fill_symbol_scope_proof(
            &symbol_identity,
            symbol_key.as_deref(),
            &exchange,
            &market_kind,
        );
        let (resolved_scope_known, verdict, evidence) = match &proof {
            Ok(()) => (
                previous_scope_known,
                FILL_SCOPE_VERDICT_PROVEN,
                format!(
                    "symbol_key={symbol_key:?} exchange={exchange:?} market_kind={market_kind:?} \
                     proves scope {symbol_identity:?}"
                ),
            ),
            Err(reason) => (0_i64, FILL_SCOPE_VERDICT_UNPROVABLE, reason.clone()),
        };
        outcome.examined += 1;
        if proof.is_ok() {
            outcome.proven += 1;
        } else {
            if previous_scope_known != 0 {
                tracing::warn!(
                    %credential_id,
                    %symbol_identity,
                    %exchange_trade_id,
                    evidence = %evidence,
                    "legacy fill row was marked scope-known by migration 0017 but its trading \
                     identity cannot be proven; it no longer seeds a fill recovery scope"
                );
            }
            outcome.demoted += 1;
        }
        sqlx::query(
            r#"
            UPDATE engine_fills
            SET symbol_scope_known = ?, symbol_scope_proved_at_ms = ?
            WHERE credential_id = ? AND symbol_identity = ? AND exchange_trade_id = ?
            "#,
        )
        .bind(resolved_scope_known)
        .bind(now_ms)
        .bind(&credential_id)
        .bind(&symbol_identity)
        .bind(&exchange_trade_id)
        .execute(&mut *tx)
        .await
        .context("write revalidated fill symbol scope")?;
        let audit = match store.kind() {
            DbKind::MySql => {
                r#"
                INSERT IGNORE INTO fill_symbol_scope_revalidations (
                  credential_id, symbol_identity, exchange_trade_id, previous_scope_known,
                  resolved_scope_known, verdict, evidence, revalidated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#
            }
            _ => {
                r#"
                INSERT OR IGNORE INTO fill_symbol_scope_revalidations (
                  credential_id, symbol_identity, exchange_trade_id, previous_scope_known,
                  resolved_scope_known, verdict, evidence, revalidated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#
            }
        };
        sqlx::query(audit)
            .bind(&credential_id)
            .bind(&symbol_identity)
            .bind(&exchange_trade_id)
            .bind(previous_scope_known)
            .bind(resolved_scope_known)
            .bind(verdict)
            .bind(&evidence)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .context("append fill symbol scope revalidation audit")?;
    }
    tx.commit()
        .await
        .context("commit fill symbol scope revalidation")?;
    if outcome.demoted > 0 {
        tracing::warn!(
            examined = outcome.examined,
            proven = outcome.proven,
            demoted = outcome.demoted,
            "fill symbol scope revalidation demoted legacy rows to unknown"
        );
    }
    Ok(outcome)
}

pub(crate) const SCOPE_ORIGIN_SCOPES: &str = "fill_recovery_scopes";
pub(crate) const SCOPE_ORIGIN_CURSORS: &str = "fill_replay_cursors";

pub(crate) const SCOPE_ORIGIN_VERDICT_PROVED_BY_FILL: &str = "proved_by_revalidated_fill";
pub(crate) const SCOPE_ORIGIN_VERDICT_PROVED_BY_ORDER: &str = "proved_by_agreeing_engine_order";
pub(crate) const SCOPE_ORIGIN_VERDICT_UNPROVABLE: &str = "unprovable_origin";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ScopeOriginRevalidation {
    pub examined: u64,
    pub proven: u64,
    pub demoted: u64,
}

/// **一条旧派生 `(credential_id, symbol)` 行凭什么是一个恢复域。**
///
/// 。`fill_recovery_scopes` / `fill_replay_cursors`
/// 只有两列，没有任何交易身份，所以这一行**自己证不了自己**——证明只能来自
/// 引擎里已经过生产谓词判定的源：
///
/// 1. symbol 字符串本身必须落在 USDⓈ-M 闭世界内（`usdm_scope_symbol`）；
/// 2. 且这个 credential 至少有一条**已复核且已知域**的 `engine_fills` 行落在
///    这个 symbol 上（0024 的结论，`symbol_scope_proved_at_ms IS NOT NULL`），
///    **或**至少有一条 `engine_orders` 行，其外层 `exchange`/`market_kind`
///    与内层 `symbol_key` 双源一致（`trading_identity_sources_agree`）
///    且 key 推出的 USDⓈ-M symbol 与这一行逐字相同。
///
/// 这两条正是 `discover_fill_recovery_scopes` 另外两个 UNION 分支的判据
/// ——换句话说：**可证明的旧派生行本来就会被那两个分支重建出来**，
/// 证不出来的那些才是这条修法要挡住的。
///
/// 返回 `Err(reason)`：reason 是逐行审计证据，原样落 `evidence` 列。
async fn fill_scope_origin_proof(
    store: &Store,
    credential_id: &str,
    symbol: &str,
) -> anyhow::Result<Result<&'static str, String>> {
    let Some(symbol) = crate::binance_futures::usdm_scope_symbol(symbol) else {
        return Ok(Err(format!(
            "legacy scope symbol {symbol:?} is not a binance futures USDⓈ-M identity; \
             it must not seed a recovery scope"
        )));
    };
    let proved_fills = sqlx::query(
        r#"
        SELECT COUNT(*) AS matches
        FROM engine_fills
        WHERE credential_id = ?
          AND symbol_scope_known = 1
          AND symbol_scope_proved_at_ms IS NOT NULL
          AND UPPER(TRIM(symbol_identity)) = ?
        "#,
    )
    .bind(credential_id)
    .bind(&symbol)
    .fetch_one(store.pool())
    .await
    .context("count revalidated fills supporting a legacy fill recovery scope")?
    .get::<i64, _>("matches");
    if proved_fills > 0 {
        return Ok(Ok(SCOPE_ORIGIN_VERDICT_PROVED_BY_FILL));
    }
    let order_rows = sqlx::query(
        "SELECT DISTINCT exchange, market_kind, symbol_key FROM engine_orders \
         WHERE credential_id = ?",
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .context("load engine orders supporting a legacy fill recovery scope")?;
    for row in order_rows {
        let symbol_key = row.get::<String, _>("symbol_key");
        let exchange = crate::store::try_get_nullable_string(&row, "exchange")?.unwrap_or_default();
        let market_kind =
            crate::store::try_get_nullable_string(&row, "market_kind")?.unwrap_or_default();
        if crate::binance_futures::trading_identity_sources_agree(
            &symbol_key,
            &exchange,
            &market_kind,
        )
        .is_err()
        {
            continue;
        }
        if crate::binance_futures::usdm_symbol(&symbol_key)
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(&symbol))
        {
            return Ok(Ok(SCOPE_ORIGIN_VERDICT_PROVED_BY_ORDER));
        }
    }
    Ok(Err(format!(
        "no revalidated fill and no engine order with two agreeing trading identities \
         supports scope {symbol:?} for credential {credential_id:?}"
    )))
}

/// **对 0017/0018 旧派生恢复域的可审计复核**（迁移 0025 的执行侧）。
///
/// 迁移本身零 DML：它只给两张旧表各加了 `scope_known` / `scope_proved_at_ms`，
/// 于是**所有**存量行都是"未复核"，`discover_fill_recovery_scopes` 与
/// `scopes_for_credential` 立刻对它们 fail-closed。这个函数在 `Store::connect`
/// 里、迁移之后、**0024 的 fill 复核之后**、任何恢复域发现之前，用生产谓词
/// 逐行复核并写下证据（顺序不能反：本函数的第一条判据消费的就是 0024 的结论）。
///
/// 幂等：只取 `scope_proved_at_ms IS NULL` 的行；跑过一次之后再跑是空集。
/// 审计行 PK 与旧行同形，重跑不会重复计数。
pub(crate) async fn revalidate_legacy_fill_scope_origins(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<ScopeOriginRevalidation> {
    let mut outcome = ScopeOriginRevalidation::default();
    for origin in [SCOPE_ORIGIN_SCOPES, SCOPE_ORIGIN_CURSORS] {
        let rows = sqlx::query(&format!(
            "SELECT credential_id, symbol FROM {origin} \
             WHERE scope_proved_at_ms IS NULL ORDER BY credential_id, symbol"
        ))
        .fetch_all(store.pool())
        .await
        .with_context(|| format!("load unrevalidated rows from {origin}"))?;
        for row in rows {
            let credential_id = row.get::<String, _>("credential_id");
            let symbol = row.get::<String, _>("symbol");
            let proof = fill_scope_origin_proof(store, &credential_id, &symbol).await?;
            let (resolved_scope_known, verdict, evidence) = match &proof {
                Ok(verdict) => (
                    1_i64,
                    *verdict,
                    format!("scope {symbol:?} for credential {credential_id:?} is {verdict}"),
                ),
                Err(reason) => (0_i64, SCOPE_ORIGIN_VERDICT_UNPROVABLE, reason.clone()),
            };
            outcome.examined += 1;
            if proof.is_ok() {
                outcome.proven += 1;
            } else {
                tracing::warn!(
                    %credential_id,
                    %symbol,
                    %origin,
                    evidence = %evidence,
                    "legacy fill recovery origin row has no proved fill and no engine order \
                     with two agreeing trading identities; it no longer seeds a recovery scope"
                );
                outcome.demoted += 1;
            }
            let mut tx = store
                .pool()
                .begin()
                .await
                .context("begin fill scope origin revalidation")?;
            sqlx::query(&format!(
                "UPDATE {origin} SET scope_known = ?, scope_proved_at_ms = ? \
                 WHERE credential_id = ? AND symbol = ?"
            ))
            .bind(resolved_scope_known)
            .bind(now_ms)
            .bind(&credential_id)
            .bind(&symbol)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("write revalidated scope origin into {origin}"))?;
            let audit = match store.kind() {
                DbKind::MySql => {
                    r#"
                    INSERT IGNORE INTO fill_scope_origin_revalidations (
                      origin_table, credential_id, symbol, resolved_scope_known,
                      verdict, evidence, revalidated_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#
                }
                _ => {
                    r#"
                    INSERT OR IGNORE INTO fill_scope_origin_revalidations (
                      origin_table, credential_id, symbol, resolved_scope_known,
                      verdict, evidence, revalidated_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#
                }
            };
            sqlx::query(audit)
                .bind(origin)
                .bind(&credential_id)
                .bind(&symbol)
                .bind(resolved_scope_known)
                .bind(verdict)
                .bind(&evidence)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .context("append fill scope origin revalidation audit")?;
            tx.commit()
                .await
                .context("commit fill scope origin revalidation")?;
        }
    }
    if outcome.demoted > 0 {
        tracing::warn!(
            examined = outcome.examined,
            proven = outcome.proven,
            demoted = outcome.demoted,
            "fill scope origin revalidation demoted legacy derived rows to unknown"
        );
    }
    Ok(outcome)
}

fn normalize_symbol(symbol: &str) -> anyhow::Result<String> {
    let symbol = require_nonempty(symbol, "fill replay symbol")?;
    Ok(symbol.to_ascii_uppercase())
}

fn require_nonempty<'a>(value: &'a str, label: &str) -> anyhow::Result<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        Err(anyhow!("{label} must not be empty"))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Once;

    use sqlx::any::AnyPoolOptions;
    use sqlx::migrate::Migrator;

    use super::*;

    static INSTALL_TEST_DRIVERS: Once = Once::new();

    async fn test_store(label: &str) -> (Store, std::path::PathBuf) {
        INSTALL_TEST_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-t39-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect fill replay test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: std::path::PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn insert_test_fill(
        store: &Store,
        credential_id: &str,
        symbol: &str,
        trade_id: &str,
    ) -> bool {
        let mut tx = store.pool().begin().await.expect("begin test fill insert");
        let inserted = crate::svc::insert_fill_if_new(
            &mut tx,
            store.kind(),
            credential_id,
            trade_id,
            &format!("order-{symbol}-{trade_id}"),
            None,
            None,
            "binance",
            "futures",
            None,
            Some(symbol),
            Some("BUY"),
            Some("0.01"),
            Some("100"),
            None,
            None,
            Some("0"),
            Some(1_000),
            Some(1_000),
            None,
        )
        .await
        .expect("insert symbol-scoped fill");
        tx.commit().await.expect("commit test fill insert");
        inserted
    }

    fn rest_trade(
        id: i64,
        symbol: &str,
        order_id: i64,
    ) -> crate::binance_futures::BinanceUserTrade {
        crate::binance_futures::BinanceUserTrade {
            id,
            symbol: symbol.to_string(),
            order_id: Some(order_id),
            side: Some("BUY".to_string()),
            price: Some("100".to_string()),
            qty: Some("0.01".to_string()),
            quote_qty: Some("1".to_string()),
            commission: Some("0.001".to_string()),
            commission_asset: Some("USDT".to_string()),
            realized_pnl: Some("0".to_string()),
            time: Some(1_000 + id),
            additional_fields: serde_json::Map::new(),
        }
    }

    async fn fill_set(store: &Store, credential_id: &str) -> Vec<(String, String, String)> {
        sqlx::query(
            r#"
            SELECT symbol_identity, exchange_trade_id, engine_order_id
            FROM engine_fills WHERE credential_id = ?
            ORDER BY symbol_identity, exchange_trade_id, engine_order_id
            "#,
        )
        .bind(credential_id)
        .fetch_all(store.pool())
        .await
        .expect("load complete fill set")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("symbol_identity"),
                row.get::<String, _>("exchange_trade_id"),
                row.get::<String, _>("engine_order_id"),
            )
        })
        .collect()
    }

    fn success(last_trade_id: i64, completed_at_ms: i64) -> BackfillAttempt {
        BackfillAttempt {
            attempted_last_trade_id: last_trade_id,
            completed_at_ms,
            completion: BackfillCompletion::Succeeded,
        }
    }

    #[test]
    fn t39_fill_symbol_identity_prefers_explicit_symbol() {
        assert_eq!(
            fill_symbol_identity(Some(" ethusdt "), Some("binance|futures|BTC|USDT|USDT")),
            FillSymbolIdentity {
                value: "ETHUSDT".to_string(),
                scope_known: true,
            }
        );
    }

    #[test]
    fn t39_fill_symbol_identity_derives_from_symbol_key() {
        assert_eq!(
            fill_symbol_identity(None, Some("binance|futures|BTC|USDT|USDT")),
            FillSymbolIdentity {
                value: "BTCUSDT".to_string(),
                scope_known: true,
            }
        );
    }

    #[test]
    fn t39_fill_symbol_identity_marks_unrecoverable_legacy_rows_unknown() {
        assert_eq!(
            fill_symbol_identity(None, None),
            FillSymbolIdentity {
                value: LEGACY_UNKNOWN_SYMBOL.to_string(),
                scope_known: false,
            }
        );
    }

    #[test]
    fn t39_cursor_symbol_normalization_is_exact_and_rejects_empty_input() {
        assert_eq!(
            normalize_symbol(" ethusdt \t").expect("normalize symbol"),
            "ETHUSDT"
        );
        assert_eq!(
            format!(
                "{:#}",
                normalize_symbol("  ").expect_err("empty cursor symbol must fail")
            ),
            "fill replay symbol must not be empty"
        );
    }

    #[test]
    fn t39_cursor_write_plan_has_distinct_success_and_failure_outcomes() {
        assert_eq!(
            cursor_write_plan(success(42, 1_700)),
            CursorWritePlan::Advance {
                last_trade_id: 42,
                completed_at_ms: 1_700,
            }
        );
        assert_eq!(
            cursor_write_plan(BackfillAttempt {
                attempted_last_trade_id: 99,
                completed_at_ms: 1_800,
                completion: BackfillCompletion::Failed,
            }),
            CursorWritePlan::PreserveAfterFailure
        );
    }

    #[tokio::test]
    async fn t39_failed_backfill_does_not_advance_cursor() {
        let (store, path) = test_store("failed-preserves").await;
        finalize_fill_backfill(&store, "cred-a", "BTCUSDT", success(10, 100))
            .await
            .expect("seed cursor");
        let outcome = finalize_fill_backfill(
            &store,
            "cred-a",
            "BTCUSDT",
            BackfillAttempt {
                attempted_last_trade_id: 20,
                completed_at_ms: 200,
                completion: BackfillCompletion::Failed,
            },
        )
        .await
        .expect("finalize failed backfill without writing");
        assert_eq!(outcome, CursorFinalizeOutcome::PreservedAfterFailure);
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-a", "BTCUSDT")
                .await
                .expect("load cursor"),
            Some(10)
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39_cursor_is_independent_per_credential_and_symbol() {
        let (store, path) = test_store("scope").await;
        for (credential, symbol, value) in [
            ("cred-a", "BTCUSDT", 11),
            ("cred-a", "ETHUSDT", 22),
            ("cred-b", "BTCUSDT", 33),
        ] {
            finalize_fill_backfill(&store, credential, symbol, success(value, value))
                .await
                .expect("store scoped cursor");
        }
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-a", "BTCUSDT")
                .await
                .expect("load A BTC"),
            Some(11)
        );
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-a", "ETHUSDT")
                .await
                .expect("load A ETH"),
            Some(22)
        );
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-b", "BTCUSDT")
                .await
                .expect("load B BTC"),
            Some(33)
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39_cursor_survives_store_restart() {
        let (store, path) = test_store("restart").await;
        finalize_fill_backfill(&store, "cred-restart", "BTCUSDT", success(44, 400))
            .await
            .expect("store cursor before restart");
        store.pool().close().await;
        let reopened = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("reopen fill replay store");
        assert_eq!(
            load_fill_replay_cursor(&reopened, "cred-restart", "BTCUSDT")
                .await
                .expect("load cursor after restart"),
            Some(44)
        );
        cleanup(reopened, path).await;
    }

    #[tokio::test]
    async fn t39_cursor_success_is_monotonic_without_reset() {
        let (store, path) = test_store("monotonic").await;
        assert_eq!(
            finalize_fill_backfill(&store, "cred-mono", "BTCUSDT", success(50, 500))
                .await
                .expect("advance cursor"),
            CursorFinalizeOutcome::Stored { last_trade_id: 50 }
        );
        assert_eq!(
            finalize_fill_backfill(&store, "cred-mono", "BTCUSDT", success(40, 600))
                .await
                .expect("ignore cursor regression"),
            CursorFinalizeOutcome::Stored { last_trade_id: 50 }
        );
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-mono", "BTCUSDT")
                .await
                .expect("load monotonic cursor"),
            Some(50)
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39_explicit_cursor_reset_is_atomic_and_audited() {
        let (store, path) = test_store("reset").await;
        finalize_fill_backfill(&store, "cred-reset", "BTCUSDT", success(70, 700))
            .await
            .expect("seed cursor");
        let reset_id = reset_fill_replay_cursor(
            &store,
            "cred-reset",
            "BTCUSDT",
            12,
            "operator-approved replay",
            "local-test-operator",
            800,
        )
        .await
        .expect("reset cursor with audit");
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-reset", "BTCUSDT")
                .await
                .expect("load reset cursor"),
            Some(12)
        );
        let row = sqlx::query(
            r#"
            SELECT reset_id, credential_id, symbol, old_last_trade_id,
                   new_last_trade_id, reason, actor, reset_at_ms
            FROM fill_replay_cursor_resets
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load reset audit");
        assert_eq!(row.get::<String, _>("reset_id"), reset_id);
        assert_eq!(row.get::<String, _>("credential_id"), "cred-reset");
        assert_eq!(row.get::<String, _>("symbol"), "BTCUSDT");
        assert_eq!(row.get::<i64, _>("old_last_trade_id"), 70);
        assert_eq!(row.get::<i64, _>("new_last_trade_id"), 12);
        assert_eq!(row.get::<String, _>("reason"), "operator-approved replay");
        assert_eq!(row.get::<String, _>("actor"), "local-test-operator");
        assert_eq!(row.get::<i64, _>("reset_at_ms"), 800);
        cleanup(store, path).await;
    }

    /// **sqlx-i64 核查批· 真实量级的 trade id 必须原样读回。**
    ///
    /// 本模块既有的游标用例一律用 `12` / `50` / `70` 这类小值，截断对它们
    /// 是恒等，所以对本缺陷完全失明。真实的币安 USDⓈ-M trade id 早已越过
    /// `i32::MAX`（主流合约在 10^9–10^10 量级）。旧 sqlx 0.7.4
    /// `Any` + SQLite 会把它截成低 32 位——而这个游标是**成交去重**的锚点：
    ///
    /// - `5_000_000_123` → `705_032_827`：回补会从一个早得多的 trade id 重放，
    ///   同一批成交被重复入账；
    /// - `8_000_000_000` → `-589_934_592`（低 32 位最高位为 1）：既是错值，
    ///   还会被 `load_fill_stream_state` 的 `>= 0` 判据当成"没有游标"。
    ///
    /// 两条都是账务错误，所以这条用例同时钉住三个读点
    /// （`load_fill_replay_cursor` / `reset_fill_replay_cursor` 的审计行 /
    /// `load_fill_stream_state` 的 `cursor_last_trade_id`）。
    #[tokio::test]
    async fn t39_cursors_round_trip_real_sized_exchange_trade_ids() {
        let (store, path) = test_store("i64-cursor").await;
        // 正域越界 + 低 32 位为负 的两个真实量级样本。
        for (symbol, trade_id) in [
            ("BTCUSDT", 5_000_000_123_i64),
            ("ETHUSDT", 8_000_000_000_i64),
        ] {
            assert!(trade_id > i64::from(i32::MAX));
            finalize_fill_backfill(&store, "cred-wide", symbol, success(trade_id, 700))
                .await
                .expect("advance the cursor to a real-sized trade id");
            assert_eq!(
                load_fill_replay_cursor(&store, "cred-wide", symbol)
                    .await
                    .expect("load the cursor"),
                Some(trade_id),
                "the dedup cursor must read back as the stored trade id, not its low 32 bits"
            );
        }

        // 重置的审计行记的是**旧游标**，截断会让审计留下一个从未存在过的值。
        let old = 5_000_000_123_i64;
        let new = 6_000_000_456_i64;
        reset_fill_replay_cursor(
            &store,
            "cred-wide",
            "BTCUSDT",
            new,
            "operator-approved replay",
            "local-test-operator",
            1_787_429_492_942,
        )
        .await
        .expect("reset the cursor");
        let row = sqlx::query(
            "SELECT CAST(old_last_trade_id AS CHAR) AS old_text, \
                    CAST(new_last_trade_id AS CHAR) AS new_text \
             FROM fill_replay_cursor_resets WHERE credential_id='cred-wide'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load the reset audit row");
        assert_eq!(
            crate::store::try_get_i64_text(&row, "old_text").expect("decode old cursor"),
            old,
            "the audit row must record the real previous cursor"
        );
        assert_eq!(
            crate::store::try_get_i64_text(&row, "new_text").expect("decode new cursor"),
            new
        );

        // handoff 状态里的游标投影同样是真值。
        record_recovery_failure(
            &store,
            "cred-wide",
            "backfill",
            Some("BTCUSDT"),
            Some(new),
            "probe",
            1_787_429_492_942,
        )
        .await
        .expect("write the fill stream state");
        let state = load_fill_stream_state(&store, "cred-wide")
            .await
            .expect("load the fill stream state")
            .expect("the state row exists");
        assert_eq!(
            state.cursor_last_trade_id,
            Some(new),
            "the handoff projection must carry the real cursor"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39_migration_preserves_and_classifies_preexisting_fill_rows() {
        INSTALL_TEST_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-t39-migration-{}.db", ulid::Ulid::new()));
        std::fs::File::create(&path).expect("create pre-0017 SQLite database file");
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect pre-0017 database");
        let all = sqlx::migrate!("./migrations");
        let pre_0017 = Migrator {
            migrations: Cow::Owned(
                all.iter()
                    .filter(|migration| migration.version < 17)
                    .cloned()
                    .collect(),
            ),
            ignore_missing: false,
            locking: true,
            no_tx: false,
        };
        pre_0017
            .run(&pool)
            .await
            .expect("apply migrations through 0016");
        for (trade_id, symbol) in [("known-id", Some("btcusdt")), ("unknown-id", None)] {
            sqlx::query(
                r#"
                INSERT INTO engine_fills (
                  credential_id, exchange_trade_id, engine_order_id,
                  exchange, market_kind, symbol, created_at_ms
                ) VALUES ('legacy-cred', ?, 'legacy-order', 'binance',
                          'futures', ?, 1)
                "#,
            )
            .bind(trade_id)
            .bind(symbol)
            .execute(&pool)
            .await
            .expect("insert legacy fill fixture");
        }
        all.run(&pool).await.expect("apply 0017 migration");
        let rows = sqlx::query(
            r#"
            SELECT exchange_trade_id, symbol_identity, symbol_scope_known
            FROM engine_fills ORDER BY exchange_trade_id
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("load migrated fills");
        let actual = rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("exchange_trade_id"),
                    row.get::<String, _>("symbol_identity"),
                    row.get::<i64, _>("symbol_scope_known"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                ("known-id".to_string(), "BTCUSDT".to_string(), 1),
                (
                    "unknown-id".to_string(),
                    LEGACY_UNKNOWN_SYMBOL.to_string(),
                    0,
                ),
            ]
        );
        let legacy_count = sqlx::query("SELECT COUNT(*) AS n FROM engine_fills_legacy_0004")
            .fetch_one(&pool)
            .await
            .expect("load preserved legacy table")
            .get::<i64, _>("n");
        assert_eq!(legacy_count, 2);
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn t39_exchange_trade_id_is_unique_within_not_across_symbols() {
        let (store, path) = test_store("symbol-unique").await;
        assert!(insert_test_fill(&store, "cred-symbol", "BTCUSDT", "77").await);
        assert!(insert_test_fill(&store, "cred-symbol", "ETHUSDT", "77").await);
        assert!(!insert_test_fill(&store, "cred-symbol", "BTCUSDT", "77").await);
        let rows = sqlx::query(
            r#"
            SELECT symbol_identity, exchange_trade_id
            FROM engine_fills
            WHERE credential_id = 'cred-symbol'
            ORDER BY symbol_identity
            "#,
        )
        .fetch_all(store.pool())
        .await
        .expect("load symbol-scoped fills");
        let actual = rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("symbol_identity"),
                    row.get::<String, _>("exchange_trade_id"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                ("BTCUSDT".to_string(), "77".to_string()),
                ("ETHUSDT".to_string(), "77".to_string()),
            ]
        );
        cleanup(store, path).await;
    }

    #[test]
    fn t39b_startup_workflow_includes_backfill_before_reconcile_and_ready() {
        assert_eq!(
            recovery_workflow(RecoveryTrigger::Startup),
            [
                RecoveryStep::ConnectWebSocket,
                RecoveryStep::BackfillRest,
                RecoveryStep::Reconcile,
                RecoveryStep::DeclareReady,
            ]
        );
    }

    #[test]
    fn t39b_reconnect_workflow_includes_backfill_before_reconcile_and_ready() {
        assert_eq!(
            recovery_workflow(RecoveryTrigger::Reconnect),
            [
                RecoveryStep::ConnectWebSocket,
                RecoveryStep::BackfillRest,
                RecoveryStep::Reconcile,
                RecoveryStep::DeclareReady,
            ]
        );
    }

    #[test]
    fn t39b_ready_is_not_written_while_backfill_is_pending() {
        assert_eq!(
            readiness_write(
                RecoveryPhaseResult::Pending,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
            ),
            ReadinessWrite::PreserveNotReady
        );
    }

    #[test]
    fn t39b_ready_is_not_written_after_backfill_failure() {
        assert_eq!(
            readiness_write(
                RecoveryPhaseResult::Failed,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
            ),
            ReadinessWrite::PreserveNotReady
        );
    }

    #[test]
    fn t39b_ready_is_not_written_after_reconcile_failure() {
        assert_eq!(
            readiness_write(
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Failed,
                RecoveryPhaseResult::Succeeded,
            ),
            ReadinessWrite::PreserveNotReady
        );
    }

    #[test]
    fn t39b_ready_is_not_written_after_stream_disconnect() {
        assert_eq!(
            readiness_write(
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Failed,
            ),
            ReadinessWrite::PreserveNotReady
        );
    }

    #[test]
    fn t39b_ready_requires_backfill_and_reconcile_success() {
        assert_eq!(
            readiness_write(
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
            ),
            ReadinessWrite::Ready
        );
    }

    #[tokio::test]
    async fn t39b_new_scope_invalidates_existing_ready_state() {
        let (store, path) = test_store("scope-invalidates-ready").await;
        let cycle = begin_recovery_cycle(&store, "cred-ready", RecoveryTrigger::Startup, 10)
            .await
            .expect("start initial cycle");
        assert_eq!(
            finalize_recovery_readiness(
                &store,
                "cred-ready",
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                20,
            )
            .await
            .expect("mark initial cycle ready"),
            ReadinessWrite::Ready
        );
        assert!(
            load_fill_stream_state(&store, "cred-ready")
                .await
                .expect("load ready state")
                .expect("state exists")
                .ready
        );
        assert!(
            register_fill_recovery_scope(&store, "cred-ready", "BTCUSDT", 30)
                .await
                .expect("register new scope")
        );
        assert_eq!(
            load_fill_stream_state(&store, "cred-ready")
                .await
                .expect("load invalidated state")
                .expect("state exists")
                .ready,
            false
        );
        cleanup(store, path).await;
    }

    /// **同一周期里失败过就不许再 ready。**
    ///
    /// 这是 `run_once` 的真实交错：主干在 spawn reader 之后采样一次
    /// `reader_finished()`（那一刻 reader 还活着 ⇒ `Succeeded`），随后跑完
    /// 回补与对账；reader 在这中间读到一帧解析不了的真值，写下
    /// `ready=0 / phase=ws_parse`。R16 的 ready 写入没有任何谓词，会把这行
    /// **整行覆盖**成 `ready=1 / phase=ready / failure_reason=NULL`。
    ///
    /// 绑代之后：失败推了代际，本周期那个号已经不在库里，CAS 一行不命中 ⇒
    /// `PreserveNotReady`，失败痕迹一个字段都没被抹掉。
    #[tokio::test]
    async fn t39b_a_failure_inside_the_cycle_blocks_a_stale_ready_write() {
        let (store, path) = test_store("stale-ready").await;
        let cycle = begin_recovery_cycle(&store, "cred-stale", RecoveryTrigger::Startup, 10)
            .await
            .expect("start the cycle");
        // reader 任务在主干采样之后才失败。
        record_recovery_failure(
            &store,
            "cred-stale",
            "ws_parse",
            None,
            None,
            "user stream frame with truth-bearing event type ORDER_TRADE_UPDATE could not be \
             parsed",
            20,
        )
        .await
        .expect("record the reader failure");

        assert_eq!(
            finalize_recovery_readiness(
                &store,
                "cred-stale",
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                30,
            )
            .await
            .expect("a superseded cycle is not an error"),
            ReadinessWrite::PreserveNotReady,
            "a cycle that failed mid-flight must not be able to declare readiness"
        );
        let state = load_fill_stream_state(&store, "cred-stale")
            .await
            .expect("load state")
            .expect("state exists");
        assert!(
            !state.ready,
            "the stale ready write must not land: {state:?}"
        );
        assert_eq!(state.phase, "ws_parse");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("could not be parsed"),
            "the reader's failure reason must survive: {state:?}"
        );
        assert!(
            state.generation > cycle.generation(),
            "a failure inside the cycle must move the recovery generation: {state:?}"
        );
        cleanup(store, path).await;
    }

    /// **主干的进度写入不得抹掉
    /// reader 已经写下的失败。**
    ///
    /// R16 的进度写入是无谓词整行覆盖（`failure_reason` 传 `None`）：reader
    /// 写下 `ws_parse` + 理由之后，主干下一步 `backfill`/`reconcile` 就把它
    /// 抹成"一切正常，正在对账"。真实二进制上实测到的 phase 序列因此是
    /// `startup → reconcile → cycle_failed`——`ws_parse` 一次都留不住。
    #[tokio::test]
    async fn t39b_progress_from_a_superseded_cycle_does_not_erase_the_failure() {
        let (store, path) = test_store("stale-progress").await;
        let cycle = begin_recovery_cycle(&store, "cred-progress", RecoveryTrigger::Startup, 10)
            .await
            .expect("start the cycle");
        record_recovery_failure(
            &store,
            "cred-progress",
            "ws_parse",
            None,
            None,
            "user stream frame with unknown event type listenKeyExpired could not be parsed",
            20,
        )
        .await
        .expect("record the reader failure");

        // 主干这时才走到"回补完成，开始对账"。
        record_recovery_progress(&store, "cred-progress", cycle, "reconcile", None, None, 30)
            .await
            .expect("a superseded progress write is not an error");

        let state = load_fill_stream_state(&store, "cred-progress")
            .await
            .expect("load state")
            .expect("state exists");
        assert_eq!(
            state.phase, "ws_parse",
            "the failure phase must survive a superseded progress write: {state:?}"
        );
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("listenKeyExpired"),
            "the failure reason must survive: {state:?}"
        );
        assert!(!state.ready);
        cleanup(store, path).await;
    }

    /// **反向：没有任何东西推走代际时，同一个号照常写下 ready。**
    ///
    /// 修法不是"永远不写 ready"——CAS 必须是可通过的，否则 readiness 闸门
    /// 会把新开仓永久挡死（一条可用性故障冒充成安全判据）。
    #[tokio::test]
    async fn t39b_an_undisturbed_cycle_still_writes_ready() {
        let (store, path) = test_store("undisturbed-ready").await;
        let cycle = begin_recovery_cycle(&store, "cred-ok", RecoveryTrigger::Startup, 10)
            .await
            .expect("start the cycle");
        record_recovery_progress(&store, "cred-ok", cycle, "reconcile", None, None, 15)
            .await
            .expect("progress must not move the generation");
        assert_eq!(
            finalize_recovery_readiness(
                &store,
                "cred-ok",
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                20,
            )
            .await
            .expect("mark ready"),
            ReadinessWrite::Ready
        );
        let state = load_fill_stream_state(&store, "cred-ok")
            .await
            .expect("load state")
            .expect("state exists");
        assert!(state.ready, "{state:?}");
        assert_eq!(state.phase, "ready");
        assert_eq!(state.generation, cycle.generation());
        cleanup(store, path).await;
    }

    /// **新登记一个恢复域同样作废在途的那一轮。**
    ///
    /// 在途周期的回补根本没扫过这个新 symbol；让它写回 `ready` 等于宣称
    /// 一个从未被回补过的域已经与交易所对齐。
    #[tokio::test]
    async fn t39b_a_new_scope_supersedes_an_in_flight_cycle() {
        let (store, path) = test_store("scope-supersedes").await;
        let cycle = begin_recovery_cycle(&store, "cred-scoped", RecoveryTrigger::Startup, 10)
            .await
            .expect("start the cycle");
        assert!(
            register_fill_recovery_scope(&store, "cred-scoped", "ETHUSDT", 15)
                .await
                .expect("register a new scope mid-cycle")
        );
        assert_eq!(
            finalize_recovery_readiness(
                &store,
                "cred-scoped",
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                20,
            )
            .await
            .expect("a superseded cycle is not an error"),
            ReadinessWrite::PreserveNotReady
        );
        assert_eq!(
            load_fill_stream_state(&store, "cred-scoped")
                .await
                .expect("load state")
                .expect("state exists")
                .ready,
            false
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_failure_record_keeps_credential_symbol_cursor_and_reason() {
        let (store, path) = test_store("structured-failure").await;
        begin_recovery_cycle(&store, "cred-failure", RecoveryTrigger::Startup, 10)
            .await
            .expect("start failure cycle");
        record_recovery_failure(
            &store,
            "cred-failure",
            "backfill",
            Some("BTCUSDT"),
            Some(41),
            "injected page failure",
            20,
        )
        .await
        .expect("record structured failure");
        assert_eq!(
            load_fill_stream_state(&store, "cred-failure")
                .await
                .expect("load structured failure")
                .expect("failure state exists"),
            FillStreamState {
                // ****：失败推代际 —— 周期开始 1、
                // 这次失败之后 2。号本身就是"这一轮已经作废"的凭据。
                generation: 2,
                ready: false,
                phase: "backfill".to_string(),
                current_symbol: Some("BTCUSDT".to_string()),
                cursor_last_trade_id: Some(41),
                failure_reason: Some("injected page failure".to_string()),
            }
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_unknown_symbol_rows_never_seed_cross_symbol_recovery_scopes() {
        let (store, path) = test_store("unknown-scope-excluded").await;
        assert!(insert_test_fill(&store, "cred-scope", "BTCUSDT", "10").await);
        sqlx::query(
            r#"
            INSERT INTO engine_fills (
              credential_id, symbol_identity, symbol_scope_known,
              exchange_trade_id, engine_order_id, exchange, market_kind, symbol,
              created_at_ms
            ) VALUES (?, ?, 0, ?, ?, 'binance', 'futures', 'ETHUSDT', 1)
            "#,
        )
        .bind("cred-scope")
        .bind(LEGACY_UNKNOWN_SYMBOL)
        .bind("11")
        .bind("legacy-order")
        .execute(store.pool())
        .await
        .expect("insert unknown-symbol legacy row");
        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![FillRecoveryScope {
                credential_id: "cred-scope".to_string(),
                symbol: "BTCUSDT".to_string(),
            }]
        );
        cleanup(store, path).await;
    }

    // ==================================================================
    // 恢复域的身份判据与交易所边界同源
    // ==================================================================

    async fn seed_order_with_key(store: &Store, engine_order_id: &str, symbol_key: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, created_at_ms, updated_at_ms
            ) VALUES (?, 'cred-r13scope', 'bot-r13scope', ?,
              'binance', 'futures', ?, 'open_long', 'long', 'market',
              '1', 0, 'acked', 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(symbol_key)
        .execute(store.pool())
        .await
        .expect("seed engine order");
    }

    /// **regression（High）· 认不出 USDⓈ-M 身份的 `symbol_key` 不产生
    /// 恢复域。**
    ///
    /// 这里此前是裸 `symbol_from_symbol_key`：它只取 `base`/`quote`，
    /// 把 `venue`/`market_kind`/`settle` 整段丢掉。于是
    /// `okx|spot|BTC|USDT|BTC` 会被换算成 `BTCUSDT` 并登记成一个
    /// **Binance USDⓈ-M 的已知恢复域**——fill recovery 随后就会拿着它
    /// 去 Binance 逐页回补别人的成交，并把结果记成本域的真值。
    ///
    /// 两个方向：闭世界内的 key 照常产生域（否则"一律不产生"也全绿），
    /// 闭世界外的三种断裂（venue / market_kind / settle）一个都不产生。
    #[tokio::test]
    async fn only_a_usdm_order_symbol_key_seeds_a_fill_recovery_scope() {
        let (store, path) = test_store("usdm-scope-only").await;
        seed_order_with_key(&store, "eng_usdm", "binance|futures|BTC|USDT|USDT").await;
        seed_order_with_key(&store, "eng_venue", "okx|futures|ETH|USDT|USDT").await;
        seed_order_with_key(&store, "eng_market", "binance|spot|SOL|USDT|USDT").await;
        seed_order_with_key(&store, "eng_settle", "binance|futures|XRP|USDT|XRP").await;

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![FillRecoveryScope {
                credential_id: "cred-r13scope".to_string(),
                symbol: "BTCUSDT".to_string(),
            }],
            "only the closed-world USDⓈ-M order may seed a Binance USDⓈ-M recovery scope"
        );
        cleanup(store, path).await;
    }

    /// 同一条纪律的另一处：**派生**的 fill 身份。
    ///
    /// 裸 `symbol_from_symbol_key` 会把 `okx|spot|BTC|USDT|BTC` 折成
    /// `BTCUSDT` 并把这条 fill 标成 `symbol_scope_known=1`——
    /// 而 `discover_fill_recovery_scopes` 正是从 `symbol_scope_known=1`
    /// 里捞域的，于是一个错的 venue/market/settle 绕一圈又变成了已知域。
    #[test]
    fn t39_fill_symbol_identity_refuses_to_derive_from_a_non_usdm_key() {
        for alien in [
            "okx|futures|BTC|USDT|USDT",
            "binance|spot|BTC|USDT|USDT",
            "binance|futures|BTC|USDT|BTC",
            "binance|futures|BTC|USDT",
        ] {
            assert_eq!(
                fill_symbol_identity(None, Some(alien)),
                FillSymbolIdentity {
                    value: LEGACY_UNKNOWN_SYMBOL.to_string(),
                    scope_known: false,
                },
                "{alien} must not derive a Binance USDⓈ-M identity"
            );
        }
    }

    // ==================================================================
    // 恢复域的两个身份来源
    // 与 0017 遗留标记的可审计复核
    // ==================================================================

    async fn seed_order_with_declared_market(
        store: &Store,
        engine_order_id: &str,
        exchange: &str,
        market_kind: &str,
        symbol_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, created_at_ms, updated_at_ms
            ) VALUES (?, 'cred-r14scope', 'bot-r14scope', ?, ?, ?, ?,
              'open_long', 'long', 'market', '1', 0, 'acked', 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(exchange)
        .bind(market_kind)
        .bind(symbol_key)
        .execute(store.pool())
        .await
        .expect("seed engine order with an explicit declared market");
    }

    /// **regression（High）· 外层声明与内层 key 矛盾的单不产生恢复域。**
    ///
    /// R13 这里只把 `symbol_key` 送进 `usdm_symbol`，同一行的
    /// `exchange` / `market_kind` 一列都没看。于是一条 okx/spot 的单只要
    /// 内层 key 写着 `binance|futures|…`，就会播种一个 Binance USDⓈ-M 域，
    /// fill recovery 随后拿着它去 Binance 逐页回补别人的成交。
    ///
    /// 两个方向：两源一致的 USDⓈ-M 单照常产生域；矛盾行一个都不产生。
    #[tokio::test]
    async fn an_order_whose_two_identity_sources_disagree_seeds_no_scope() {
        let (store, path) = test_store("two-source-scope").await;
        seed_order_with_declared_market(
            &store,
            "eng_ok",
            "binance",
            "futures",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        // 外层 okx/spot，内层却自称 binance USDⓈ-M：矛盾行。
        seed_order_with_declared_market(
            &store,
            "eng_venue_lie",
            "okx",
            "spot",
            "binance|futures|ETH|USDT|USDT",
        )
        .await;
        // 外层 binance/futures，内层却自称 okx/spot：同样矛盾。
        seed_order_with_declared_market(
            &store,
            "eng_key_lie",
            "binance",
            "futures",
            "okx|spot|SOL|USDT|USDT",
        )
        .await;

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![FillRecoveryScope {
                credential_id: "cred-r14scope".to_string(),
                symbol: "BTCUSDT".to_string(),
            }],
            "only a row whose two identity sources agree may seed a recovery scope"
        );
        cleanup(store, path).await;
    }

    /// 0017 那条 backfill 的形状：非空 `symbol` 即 `symbol_scope_known=1`，
    /// 而 `symbol_scope_proved_at_ms` 为 NULL（从未复核）。
    async fn seed_legacy_marked_fill(
        store: &Store,
        trade_id: &str,
        symbol_identity: &str,
        exchange: &str,
        market_kind: &str,
        symbol_key: Option<&str>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_fills (
              credential_id, symbol_identity, symbol_scope_known,
              symbol_scope_proved_at_ms, exchange_trade_id, engine_order_id,
              exchange, market_kind, symbol_key, symbol, created_at_ms
            ) VALUES ('cred-r14fill', ?, 1, NULL, ?, 'legacy-order',
                      ?, ?, ?, ?, 1)
            "#,
        )
        .bind(symbol_identity)
        .bind(trade_id)
        .bind(exchange)
        .bind(market_kind)
        .bind(symbol_key)
        .bind(symbol_identity)
        .execute(store.pool())
        .await
        .expect("seed a fill row carrying the 0017 legacy marking");
    }

    async fn scope_known_of(store: &Store, trade_id: &str) -> i64 {
        sqlx::query("SELECT symbol_scope_known FROM engine_fills WHERE exchange_trade_id=?")
            .bind(trade_id)
            .fetch_one(store.pool())
            .await
            .expect("read back the fill row")
            .get::<i64, _>("symbol_scope_known")
    }

    async fn revalidation_verdicts(store: &Store) -> Vec<(String, String)> {
        sqlx::query(
            "SELECT exchange_trade_id, verdict FROM fill_symbol_scope_revalidations              ORDER BY exchange_trade_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("read the revalidation audit")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("exchange_trade_id"),
                row.get::<String, _>("verdict"),
            )
        })
        .collect()
    }

    /// **0017 旧数据（High）· 未经复核的遗留标记不得播种恢复域。**
    ///
    /// `0017_fill_replay_data_layer.sql:41-53` 把**所有非空** legacy `symbol`
    /// 标成 `symbol_scope_known=1`，而 `discover_fill_recovery_scopes` 正是
    /// 从这个标记里捞域的。一条 venue/market/settle 从未被验证过的旧行，
    /// 就这样变成了一个 Binance USDⓈ-M 恢复域。
    ///
    /// 复核用的是生产谓词本身（`usdm_identity` + 双源一致），三个方向：
    ///
    /// * 有 key、两源一致、推得出同一个 symbol → 保持 known，仍然建域；
    /// * 没有 key（0004 时代的行）→ 证不出来，降级 unknown；
    /// * 有 key 但外层与内层矛盾 → 降级 unknown。
    ///
    /// 复核之前，三条**一条都不建域**（fail-closed：未证明即无域）。
    #[tokio::test]
    async fn legacy_scope_markings_seed_no_scope_until_they_are_revalidated() {
        let (store, path) = test_store("legacy-revalidation").await;
        seed_legacy_marked_fill(
            &store,
            "provable",
            "BTCUSDT",
            "binance",
            "futures",
            Some("binance|futures|BTC|USDT|USDT"),
        )
        .await;
        seed_legacy_marked_fill(&store, "keyless", "ETHUSDT", "binance", "futures", None).await;
        seed_legacy_marked_fill(
            &store,
            "contradictory",
            "SOLUSDT",
            "okx",
            "spot",
            Some("binance|futures|SOL|USDT|USDT"),
        )
        .await;

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            Vec::<FillRecoveryScope>::new(),
            "an unrevalidated 0017 marking must not seed any recovery scope"
        );

        let outcome = revalidate_legacy_fill_symbol_scopes(&store, 4_242)
            .await
            .expect("revalidate legacy scope markings");
        assert_eq!(
            outcome,
            FillScopeRevalidation {
                examined: 3,
                proven: 1,
                demoted: 2,
            }
        );
        assert_eq!(scope_known_of(&store, "provable").await, 1);
        assert_eq!(scope_known_of(&store, "keyless").await, 0);
        assert_eq!(scope_known_of(&store, "contradictory").await, 0);
        assert_eq!(
            revalidation_verdicts(&store).await,
            vec![
                (
                    "contradictory".to_string(),
                    FILL_SCOPE_VERDICT_UNPROVABLE.to_string()
                ),
                (
                    "keyless".to_string(),
                    FILL_SCOPE_VERDICT_UNPROVABLE.to_string()
                ),
                (
                    "provable".to_string(),
                    FILL_SCOPE_VERDICT_PROVEN.to_string()
                ),
            ],
            "every examined row must leave an auditable verdict"
        );

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes after revalidation"),
            vec![FillRecoveryScope {
                credential_id: "cred-r14fill".to_string(),
                symbol: "BTCUSDT".to_string(),
            }],
            "only the row whose identity was really proven may seed a scope"
        );

        // 幂等：再跑一次是空集，审计不重复。
        assert_eq!(
            revalidate_legacy_fill_symbol_scopes(&store, 4_243)
                .await
                .expect("second revalidation pass"),
            FillScopeRevalidation::default()
        );
        assert_eq!(revalidation_verdicts(&store).await.len(), 3);
        cleanup(store, path).await;
    }

    /// **写入侧不受 0024 的 fail-closed 影响。**
    ///
    /// 运行期写进来的 fill 是**当场**用生产谓词判定的，尤其 REST 回补的
    /// 外部成交（没有本地单、因此没有 `symbol_key`）——它的域身份来自
    /// 上游 Binance USDⓈ-M 端点本身。这条钉住：新写入的行天生"已复核"，
    /// 不需要等下一次启动，也不会被启动复核降级。
    #[tokio::test]
    async fn runtime_written_fills_are_born_revalidated() {
        let (store, path) = test_store("runtime-proved").await;
        assert!(insert_test_fill(&store, "cred-runtime", "BTCUSDT", "77").await);
        assert_eq!(
            revalidate_legacy_fill_symbol_scopes(&store, 5_000)
                .await
                .expect("revalidate"),
            FillScopeRevalidation::default(),
            "a runtime-written fill was already adjudicated at insert time"
        );
        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![FillRecoveryScope {
                credential_id: "cred-runtime".to_string(),
                symbol: "BTCUSDT".to_string(),
            }]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_fills_never_seed_exchange_fill_recovery_scopes() {
        let (store, path) = test_store("paper-scope-excluded").await;
        assert!(insert_test_fill(&store, "paper", "BTCUSDT", "10").await);
        assert!(discover_fill_recovery_scopes(&store)
            .await
            .expect("discover scopes")
            .is_empty());
        cleanup(store, path).await;
    }

    /// **paper 凭据隔离（2026-09-06 · 生产缺陷，High）· canonical 模拟凭据不得
    /// 从任何来源播种交易所恢复域。**
    ///
    /// `discover_fill_recovery_scopes` 各条 SQL 的判据是 `credential_id <>
    /// 'paper'`，只挡住了 legacy 字面量；Product Demo 的 canonical 凭据是
    /// `paper:<32位小写hex>`（与 `tests/fixtures/product-demo-credential-
    /// identities.json` 同形）。生产缺陷路径：已证明形状的 scopes / cursors
    /// 行、运行期写入的 paper fills、paper 引擎单全部通过该判据，直达
    /// `binance_user_stream::start_discovered`——`keys_from_env` 对模拟凭据
    /// 必然 `Ok(None)`，于是每次启动都写一条 `startup_credentials` 失败并
    /// 推进恢复代际：模拟数据在真实恢复域里制造持续的失败噪声与代际抖动。
    ///
    /// 两个方向都要钉：legacy `paper` 与 canonical `paper:<32hex>` 在四个
    /// 来源上一个域都不产生；同库**同形状**的交易所凭据（正域对照）照常从
    /// 全部四个来源产生——否则"一律不产生"也能让这条全绿。
    #[tokio::test]
    async fn paper_credentials_seed_no_exchange_recovery_scope_from_any_source() {
        let (store, path) = test_store("paper-credential-isolation").await;
        let canonical_paper = "paper:7b6f4495fde3706c999256af2985a8f4";
        // ① 已证明形状的持久化 scopes / cursors（生产缺陷正是这些行被放行）。
        seed_proved_scope_row(&store, "paper", "BTCUSDT").await;
        seed_proved_scope_row(&store, canonical_paper, "BTCUSDT").await;
        seed_proved_cursor_row(&store, canonical_paper, "ETHUSDT").await;
        // ② 运行期写入的 paper fill（天生已复核，`symbol_scope_known=1`）。
        assert!(insert_test_fill(&store, "paper", "ETHUSDT", "10").await);
        assert!(insert_test_fill(&store, canonical_paper, "SOLUSDT", "11").await);
        // ③ paper 引擎单——双源一致的 USDⓈ-M 身份，单看身份判据完全合格。
        seed_order_for_credential(
            &store,
            canonical_paper,
            "eng-paper-order",
            "binance",
            "futures",
            "binance|futures|SOL|USDT|USDT",
        )
        .await;
        // 正域对照：同样的四个来源、同样已证明的形状，凭据是交易所身份。
        seed_proved_scope_row(&store, "cred-live-control", "BTCUSDT").await;
        seed_proved_cursor_row(&store, "cred-live-control", "ETHUSDT").await;
        assert!(insert_test_fill(&store, "cred-live-control", "SOLUSDT", "12").await);
        seed_order_for_credential(
            &store,
            "cred-live-control",
            "eng-live-control",
            "binance",
            "futures",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![
                FillRecoveryScope {
                    credential_id: "cred-live-control".to_string(),
                    symbol: "BTCUSDT".to_string(),
                },
                FillRecoveryScope {
                    credential_id: "cred-live-control".to_string(),
                    symbol: "ETHUSDT".to_string(),
                },
                FillRecoveryScope {
                    credential_id: "cred-live-control".to_string(),
                    symbol: "SOLUSDT".to_string(),
                },
            ],
            "paper（legacy 与 canonical）一个域都不产生；交易所凭据照常从全部来源产生"
        );
        cleanup(store, path).await;
    }

    /// **保留命名空间下的畸形串也不能成为交易所 fallback。**
    ///
    /// `venue::credential_kind` 对 `paper` 打头却不合模拟凭据文法的串判
    /// `UnprovenIdentity`——它们此前同样通过 `<> 'paper'` 的 SQL 判据：
    /// `keys_from_env` 对它们返回 `Ok(None)`，照样走进 `startup_credentials`
    /// 失败路径。身份未被证明的一律不得进入交易所恢复域（fail-closed）。
    #[tokio::test]
    async fn malformed_reserved_paper_identifiers_seed_no_exchange_recovery_scope() {
        let (store, path) = test_store("paper-malformed-reserved").await;
        for (label, malformed) in [
            ("empty-account-segment", "paper:"),
            ("one-hex-short", "paper:7b6f4495fde3706c999256af2985a8f"),
            ("uppercase-hex", "paper:7B6F4495FDE3706C999256AF2985A8F4"),
        ] {
            assert_eq!(
                crate::venue::credential_kind(malformed),
                crate::venue::CredentialKind::UnprovenIdentity,
                "fixture {label} must exercise the reserved-namespace malformed class"
            );
            seed_proved_scope_row(&store, malformed, "BTCUSDT").await;
            seed_proved_cursor_row(&store, malformed, "ETHUSDT").await;
            assert!(insert_test_fill(&store, malformed, "SOLUSDT", "10").await);
            seed_order_for_credential(
                &store,
                malformed,
                &format!("eng-malformed-{label}"),
                "binance",
                "futures",
                "binance|futures|SOL|USDT|USDT",
            )
            .await;
        }
        // 正域对照：畸形的保留命名空间串挡住之后，交易所凭据不受影响。
        seed_proved_scope_row(&store, "cred-live-malformed-control", "BTCUSDT").await;

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![FillRecoveryScope {
                credential_id: "cred-live-malformed-control".to_string(),
                symbol: "BTCUSDT".to_string(),
            }],
            "a malformed reserved paper identifier must not become an exchange fallback"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_overlap_dedup_uses_symbol_scoped_primary_key_with_complete_set_equality() {
        let (store, path) = test_store("overlap-complete-set").await;
        let sink = crate::svc::BinanceEventSink::new(store.clone());
        assert!(sink
            .handle_rest_user_trade("cred-overlap", &rest_trade(77, "BTCUSDT", 700))
            .await
            .expect("ingest REST fill"));
        assert!(!insert_test_fill(&store, "cred-overlap", "BTCUSDT", "77").await);
        assert!(insert_test_fill(&store, "cred-overlap", "ETHUSDT", "77").await);
        assert_eq!(
            fill_set(&store, "cred-overlap").await,
            vec![
                (
                    "BTCUSDT".to_string(),
                    "77".to_string(),
                    "external:700".to_string(),
                ),
                (
                    "ETHUSDT".to_string(),
                    "77".to_string(),
                    "order-ETHUSDT-77".to_string(),
                ),
            ]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_mid_process_crash_replays_from_old_cursor_without_duplicate_fill() {
        let (store, path) = test_store("crash-replay").await;
        finalize_fill_backfill(&store, "cred-crash", "BTCUSDT", success(9, 90))
            .await
            .expect("seed cursor");
        let sink = crate::svc::BinanceEventSink::new(store.clone());
        assert!(sink
            .handle_rest_user_trade("cred-crash", &rest_trade(10, "BTCUSDT", 100))
            .await
            .expect("ingest before crash"));
        assert_eq!(
            finalize_fill_backfill(
                &store,
                "cred-crash",
                "BTCUSDT",
                BackfillAttempt {
                    attempted_last_trade_id: 10,
                    completed_at_ms: 100,
                    completion: BackfillCompletion::Failed,
                },
            )
            .await
            .expect("record interrupted attempt"),
            CursorFinalizeOutcome::PreservedAfterFailure
        );
        drop(sink);
        store.pool().close().await;
        let reopened = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("restart store");
        assert_eq!(
            load_fill_replay_cursor(&reopened, "cred-crash", "BTCUSDT")
                .await
                .expect("load old cursor after crash"),
            Some(9)
        );
        let sink = crate::svc::BinanceEventSink::new(reopened.clone());
        assert!(!sink
            .handle_rest_user_trade("cred-crash", &rest_trade(10, "BTCUSDT", 100))
            .await
            .expect("dedupe replayed pre-crash fill"));
        assert!(sink
            .handle_rest_user_trade("cred-crash", &rest_trade(11, "BTCUSDT", 101))
            .await
            .expect("ingest remaining fill"));
        finalize_fill_backfill(&reopened, "cred-crash", "BTCUSDT", success(11, 110))
            .await
            .expect("advance only after complete retry");
        assert_eq!(
            fill_set(&reopened, "cred-crash").await,
            vec![
                (
                    "BTCUSDT".to_string(),
                    "10".to_string(),
                    "external:100".to_string(),
                ),
                (
                    "BTCUSDT".to_string(),
                    "11".to_string(),
                    "external:101".to_string(),
                ),
            ]
        );
        assert_eq!(
            load_fill_replay_cursor(&reopened, "cred-crash", "BTCUSDT")
                .await
                .expect("load completed cursor"),
            Some(11)
        );
        cleanup(reopened, path).await;
    }

    #[tokio::test]
    async fn t39b_out_of_order_rest_fills_preserve_complete_set_and_max_cursor() {
        let (store, path) = test_store("out-of-order").await;
        let sink = crate::svc::BinanceEventSink::new(store.clone());
        let trades = [
            rest_trade(12, "BTCUSDT", 102),
            rest_trade(10, "BTCUSDT", 100),
            rest_trade(11, "BTCUSDT", 101),
        ];
        let mut cursor = 9;
        for trade in &trades {
            assert!(sink
                .handle_rest_user_trade("cred-order", trade)
                .await
                .expect("ingest out-of-order fill"));
            cursor = next_backfill_cursor(cursor, trade.id);
        }
        finalize_fill_backfill(&store, "cred-order", "BTCUSDT", success(cursor, 120))
            .await
            .expect("finalize out-of-order page");
        assert_eq!(
            fill_set(&store, "cred-order").await,
            vec![
                (
                    "BTCUSDT".to_string(),
                    "10".to_string(),
                    "external:100".to_string(),
                ),
                (
                    "BTCUSDT".to_string(),
                    "11".to_string(),
                    "external:101".to_string(),
                ),
                (
                    "BTCUSDT".to_string(),
                    "12".to_string(),
                    "external:102".to_string(),
                ),
            ]
        );
        assert_eq!(
            load_fill_replay_cursor(&store, "cred-order", "BTCUSDT")
                .await
                .expect("load max cursor"),
            Some(12)
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_ws_then_rest_duplicate_event_keeps_exact_fill_set() {
        let (store, path) = test_store("ws-rest-duplicate").await;
        assert!(insert_test_fill(&store, "cred-duplicate", "BTCUSDT", "88").await);
        let sink = crate::svc::BinanceEventSink::new(store.clone());
        assert!(!sink
            .handle_rest_user_trade("cred-duplicate", &rest_trade(88, "BTCUSDT", 800))
            .await
            .expect("dedupe REST overlap after WS"));
        assert_eq!(
            fill_set(&store, "cred-duplicate").await,
            vec![(
                "BTCUSDT".to_string(),
                "88".to_string(),
                "order-BTCUSDT-88".to_string(),
            )]
        );
        cleanup(store, path).await;
    }
    // ==================================================================
    // 旧派生 scope / cursor 的可审计复核
    // ==================================================================

    /// **已证明形状**的持久化恢复域行（`scope_known=1` 且已盖章）——运行期
    /// 登记 / 成功回补之后落到库里的真实形状，也是 paper 凭据隔离缺陷里
    /// 被 `<> 'paper'` SQL 判据放行的那种行。
    async fn seed_proved_scope_row(store: &Store, credential_id: &str, symbol: &str) {
        sqlx::query(
            "INSERT INTO fill_recovery_scopes (credential_id, symbol, registered_at_ms, \
             updated_at_ms, scope_known, scope_proved_at_ms) VALUES (?, ?, 1, 1, 1, 1)",
        )
        .bind(credential_id)
        .bind(symbol)
        .execute(store.pool())
        .await
        .expect("seed a proved fill_recovery_scopes row");
    }

    async fn seed_proved_cursor_row(store: &Store, credential_id: &str, symbol: &str) {
        sqlx::query(
            "INSERT INTO fill_replay_cursors (credential_id, symbol, last_trade_id, \
             last_success_at_ms, updated_at_ms, scope_known, scope_proved_at_ms) \
             VALUES (?, ?, 7, 1, 1, 1, 1)",
        )
        .bind(credential_id)
        .bind(symbol)
        .execute(store.pool())
        .await
        .expect("seed a proved fill_replay_cursors row");
    }

    /// 0018 / 0017 建表时留下的形状：只有 `(credential_id, symbol)`，
    /// `scope_proved_at_ms` 为 NULL（迁移 0025 零 DML，存量行天生"未复核"）。
    async fn seed_legacy_scope_row(store: &Store, credential_id: &str, symbol: &str) {
        sqlx::query(
            "INSERT INTO fill_recovery_scopes (credential_id, symbol, registered_at_ms, \
             updated_at_ms, scope_known, scope_proved_at_ms) VALUES (?, ?, 1, 1, 1, NULL)",
        )
        .bind(credential_id)
        .bind(symbol)
        .execute(store.pool())
        .await
        .expect("seed a legacy fill_recovery_scopes row");
    }

    async fn seed_legacy_cursor_row(store: &Store, credential_id: &str, symbol: &str) {
        sqlx::query(
            "INSERT INTO fill_replay_cursors (credential_id, symbol, last_trade_id, \
             last_success_at_ms, updated_at_ms, scope_known, scope_proved_at_ms) \
             VALUES (?, ?, 7, 1, 1, 1, NULL)",
        )
        .bind(credential_id)
        .bind(symbol)
        .execute(store.pool())
        .await
        .expect("seed a legacy fill_replay_cursors row");
    }

    async fn seed_proved_fill(store: &Store, credential_id: &str, symbol: &str, trade_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_fills (
              credential_id, symbol_identity, symbol_scope_known,
              symbol_scope_proved_at_ms, exchange_trade_id, engine_order_id,
              exchange, market_kind, symbol_key, symbol, created_at_ms
            ) VALUES (?, ?, 1, 99, ?, 'proved-order', 'binance', 'futures',
                      NULL, ?, 1)
            "#,
        )
        .bind(credential_id)
        .bind(symbol)
        .bind(trade_id)
        .bind(symbol)
        .execute(store.pool())
        .await
        .expect("seed a revalidated fill row");
    }

    async fn seed_order_for_credential(
        store: &Store,
        credential_id: &str,
        engine_order_id: &str,
        exchange: &str,
        market_kind: &str,
        symbol_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot-r15scope', ?, ?, ?, ?,
              'open_long', 'long', 'market', '1', 0, 'acked', 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(credential_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(exchange)
        .bind(market_kind)
        .bind(symbol_key)
        .execute(store.pool())
        .await
        .expect("seed engine order for a scope origin proof");
    }

    async fn origin_verdicts(store: &Store) -> Vec<(String, String, String)> {
        sqlx::query(
            "SELECT origin_table, credential_id, symbol, verdict \
             FROM fill_scope_origin_revalidations \
             ORDER BY origin_table, credential_id, symbol",
        )
        .fetch_all(store.pool())
        .await
        .expect("read the scope origin audit")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("origin_table"),
                row.get::<String, _>("symbol"),
                row.get::<String, _>("verdict"),
            )
        })
        .collect()
    }

    /// **第十一轮 §8.3（High）· 未经复核的旧派生行不得播种恢复域，也不得驱动回补。**
    ///
    /// 0024 只复核了 `engine_fills`。恢复域另外两个来源——0018 的
    /// `fill_recovery_scopes` 与 0017 的 `fill_replay_cursors`——**一列身份
    /// 都没有**，此前被无条件 UNION 进 `discover_fill_recovery_scopes`，
    /// 而真正驱动 REST 逐页回补的 `scopes_for_credential` 连 UNION 都不经过、
    /// 直接读表。于是一条没有任何证明的旧行可以让引擎去 Binance 回补
    /// 别人的成交，并把结果记成本域真值。
    ///
    /// 四个方向：
    ///
    /// * 复核**之前**：三条旧行一条都不建域、也一条都不驱动回补（fail-closed）；
    /// * 由**已复核 fill** 支撑的旧 scope → proved，重新建域；
    /// * 由**双源一致 engine_order** 支撑的旧 cursor → proved，重新建域；
    /// * 既无 proved fill 又无一致 order 的旧行 → unknown，永不建域，
    ///   逐行审计裁决可读；
    /// * 再跑一次是空集且审计不重复（幂等）。
    #[tokio::test]
    async fn legacy_derived_scopes_seed_nothing_until_a_proved_source_backs_them() {
        let (store, path) = test_store("legacy-scope-origin").await;
        // ① 由一条已复核 fill 支撑。
        seed_legacy_scope_row(&store, "cred-proved-fill", "BTCUSDT").await;
        seed_proved_fill(&store, "cred-proved-fill", "BTCUSDT", "t-1").await;
        // ② 由一条双源一致的 engine_order 支撑（游标来源）。
        seed_legacy_cursor_row(&store, "cred-proved-order", "ETHUSDT").await;
        seed_order_for_credential(
            &store,
            "cred-proved-order",
            "eng-r15-eth",
            "binance",
            "futures",
            "binance|futures|ETH|USDT|USDT",
        )
        .await;
        // ③ 什么都没有：一条来源不明的旧行。
        seed_legacy_scope_row(&store, "cred-orphan", "SOLUSDT").await;
        // ④ 有 engine_order，但那一行的两个身份来源自相矛盾——它证明不了任何域。
        seed_legacy_scope_row(&store, "cred-liar", "ADAUSDT").await;
        seed_order_for_credential(
            &store,
            "cred-liar",
            "eng-r15-ada",
            "okx",
            "spot",
            "binance|futures|ADA|USDT|USDT",
        )
        .await;

        assert_eq!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes"),
            vec![
                FillRecoveryScope {
                    credential_id: "cred-proved-fill".to_string(),
                    symbol: "BTCUSDT".to_string(),
                },
                FillRecoveryScope {
                    credential_id: "cred-proved-order".to_string(),
                    symbol: "ETHUSDT".to_string(),
                },
            ],
            "复核之前，发现集合里只剩下两个**自带身份**的分支：已复核的 fill \
             与双源一致的 engine_order。`cred-orphan` 那条来源不明的旧派生行 \
             一个域都不产生——而它此前是无条件 UNION 进来的。"
        );
        for credential_id in [
            "cred-proved-fill",
            "cred-proved-order",
            "cred-orphan",
            "cred-liar",
        ] {
            assert_eq!(
                scopes_for_credential(&store, credential_id)
                    .await
                    .expect("load backfill scopes"),
                Vec::<String>::new(),
                "an unrevalidated legacy row must not drive REST backfill for {credential_id}"
            );
        }

        let outcome = revalidate_legacy_fill_scope_origins(&store, 5_150)
            .await
            .expect("revalidate legacy scope origins");
        assert_eq!(
            outcome,
            ScopeOriginRevalidation {
                examined: 4,
                proven: 2,
                demoted: 2,
            }
        );
        assert_eq!(
            origin_verdicts(&store).await,
            vec![
                (
                    SCOPE_ORIGIN_SCOPES.to_string(),
                    "ADAUSDT".to_string(),
                    SCOPE_ORIGIN_VERDICT_UNPROVABLE.to_string(),
                ),
                (
                    SCOPE_ORIGIN_SCOPES.to_string(),
                    "SOLUSDT".to_string(),
                    SCOPE_ORIGIN_VERDICT_UNPROVABLE.to_string(),
                ),
                (
                    SCOPE_ORIGIN_SCOPES.to_string(),
                    "BTCUSDT".to_string(),
                    SCOPE_ORIGIN_VERDICT_PROVED_BY_FILL.to_string(),
                ),
                (
                    SCOPE_ORIGIN_CURSORS.to_string(),
                    "ETHUSDT".to_string(),
                    SCOPE_ORIGIN_VERDICT_PROVED_BY_ORDER.to_string(),
                ),
            ]
        );

        assert_eq!(
            scopes_for_credential(&store, "cred-proved-fill")
                .await
                .expect("load backfill scopes"),
            vec!["BTCUSDT".to_string()],
            "a proved legacy scope drives REST backfill again"
        );
        for credential_id in ["cred-orphan", "cred-liar"] {
            assert_eq!(
                scopes_for_credential(&store, credential_id)
                    .await
                    .expect("load backfill scopes"),
                Vec::<String>::new(),
                "an unprovable legacy scope never drives REST backfill for {credential_id}"
            );
        }
        let discovered = discover_fill_recovery_scopes(&store)
            .await
            .expect("discover scopes after revalidation");
        assert!(
            discovered.contains(&FillRecoveryScope {
                credential_id: "cred-proved-fill".to_string(),
                symbol: "BTCUSDT".to_string(),
            }) && discovered.contains(&FillRecoveryScope {
                credential_id: "cred-proved-order".to_string(),
                symbol: "ETHUSDT".to_string(),
            }),
            "both proved origins are back in the discovery set: {discovered:?}"
        );
        assert!(
            !discovered
                .iter()
                .any(|scope| scope.symbol == "SOLUSDT" || scope.symbol == "ADAUSDT"),
            "an unprovable origin never comes back: {discovered:?}"
        );

        assert_eq!(
            revalidate_legacy_fill_scope_origins(&store, 5_151)
                .await
                .expect("second revalidation pass"),
            ScopeOriginRevalidation::default(),
            "revalidation is idempotent: the second pass sees an empty set"
        );
        assert_eq!(
            origin_verdicts(&store).await.len(),
            4,
            "a rerun must not duplicate the per-row audit"
        );
        cleanup(store, path).await;
    }

    /// **接线证明：启动复核是在 `Store::connect` 里发生的，不是靠用例手工调用。**
    ///
    /// 上面几条都直接调 `revalidate_legacy_fill_scope_origins`——那证明的是
    /// 判据本身，不是"这条链会执行"（engine safety contract「存在 ≠ 生效」）。这里
    /// 走真实入口：把一条旧派生行写进库、关掉这个 `Store`，再对**同一个
    /// 库文件**重新 `Store::connect`（进程重启在库这一侧的等价形态）。
    /// 复核必须已经在连接返回之前发生，且发生在 0024 的 fill 复核之后。
    #[tokio::test]
    async fn connecting_the_store_revalidates_legacy_scope_origins() {
        let (store, path) = test_store("connect-revalidation").await;
        seed_legacy_scope_row(&store, "cred-connect", "SOLUSDT").await;
        seed_legacy_cursor_row(&store, "cred-connect", "BTCUSDT").await;
        seed_proved_fill(&store, "cred-connect", "BTCUSDT", "t-connect").await;
        store.pool().close().await;

        let reopened = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("reconnect the same database file");
        assert_eq!(
            origin_verdicts(&reopened).await,
            vec![
                (
                    SCOPE_ORIGIN_SCOPES.to_string(),
                    "SOLUSDT".to_string(),
                    SCOPE_ORIGIN_VERDICT_UNPROVABLE.to_string(),
                ),
                (
                    SCOPE_ORIGIN_CURSORS.to_string(),
                    "BTCUSDT".to_string(),
                    SCOPE_ORIGIN_VERDICT_PROVED_BY_FILL.to_string(),
                ),
            ],
            "Store::connect must revalidate every legacy origin before it hands back a store"
        );
        assert_eq!(
            scopes_for_credential(&reopened, "cred-connect")
                .await
                .expect("load backfill scopes"),
            Vec::<String>::new(),
            "the unprovable scope row must not drive REST backfill after the restart"
        );
        cleanup(reopened, path).await;
    }

    /// **闭世界形状也是判据的一部分。**
    ///
    /// 一条旧行的 symbol 若压根不是 USDⓈ-M（币本位 `BTCUSD`、空 base
    /// `USDT`、垃圾串），即便这个 credential 在别处有 proved fill 也不建域
    /// ——判据的第一步就把它挡住了。没有这一条，用「只看有没有 proved fill」
    /// 也能让上一条全绿。
    #[tokio::test]
    async fn a_legacy_scope_symbol_outside_the_usdm_closed_world_is_never_proved() {
        let (store, path) = test_store("legacy-scope-shape").await;
        seed_proved_fill(&store, "cred-shape", "BTCUSDT", "t-shape").await;
        // **归属证明存在、形状仍然不合格**的那一条：`binance|futures|BTC|USD|USD`
        // 满足 `usdm_identity`（settle==quote），它的 symbol 就是 `BTCUSD`——
        // 但本引擎只交易 USDT 计价的 USDⓈ-M 永续。形状判据若被删掉，这条
        // 币本位域会被一条**自己 credential 名下、已复核**的 fill 证明出来。
        seed_proved_fill(&store, "cred-shape", "BTCUSD", "t-coinm").await;
        for symbol in ["BTCUSD", "USDT", "NOTASYMBOL"] {
            seed_legacy_scope_row(&store, "cred-shape", symbol).await;
        }
        let outcome = revalidate_legacy_fill_scope_origins(&store, 5_160)
            .await
            .expect("revalidate legacy scope origins");
        assert_eq!(
            outcome,
            ScopeOriginRevalidation {
                examined: 3,
                proven: 0,
                demoted: 3,
            }
        );
        assert_eq!(
            scopes_for_credential(&store, "cred-shape")
                .await
                .expect("load backfill scopes"),
            Vec::<String>::new()
        );
        cleanup(store, path).await;
    }

    /// **运行期登记的域天生"已证明"，且不被启动复核降级。**
    ///
    /// `register_fill_recovery_scope` 只从已经解析成功的 USDⓈ-M 身份被调用
    /// （`ensure_running` 的 `exchange_identity.symbol`）。这一条同时钉住
    /// 反向：一条**存量未复核**的旧行被同一条生产入口再登记一次时会被
    /// **升级**成已证明——旧行不会因为一次复核降级就永久失去恢复域。
    #[tokio::test]
    async fn a_registered_scope_is_born_proved_and_upgrades_a_legacy_row() {
        let (store, path) = test_store("registered-proof").await;
        assert!(
            register_fill_recovery_scope(&store, "cred-live", "BTCUSDT", 900)
                .await
                .expect("register a live scope")
        );
        assert_eq!(
            scopes_for_credential(&store, "cred-live")
                .await
                .expect("load backfill scopes"),
            vec!["BTCUSDT".to_string()],
            "a scope registered from a resolved USDⓈ-M identity is proved on the spot"
        );
        assert_eq!(
            revalidate_legacy_fill_scope_origins(&store, 901)
                .await
                .expect("startup revalidation")
                .examined,
            0,
            "a born-proved row is not re-examined by the startup revalidation"
        );

        // 存量未复核旧行 → 复核降级 → 生产入口再登记一次 → 升级回已证明。
        seed_legacy_scope_row(&store, "cred-legacy", "ETHUSDT").await;
        assert_eq!(
            revalidate_legacy_fill_scope_origins(&store, 902)
                .await
                .expect("startup revalidation")
                .demoted,
            1
        );
        assert_eq!(
            scopes_for_credential(&store, "cred-legacy")
                .await
                .expect("load backfill scopes"),
            Vec::<String>::new()
        );
        register_fill_recovery_scope(&store, "cred-legacy", "ETHUSDT", 903)
            .await
            .expect("re-register the demoted scope from a live identity");
        assert_eq!(
            scopes_for_credential(&store, "cred-legacy")
                .await
                .expect("load backfill scopes"),
            vec!["ETHUSDT".to_string()],
            "a demoted legacy row is upgraded again by a live registration"
        );
        cleanup(store, path).await;
    }

    /// **成功的回补给游标盖章；运维手工重置不盖章。**
    ///
    /// 游标是恢复域发现的第二个旧派生来源。一次成功回补推进的游标，其 symbol
    /// 来自已证明的域——那就是它自己的证明。而 `reset_fill_replay_cursor`
    /// 是一次运维动作，它**不**构成"这个 credential 在这张合约上交易过"，
    /// 因此新建的游标行不得凭空变成一个恢复域。
    #[tokio::test]
    async fn a_successful_backfill_proves_its_cursor_but_a_manual_reset_does_not() {
        let (store, path) = test_store("cursor-proof").await;
        finalize_fill_backfill(
            &store,
            "cred-cursor",
            "BTCUSDT",
            BackfillAttempt {
                attempted_last_trade_id: 12,
                completed_at_ms: 120,
                completion: BackfillCompletion::Succeeded,
            },
        )
        .await
        .expect("advance a cursor through a successful backfill");
        assert!(
            discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes")
                .contains(&FillRecoveryScope {
                    credential_id: "cred-cursor".to_string(),
                    symbol: "BTCUSDT".to_string(),
                }),
            "a cursor advanced by a successful backfill is a proved recovery origin"
        );

        reset_fill_replay_cursor(&store, "cred-reset", "ETHUSDT", 5, "ops", "operator", 130)
            .await
            .expect("manually reset a cursor for a symbol that never traded");
        assert!(
            !discover_fill_recovery_scopes(&store)
                .await
                .expect("discover scopes")
                .iter()
                .any(|scope| scope.credential_id == "cred-reset"),
            "a manual cursor reset must not conjure a recovery scope"
        );
        cleanup(store, path).await;
    }
}
