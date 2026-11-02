use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use sqlx::Row;

use crate::rate_limiter::RequestClass;
use crate::store::{DbKind, Store};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    None,
    PauseNew,
    ManageOnly,
    Flatten,
}

impl Level {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "none" => Some(Self::None),
            "pause_new" => Some(Self::PauseNew),
            "manage_only" => Some(Self::ManageOnly),
            "flatten" => Some(Self::Flatten),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::PauseNew => "pause_new",
            Self::ManageOnly => "manage_only",
            Self::Flatten => "flatten",
        }
    }

    fn severity(self) -> u8 {
        match self {
            Self::None => 0,
            Self::PauseNew => 1,
            Self::ManageOnly => 2,
            Self::Flatten => 3,
        }
    }
}

/// Decode a level read from durable storage.
///
/// Unlike [`Level::parse`], this deliberately does not trim. API and kill-file
/// inputs may carry surrounding whitespace, but an authority row must contain
/// one of the four canonical byte strings. Treating `" FLATTEN "` or
/// `"FLATTEN"` as canonical would let a case-insensitive MySQL collation hide
/// corruption in the safety latch.
pub(crate) fn parse_persisted_level(value: &str) -> Option<Level> {
    match value {
        "none" => Some(Level::None),
        "pause_new" => Some(Level::PauseNew),
        "manage_only" => Some(Level::ManageOnly),
        "flatten" => Some(Level::Flatten),
        _ => None,
    }
}

/// A durable kill-switch authority row whose level is not a canonical token.
///
/// This is also the typed error returned by write paths that refuse to
/// overwrite an undecodable latch. The raw value is retained verbatim for
/// diagnosis; callers should log it as a structured/debug field so control
/// characters cannot forge log lines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidPersistedLevel {
    pub scope_key: String,
    pub raw_level: String,
}

impl std::fmt::Display for InvalidPersistedLevel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid persisted kill-switch level for {}: {:?}",
            self.scope_key, self.raw_level
        )
    }
}

impl std::error::Error for InvalidPersistedLevel {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveState {
    pub scope_key: String,
    pub level: Level,
    pub reason: Option<String>,
    pub flatten_state: Option<String>,
}

/// Complete result of an effective-state read.
///
/// Valid sibling rows still participate in the strictest-level calculation,
/// while every invalid authority row is reported independently. This prevents
/// both unsafe interpretations of corruption: silently treating it as `none`
/// and abandoning a valid sibling because another row was malformed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveStateRead {
    pub effective: Option<EffectiveState>,
    pub invalid_levels: Vec<InvalidPersistedLevel>,
}

#[derive(Clone, Debug)]
pub struct ArmRequest<'a> {
    pub scope_key: &'a str,
    pub level: Level,
    pub trigger_source: &'a str,
    pub reason: &'a str,
    pub now_ms: i64,
}

/// P4b · 会员 arm 的执行上下文（r2 §1.1'：降险链无挑战/无 revision 门，
/// 但身份与请求关联仍然精确）。仅 `member_api` 源携带；平台/自动路径为
/// `None`，字节零变化的既有语义（W 冻结要点 1）。
#[derive(Clone, Copy, Debug)]
pub struct MemberArmContext<'a> {
    pub member_id: i64,
    pub request_ref: &'a str,
}

fn scope(scope_key: &str) -> Option<&'static str> {
    if scope_key == "global" {
        Some("global")
    } else if scope_key
        .strip_prefix("cred:")
        .is_some_and(|value| !value.trim().is_empty())
    {
        Some("credential")
    } else if scope_key
        .strip_prefix("bot:")
        .is_some_and(|value| !value.trim().is_empty())
    {
        Some("bot")
    } else {
        None
    }
}

fn scope_priority(scope_key: &str) -> u8 {
    match scope(scope_key) {
        Some("global") => 3,
        Some("credential") => 2,
        Some("bot") => 1,
        _ => 0,
    }
}

fn source_is_manual(source: &str) -> bool {
    matches!(source, "manual_api" | "manual_file" | "signal")
}

/// P4b（#35 D3）· 会员来源值。**不得**进入 `source_is_manual` 集合——
/// `global` 的人工特权（上面的判据）只向平台开放；member_api 同样不得混进
/// 人工集合（变异 M2 的击杀点：`source_is_manual_for_test` 断言它为假）。
pub const TRIGGER_SOURCE_MEMBER_API: &str = "member_api";

/// P4b（#35 D10/D1）· 归属编码（FA C1.1 迁移列）。整数列，任何 collation
/// 下比较都字节精确。
#[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
pub const AUTHORITY_KIND_NONE: i32 = 0;
pub const AUTHORITY_KIND_PLATFORM: i32 = 1;
pub const AUTHORITY_KIND_AUTO: i32 = 2;
pub const AUTHORITY_KIND_MEMBER: i32 = 3;

/// 写者的归属类别（r2 §2.2 转换表的输入）。分类是**闭世界**的：
///
/// * `member_api` → member（整数身份必经 `MemberArmContext`，见 arm 层校验）；
/// * 人工三源 `manual_api|manual_file|signal` → platform——这同时也是
///   `global` 特权的判据（W3），`source_is_manual` 是唯一入口，谁都不许
///   借别的串混进这个集合（变异 M2）；
/// * **其余非空来源一律归 auto 桶**。引擎内部存在多个不经过 svc 触发契约
///   的自动写者（`risk_budget`、`auto_daily_loss`、`reconcile_diff`、
///   `auto_protection_timeout`、`auto_*`……）——它们既不是会员也不是人，
///   auto 桶只是"非会员、非人工"的归属记账，不授予 global 权（那只认
///   精确的人工三源）也不授予任何会员资格。
///
/// HTTP 入口的来源闭世界在 `svc_v2::validate_kill_switch_trigger_contract`
/// 处强制；本函数的闭世界只决定归属列怎么记，空串仍拒绝（无法记账）。
fn authority_kind_for_source(source: &str) -> Option<i32> {
    if source == TRIGGER_SOURCE_MEMBER_API {
        Some(AUTHORITY_KIND_MEMBER)
    } else if source_is_manual(source) {
        Some(AUTHORITY_KIND_PLATFORM)
    } else if !source.trim().is_empty() {
        Some(AUTHORITY_KIND_AUTO)
    } else {
        None
    }
}

/// A3/C1-e 测试缝：设置后 member reset 在准入门读取后暂停（行锁仍持有）。
#[cfg(test)]
pub(crate) type AdmissionPauseFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

#[cfg(test)]
pub(crate) static ADMISSION_PAUSE_HOOK: std::sync::Mutex<
    Option<Box<dyn Fn() -> AdmissionPauseFuture + Send>>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn test_admission_pause_hook() -> Option<AdmissionPauseFuture> {
    let guard = ADMISSION_PAUSE_HOOK.lock().unwrap();
    guard.as_ref().map(|f| f())
}

/// P4b · canonical ULID 闭世界（Crockford base32，26 位大写：0-9 与
/// A-HJKMNP-TV-Z，排除 I/L/O/U）。`request_ref` 写入与查询两侧同验；
/// MySQL 侧查询另用 `CAST(col AS BINARY)=CAST(? AS BINARY)` 做字节精确比较
/// ——整数 owner 解决不了的字符串比较问题由「闭世界验证+BINARY 比较」双件
/// 解决（FA C2.4，变异 M9 的击杀点之一）。
pub(crate) fn is_canonical_request_ref(value: &str) -> bool {
    value.len() == 26
        && value.bytes().all(|b| {
            b.is_ascii_digit()
                || matches!(b, b'A'..=b'H' | b'J' | b'K' | b'M' | b'N' | b'P'..=b'T' | b'V'..=b'Z')
        })
}

/// 供 `auto_triggers` 的测试断言"自动源不得混进人工集合"。人工源与自动源的
/// 降级前置不同（`global` 只允许人工 arm），混淆会让自动触发拿到人工权限。
#[cfg(test)]
pub(crate) fn source_is_manual_for_test(source: &str) -> bool {
    source_is_manual(source)
}

async fn reconcile_targets_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    scope_key: &str,
) -> Result<Vec<String>, sqlx::Error> {
    if let Some(credential_id) = scope_key.strip_prefix("cred:") {
        return Ok(vec![credential_id.to_string()]);
    }
    let rows = if let Some(bot_uid) = scope_key.strip_prefix("bot:") {
        sqlx::query(
            "SELECT credential_id FROM position_state WHERE bot_uid=? UNION SELECT credential_id FROM engine_orders WHERE bot_uid=?",
        )
        .bind(bot_uid)
        .bind(bot_uid)
        .fetch_all(&mut **tx)
        .await?
    } else {
        sqlx::query(
            "SELECT credential_id FROM risk_budget UNION SELECT credential_id FROM engine_orders UNION SELECT credential_id FROM position_state",
        )
        .fetch_all(&mut **tx)
        .await?
    };
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("credential_id"))
        .collect())
}

/// **本 revision 在 arm 那一刻冻结下来的 expected target**
/// 。
///
/// 只由 `reset_reconcile_targets_in_tx` 在 arm 事务里写。它是 frozen expected
/// target 集合的**唯一**来源——`record_reconcile_for_scope` 的投影路径一行
/// 都不能写成这个值，否则"谁是成员"的输入又会包含它自己的输出。
const TARGET_FROM_ARM_SNAPSHOT: &str = "arm_snapshot";

/// **投影路径为"此刻的成员"新建的行**：它靠 `reconcile_targets_in_tx` 此刻
/// 还认不认得这个 credential 参与收敛，不靠自己这一行。
const TARGET_FROM_RECONCILE_DISCOVERY: &str = "reconcile_discovery";

/// **R19 启动期复核判定"证不出归属"的存量行**。
///
/// R19 用「此刻还是不是成员」把 `NULL` provenance 行二分 adopted/isolated，
/// 而"arm 时冻结、升级时敞口已消失"的**真实** target 恰好落在 isolated 那一侧
/// （regression）。这个值因此只在**读**侧保留：升级到 R20 的库里若还有
/// `legacy_isolated` 行，启动期复核会拿新判据把它们重判一遍
/// （`ReconcileStatusRow::needs_revalidation`）。
const TARGET_LEGACY_ISOLATED: &str = "legacy_isolated";

/// **启动期复核证不出来源、等一次结构化人工处置的存量行**
/// 。
///
/// 语义与 R19 的 `legacy_isolated` 有一处关键差别：它**不**声称"这一行
/// 不是本 revision 的 target"——那正是 R19 用「当前不是成员」推导出来的、
/// 证不出来的结论。它声称的只是"这一行的来源在数据里答不出来"：
///
/// * 它**不投票**（不进 expected 集合，clean/flat 一格不算）——outsider
///   不能自授权；
/// * 它**不阻塞**（聚合与 `reset` 都不因为它而拒绝）——一条来源不明的行
///   不得把一个已经完整收敛的 scope 永久钉死（第十六轮 §8.6 · regression）；
/// * 它**留着可审计**，并让这个 scope 进入**可重新冻结**状态：人工
///   `SetKillSwitch(manual, flatten)` 会为它开一轮新 revision，
///   按 `reconcile_targets_in_tx` 重新冻结成员
///   （`arm_in_tx_with_round` 的 re-freeze 分支）。
const TARGET_LEGACY_UNPROVABLE: &str = "legacy_unprovable";

async fn reset_reconcile_targets_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    scope_key: &str,
    generation: i64,
) -> anyhow::Result<()> {
    let targets = reconcile_targets_in_tx(tx, scope_key).await?;
    sqlx::query("DELETE FROM kill_switch_reconcile_status WHERE scope_key=?")
        .bind(scope_key)
        .execute(&mut **tx)
        .await?;
    // **冻结成员行带 provenance。**
    //
    // 这一句是 frozen expected target 的**产地**：只有从这里写出去的行才
    // 允许充当"本 revision 在 arm 那一刻认下的成员"。投影路径写的行拿的是
    // 另一个值（`TARGET_FROM_RECONCILE_DISCOVERY`），于是"谁是 expected 成员"
    // 的判据再也不会把自己的输出当输入。
    let insert = match kind {
        DbKind::MySql => {
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, expected_provenance) VALUES (?, ?, 0, 0, 0, ?, ?) ON DUPLICATE KEY UPDATE reconcile_clean=0, flatten_complete=0, reconciled_at_ms=0, flatten_generation=VALUES(flatten_generation), expected_provenance=VALUES(expected_provenance)"
        }
        _ => {
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, expected_provenance) VALUES (?, ?, 0, 0, 0, ?, ?) ON CONFLICT(scope_key, credential_id) DO UPDATE SET reconcile_clean=0, flatten_complete=0, reconciled_at_ms=0, flatten_generation=excluded.flatten_generation, expected_provenance=excluded.expected_provenance"
        }
    };
    for credential_id in targets {
        sqlx::query(insert)
            .bind(scope_key)
            .bind(credential_id)
            .bind(generation)
            .bind(TARGET_FROM_ARM_SNAPSHOT)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub async fn effective_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    bot_uid: &str,
) -> Result<EffectiveStateRead, sqlx::Error> {
    let credential_scope = format!("cred:{credential_id}");
    let bot_scope = format!("bot:{bot_uid}");
    let rows = sqlx::query(
        r#"
        SELECT scope_key, level, reason,
               COALESCE(flatten_state, '') AS flatten_state
        FROM kill_switch_state
        WHERE scope_key = 'global' OR scope_key = ? OR scope_key = ?
        "#,
    )
    .bind(&credential_scope)
    .bind(&bot_scope)
    .fetch_all(&mut **tx)
    .await?;

    let mut effective: Option<EffectiveState> = None;
    let mut invalid_levels = Vec::new();
    for row in rows {
        let scope_key = row.get::<String, _>("scope_key");
        let raw_level = row.get::<String, _>("level");
        let Some(level) = parse_persisted_level(&raw_level) else {
            invalid_levels.push(InvalidPersistedLevel {
                scope_key,
                raw_level,
            });
            continue;
        };
        let candidate = EffectiveState {
            scope_key,
            level,
            reason: nonempty(crate::store::try_get_text(&row, "reason")?.unwrap_or_default()),
            flatten_state: nonempty(row.get::<String, _>("flatten_state")),
        };
        let replace = effective.as_ref().is_none_or(|current| {
            candidate.level.severity() > current.level.severity()
                || (candidate.level == current.level
                    && scope_priority(&candidate.scope_key) > scope_priority(&current.scope_key))
        });
        if replace {
            effective = Some(candidate);
        }
    }
    invalid_levels.sort_by(|left, right| {
        left.scope_key
            .cmp(&right.scope_key)
            .then_with(|| left.raw_level.cmp(&right.raw_level))
    });
    Ok(EffectiveStateRead {
        effective,
        invalid_levels,
    })
}

/// 一次 arm 是**沿用当前这一轮 flatten 处置**，还是**强制开新一轮**。
///
/// 。R14 把"开新一轮"写成了
/// `arm_in_tx` 之后再来一次 `SELECT generation` + `UPDATE generation+1`：
/// 两条独立语句之间读到的号，落笔时早已不是库里的号——两条并发 latch
/// 同读 N、同写 N+1，于是**两个不同的裸敞口共用一个代际号**，
/// 先回来的那份 flat truth 可以把后一个敞口一并写成 `flattened`。
///
/// 现在代际号只在**一条 upsert 语句里自增**（`generation = generation + 1`），
/// 由数据库保证原子；"开新一轮"因此不再是调用方的第二次写，而是这里的一个入参。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatchRound {
    /// 常规 arm：当前档位已经不低于请求档位就原样返回，不动代际号。
    Existing,
    /// `latch_flatten_pending`：无条件重开一轮 flatten 处置。
    ForceNew,
}

/// **一次人工重新冻结的准入判据**
/// 。
///
/// `arm` 有一条刻意的幂等早返回：当前档位的严重度 `>=` 请求档位时原样返回、
/// 一个字节都不写。它挡住的是"同一件事被 arm 两次就凭空推一格代际"。
///
/// 可是这条早返回同时堵死了**唯一**一条人工出口。0028 之前写下的存量行里
/// 有一部分来源在数据里答不出来（`legacy_unprovable`）：它们不投票、也不
/// 阻塞，但**万一其中真有一条是本 revision 的 target**，这一轮的 expected
/// 集合就是残缺的，而运维手上没有任何办法让它重新完整——`reset` 要么被
/// 缺席的成员挡住，要么清掉一个其实还没平干净的 latch。
///
/// 重新冻结就是那条出口，四个条件缺一不可：
///
/// * **人来做**（`source_is_manual`；`svc_v2::set_kill_switch` 对 `manual_api`
///   还额外要求非空 `actor`）——自动触发源不得凭空推代际；
/// * **只对 flatten**：请求档位与当前档位都是 `flatten`。它是唯一带
///   per-credential 收敛判据、因而会被残缺 expected 集合卡住的档位；
/// * **这一轮确实卡着**：本代真的存在来源证不出来的行。健康 scope 上
///   这个函数恒为假，幂等早返回逐字不变；
/// * 走的仍是 `arm` 那条**普通**路径：代际 +1、
///   `reset_reconcile_targets_in_tx` 按 `reconcile_targets_in_tx` 重新冻结
///   成员、写一条新的 `kill_switch_events` 审计行。
///
/// 它**不清 latch、不写任何 clean/flat 票**：新一轮从"未收敛"开始，
/// 每个成员都必须重新报一次真值。因此它不是"绕过收敛判据"，
/// 而是"把判据的输入重建成可证明的"。旧一代那些证不出来的行留在旧代际上，
/// 一票不算，可审计。
///
/// **这条出口
/// 自己的两个洞。** 判定为真之后，调用方（`arm_in_tx_with_round`）还要再过
/// 两道，缺一道这条出口就会把 scope 弄进它本该救出来的那种状态：
///
/// * **当前 target 不得为空**——空集重新冻结出来的是一个 `total = 0`、
///   永远收敛不了的代，而 quarantine 行（本函数唯一的触发条件）在同一步
///   被删光，出口就此消失（死锁）。空集一律拒绝，旧 snapshot 原样留着；
/// * **这一读必须拿 `kill_switch_state` 的行锁**——本函数读 quarantine 行
///   用的是调用方传进来的 `generation`，两者都不带锁时两路并发人工
///   re-freeze 会同读同判、各推一代（N→N+2）。拿锁之后第二路重读时
///   quarantine 已被清空，本函数返回假，它落回幂等早返回。
async fn scope_awaits_refreeze(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    scope_key: &str,
    generation: i64,
    current_level: Level,
    requested_level: Level,
    trigger_source: &str,
) -> anyhow::Result<bool> {
    if requested_level != Level::Flatten
        || current_level != Level::Flatten
        || !source_is_manual(trigger_source)
    {
        return Ok(false);
    }
    Ok(reconcile_status_rows_in_tx(tx, scope_key, generation)
        .await?
        .iter()
        .any(|row| row.is_unprovable()))
}

pub async fn arm_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    request: ArmRequest<'_>,
) -> anyhow::Result<EffectiveState> {
    arm_in_tx_with_round(tx, kind, request, LatchRound::Existing, None).await
}

async fn arm_in_tx_with_round(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    request: ArmRequest<'_>,
    round: LatchRound,
    member: Option<MemberArmContext<'_>>,
) -> anyhow::Result<EffectiveState> {
    let scope_name = scope(request.scope_key)
        .ok_or_else(|| anyhow!("scope_key must be global, cred:<id>, or bot:<uid>"))?;
    if request.level == Level::None {
        return Err(anyhow!("arming level must not be none; use reset"));
    }
    if scope_name == "global" && !source_is_manual(request.trigger_source) {
        return Err(anyhow!("global scope may only be armed manually"));
    }
    // **P4b（#35 D3/D1）· 会员源的引擎侧闭世界（FA C2.3 的身份来源）。**
    //
    // 归属类别只认人工（平台）/auto_*/member_api 三类；member_api 必须携带
    // MemberArmContext（整数身份 + canonical ULID），且只允许 cred: scope、
    // pause_new/flatten 两档。svc 层的触发契约先拒过一遍，这里是引擎执行
    // 点的第二道（防控制面旁路，变异 M2/M10 的纵深）。
    let writer_kind = authority_kind_for_source(request.trigger_source).ok_or_else(|| {
        anyhow!(
            "trigger_source {} is not in the closed authority set; refusing to arm",
            request.trigger_source
        )
    })?;
    let member = match (writer_kind, member) {
        (AUTHORITY_KIND_MEMBER, Some(context)) => {
            if !scope_key_is_credential(request.scope_key) {
                return Err(anyhow!("member_api may only arm a cred: scope"));
            }
            if request.level == Level::ManageOnly {
                return Err(anyhow!("member_api may only arm pause_new or flatten"));
            }
            if !is_canonical_request_ref(context.request_ref) {
                return Err(anyhow!("member arm requires a canonical ULID request_ref"));
            }
            Some(context)
        }
        (AUTHORITY_KIND_MEMBER, None) => {
            return Err(anyhow!(
                "member_api requires an authority_member_id and request_ref"
            ));
        }
        (_, Some(_)) => {
            return Err(anyhow!(
                "member context is only valid with the member_api trigger source"
            ));
        }
        _ => None,
    };
    if request.now_ms <= 0 {
        return Err(anyhow!("now_ms must be positive"));
    }

    // **这一读必须拿行锁。**
    //
    // （历史注释保留）R20 用的是一条普通 SELECT：它读的 `flatten_generation`
    // 与 `scope_awaits_refreeze` 随后读的 quarantine 行都不带锁，而"推不推
    // 代际"的决定就架在这两读之上。两路并发的人工 re-freeze 因此同读一份
    // quarantine、同判"这一轮卡着"，各自走完下面那条原子自增——代际从 N
    // 直接跳到 N+2。修法是把这条读升级成 `kill_switch_state` 那一行的排他
    // 行锁：第二路在这里阻塞，等第一路提交之后重读。锁的对象与
    // `revalidate_one_scope_reconcile_targets` 是同一行、同一顺序。
    //
    // **P4b 增补**：这把行锁同样是归属转换表（r2 §2.2）的判定基础——
    // 旧归属（authority_kind/member_id）与档位在同一把锁下读出，转换决策
    // 与写入之间没有窗口。SQLite 后端单连接天然串行。
    //
    // ForceNew 也必须经过这次读取。它虽然不走档位幂等判定，却仍是一个
    // durable-state 写者；若跳过严格解码，它会用 canonical `flatten` 静默
    // 覆盖一条损坏的 raw level，既丢审计证据又把未知状态伪装成已理解。
    let locked = match kind {
        DbKind::MySql => {
            "SELECT level, reason, COALESCE(flatten_state, '') AS flatten_state, \
             flatten_generation, authority_kind, authority_member_id \
             FROM kill_switch_state WHERE scope_key = ? FOR UPDATE"
        }
        _ => {
            "SELECT level, reason, COALESCE(flatten_state, '') AS flatten_state, \
             flatten_generation, authority_kind, authority_member_id \
             FROM kill_switch_state WHERE scope_key = ?"
        }
    };
    let current = sqlx::query(locked)
        .bind(request.scope_key)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .next();
    if let Some(row) = &current {
        let raw_level = row.get::<String, _>("level");
        let level = parse_persisted_level(&raw_level).ok_or_else(|| {
            anyhow::Error::new(InvalidPersistedLevel {
                scope_key: request.scope_key.to_string(),
                raw_level,
            })
        })?;
        let armed = level != Level::None;
        let current_authority_kind = row.get::<i64, _>("authority_kind") as i32;
        let current_authority_member: Option<i64> =
            row.try_get::<i64, _>("authority_member_id").ok();
        // **P4b · 归属转换表的前置拒绝（C2：异会员；r2 §2.2）。**
        //
        // 已闩且归属是另一位会员时，任何会员 arm（无论同级还是升档）一律
        // 拒绝、零写——scope 由设它的那位会员与平台/自动处置。
        if armed
            && writer_kind == AUTHORITY_KIND_MEMBER
            && current_authority_kind == AUTHORITY_KIND_MEMBER
            && current_authority_member != member.map(|m| m.member_id)
        {
            return Err(anyhow!(
                "kill switch scope {} is armed under another member's authority; this \
                 member may raise the protection level only through the platform path",
                request.scope_key
            ));
        }
        if round == LatchRound::Existing && level.severity() >= request.level.severity() {
            // **P4b · 同级平台/自动接管的同事务双写（r2 §2.1 修正③、D4）。**
            //
            // 幂等早返回原本零写——平台同级重设因此不可见，会员解除资格
            // 得以残留。现在：平台/自动写者在早返回前，若归属与自己的身份
            // 不同，先做一次**绑代 CAS 的归属接管**（state + 当前代 active
            // 事件行的 authority 列同事务双写；档位/时间/代际一字不动，
            // `trigger_source` 与 arm_actor 列不可变——原 arm 来源证据保留
            // 在事件行上，不被接管洗掉）。会员写者不接管：对平台/自动/未知
            // 归属的同级请求只是早返回（strictest 已满足，无需写入）。
            if armed && writer_kind != AUTHORITY_KIND_MEMBER {
                let target_member: Option<i64> = None;
                if current_authority_kind != writer_kind
                    || current_authority_member != target_member
                {
                    let generation = row.get::<i64, _>("flatten_generation");
                    take_authority_in_tx(
                        tx,
                        kind,
                        request.scope_key,
                        generation,
                        writer_kind,
                        target_member,
                    )
                    .await?;
                }
            }
            if !scope_awaits_refreeze(
                tx,
                request.scope_key,
                row.get::<i64, _>("flatten_generation"),
                level,
                request.level,
                request.trigger_source,
            )
            .await?
            {
                return Ok(EffectiveState {
                    scope_key: request.scope_key.to_string(),
                    level,
                    reason: nonempty(
                        crate::store::try_get_text(row, "reason")?.unwrap_or_default(),
                    ),
                    flatten_state: nonempty(row.get::<String, _>("flatten_state")),
                });
            }
            // **零目标
            // re-freeze 必须 fail-closed。**
            //
            // 走到这里 = 这是一次**人工重新冻结**（唯一穿过档位幂等
            // 早返回的路径）。它下面会 `reset_reconcile_targets_in_tx`：
            // 先 `DELETE` 掉本 scope 的全部 status 行，再按
            // `reconcile_targets_in_tx` 此刻的 target 重新冻结。
            //
            // 当前 target 为空时这两步合起来是一个**死锁代**：
            //
            // * 新代一行都没有 ⇒ `ExpectedConvergence::total = 0` ⇒
            //   `complete_membership()` 恒假 ⇒ 这一代**永远**收敛不了，
            //   `reset` 也永远拒绝；
            // * 而那些 quarantine 行——`scope_awaits_refreeze` 唯一的
            //   触发条件——刚刚被 `DELETE` 一并清掉了 ⇒ 下一次人工
            //   `SetKillSwitch(manual, flatten)` 在档位幂等早返回处
            //   原样返回，**再也开不出新一轮**。出口自己把出口堵死了。
            //
            // 因此在写任何一个字节之前就拒绝：旧 snapshot 原样留着
            // （quarantine 行还在，出口仍然可用），并如实说明为什么
            // 这一刻无法重新冻结（engine safety contract：无法诚实产出就
            // 标阻塞，不得用一个空集冒充"重新冻结成功"）。
            let live_targets = reconcile_targets_in_tx(tx, request.scope_key).await?;
            if live_targets.is_empty() {
                return Err(anyhow!(
                    "re-freeze rejected: {} is latched at flatten with unprovable \
                     reconcile status rows, but it currently has no target credentials \
                     to freeze; refreezing an empty target set would drop the existing \
                     snapshot and leave a generation that can never converge. Resolve \
                     the scope's exposure (or its quarantined rows) first",
                    request.scope_key
                ));
            }
        }
    }

    // **P4b · 归属转换表（r2 §2.2 全表 + 三处修正）· 写入侧的归属决策。**
    //
    // 平台/自动写者无条件索取归属 `(kind, NULL)`；会员写者只在
    // 「未闩（无行或 level=none）」或「已闩且归属本就是同一会员」时索取
    // `(3, member_id)`，对平台/自动/未知（kind=0）活闩升档时**保持原归属**
    // ——升档允许（strictest 是安全方向），但「未知」不得因会员升档被
    // 洗成「已证明会员」（复核 R2 的关闭条件）。
    let (authority_kind, authority_member_id): (i32, Option<i64>) = match member {
        None => (writer_kind, None),
        Some(context) => {
            // 已闩（level != none）时保留既有归属（同会员的 (3,M) 原样、
            // 平台/自动/未知不因会员升档被洗）；未闩才索取 (3,M)。
            let armed_authority = current.as_ref().and_then(|row| {
                let raw = row.get::<String, _>("level");
                let armed = parse_persisted_level(&raw)
                    .map(|l| l != Level::None)
                    .unwrap_or(false);
                armed.then(|| {
                    (
                        row.get::<i64, _>("authority_kind") as i32,
                        row.try_get::<i64, _>("authority_member_id").ok(),
                    )
                })
            });
            match armed_authority {
                None => (AUTHORITY_KIND_MEMBER, Some(context.member_id)),
                Some((kind, member_id)) => (kind, member_id),
            }
        }
    };

    let flatten_state = (request.level == Level::Flatten).then_some(FLATTEN_PENDING);
    // ****：写下一条**未收敛**的 flatten
    // latch 就是开新一轮处置。任何在此之前观察到的交易所真值都早于这个敞口，
    // 不得用来收敛它——代际号 +1 让"早于"成为可比的事实（见 0023 迁移）。
    //
    // **自增必须是原子的。**
    // +1 写在 upsert 的 DO UPDATE / ON DUPLICATE KEY UPDATE 子句里，由数据库
    // 在行锁内完成；新号随后在**同一个事务里**读回，后面的
    // `reset_reconcile_targets_in_tx` 与事件行都绑这同一个号。
    //
    // **每一次 arm 都推进这个号。**
    // 号的语义是"第几次安全 level arm"——一个单调 revision；幂等的重复 arm
    // 在上面早返回，不推号。
    let generation_conflict_clause = "flatten_generation=flatten_generation + 1,";
    let initial_generation: i64 = 1;
    let arm_request_ref: Option<&str> = member.map(|m| m.request_ref);
    let arm_actor_member_id: Option<i64> = member.map(|m| m.member_id);
    let upsert = match kind {
        DbKind::MySql => {
            format!(
                r#"
            INSERT INTO kill_switch_state (
              scope_key, level, reason, trigger_source, armed_at_ms,
              flatten_state, flatten_generation, last_reconcile_clean,
              last_reconcile_at_ms, updated_at_ms,
              authority_kind, authority_member_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              level=VALUES(level), reason=VALUES(reason),
              trigger_source=VALUES(trigger_source), armed_at_ms=VALUES(armed_at_ms),
              flatten_state=VALUES(flatten_state),
              {generation_conflict_clause} last_reconcile_clean=NULL,
              last_reconcile_at_ms=NULL, updated_at_ms=VALUES(updated_at_ms),
              authority_kind=VALUES(authority_kind),
              authority_member_id=VALUES(authority_member_id)
            "#
            )
        }
        _ => {
            format!(
                r#"
            INSERT INTO kill_switch_state (
              scope_key, level, reason, trigger_source, armed_at_ms,
              flatten_state, flatten_generation, last_reconcile_clean,
              last_reconcile_at_ms, updated_at_ms,
              authority_kind, authority_member_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
            ON CONFLICT(scope_key) DO UPDATE SET
              level=excluded.level, reason=excluded.reason,
              trigger_source=excluded.trigger_source, armed_at_ms=excluded.armed_at_ms,
              flatten_state=excluded.flatten_state,
              {generation_conflict_clause} last_reconcile_clean=NULL,
              last_reconcile_at_ms=NULL, updated_at_ms=excluded.updated_at_ms,
              authority_kind=excluded.authority_kind,
              authority_member_id=excluded.authority_member_id
            "#
            )
        }
    };
    sqlx::query(&upsert)
        .bind(request.scope_key)
        .bind(request.level.as_str())
        .bind(request.reason)
        .bind(request.trigger_source)
        .bind(request.now_ms.to_string())
        .bind(flatten_state)
        .bind(initial_generation)
        .bind(request.now_ms.to_string())
        .bind(authority_kind as i64)
        .bind(authority_member_id)
        .execute(&mut **tx)
        .await?;
    let next_generation =
        sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key = ?")
            .bind(request.scope_key)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .next()
            .map(|row| row.get::<i64, _>("flatten_generation"))
            .ok_or_else(|| anyhow!("kill switch state row disappeared while it was being armed"))?;
    reset_reconcile_targets_in_tx(tx, kind, request.scope_key, next_generation).await?;

    // **P4b（FA C2.3）· 事件行的两类身份分列：**
    // `authority_kind/authority_member_id` = 当前解除权归属（接管/降级可变，
    // 与 state 行同事务双写）；`arm_actor_kind/arm_actor_member_id` +
    // `arm_request_ref` = 本次 arm 的**不可变**执行身份（此后任何写者不得
    // 更新）。`trigger_source` 仍是最新动作事实。会员对平台档升档时：
    // authority 保持平台，arm_actor 记会员——这正是 C2.3 要的可分别识别。
    let event_id = ulid::Ulid::new().to_string();
    sqlx::query(
        r#"
        INSERT INTO kill_switch_events (
          event_id, scope, scope_key, level, trigger_source, reason,
          armed_at_ms, flatten_state, reconcile_clean, reset_by, reset_at_ms,
          created_at_ms, updated_at_ms, flatten_generation,
          authority_kind, authority_member_id,
          arm_request_ref, arm_actor_kind, arm_actor_member_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(event_id)
    .bind(scope_name)
    .bind(request.scope_key)
    .bind(request.level.as_str())
    .bind(request.trigger_source)
    .bind(request.reason)
    .bind(request.now_ms.to_string())
    .bind(flatten_state)
    .bind(request.now_ms.to_string())
    .bind(request.now_ms.to_string())
    .bind(next_generation)
    .bind(authority_kind as i64)
    .bind(authority_member_id)
    .bind(arm_request_ref)
    .bind(writer_kind as i64)
    .bind(arm_actor_member_id)
    .execute(&mut **tx)
    .await?;

    Ok(EffectiveState {
        scope_key: request.scope_key.to_string(),
        level: request.level,
        reason: Some(request.reason.to_string()),
        flatten_state: flatten_state.map(str::to_string),
    })
}

/// 同级接管 / 降级的归属双写（r2 §2.1 修正③）：state 行与**当前代 active
/// 事件行**（与 `reset` 同一条选取规则）的 authority 列在同一次调用里
/// 更新，绑代 CAS + rows_affected 双检。`trigger_source`、`arm_actor_*`、
/// `*_request_ref` 不可变——本函数绝不触碰它们。
async fn take_authority_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    scope_key: &str,
    generation: i64,
    target_kind: i32,
    target_member: Option<i64>,
) -> anyhow::Result<()> {
    // `<=>` 是 MySQL 的 NULL 安全等值；SQLite 没有它，用 `IS`（同一语义）。
    // 两个后端的谓词必须等价，否则同一状态在一侧判「已接管」另一侧判
    // 「未接管」，rows_affected 的语义就漂了。
    let null_safe = match kind {
        DbKind::MySql => "authority_kind <=> ? AND authority_member_id <=> ?",
        _ => "authority_kind IS ? AND authority_member_id IS ?",
    };
    let state_sql = format!(
        "UPDATE kill_switch_state \
         SET authority_kind=?, authority_member_id=?, updated_at_ms=updated_at_ms \
         WHERE scope_key=? AND level <> 'none' AND flatten_generation=? \
         AND NOT ({null_safe})"
    );
    let state_rows = sqlx::query(&state_sql)
        .bind(target_kind as i64)
        .bind(target_member)
        .bind(scope_key)
        .bind(generation)
        .bind(target_kind as i64)
        .bind(target_member)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if state_rows == 0 {
        // 归属已是目标值（幂等），或代际/档位在锁内重读后不符。前者在
        // 调用方已比对过、不应出现；后者与 arm 的既有失败语义一致。
        return Err(anyhow!(
            "authority takeover on {} missed at generation {generation}: the latch moved \
             or the authority already differs; nothing was written",
            scope_key
        ));
    }
    let event_sql = format!(
        "UPDATE kill_switch_events \
         SET authority_kind=?, authority_member_id=?, updated_at_ms=updated_at_ms \
         WHERE scope_key=? AND flatten_generation=? AND reset_at_ms IS NULL \
         AND NOT ({null_safe})"
    );
    let event_rows = sqlx::query(&event_sql)
        .bind(target_kind as i64)
        .bind(target_member)
        .bind(scope_key)
        .bind(generation)
        .bind(target_kind as i64)
        .bind(target_member)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if event_rows == 0 {
        return Err(anyhow!(
            "authority takeover on {} found no active event row at generation {generation}; \
             refusing to leave state and event authority inconsistent",
            scope_key
        ));
    }
    tracing::warn!(
        scope_key,
        generation,
        authority_kind = target_kind,
        "kill-switch authority taken over by a same-level writer; member reset \
         eligibility is revoked from this revision (state and event written together)"
    );
    Ok(())
}

fn scope_key_is_credential(scope_key: &str) -> bool {
    scope_key
        .strip_prefix("cred:")
        .is_some_and(|v| !v.trim().is_empty())
}

pub async fn arm(store: &Store, request: ArmRequest<'_>) -> anyhow::Result<EffectiveState> {
    let mut tx = store.pool().begin().await?;
    let state = arm_in_tx(&mut tx, store.kind(), request).await?;
    tx.commit().await?;
    Ok(state)
}

/// P4b · 会员降险链入口（r2 §1.1'）：无挑战、无密码因子、无强制读 RPC、
/// 无 revision 门——身份（整数 member_id）与请求关联（canonical ULID）
/// 仍严格。`arm_in_tx_with_round` 内做归属转换与 strictest 原子设档。
pub async fn arm_member(
    store: &Store,
    request: ArmRequest<'_>,
    member: MemberArmContext<'_>,
) -> anyhow::Result<EffectiveState> {
    let mut tx = store.pool().begin().await?;
    let state = arm_in_tx_with_round(
        &mut tx,
        store.kind(),
        request,
        LatchRound::Existing,
        Some(member),
    )
    .await?;
    tx.commit().await?;
    Ok(state)
}

/// **把一个 scope 闩到 `flatten` 且必然处于「未收敛」。**
///
/// 。`arm` 有一条刻意的早返回：
/// 当前档位的严重度 `>=` 请求档位时原样返回，不写库。对 `flatten` 而言
/// 这条早返回会带来一个安全洞——上一轮处置已经收敛（`flatten_state`
/// 已经是 `flattened`）之后**又**发现了一个新的裸敞口，`arm(Flatten)`
/// 会直接返回那条 `flattened` 的旧状态，于是：
///
/// * `spawn_action_retry_task` 的扫描条件是
///   `level='flatten' AND flatten_state <> 'flattened'`，这个 scope 不会被扫到；
/// * 调用方看到 `flattened` 会以为"已经平掉了"。
///
/// 新敞口必须让 latch **回到 `flattening`**：清掉上一轮的收敛结论
/// （连同 `kill_switch_reconcile_status` 的逐 credential 行），
/// 让重试任务重新驱动「撤单 → reduce-only flatten → 对账」，
/// 只有 `ReconcileTruth::flatten_complete()` 能再次把它写成 `flattened`。
pub(crate) async fn latch_flatten_pending(
    store: &Store,
    scope_key: &str,
    trigger_source: &str,
    reason: &str,
    now_ms: i64,
) -> anyhow::Result<EffectiveState> {
    let mut tx = store.pool().begin().await?;
    // **每次闩都是新一轮。**
    //
    // R13 这里有一条早返回：`flatten_state` 已经是 `flattening` 就原样返回。
    // 它把两件不同的事当成了同一件——"上一轮还没收敛"与"刚刚又发现了一个
    // 新的裸敞口"。后者必须开新一轮：任何**早于这一刻**观察到的交易所真值
    // 都不认识这个敞口，却完全可能是一份"全平"的 flat truth，落库时把这条
    // latch 直接写成 `flattened`（TOCTOU）。
    //
    // **重开只能是一条语句。**
    //
    // R14 的形状是"先 `arm_in_tx`，再自己 `SELECT generation` + `UPDATE
    // generation+1`"——第二段是读改写，并发下两条 latch 同读 N 同写 N+1，
    // 代际号于是不再能区分两个敞口；而且 `arm_in_tx` 早返回时它写下的
    // 新号与那条**事件行**上的号也对不上，审计面会停在上一轮。
    //
    // 现在"重开一轮"就是 `arm_in_tx` 自己的一个模式（`LatchRound::ForceNew`）：
    // 跳过档位早返回，代际号在 upsert 里原子 +1，逐 credential 行与新的
    // 事件行都绑这同一个号。重复闩是幂等的（同一个敞口连闩两次只是多推一个
    // 号），而漏闩是敞口丢失。
    let state = arm_in_tx_with_round(
        &mut tx,
        store.kind(),
        ArmRequest {
            scope_key,
            level: Level::Flatten,
            trigger_source,
            reason,
            now_ms,
        },
        LatchRound::ForceNew,
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(state)
}

/// Retire a protection confirmation obligation only for its exact, provably
/// closed position. The current row lock and the ForceNew arm share one
/// transaction, so a close committed during a venue query cannot rearm the bot.
/// Missing, malformed, nonzero or mismatched rows never count as closed proof.
pub(crate) async fn latch_flatten_unless_position_closed(
    store: &Store,
    position_id: &str,
    credential_id: &str,
    bot_uid: &str,
    reason: &str,
    now_ms: i64,
) -> anyhow::Result<Option<EffectiveState>> {
    let mut tx = store.pool().begin().await?;
    // SQLite has no FOR UPDATE. Acquire its writer lock before the read;
    // the no-op leaves the business timestamp and all position facts intact.
    if store.kind() != DbKind::MySql {
        sqlx::query("UPDATE position_state SET updated_at_ms=updated_at_ms WHERE position_id=?")
            .bind(position_id)
            .execute(&mut *tx)
            .await?;
    }
    let query = if store.kind() == DbKind::MySql {
        "SELECT credential_id, bot_uid, state, qty FROM position_state WHERE position_id=? FOR UPDATE"
    } else {
        "SELECT credential_id, bot_uid, state, qty FROM position_state WHERE position_id=?"
    };
    let row = sqlx::query(query)
        .bind(position_id)
        .fetch_optional(&mut *tx)
        .await?;
    let closed = if let Some(row) = row.as_ref() {
        let owner_credential = crate::store::try_get_nullable_string(row, "credential_id")?;
        let owner_bot = crate::store::try_get_nullable_string(row, "bot_uid")?;
        let state = crate::store::try_get_nullable_string(row, "state")?;
        let quantity = crate::store::try_get_nullable_string(row, "qty")?;
        owner_credential.as_deref() == Some(credential_id)
            && owner_bot.as_deref() == Some(bot_uid)
            && state.as_deref() == Some("closed")
            && quantity
                .as_deref()
                .and_then(crate::reconcile::snapshot_decimal_is_zero)
                == Some(true)
    } else {
        false
    };
    if closed {
        tx.commit().await?;
        return Ok(None);
    }
    let scope_key = format!("bot:{bot_uid}");
    let state = arm_in_tx_with_round(
        &mut tx,
        store.kind(),
        ArmRequest {
            scope_key: &scope_key,
            level: Level::Flatten,
            trigger_source: "auto_protection_timeout",
            reason,
            now_ms,
        },
        LatchRound::ForceNew,
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(Some(state))
}

/// **一次交易所真值观察所对应的 latch 代际快照。**
///
/// 。对账要先读交易所、后落库，中间
/// 隔着好几个网络往返；这中间新开的 latch 让"落库时看到的敞口"与
/// "读交易所时的敞口"不再是同一个。调用方必须在**动身去读交易所之前**
/// 取一次快照，落库时把它交回 `record_reconcile_for_scope`——
/// 号对不上的真值一律丢弃。
///
/// 一次查全表：scope 只有 global / cred:* / bot:* 三类，行数与凭据数同阶；
/// 逐 scope 现取会让不同 scope 的观察点错开，等于没有观察点。
#[derive(Debug, Clone, Default)]
pub struct FlattenGenerations {
    observed: std::collections::HashMap<String, i64>,
}

impl FlattenGenerations {
    /// 快照里没有这个 scope = 观察那一刻它还没有过任何 flatten latch，
    /// 与代际 0 等价。
    pub fn observed_for(&self, scope_key: &str) -> i64 {
        self.observed.get(scope_key).copied().unwrap_or(0)
    }
}

/// 读事务内的代际快照（`get_kill_switch_states` 专用：与行读取同事务）。
async fn generations_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
) -> anyhow::Result<FlattenGenerations> {
    let rows = sqlx::query("SELECT scope_key, flatten_generation FROM kill_switch_state")
        .fetch_all(&mut **tx)
        .await?;
    Ok(FlattenGenerations {
        observed: rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("scope_key"),
                    row.get::<i64, _>("flatten_generation"),
                )
            })
            .collect(),
    })
}

pub async fn observe_flatten_generations(store: &Store) -> anyhow::Result<FlattenGenerations> {
    let rows = sqlx::query("SELECT scope_key, flatten_generation FROM kill_switch_state")
        .fetch_all(store.pool())
        .await?;
    Ok(FlattenGenerations {
        observed: rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("scope_key"),
                    row.get::<i64, _>("flatten_generation"),
                )
            })
            .collect(),
    })
}

/// 一次对账真值向某个 scope 的投影结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileProjection {
    /// 真值与当前 latch 同代，已按原有语义落库。
    Applied,
    /// 真值早于当前 latch：**丢弃**。它观察的是上一轮的敞口，
    /// 不能收敛这一轮的。
    DiscardedStale { observed: i64, current: i64 },
    /// 读到代际号之后、写下去之前，**这一轮已经不是那一轮了**：绑代写入
    /// 一行都没命中。——R14 的写入
    /// 带着 `flatten_generation=?` 谓词，却从不看 `rows_affected()`：
    /// 0 行命中与"写成功了"返回同一个 `Applied`，而事件行那一步压根没有
    /// 代际条件，照样会把这份真值写进新一轮的审计事件里。
    /// 整个事务回滚，调用方不得把它当成一次成功投影。
    DiscardedRaced { generation: i64 },
    /// 这个 scope 上**没有活着的 latch**：没有 revision 需要被收敛，
    /// 这份真值无处投影。不是错误——生产对账对 `global` / `cred:*` / `bot:*`
    /// 三类 scope 无条件投影一遍，其中绝大多数根本没闩过
    /// （`reconcile::reconcile_binance_futures_truth_with_client`）。
    ///
    /// ****：R17 在这条路径上仍然写一行
    /// 逐 credential"事实记录"。那行没有任何读者（聚合与 `reset` 都只数
    /// 本 revision 的行，而下一次 arm 会把整个 scope 的行删掉重建），
    /// 却正是"任意 credential 都能凭空建行"的载体。
    NoActiveLatch,
    /// 这个 credential **不在本 revision 冻结下来的 target 集合里**：拒绝。
    ///
    /// ****：R17 的写入是一条无条件 upsert,
    /// 任何非空 `credential_id` 都能给这个 scope 添一行；聚合又只对
    /// "本 revision 已经出现的行"判 clean。两条合起来是：**第一个任意
    /// credential 报一次 flat，整个 scope 就收敛**——latch 写成 `flattened`、
    /// 离开 `scopes_needing_action_retry` 的重试集合，而它真正的敞口
    /// 一次都没被平过（engine safety contract「对账失败后继续交易」）。
    RejectedNonTarget { generation: i64 },
}

/// `flatten_state` 的两个值——判据与写入共用一份，免得扫描条件与写入各写各的。
pub(crate) const FLATTEN_PENDING: &str = "flattening";
pub(crate) const FLATTEN_CONVERGED: &str = "flattened";

/// **重试任务的扫描集合**（唯一一份）。
///
/// 抽成生产函数是为了让「重启之后这个 scope 还会不会被重试」这件事
/// 可以被用例**用生产判据本身**观察：
/// 之前它只是 `spawn_action_retry_task` 里的一条内联 SQL，用例要么复写
/// 一遍这段 SQL（测的是测试自己），要么去读源码文本。
/// 重试扫描的一条目标：scope、档位，以及**这一次 arm 的代际**。
///
/// 代际必须随扫描结果一起出来。pacer 若只按 `scope_key` 存退避，一个已退避到
/// 60 秒的 scope 上出现**新一代裸敞口**时——它从未离开扫描集合——旧退避会被
/// 整个继承，新敞口首次失败后的重试与真值收敛就被拖到一分钟后，直接违反
/// "刚闩上一秒内处置"。`flatten_generation` 正是每次 arm 都推进的单调 revision。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActionRetryTarget {
    pub(crate) scope_key: String,
    pub(crate) level: Level,
    pub(crate) generation: i64,
    pub(crate) invalid_level: Option<InvalidPersistedLevel>,
}

pub(crate) async fn action_retry_targets(
    store: &Store,
) -> Result<Vec<ActionRetryTarget>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT scope_key, level, COALESCE(flatten_state, '') AS flatten_state, \
         flatten_generation FROM kill_switch_state",
    )
    .fetch_all(store.pool())
    .await?;
    let mut targets = Vec::new();
    for row in rows {
        let scope_key = row.get::<String, _>("scope_key");
        let raw_level = row.get::<String, _>("level");
        let generation = row.get::<i64, _>("flatten_generation");
        let flatten_state = row.get::<String, _>("flatten_state");
        match parse_persisted_level(&raw_level) {
            Some(Level::ManageOnly) => targets.push(ActionRetryTarget {
                scope_key,
                level: Level::ManageOnly,
                generation,
                invalid_level: None,
            }),
            Some(Level::Flatten) if flatten_state != FLATTEN_CONVERGED => {
                targets.push(ActionRetryTarget {
                    scope_key,
                    level: Level::Flatten,
                    generation,
                    invalid_level: None,
                });
            }
            Some(_) => {}
            None => {
                let invalid_level = InvalidPersistedLevel {
                    scope_key: scope_key.clone(),
                    raw_level,
                };
                targets.push(ActionRetryTarget {
                    scope_key,
                    level: Level::Flatten,
                    generation,
                    invalid_level: Some(invalid_level),
                });
            }
        }
    }
    targets.sort_by(|left, right| left.scope_key.cmp(&right.scope_key));
    Ok(targets)
}

#[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
pub(crate) async fn scopes_needing_action_retry(
    store: &Store,
) -> Result<Vec<(String, Level)>, sqlx::Error> {
    Ok(action_retry_targets(store)
        .await?
        .into_iter()
        .map(|target| (target.scope_key, target.level))
        .collect())
}

/// 动作重试的**退避下限与上限**（毫秒）。
///
/// 循环本身仍按 1 Hz 打点，好让**刚闩上**的 scope 一秒内就被处置——那是安全属性，
/// 不能动。但一个**反复失败**的 scope 不该继续按这个速率付账：未收敛的 `flatten`
/// 每次尝试要跑一整轮真值对账（按 `dd99c36` 复算为 175 权重＝openOrders 40 ＋
/// openAlgoOrders 40 ＋ positionRisk 5 ＋ balance 5 ＋ 撤单/平仓调用），
/// 1 Hz 下单个卡住的 scope 就要 10500 权重/分钟，而 Binance 公布的 per-IP 额度是
/// 2400/分钟。**退避是"重试"与"自我 DoS"之间的全部差别。**
const ACTION_RETRY_BACKOFF_BASE_MS: u64 = 1_000;
const ACTION_RETRY_BACKOFF_CAP_MS: u64 = 60_000;

/// 连续失败 `consecutive_failures` 次之后，下一次尝试要等多久。
///
/// 抽成纯函数是为了让**上限**本身可被直接钉住：没有上限的指数退避在长时间不收敛时
/// 会退成小时级，等于悄悄放弃了 L3；而没有退避则是上面那条自我 DoS。两端都是缺陷，
/// 用例必须能各自观察。第 0 次失败（也就是还没失败过）返回 0——立即到期。
pub(crate) fn action_retry_backoff_ms(consecutive_failures: u32) -> u64 {
    if consecutive_failures == 0 {
        return 0;
    }
    ACTION_RETRY_BACKOFF_BASE_MS
        .saturating_mul(
            1_u64
                .checked_shl(consecutive_failures.saturating_sub(1))
                .unwrap_or(u64::MAX),
        )
        .min(ACTION_RETRY_BACKOFF_CAP_MS)
}

#[derive(Clone, Copy, Debug, Default)]
struct ScopeBackoff {
    generation: i64,
    consecutive_failures: u32,
    next_due_ms: i64,
}

/// 每个 scope 的重试节奏。
///
/// 抽成可直接构造的类型（而不是留在 `spawn_action_retry_task` 的闭包里）是为了让
/// 五条不变量各自可被观察：①**新 scope 立即到期**（闩上就动作）；②失败后按退避跳过；
/// ③**成功即清零**（收敛后恢复 1 Hz 响应力）；④**离开扫描集合的 scope 不留状态**，
/// 于是 reset 之后重新闩上会立刻动作、而不是继承上一轮的退避；⑤**同一 scope 的
/// 新一代 arm 立即到期**——它从未离开扫描集合，但那是新的裸敞口，不是旧的失败。
#[derive(Debug, Default)]
pub(crate) struct ActionRetryPacer {
    scopes: HashMap<String, ScopeBackoff>,
}

impl ActionRetryPacer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 本轮是否该处置这个 scope。没有记录 = 从未失败过 = 立即到期；
    /// 记录属于**另一代** = 那是旧敞口的退避，与这一代无关 = 立即到期。
    pub(crate) fn is_due(&self, scope_key: &str, generation: i64, now_ms: i64) -> bool {
        match self.scopes.get(scope_key) {
            None => true,
            Some(state) if state.generation != generation => true,
            Some(state) => now_ms >= state.next_due_ms,
        }
    }

    /// 记录一次尝试的结果并算出下次到期时间。换代即从第一次失败重新数。
    pub(crate) fn record(
        &mut self,
        scope_key: &str,
        generation: i64,
        succeeded: bool,
        now_ms: i64,
    ) {
        if succeeded {
            self.scopes.remove(scope_key);
            return;
        }
        let state = self.scopes.entry(scope_key.to_string()).or_default();
        if state.generation != generation {
            *state = ScopeBackoff {
                generation,
                ..ScopeBackoff::default()
            };
        }
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        let backoff_ms = action_retry_backoff_ms(state.consecutive_failures);
        // 抖动按 scope_key 派生，确定且无需时钟源：多个 scope 同时卡住时不会
        // 在同一毫秒一起重入，把退避省下来的预算又以尖峰形式还回去。
        let jitter_ms =
            crate::rate_limiter::deterministic_jitter_ms(scope_key, state.consecutive_failures);
        let delay_ms = backoff_ms
            .saturating_add(jitter_ms)
            .min(ACTION_RETRY_BACKOFF_CAP_MS);
        state.next_due_ms = now_ms.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX));
    }

    /// 丢弃已不在扫描集合里的 scope 的退避状态。
    pub(crate) fn retain_only(&mut self, live_scope_keys: &HashSet<String>) {
        self.scopes
            .retain(|scope_key, _| live_scope_keys.contains(scope_key));
    }
}

pub async fn reset(
    store: &Store,
    scope_key: &str,
    reset_by: &str,
    request_ref: Option<&str>,
    now_ms: i64,
) -> anyhow::Result<()> {
    if scope(scope_key).is_none() {
        return Err(anyhow!("invalid scope_key"));
    }
    if reset_by.trim().is_empty() {
        return Err(anyhow!("reset_by is required"));
    }
    // P4b · 平台路径的 request_ref 可选，但给了就必须是 canonical ULID。
    if let Some(reference) = request_ref {
        if !is_canonical_request_ref(reference) {
            return Err(anyhow!(
                "request_ref must be a canonical ULID when provided"
            ));
        }
    }
    let mut tx = store.pool().begin().await?;
    let row = sqlx::query(
        r#"
        SELECT level, CAST(armed_at_ms AS CHAR) AS armed_at_ms_text,
               COALESCE(last_reconcile_clean, -1) AS last_reconcile_clean,
               COALESCE(CAST(last_reconcile_at_ms AS CHAR), '') AS last_reconcile_at_ms_text,
               COALESCE(flatten_state, '') AS flatten_state,
               flatten_generation
        FROM kill_switch_state WHERE scope_key = ?
        "#,
    )
    .bind(scope_key)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| anyhow!("kill switch scope not found"))?;
    let raw_level = row.get::<String, _>("level");
    let level = parse_persisted_level(&raw_level).ok_or_else(|| {
        anyhow::Error::new(InvalidPersistedLevel {
            scope_key: scope_key.to_string(),
            raw_level,
        })
    })?;
    let generation = row.get::<i64, _>("flatten_generation");
    let flatten_state = row.get::<String, _>("flatten_state");
    if level == Level::None {
        return Err(anyhow!("kill switch is not armed"));
    }
    let armed_at = row
        .get::<String, _>("armed_at_ms_text")
        .parse::<i64>()
        .unwrap_or_default();
    let reconcile_clean = row.get::<i64, _>("last_reconcile_clean") == 1;
    let reconcile_at = row
        .get::<String, _>("last_reconcile_at_ms_text")
        .parse::<i64>()
        .ok()
        .unwrap_or_default();
    if !reconcile_clean || reconcile_at < armed_at {
        return Err(anyhow!(
            "reset rejected: latest reconcile after arm must have zero diff"
        ));
    }
    // **clean 真值也要绑当前 revision。**
    //
    // 上面那两条判据读的是 `kill_switch_state` 上的**投影**：`last_reconcile_clean`
    // 是一个布尔，`reconcile_at >= armed_at` 是一次**墙钟**比较。两条都不带
    // revision，而墙钟会回拨——一次早于当前 arm 的对账，只要它的时间戳恰好
    // 不小于 `armed_at_ms`，就能替这一轮背书。
    //
    // 这里回到**逐 credential 的真值行**本身，并且只数**本 revision** 的：
    // 判据与 `record_reconcile_for_scope` 的聚合逐字同源（`total > 0` 且
    // `dirty = 0`），因此"reset 认为已经 clean"与"对账认为已经 clean"不可能
    // 各说各话。本 revision 一行都没有 = 这一轮从未被对账过，fail-closed。
    // **reset 的过滤与聚合
    // 用同一个 expected predicate。**
    //
    // R19 这里是「本代**全部**行」的两条计数（`COUNT(*)` / `COUNT(clean=0)`），
    // 而 `record_reconcile_for_scope` 的聚合只数 expected 成员的行。两份判据
    // 因此能各说各话：一条聚合明确排除掉的行——`legacy_isolated`、或任何一条
    // 本 revision 从未认下的 outsider dirty 行——会让 `dirty_rows > 0` 恒成立，
    // 把一个 latch 已经写成 `flattened`、已经退出重试集合的 scope **永久**
    // 钉在"reset 拒绝"上。运行态说"平完了"，人工却清不掉它。
    //
    // 现在两条路径读同一个 `evaluate_expected_convergence`：非 expected 的行
    // 一票不算（赞成票不算，反对票也不算），来源证不出来的行既不投票也不
    // 阻塞，成员少报一个仍然不许清。
    let convergence = evaluate_expected_convergence(&mut tx, scope_key, generation).await?;
    if !convergence.quarantined.is_empty() {
        tracing::warn!(
            %scope_key,
            generation,
            quarantined = convergence.quarantined.join(","),
            "this kill switch revision carries reconcile status rows whose origin cannot be \
             proven; they do not block this reset"
        );
    }
    if !convergence.clean() {
        return Err(anyhow!(
            "reset rejected: revision {generation} expects {} target credentials but only {} \
             of them left reconcile rows for it ({} still report a diff, {} never reported); \
             only a fully reconciled revision may be cleared",
            convergence.expected.len(),
            convergence.total,
            convergence.dirty,
            convergence.unreported.len()
        ));
    }
    // **L3 的 reset 要查
    // flatten 真值，不是只查 `reconcile_clean`。**
    //
    // `docs/SAFETY_MODEL.md` 把 L3 的完成定义写成「强制 reconcile 同时证明
    // **零仓位、零普通挂单、零 Algo 挂单与本地零差异**」——那正是
    // `ReconcileTruth::flatten_complete()`。R19 的 reset 只要求
    // `reconcile_clean`（`!has_diff && local_position_zero_diff`）：一份
    // **clean 但仓位还在**的真值（本地与交易所一致地都还持着仓）满足它。
    // 于是人工可以把一条**一次都没平掉**的 flatten latch 直接清成 `none`,
    // 敞口连同它的重试任务一起消失（engine safety contract「运行状态丢失」）。
    //
    // 两条一起查：本代每一个 expected 成员都 `flatten_complete=1`，
    // **并且** state 上的 latch 真的写着 `flattened`（那一位是聚合按同一份
    // 行算出来、随事务落库的，两者不一致本身就是代际错位）。
    if level == Level::Flatten
        && (!convergence.flatten_complete() || flatten_state != FLATTEN_CONVERGED)
    {
        return Err(anyhow!(
            "reset rejected: revision {generation} is latched at flatten but {} of its {} \
             reported targets have not proved a completed flatten (flatten_state={}); a clean \
             reconcile alone does not meet the L3 completion definition",
            convergence.incomplete,
            convergence.total,
            if flatten_state.is_empty() {
                "<unset>"
            } else {
                flatten_state.as_str()
            }
        ));
    }

    // **reset 也必须绑代
    // 并消费命中数。**
    //
    // R15 这里是「无条件 `UPDATE kill_switch_state`」+「按 `armed_at_ms DESC`
    // 取一条未 reset 的事件」，一个代际条件、一个 `rows_affected()` 都没有：
    //
    // * `armed_at_ms` 是**墙钟**。时钟回拨之后新一轮 latch 的 `armed_at_ms`
    //   可以**小于**上一轮遗留的那条未 reset 事件，`DESC` 于是选中**旧**事件
    //   ——旧事件被标成 reset，而当前这一轮的事件仍然挂着"未 reset"，
    //   state 却已经被清成 `none`。审计面与运行态从此各说各话。
    // * 读状态与写状态之间插进来一次 `latch_flatten_pending`（代际 +1、
    //   `flatten_state='flattening'`），无谓词的 UPDATE 会把**新一轮**的 latch
    //   直接清掉：那个敞口一次都没被平过，而 reset 校验的是**上一轮**的
    //   对账真值。
    //
    // 现在两条写入都绑当前代际并检查命中数，任一为 0 就整个事务回滚：
    // 「不知道自己写没写成功」不得宣称 reset 成功（engine safety contract）。
    let state_rows = sqlx::query(
        r#"
        UPDATE kill_switch_state
        SET level='none', reason='manual reset', trigger_source='manual_reset',
            armed_at_ms=?, flatten_state=NULL, updated_at_ms=?,
            authority_kind=0, authority_member_id=NULL
        WHERE scope_key=? AND level <> 'none' AND flatten_generation=?
        "#,
    )
    .bind(now_ms.to_string())
    .bind(now_ms.to_string())
    .bind(scope_key)
    .bind(generation)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if state_rows == 0 {
        tx.rollback().await?;
        return Err(anyhow!(
            "reset rejected: the kill switch state moved between validating it and clearing it"
        ));
    }
    let active_event = sqlx::query(
        r#"
        SELECT event_id FROM kill_switch_events
        WHERE scope_key=? AND reset_at_ms IS NULL AND flatten_generation=?
        ORDER BY armed_at_ms DESC LIMIT 1
        "#,
    )
    .bind(scope_key)
    .bind(generation)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .next();
    let Some(active_event) = active_event else {
        // armed 的 scope 必然有一条本代事件（`arm_in_tx` 在同一个事务里写
        // state 与事件行）。没有 = 审计链断了，清掉 latch 会留下一条无从
        // 追溯的处置。启动期的
        // `revalidate_legacy_event_generations`（迁移 0027）负责把这种
        // 存量错位收敛回来；在那之前 reset 一律 fail-closed。
        tx.rollback().await?;
        return Err(anyhow!(
            "reset rejected: this scope has no active kill switch event at generation \
             {generation}; clearing it would leave the reset unauditable"
        ));
    };
    let event_rows = sqlx::query(
        "UPDATE kill_switch_events \
         SET reset_by=?, reset_at_ms=?, updated_at_ms=?, reset_request_ref=? \
         WHERE event_id=? AND reset_at_ms IS NULL AND flatten_generation=? \
         AND reset_request_ref IS NULL",
    )
    .bind(reset_by)
    .bind(now_ms.to_string())
    .bind(now_ms.to_string())
    .bind(request_ref)
    .bind(active_event.get::<String, _>("event_id"))
    .bind(generation)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if event_rows == 0 {
        tx.rollback().await?;
        return Err(anyhow!(
            "reset rejected: the active kill switch event moved between selecting it and \
             marking it reset"
        ));
    }
    tx.commit().await?;
    Ok(())
}

// ============================================================================
// P4b · 会员解除（增险链）、准入门、authority 栅栏与只读投影（#35/#36）
// ============================================================================

/// 会员 reset 的结果（供 svc 层映射 proto/HTTP）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberResetOutcome {
    /// COMMIT 完成：已执行证明 = 提交后的事件行（含 reset_request_ref）。
    Executed { generation: i64 },
    /// 引擎业务拒绝（未闩 / 收敛前置 / 归属谓词失配等），零写或已回滚。
    Rejected(String),
    /// 准入门未对本 boot 开启（FA C1.5 ①的锁内读门）。
    NotAdmitted,
}

/// **P4b · 增险链的引擎终局（r2 §1.2 / FA2 §1.4）。**
///
/// 三段锁序固定为 `member_reset_admission(id=1) → kill_switch_state(scope_key)
/// → kill_switch_events`，与 `admit_member_reset` 的 post_rollback 降级、
/// 启动失效共用同一顺序（无环、有界）。**线性化点是 COMMIT**：最终
/// UPDATE 命中只是锁内条件成立，持久生效在提交——已执行证明一律取提交后
/// 的事件行，不用 CAS 日志、不用 UPDATE 命中冒充。
///
/// 与平台 `reset` 的差异全部来自冻结要点：D1（仅本人 L1 `pause_new`）、
/// 授权代际 g（挑战签发时的快照，贯穿转发，禁止重读替换）、准入门锁内
/// 重读（撤销与在途 reset 的竞争由同一行锁串行化——已过 handler 入口的
/// 旧请求在重读处阻塞，不符即回滚）。
pub async fn reset_member(
    store: &Store,
    scope_key: &str,
    member_id: i64,
    expected_generation: i64,
    request_ref: &str,
    now_ms: i64,
) -> anyhow::Result<MemberResetOutcome> {
    if scope(scope_key).is_none() {
        return Ok(MemberResetOutcome::Rejected("invalid scope_key".into()));
    }
    if member_id <= 0 {
        return Ok(MemberResetOutcome::Rejected(
            "authority_member_id is required".into(),
        ));
    }
    if expected_generation <= 0 {
        return Ok(MemberResetOutcome::Rejected(
            "expected_authority_revision must be positive for a member reset".into(),
        ));
    }
    if !is_canonical_request_ref(request_ref) {
        return Ok(MemberResetOutcome::Rejected(
            "request_ref must be a canonical ULID".into(),
        ));
    }
    let mut tx = store.pool().begin().await?;

    // ① 锁内读门（FA C1.5 / FA2 §1.4 主方案——唯一的 reset 检查形态，
    // 「等价条件形式备选」已被裁决删除，不得实现）。
    let admission_sql = match store.kind() {
        DbKind::MySql => {
            "SELECT enabled, admitted_boot_id FROM member_reset_admission \
             WHERE id = 1 FOR UPDATE"
        }
        _ => "SELECT enabled, admitted_boot_id FROM member_reset_admission WHERE id = 1",
    };
    let admission = sqlx::query(admission_sql)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| {
            tracing::error!(
                %error,
                "member reset could not read the admission gate; failing closed"
            );
            error
        })?;
    let admitted = admission.is_some_and(|row| {
        row.get::<i64, _>("enabled") == 1
            && crate::store::try_get_text(&row, "admitted_boot_id")
                .ok()
                .flatten()
                .is_some_and(|boot| boot == store.boot_id())
    });
    if !admitted {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::NotAdmitted);
    }
    // A3/C1-e 测试缝：准入门已过、行锁仍在手上时暂停，允许测试在并发连接上
    // 执行撤销（新 boot 推号）后再继续——验证锁内串行化语义。生产路径无操作。
    #[cfg(test)]
    if let Some(pause) = crate::kill_switch::test_admission_pause_hook() {
        pause.await;
    }

    // ② 授权代际预检 + 既有收敛前置（D1：仅 L1；L3 另有同代全员
    // flatten_complete 判据，本函数按 level 分流复用既有判据）。
    let row = sqlx::query(
        r#"
        SELECT level, CAST(armed_at_ms AS CHAR) AS armed_at_ms_text,
               COALESCE(last_reconcile_clean, -1) AS last_reconcile_clean,
               COALESCE(CAST(last_reconcile_at_ms AS CHAR), '') AS last_reconcile_at_ms_text,
               COALESCE(flatten_state, '') AS flatten_state,
               flatten_generation, authority_kind, authority_member_id
        FROM kill_switch_state WHERE scope_key = ?
        "#,
    )
    .bind(scope_key)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .next();
    let Some(row) = row else {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(
            "kill switch scope not found".into(),
        ));
    };
    let raw_level = row.get::<String, _>("level");
    let level = match parse_persisted_level(&raw_level) {
        Some(level) => level,
        None => {
            tx.rollback().await?;
            return Ok(MemberResetOutcome::Rejected(format!(
                "invalid persisted level for {scope_key}"
            )));
        }
    };
    let generation = row.get::<i64, _>("flatten_generation");
    if generation != expected_generation {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(format!(
            "authority revision moved since authorization: expected {expected_generation}, \
             engine is at {generation}; re-issue against the current revision"
        )));
    }
    if level == Level::None {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(
            "kill switch is not armed".into(),
        ));
    }
    // D1：会员自解除仅 L1。引擎侧硬编码——控制面旁路到 L2/L3 的 reset 在
    // 这里仍然被拒（变异 M1 的击杀点之一）。
    if level != Level::PauseNew {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(
            "member reset only clears a self-armed pause_new latch; higher levels and \
             non-member ownership go through platform review"
                .into(),
        ));
    }
    let armed_at = row
        .get::<String, _>("armed_at_ms_text")
        .parse::<i64>()
        .unwrap_or_default();
    let reconcile_clean = row.get::<i64, _>("last_reconcile_clean") == 1;
    let reconcile_at = row
        .get::<String, _>("last_reconcile_at_ms_text")
        .parse::<i64>()
        .ok()
        .unwrap_or_default();
    if !reconcile_clean || reconcile_at < armed_at {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(
            "reset rejected: latest reconcile after arm must have zero diff".into(),
        ));
    }
    let convergence = evaluate_expected_convergence(&mut tx, scope_key, generation).await?;
    if !convergence.clean() {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(format!(
            "reset rejected: revision {generation} expects {} target credentials but only {} \
             of them left reconcile rows for it ({} still report a diff, {} never reported)",
            convergence.expected.len(),
            convergence.total,
            convergence.dirty,
            convergence.unreported.len()
        )));
    }

    // ③ 最终谓词 UPDATE（r1 §1.2 + 归属两列；UPDATE 命中 ≠ 持久生效）。
    let state_rows = sqlx::query(
        r#"
        UPDATE kill_switch_state
        SET level='none', reason='member reset', trigger_source='manual_reset',
            armed_at_ms=?, flatten_state=NULL, updated_at_ms=?,
            authority_kind=0, authority_member_id=NULL
        WHERE scope_key=? AND level <> 'none' AND flatten_generation=?
          AND level='pause_new' AND authority_kind=? AND authority_member_id=?
        "#,
    )
    .bind(now_ms.to_string())
    .bind(now_ms.to_string())
    .bind(scope_key)
    .bind(generation)
    .bind(AUTHORITY_KIND_MEMBER as i64)
    .bind(member_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if state_rows == 0 {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(
            "reset rejected: the latch is not the member-owned pause_new revision this \
             authorization was issued against (takeover, level change, or generation moved)"
                .into(),
        ));
    }
    let active_event = sqlx::query(
        r#"
        SELECT event_id FROM kill_switch_events
        WHERE scope_key=? AND reset_at_ms IS NULL AND flatten_generation=?
        ORDER BY armed_at_ms DESC LIMIT 1
        "#,
    )
    .bind(scope_key)
    .bind(generation)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .next();
    let Some(active_event) = active_event else {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(format!(
            "reset rejected: no active kill switch event at generation {generation}"
        )));
    };
    let event_rows = sqlx::query(
        "UPDATE kill_switch_events \
         SET reset_by=?, reset_at_ms=?, updated_at_ms=?, reset_request_ref=? \
         WHERE event_id=? AND reset_at_ms IS NULL AND flatten_generation=? \
         AND reset_request_ref IS NULL",
    )
    .bind(format!("member:{member_id}"))
    .bind(now_ms.to_string())
    .bind(now_ms.to_string())
    .bind(request_ref)
    .bind(active_event.get::<String, _>("event_id"))
    .bind(generation)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if event_rows == 0 {
        tx.rollback().await?;
        return Ok(MemberResetOutcome::Rejected(
            "reset rejected: the active kill switch event moved between selecting it and \
             marking it reset"
                .into(),
        ));
    }
    tx.commit().await?;
    Ok(MemberResetOutcome::Executed { generation })
}

/// `AdmitMemberReset` 的结果（svc 层映射 proto）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitMemberResetOutcome {
    pub admitted: bool,
    pub admitted_boot_id: String,
    pub admission_version_after: i64,
    pub demoted_scopes: i64,
    pub message: String,
}

/// **P4b · 准入动作（FA C1.3/FA2 §1.1–1.2）。**
///
/// 期望值绑定：`expected_boot_id`/`expected_admission_version` 是运维核验时
/// 读取的引擎事实（平台链经 `get_kill_switch_states` 的
/// `engine_boot_id`/`admission_version` 字段），在 admission 行锁内与
/// 本进程 boot 及持久版本**双比对**；形状不符 INVALID（零写）、任一不符
/// FAILED_PRECONDITION（零写）。**任何一环都不得把 expected 值改写成最新
/// 值**——刷新事实 = 重新核验 + 新的批准意图。成功同事务完成
/// enabled=1、盖章、推号、history；成功后重放因版本已变而拒，且不重复
/// 推号、不重做修复（幂等拒绝）。
pub async fn admit_member_reset(
    store: &Store,
    basis: &str,
    admitted_by: &str,
    demote_unknown_member_authority: bool,
    expected_boot_id: &str,
    expected_admission_version: Option<i64>,
    now_ms: i64,
) -> anyhow::Result<Result<AdmitMemberResetOutcome, String>> {
    // 形状校验（INVALID 类，零写）。
    if basis.is_empty() || basis.len() > 200 {
        return Ok(Err("basis is required and at most 200 characters".into()));
    }
    let post_rollback = if let Some(rest) = basis.strip_prefix("post_rollback_repair:") {
        if !demote_unknown_member_authority {
            return Ok(Err(
                "post_rollback_repair: basis requires demote_unknown_member_authority".into(),
            ));
        }
        let _ = rest;
        true
    } else if let Some(rest) = basis.strip_prefix("clean_attestation:") {
        if demote_unknown_member_authority {
            return Ok(Err(
                "clean_attestation: basis must not demote member authority".into(),
            ));
        }
        let _ = rest;
        false
    } else {
        return Ok(Err(
            "basis must start with clean_attestation: or post_rollback_repair:".into(),
        ));
    };
    if admitted_by.is_empty() || admitted_by.len() > 128 {
        return Ok(Err(
            "admitted_by is required and at most 128 characters".into()
        ));
    }
    if !is_canonical_request_ref(expected_boot_id) {
        return Ok(Err("expected_boot_id must be a canonical ULID".into()));
    }
    let Some(expected_version) = expected_admission_version else {
        // presence 必填：0 是合法初值，缺席不等于 0（FA2 §1.1）。
        return Ok(Err(
            "expected_admission_version is required (presence); zero is a legal initial \
             value, absence is not"
                .into(),
        ));
    };
    if expected_version < 0 {
        return Ok(Err("expected_admission_version must not be negative".into()));
    }

    let mut tx = store.pool().begin().await?;
    // 锁序：admission(id=1) 先于 kill_switch_state（与 reset_member 一致）。
    let admission_sql = match store.kind() {
        DbKind::MySql => {
            "SELECT enabled, admitted_boot_id, admission_version              FROM member_reset_admission WHERE id = 1 FOR UPDATE"
        }
        _ => {
            "SELECT enabled, admitted_boot_id, admission_version              FROM member_reset_admission WHERE id = 1"
        }
    };
    let Some(row) = sqlx::query(admission_sql).fetch_optional(&mut *tx).await? else {
        tx.rollback().await?;
        return Ok(Err(
            "member_reset_admission singleton row is missing; admission is impossible \
             until the anomaly is investigated"
                .into(),
        ));
    };
    let _admitted_boot = crate::store::try_get_text(&row, "admitted_boot_id")?.unwrap_or_default();
    let current_version = row.get::<i64, _>("admission_version");
    // 双比对：boot 与版本任一不符即 FAILED_PRECONDITION，零写。这就是
    // C1-a..d 的落点：A 周期的迟到 Q 在 B 周期 boot 不符；同 boot 推号后的
    // 旧版本 Q 版本不符；成功后的重放版本已 +1 不符（且不再推号）。
    if expected_boot_id != store.boot_id() {
        tx.rollback().await?;
        return Ok(Err(
            "expected_boot_id does not match the running boot cycle; an admission intent \
             is bound to the boot it was verified against and is never rebound to the \
             latest value"
                .to_string(),
        ));
    }
    if expected_version != current_version {
        tx.rollback().await?;
        return Ok(Err(format!(
            "expected_admission_version {expected_version} does not match the persisted \
             admission_version {current_version}; re-verify and issue a fresh intent"
        )));
    }

    let mut demoted_scopes = 0i64;
    if post_rollback {
        // post_rollback_repair：同一事务内把全部 (kind=3) 的归属降为
        // (0,NULL)——只动归属列（state 行 + 对应 active 事件行的 authority
        // 列，保持栅栏安静）；level/generation/事件本体/保护义务一字不动。
        // 这是启动修复语义，不做任何交易性 arm/reset（D10）。
        demoted_scopes = sqlx::query(
            "UPDATE kill_switch_state \
             SET authority_kind=0, authority_member_id=NULL, updated_at_ms=updated_at_ms \
             WHERE authority_kind=?",
        )
        .bind(AUTHORITY_KIND_MEMBER as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected() as i64;
        sqlx::query(
            "UPDATE kill_switch_events \
             SET authority_kind=0, authority_member_id=NULL, updated_at_ms=updated_at_ms \
             WHERE authority_kind=?",
        )
        .bind(AUTHORITY_KIND_MEMBER as i64)
        .execute(&mut *tx)
        .await?;
    }

    let stamped = sqlx::query(
        "UPDATE member_reset_admission \
         SET enabled=1, admitted_boot_id=?, admission_version=admission_version+1, \
             basis=?, admitted_at_ms=?, admitted_by=? \
         WHERE id=1 AND admission_version=?",
    )
    .bind(store.boot_id())
    .bind(basis)
    .bind(now_ms)
    .bind(admitted_by)
    .bind(current_version)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if stamped == 0 {
        tx.rollback().await?;
        return Ok(Err(
            "admission gate moved between verifying and stamping; nothing was admitted".into(),
        ));
    }
    let version_after = current_version + 1;
    sqlx::query(
        "INSERT INTO member_reset_admission_history \
         (event_id, boot_id, action, basis, at_ms, actor) \
         VALUES (?, ?, 'admitted', ?, ?, ?)",
    )
    .bind(ulid::Ulid::new().to_string())
    .bind(store.boot_id())
    .bind(basis)
    .bind(now_ms)
    .bind(admitted_by)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::warn!(
        boot_id = %store.boot_id(),
        basis,
        demoted_scopes,
        "member reset admission granted for the current boot cycle only"
    );
    Ok(Ok(AdmitMemberResetOutcome {
        admitted: true,
        admitted_boot_id: store.boot_id().to_string(),
        admission_version_after: version_after,
        demoted_scopes,
        message: "admitted for the current boot cycle".into(),
    }))
}

/// **P4b · authority 一致性栅栏（FA C1.2 第 4 步；store::connect 调用）。**
///
/// 对每个已闩 scope：state 行归属 vs 当前代 active 事件行归属。arm/reset/
/// 接管都在同一事务双写两侧，干净库恒一致；不一致 = 旧写者或损坏的痕迹
/// ——把该 scope 的 state 归属降为 (0,NULL)（事件侧 authority 一并归零，
/// 避免每次启动都告警），并**再次推号 + 关闭准入门 + 写 revoked_fence**。
/// state/event 相等不能证明旧程序没走过零写早返回（C1-g 的诚实边界）：
/// 资格保留只信「可信版本独占 + attestation」（准入动作），本栅栏只是
/// 完整性检查与自动关门的触发器。
pub(crate) async fn revalidate_authority_consistency(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<usize> {
    let armed = sqlx::query(
        "SELECT scope_key, flatten_generation, authority_kind, authority_member_id \
         FROM kill_switch_state WHERE level <> 'none' ORDER BY scope_key",
    )
    .fetch_all(store.pool())
    .await
    .context("load armed kill-switch scopes for the authority fence")?;
    let mut demoted = 0usize;
    for row in &armed {
        let scope_key = row.get::<String, _>("scope_key");
        let generation = row.get::<i64, _>("flatten_generation");
        let state_kind = row.get::<i64, _>("authority_kind");
        let state_member: Option<i64> = row.try_get::<i64, _>("authority_member_id").ok();
        let event = sqlx::query(
            "SELECT authority_kind, authority_member_id FROM kill_switch_events \
             WHERE scope_key=? AND reset_at_ms IS NULL AND flatten_generation=? \
             ORDER BY armed_at_ms DESC LIMIT 1",
        )
        .bind(&scope_key)
        .bind(generation)
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .next();
        let consistent = event.is_some_and(|event| {
            event.get::<i64, _>("authority_kind") == state_kind
                && event.try_get::<i64, _>("authority_member_id").ok() == state_member
        });
        if consistent {
            continue;
        }
        tracing::error!(
            %scope_key,
            generation,
            error_code = "AUTHORITY_PROVENANCE_UNPROVEN",
            "state and active-event authority disagree for an armed scope; demoting the \
             state authority to unknown and closing member-reset admission"
        );
        let mut tx = store.pool().begin().await?;
        sqlx::query(
            "UPDATE kill_switch_state \
             SET authority_kind=0, authority_member_id=NULL, updated_at_ms=updated_at_ms \
             WHERE scope_key=? AND flatten_generation=?",
        )
        .bind(&scope_key)
        .bind(generation)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE kill_switch_events \
             SET authority_kind=0, authority_member_id=NULL, updated_at_ms=updated_at_ms \
             WHERE scope_key=? AND flatten_generation=? AND reset_at_ms IS NULL",
        )
        .bind(&scope_key)
        .bind(generation)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE member_reset_admission \
             SET enabled=0, admission_version=admission_version+1 WHERE id=1",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO member_reset_admission_history \
             (event_id, boot_id, action, basis, at_ms, actor) \
             VALUES (?, ?, 'revoked_fence', ?, ?, 'system:fence')",
        )
        .bind(ulid::Ulid::new().to_string())
        .bind(store.boot_id())
        .bind(format!("authority_mismatch:{scope_key}@{generation}"))
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        demoted += 1;
    }
    Ok(demoted)
}

/// 只读投影的行类型（svc 层映射 proto；本模块不依赖 proto）。
#[derive(Debug, Clone, Default)]
pub struct KillSwitchStateRead {
    pub scope_key: String,
    pub found: bool,
    pub level: String,
    pub raw_level: String,
    pub level_canonical: bool,
    pub trigger_source: String,
    pub authority_kind: i32,
    pub authority_member_id: Option<i64>,
    pub flatten_generation: i64,
    pub flatten_state: String,
    pub last_reconcile_clean: Option<bool>,
    pub last_reconcile_at_ms: Option<i64>,
    pub armed_at_ms: i64,
    pub convergence: Vec<ConvergenceReadRow>,
    pub scope_convergence: Option<ScopeConvergenceRead>,
}

#[derive(Debug, Clone)]
pub struct ConvergenceReadRow {
    pub credential_id: String,
    pub expected_provenance: String,
    pub reconcile_clean: Option<bool>,
    pub flatten_complete: Option<bool>,
    pub truth_positions_zero: Option<bool>,
    pub truth_orders_empty: Option<bool>,
    pub truth_algo_orders_empty: Option<bool>,
    pub truth_local_zero_diff: Option<bool>,
    pub truth_no_diff: Option<bool>,
    pub reconciled_at_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct ScopeConvergenceRead {
    pub expected: Vec<String>,
    pub reported: Vec<String>,
    pub unreported: Vec<String>,
    pub quarantined_expected: Vec<String>,
    pub quarantined_outsider: Vec<String>,
    pub clean: bool,
    pub flatten_complete: bool,
    pub expected_total: i64,
    pub dirty: i64,
    pub incomplete: i64,
}

#[derive(Debug, Clone)]
pub struct RequestRefHitRead {
    pub request_ref: String,
    pub scope_key: String,
    pub flatten_generation: i64,
    pub kind: String,
    pub trigger_source: String,
    pub at_ms: i64,
    pub arm_actor_kind: i32,
    pub arm_actor_member_id: Option<i64>,
    pub reset_committed: bool,
    pub current_authority_kind: i32,
    pub current_authority_member_id: Option<i64>,
}

#[derive(Debug)]
pub struct ReadStatesOutcome {
    pub rows: Vec<KillSwitchStateRead>,
    pub ref_hits: Vec<RequestRefHitRead>,
    pub snapshot_stale: bool,
    pub admission_version: i64,
}

/// 读侧 TriState：`None` = KSTRI_UNKNOWN（legacy/未投影），永不被压成 false。
fn opt_bool_to_state(value: Option<bool>) -> i32 {
    match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }
}

/// **P4b · 只读投影（r2 §4，零写副作用）。**
///
/// 单事务读：全部行 + 同源收敛判定（`evaluate_expected_convergence`——
/// 控制面只渲染不重算）+ ref 命中（闭世界校验；MySQL 用 BINARY 比较）+ 首
/// 尾代际比对（`snapshot_stale`）。上界：scopes ≤ 256、refs ≤ 64（超界
/// 整体拒绝——上界语义见 r2 §4；这些是**读侧**上界，不门控任何写路径）。
pub async fn get_kill_switch_states(
    store: &Store,
    scope_keys: &[String],
    request_refs: &[String],
) -> anyhow::Result<ReadStatesOutcome> {
    if scope_keys.is_empty() {
        return Err(anyhow!("scope_keys must not be empty"));
    }
    if scope_keys.len() > 256 {
        return Err(anyhow!("scope_keys exceeds the 256-scope read ceiling"));
    }
    if request_refs.len() > 64 {
        return Err(anyhow!("request_refs exceeds the 64-ref read ceiling"));
    }
    for key in scope_keys {
        if scope(key).is_none() {
            return Err(anyhow!(
                "scope_key {key} is not in the engine's closed scope set"
            ));
        }
    }
    for reference in request_refs {
        if !is_canonical_request_ref(reference) {
            return Err(anyhow!("request_ref {reference} is not a canonical ULID"));
        }
    }
    let mut tx = store.pool().begin().await?;
    // 代际快照必须走**同一事务**（SQLite 单连接：tx 持有期间另开 pool 查询
    // 会死锁到 pool 超时；MySQL 侧也保证首尾快照与行读取一致）。
    let observed_start = generations_in_tx(&mut tx).await?;
    let mut rows = Vec::with_capacity(scope_keys.len());
    for key in scope_keys {
        let state_row = sqlx::query(
            "SELECT scope_key, level, COALESCE(trigger_source,'') AS trigger_source, \
             CAST(armed_at_ms AS CHAR) AS armed_at_ms_text, \
             COALESCE(flatten_state,'') AS flatten_state, flatten_generation, \
             authority_kind, authority_member_id, last_reconcile_clean, \
             COALESCE(CAST(last_reconcile_at_ms AS CHAR),'') AS last_reconcile_at_ms_text \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(state_row) = state_row else {
            rows.push(KillSwitchStateRead {
                scope_key: key.clone(),
                found: false,
                ..Default::default()
            });
            continue;
        };
        let raw_level = state_row.get::<String, _>("level");
        let parsed = parse_persisted_level(&raw_level);
        let generation = state_row.get::<i64, _>("flatten_generation");
        let convergence_rows = sqlx::query(
            "SELECT credential_id, reconcile_clean, flatten_complete, \
             COALESCE(CAST(reconciled_at_ms AS CHAR),'') AS reconciled_at_ms_text, \
             COALESCE(expected_provenance,'') AS expected_provenance, \
             truth_positions_zero, truth_orders_empty, truth_algo_orders_empty, \
             truth_local_zero_diff, truth_no_diff \
             FROM kill_switch_reconcile_status \
             WHERE scope_key=? AND flatten_generation=? ORDER BY credential_id",
        )
        .bind(key)
        .bind(generation)
        .fetch_all(&mut *tx)
        .await?;
        let convergence = convergence_rows
            .iter()
            .map(|row| ConvergenceReadRow {
                credential_id: row.get::<String, _>("credential_id"),
                expected_provenance: row.get::<String, _>("expected_provenance"),
                reconcile_clean: row
                    .try_get::<i64, _>("reconcile_clean")
                    .ok()
                    .map(|v| v != 0),
                flatten_complete: row
                    .try_get::<i64, _>("flatten_complete")
                    .ok()
                    .map(|v| v != 0),
                truth_positions_zero: row
                    .try_get::<i64, _>("truth_positions_zero")
                    .ok()
                    .map(|v| v != 0),
                truth_orders_empty: row
                    .try_get::<i64, _>("truth_orders_empty")
                    .ok()
                    .map(|v| v != 0),
                truth_algo_orders_empty: row
                    .try_get::<i64, _>("truth_algo_orders_empty")
                    .ok()
                    .map(|v| v != 0),
                truth_local_zero_diff: row
                    .try_get::<i64, _>("truth_local_zero_diff")
                    .ok()
                    .map(|v| v != 0),
                truth_no_diff: row.try_get::<i64, _>("truth_no_diff").ok().map(|v| v != 0),
                reconciled_at_ms: row
                    .get::<String, _>("reconciled_at_ms_text")
                    .parse::<i64>()
                    .unwrap_or_default(),
            })
            .collect();
        let engine_convergence = evaluate_expected_convergence(&mut tx, key, generation).await?;
        let scope_convergence = ScopeConvergenceRead {
            expected: engine_convergence.expected.iter().cloned().collect(),
            reported: engine_convergence
                .expected
                .difference(
                    &engine_convergence
                        .unreported
                        .iter()
                        .cloned()
                        .collect::<std::collections::BTreeSet<_>>(),
                )
                .cloned()
                .collect(),
            unreported: engine_convergence.unreported.clone(),
            // expected 内的 quarantine 行（零票）与 outsider 行（不投不阻）
            // 分列——r0 把两类混写为"阻塞"，r2 §4 修正。
            quarantined_expected: engine_convergence
                .quarantined
                .iter()
                .filter(|credential| engine_convergence.expected.contains(*credential))
                .cloned()
                .collect(),
            quarantined_outsider: engine_convergence
                .quarantined
                .iter()
                .filter(|credential| !engine_convergence.expected.contains(*credential))
                .cloned()
                .collect(),
            clean: engine_convergence.clean(),
            flatten_complete: engine_convergence.flatten_complete(),
            expected_total: engine_convergence.expected.len() as i64,
            dirty: engine_convergence.dirty as i64,
            incomplete: engine_convergence.incomplete as i64,
        };
        rows.push(KillSwitchStateRead {
            scope_key: key.clone(),
            found: true,
            level: parsed.map(|l| l.as_str().to_string()).unwrap_or_default(),
            raw_level: if parsed.is_some() {
                String::new()
            } else {
                raw_level.clone()
            },
            level_canonical: parsed.is_some(),
            trigger_source: state_row.get::<String, _>("trigger_source"),
            authority_kind: state_row.get::<i64, _>("authority_kind") as i32,
            authority_member_id: state_row.try_get::<i64, _>("authority_member_id").ok(),
            flatten_generation: generation,
            flatten_state: state_row.get::<String, _>("flatten_state"),
            last_reconcile_clean: state_row
                .try_get::<i64, _>("last_reconcile_clean")
                .ok()
                .map(|v| v != 0),
            last_reconcile_at_ms: state_row
                .get::<String, _>("last_reconcile_at_ms_text")
                .trim()
                .parse::<i64>()
                .ok(),
            armed_at_ms: state_row
                .get::<String, _>("armed_at_ms_text")
                .parse::<i64>()
                .unwrap_or_default(),
            convergence,
            scope_convergence: Some(scope_convergence),
        });
    }
    // ref 命中查询：arm 列与 reset 列分开匹配（arm 命中 ≠ reset 成功）；
    // MySQL 用 BINARY 比较（_ci 下大小写变体不得命中——V-14 负向）。
    let mut ref_hits = Vec::new();
    if !request_refs.is_empty() {
        // 逐个 ref 查询（至多 64 个，行数与事件数同阶）。
        for reference in request_refs {
            for (column, kind_label) in [("arm_request_ref", "arm"), ("reset_request_ref", "reset")]
            {
                let sql = match store.kind() {
                    DbKind::MySql => format!(
                        "SELECT scope_key, flatten_generation, trigger_source, \
                         CAST(armed_at_ms AS CHAR) AS armed_ms, \
                         COALESCE(CAST(reset_at_ms AS CHAR),'') AS reset_ms, \
                         arm_actor_kind, arm_actor_member_id, authority_kind, \
                         authority_member_id FROM kill_switch_events \
                         WHERE CAST({column} AS BINARY) = CAST(? AS BINARY) \
                         ORDER BY armed_at_ms DESC LIMIT 8"
                    ),
                    _ => format!(
                        "SELECT scope_key, flatten_generation, trigger_source, \
                         CAST(armed_at_ms AS CHAR) AS armed_ms, \
                         COALESCE(CAST(reset_at_ms AS CHAR),'') AS reset_ms, \
                         arm_actor_kind, arm_actor_member_id, authority_kind, \
                         authority_member_id FROM kill_switch_events \
                         WHERE {column} = ? ORDER BY armed_at_ms DESC LIMIT 8"
                    ),
                };
                let hits = sqlx::query(&sql)
                    .bind(reference)
                    .fetch_all(&mut *tx)
                    .await?;
                for hit in hits {
                    let reset_ms = hit
                        .get::<String, _>("reset_ms")
                        .trim()
                        .parse::<i64>()
                        .unwrap_or_default();
                    ref_hits.push(RequestRefHitRead {
                        request_ref: reference.clone(),
                        scope_key: hit.get::<String, _>("scope_key"),
                        flatten_generation: hit.get::<i64, _>("flatten_generation"),
                        kind: kind_label.to_string(),
                        trigger_source: hit.get::<String, _>("trigger_source"),
                        at_ms: if kind_label == "reset" {
                            reset_ms
                        } else {
                            hit.get::<String, _>("armed_ms")
                                .parse::<i64>()
                                .unwrap_or_default()
                        },
                        arm_actor_kind: hit.get::<i64, _>("arm_actor_kind") as i32,
                        arm_actor_member_id: hit.try_get::<i64, _>("arm_actor_member_id").ok(),
                        // reset 命中须带已提交标记：reset_at_ms 已写 且 命中
                        // 的是 reset_request_ref 列本身（arm 命中恒 false）。
                        reset_committed: kind_label == "reset" && reset_ms > 0,
                        current_authority_kind: hit.get::<i64, _>("authority_kind") as i32,
                        current_authority_member_id: hit
                            .try_get::<i64, _>("authority_member_id")
                            .ok(),
                    });
                }
            }
        }
    }
    let admission_version =
        sqlx::query("SELECT admission_version FROM member_reset_admission WHERE id=1")
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.get::<i64, _>("admission_version"))
            .unwrap_or_default();
    let observed_end = generations_in_tx(&mut tx).await?;
    tx.commit().await?;
    let snapshot_stale = scope_keys
        .iter()
        .any(|key| observed_start.observed_for(key) != observed_end.observed_for(key));
    Ok(ReadStatesOutcome {
        rows,
        ref_hits,
        snapshot_stale,
        admission_version,
    })
}

/// 读侧 TriState 映射（svc 层用）。
pub(crate) fn read_tri_state(value: Option<bool>) -> i32 {
    opt_bool_to_state(value)
}

/// 一条存量事件行的代际复核结论（迁移 0027 的逐行证据）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventGenerationVerdict {
    /// 按 arm 身份证明它就是当前这一轮的事件行 → 收进当前代际。
    Adopted,
    /// 证不出归属（无匹配 / 多条匹配）→ 隔离：代际号原样留在旧一轮。
    Isolated,
    /// 当前代际下补一条锚事件，让这条 latch 重新能被对账收敛。
    Anchored,
    /// ****：一条**停在当前代际**、
    /// 却证不出自己是这一轮所有者的活事件。它与锚事件会争同一个
    /// `(scope_key, generation)` 归属，`reset` 的 `LIMIT 1` 只能选中其中一条、
    /// 把另一条留成"永远未 reset"。这里把它的生命周期就地关掉
    /// （代际号原样保留，行还在，审计可追），让这一轮只剩一个所有者。
    Superseded,
}

impl EventGenerationVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Adopted => "adopted",
            Self::Isolated => "isolated",
            Self::Anchored => "anchored",
            Self::Superseded => "superseded",
        }
    }
}

/// **0026 存量事件代际的可审计复核。**
///
/// 0026 把所有存量事件行置为 `flatten_generation = 0`，并假定
/// `kill_switch_state` 的存量代际也是 0。这个假定只对"0023 之后从未 latch 过"
/// 的库成立——0023 从 R14 起就在跑，任何一次 `latch_flatten_pending` 都把
/// state 的代际推到 N > 0。升级到 0026 之后，这类部署的
/// **活 latch 在当前代际下一条 active 事件都没有**：
/// `record_reconcile_for_scope` 的 `None if has_live_latch` 分支每一次都回滚并
/// 返回 `DiscardedRaced`，latch 永远收敛不了、重试任务永远重复处置。
///
/// 复核在 `Store::connect` 里跑（迁移之后、任何对账/重试任务之前），逐 scope：
///
/// * 当前代际下**已经有** active 事件 → 健康，一行都不动（重跑即空集）；
/// * 没有 → 用 **arm 身份**（`armed_at_ms` + `level` + `trigger_source` +
///   `reason`；`arm_in_tx` 在同一个事务里用同一组值写 state 与事件行）在
///   未 reset 的旧代事件里找归属：
///   * 恰好一条 → `Adopted`，收进当前代际；
///   * 零条或多条 → 全部 `Isolated`（代际号原样留在旧一轮，不猜），
///     并在当前代际补一条 `Anchored` 锚事件——它把 state 里**已经存在**的
///     事实（level / armed_at_ms / flatten_state）如实写成一条可审计的事件行，
///     `trigger_source='generation_revalidation'` 明示它的来历。
///     这就是"隔离 + 可恢复路径"：latch 重新可收敛，而没有任何一条旧事件
///     被冒充成本轮的处置记录。
///
/// 幂等：`generation_proved_at_ms` 标记已复核的行；健康 scope 不产生任何写入。
pub(crate) async fn revalidate_legacy_event_generations(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<usize> {
    // **state 只在事务里读。**
    //
    // R16 在这里 `fetch_all` 出整份 armed 状态（**事务外**），随后逐 scope
    // 拿着那份快照去开事务。快照与写入之间隔着整整一轮循环，两个同时启动的
    // 进程读到同一份快照、同时判"本代没有 active 事件"、各写一条锚——
    // 同一个 `(scope_key, generation)` 于是有两个所有者，而 `reset` 的
    // `LIMIT 1` 只会标掉其中一条，另一条永远挂着未 reset。
    //
    // 外面这一趟只取 **scope 名单**（一个可能已经过期的候选集，过期没关系：
    // 每个 scope 的身份、代际与判定全部在它自己的事务里、拿着 state 行锁重读）。
    let scope_keys = sqlx::query(
        "SELECT scope_key FROM kill_switch_state WHERE level <> 'none' ORDER BY scope_key",
    )
    .fetch_all(store.pool())
    .await
    .context("load armed kill switch scopes for generation revalidation")?
    .into_iter()
    .map(|row| row.get::<String, _>("scope_key"))
    .collect::<Vec<_>>();

    let mut written = 0usize;
    for scope_key in scope_keys {
        written += revalidate_one_scope_generation(store, &scope_key, now_ms).await?;
    }
    Ok(written)
}

/// 一条 armed latch 的代际复核，全程在**它自己的事务 + state 行锁**下完成。
async fn revalidate_one_scope_generation(
    store: &Store,
    scope_key: &str,
    now_ms: i64,
) -> anyhow::Result<usize> {
    let mut tx = store.pool().begin().await?;
    // MySQL：`FOR UPDATE` 是这条"单所有者"的执行机制——第二个启动的进程
    // 在这里**真的被阻塞**，等第一个提交之后重读，看到的是已经写好的锚。
    // SQLite：`Store` 只开一条连接，两个写者天然不可能同时在这段里。
    let locked = match store.kind() {
        DbKind::MySql => {
            r#"
            SELECT level, COALESCE(trigger_source, '') AS trigger_source, reason,
                   CAST(armed_at_ms AS CHAR) AS armed_at_ms_text,
                   COALESCE(flatten_state, '') AS flatten_state,
                   flatten_generation
            FROM kill_switch_state WHERE scope_key=? FOR UPDATE
            "#
        }
        _ => {
            r#"
            SELECT level, COALESCE(trigger_source, '') AS trigger_source, reason,
                   CAST(armed_at_ms AS CHAR) AS armed_at_ms_text,
                   COALESCE(flatten_state, '') AS flatten_state,
                   flatten_generation
            FROM kill_switch_state WHERE scope_key=?
            "#
        }
    };
    let Some(state_row) = sqlx::query(locked)
        .bind(scope_key)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .next()
    else {
        // 候选集过期：这个 scope 在我们拿锁之前已经没了。
        tx.rollback().await?;
        return Ok(0);
    };
    let level = state_row.get::<String, _>("level");
    if level.trim() == Level::None.as_str() {
        // 候选集过期：拿锁之前它已经被 reset 掉了。
        tx.rollback().await?;
        return Ok(0);
    }
    let generation = state_row.get::<i64, _>("flatten_generation");
    let trigger_source = state_row.get::<String, _>("trigger_source");
    let reason = crate::store::try_get_text(&state_row, "reason")?.unwrap_or_default();
    let armed_at_ms = state_row.get::<String, _>("armed_at_ms_text");
    let flatten_state = state_row.get::<String, _>("flatten_state");

    let identity = ArmIdentity {
        level: &level,
        trigger_source: &trigger_source,
        reason: &reason,
        armed_at_ms: &armed_at_ms,
    };

    // **代际 0 不是豁免。**
    //
    // R16 的健康判据只有一句 `COUNT(*) > 0`：本代**任意**一条未 reset 的事件行
    // 都算"健康"，身份一个字段都不看。0026 把**所有**存量事件行置为代际 0，
    // 而一个从未 flatten 过的 armed scope 的 state 代际也正是 0——于是随便
    // 一条与这一轮毫无关系的存量事件行就能让整个复核跳过，`reset` 随后按
    // `LIMIT 1` 把它当成本轮的处置记录标掉，真正的那条留在库里永远未 reset。
    //
    // 现在本代的每一条活事件都要过"**唯一 + 可证明**"：
    //
    // * **身份相符** —— `armed_at_ms` / `level` / `trigger_source` / `reason`
    //   与 state 逐字相同（`arm_in_tx` 在同一个事务里用同一组值写两边）；
    // * **或已被裁决过** —— `generation_proved_at_ms IS NOT NULL`：它是上一次
    //   复核 adopt 进来的、或者复核自己写下的锚（锚的 `trigger_source` /
    //   `reason` 刻意与 state 不同，只能靠这一条认出来，否则每次启动都会
    //   把上一次的锚再隔离一遍、无限长草）。
    //
    // 恰好一个所有者才算健康；零个、多个、或者"有所有者但旁边还挂着别的活行"
    // 都要收敛到单所有者。
    let live_rows = sqlx::query(
        r#"
        SELECT event_id, level, COALESCE(trigger_source, '') AS trigger_source, reason,
               CAST(armed_at_ms AS CHAR) AS armed_at_ms_text,
               CASE WHEN generation_proved_at_ms IS NULL THEN 0 ELSE 1 END AS adjudicated
        FROM kill_switch_events
        WHERE scope_key=? AND reset_at_ms IS NULL AND flatten_generation=?
        ORDER BY event_id
        "#,
    )
    .bind(scope_key)
    .bind(generation)
    .fetch_all(&mut *tx)
    .await?;

    let mut owners = Vec::new();
    let mut live_ids = Vec::new();
    for row in &live_rows {
        let event_id = row.get::<String, _>("event_id");
        let adjudicated = row.get::<i64, _>("adjudicated") == 1;
        if adjudicated || identity.matches(row)? {
            owners.push(event_id.clone());
        }
        live_ids.push(event_id);
    }

    if owners.len() == 1 && live_ids.len() == 1 {
        // 健康：本代恰好一个、且证得出的所有者。重跑到这里是纯读，零写入。
        tx.rollback().await?;
        return Ok(0);
    }

    let mut written = 0usize;

    if owners.len() == 1 {
        // 有唯一所有者，但旁边还挂着证不出归属的活行：把那些行就地关掉，
        // 本代于是只剩一个所有者。**不补锚**——真正的那条还在。
        let owner = owners[0].clone();
        let evidence = format!(
            "revision {generation} had {} live events; exactly one is provably this arm \
             (armed_at_ms={armed_at_ms} level={level} trigger_source={trigger_source}), the \
             others cannot claim this revision",
            live_ids.len()
        );
        for event_id in live_ids.iter().filter(|id| **id != owner) {
            supersede_live_event(
                &mut tx,
                store.kind(),
                scope_key,
                event_id,
                generation,
                &evidence,
                now_ms,
            )
            .await?;
            written += 1;
        }
        tx.commit().await?;
        tracing::warn!(
            %scope_key,
            generation,
            superseded = written,
            "an armed kill switch revision carried more than one live event; the ones that \
             could not claim it were closed out so the revision has a single owner"
        );
        return Ok(written);
    }

    // 到这里：本代要么一条活事件都没有（升级错位），要么有活事件但一条都
    // 证不出归属 / 有多条都"像"（同样证不出）。两种都收敛到「隔离 + 锚点」。
    let candidates = if live_ids.is_empty() {
        // 未 reset、代际早于当前一轮、且尚未复核过的存量事件行。
        sqlx::query(
            r#"
            SELECT event_id, level, COALESCE(trigger_source, '') AS trigger_source, reason,
                   CAST(armed_at_ms AS CHAR) AS armed_at_ms_text,
                   flatten_generation
            FROM kill_switch_events
            WHERE scope_key=? AND reset_at_ms IS NULL
              AND flatten_generation < ? AND generation_proved_at_ms IS NULL
            "#,
        )
        .bind(scope_key)
        .bind(generation)
        .fetch_all(&mut *tx)
        .await?
    } else {
        // 本代已经有人在争这个号了：再从更早的代际里"认领"一条只会多出一个
        // 所有者。这一轮只能靠锚重新可收敛。
        Vec::new()
    };

    let mut matches = Vec::new();
    for row in &candidates {
        if identity.matches(row)? {
            matches.push((
                row.get::<String, _>("event_id"),
                row.get::<i64, _>("flatten_generation"),
            ));
        }
    }

    if let [(event_id, previous_generation)] = matches.as_slice() {
        let adopted = sqlx::query(
            "UPDATE kill_switch_events SET flatten_generation=?, generation_proved_at_ms=?, \
             updated_at_ms=? WHERE event_id=? AND reset_at_ms IS NULL \
             AND flatten_generation=?",
        )
        .bind(generation)
        .bind(now_ms)
        .bind(now_ms.to_string())
        .bind(event_id)
        .bind(*previous_generation)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if adopted != 1 {
            tx.rollback().await?;
            return Err(anyhow!(
                "adopting legacy kill switch event {event_id} into generation \
                 {generation} affected {adopted} rows"
            ));
        }
        record_event_generation_revalidation(
            &mut tx,
            store.kind(),
            scope_key,
            event_id,
            *previous_generation,
            generation,
            generation,
            EventGenerationVerdict::Adopted,
            &format!(
                "arm identity matched exactly one active legacy event \
                 (armed_at_ms={armed_at_ms} level={level} trigger_source={trigger_source})"
            ),
            now_ms,
        )
        .await?;
        tx.commit().await?;
        return Ok(1);
    }

    // 证不出归属：旧代际的候选行原样隔离（一条都不改代际号），
    // **本代**那些证不出归属的活行就地关掉（否则它们会与锚争同一个号）。
    let evidence = format!(
        "arm identity matched {} active legacy events and revision {generation} carried {} \
         live events that could not claim it (armed_at_ms={armed_at_ms} level={level} \
         trigger_source={trigger_source}); generation ownership is not provable, so no event \
         was adopted",
        matches.len(),
        live_ids.len()
    );
    for row in &candidates {
        let event_id = row.get::<String, _>("event_id");
        let previous_generation = row.get::<i64, _>("flatten_generation");
        sqlx::query("UPDATE kill_switch_events SET generation_proved_at_ms=? WHERE event_id=?")
            .bind(now_ms)
            .bind(&event_id)
            .execute(&mut *tx)
            .await?;
        record_event_generation_revalidation(
            &mut tx,
            store.kind(),
            scope_key,
            &event_id,
            previous_generation,
            previous_generation,
            generation,
            EventGenerationVerdict::Isolated,
            &evidence,
            now_ms,
        )
        .await?;
        written += 1;
    }
    for event_id in &live_ids {
        supersede_live_event(
            &mut tx,
            store.kind(),
            scope_key,
            event_id,
            generation,
            &evidence,
            now_ms,
        )
        .await?;
        written += 1;
    }

    let Some(scope_name) = scope(scope_key) else {
        tx.rollback().await?;
        return Err(anyhow!(
            "armed kill switch scope {scope_key:?} is not a valid scope key"
        ));
    };
    let anchor_id = ulid::Ulid::new().to_string();
    sqlx::query(
        r#"
        INSERT INTO kill_switch_events (
          event_id, scope, scope_key, level, trigger_source, reason,
          armed_at_ms, flatten_state, reconcile_clean, reset_by, reset_at_ms,
          created_at_ms, updated_at_ms, flatten_generation, generation_proved_at_ms
        ) VALUES (?, ?, ?, ?, 'generation_revalidation', ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?)
        "#,
    )
    .bind(&anchor_id)
    .bind(scope_name)
    .bind(scope_key)
    .bind(&level)
    .bind(format!(
        "generation revalidation anchor: no provable owner existed at flatten generation \
         {generation}; original reason: {reason}"
    ))
    .bind(&armed_at_ms)
    .bind((!flatten_state.is_empty()).then_some(flatten_state.as_str()))
    .bind(now_ms.to_string())
    .bind(now_ms.to_string())
    .bind(generation)
    .bind(now_ms)
    .execute(&mut *tx)
    .await?;
    record_event_generation_revalidation(
        &mut tx,
        store.kind(),
        scope_key,
        &anchor_id,
        generation,
        generation,
        generation,
        EventGenerationVerdict::Anchored,
        &evidence,
        now_ms,
    )
    .await?;
    written += 1;
    tx.commit().await?;
    tracing::warn!(
        %scope_key,
        generation,
        isolated_events = candidates.len(),
        superseded_events = live_ids.len(),
        "an armed kill switch scope had no provable event at its current flatten generation; \
         legacy events were isolated and a revalidation anchor was written so reconcile can \
         converge this latch again"
    );
    Ok(written)
}

/// state 上那组"这一次 arm 是谁"的字段。`arm_in_tx` 在**同一个事务**里用
/// 同一组值写 `kill_switch_state` 与 `kill_switch_events`，因此它们逐字相等
/// 就是归属证明；任何一列不等都不构成证明（**level 也不例外**——两条只差档位
/// 的事件是两次不同的 arm）。
struct ArmIdentity<'a> {
    level: &'a str,
    trigger_source: &'a str,
    reason: &'a str,
    armed_at_ms: &'a str,
}

impl ArmIdentity<'_> {
    fn matches(&self, row: &sqlx::any::AnyRow) -> anyhow::Result<bool> {
        Ok(row.get::<String, _>("armed_at_ms_text") == self.armed_at_ms
            && row.get::<String, _>("level") == self.level
            && row.get::<String, _>("trigger_source") == self.trigger_source
            && crate::store::try_get_text(row, "reason")?.unwrap_or_default() == self.reason)
    }
}

/// 关掉一条**停在当前代际、却证不出自己是这一轮所有者**的活事件。
///
/// 代际号原样保留、行一个字节不删：它仍然是一条可查的审计记录，
/// 只是不再是"这一轮未 reset 的处置"，于是 `reset` 的选择不再有歧义。
#[allow(clippy::too_many_arguments)]
async fn supersede_live_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    scope_key: &str,
    event_id: &str,
    generation: i64,
    evidence: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    let closed = sqlx::query(
        "UPDATE kill_switch_events SET reset_by='generation_revalidation', reset_at_ms=?, \
         generation_proved_at_ms=?, updated_at_ms=? WHERE event_id=? AND reset_at_ms IS NULL",
    )
    .bind(now_ms.to_string())
    .bind(now_ms)
    .bind(now_ms.to_string())
    .bind(event_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if closed != 1 {
        return Err(anyhow!(
            "superseding unprovable kill switch event {event_id} at generation {generation} \
             affected {closed} rows"
        ));
    }
    record_event_generation_revalidation(
        tx,
        kind,
        scope_key,
        event_id,
        generation,
        generation,
        generation,
        EventGenerationVerdict::Superseded,
        evidence,
        now_ms,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
/// 一条存量 `kill_switch_reconcile_status` 行的来源复核结论
/// 。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileTargetVerdict {
    /// 这个 credential 此刻仍是 scope 的成员 ⇒ 收进 `arm_snapshot`，
    /// 并把 clean/complete 复位：来源不可证明的赞成票不留任。
    Adopted,
    /// ****：此刻不是成员，
    /// 但这一行带着 `reset_reconcile_targets_in_tx` 的**可证明签名**
    /// （`ReconcileStatusRow::bears_arm_freeze_signature`）⇒ 它就是 arm
    /// 冻结进来的那个 target，敞口后来消失并不改变这件事 ⇒ 收进
    /// `arm_snapshot`。
    Restored,
    /// ****：来源在数据里答不出来 ⇒ `legacy_unprovable`，
    /// 等一次结构化人工处置（既不投票也不阻塞）。
    ///
    /// R19 这一档叫 `isolated`，语义是"证明了它不是 target"——而那个证明
    /// 用的是「此刻不是成员」，推不出「arm 时不是 snapshot」（regression）。
    Quarantined,
    /// 此刻是成员、本代却一行都没有 ⇒ 补一行未收敛的成员行，
    /// 让 expected 集合重新完整。
    Anchored,
}

impl ReconcileTargetVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Adopted => "adopted",
            Self::Restored => "restored",
            Self::Quarantined => "quarantined",
            Self::Anchored => "anchored",
        }
    }

    /// 判定完之后这一行该带的 provenance。
    fn provenance(self) -> &'static str {
        match self {
            Self::Adopted | Self::Restored | Self::Anchored => TARGET_FROM_ARM_SNAPSHOT,
            Self::Quarantined => TARGET_LEGACY_UNPROVABLE,
        }
    }

    /// 收进本代快照的行必须**同时**复位 clean/complete：来源不可证明的
    /// 赞成票不得穿过升级活下来（engine safety contract）。隔离行原样留着可审计。
    fn resets_votes(self) -> bool {
        !matches!(self, Self::Quarantined)
    }
}

/// **0028 之前的存量
/// reconcile status 行的可审计复核。**
///
/// 0028 给 `kill_switch_reconcile_status` 加了 `expected_provenance`，
/// **存量行取 `NULL`**——"这一行是 arm 冻结的成员，还是某次投影自己建的？"
/// 在数据里答不出来。运行期判据因此把 `NULL` 一律排除在 expected 之外
/// （fail-closed）。可是光排除会留下一个可用性坑：一个**升级时正闩着
/// flatten** 的 scope，它本代的成员行全是 `NULL` ⇒ frozen expected 为空
/// ⇒ 只剩 `reconcile_targets_in_tx` 此刻认得的成员，而"arm 时有敞口、
/// 现在敞口没了"的那种成员（`a_frozen_target_whose_exposure_disappeared_*`
/// 钉住的形状）会连同它的真值一起被拒，latch 永远停在 `flattening`。
///
/// **不得用「当前不是成员」
/// 推导「arm 时不是 snapshot」。**
///
/// R19 的复核是一条二分：`membership.contains(cid) ? Adopted : Isolated`。
/// 上面那段说明自己就承认存在「arm 时有敞口、现在敞口没了」的成员——而那
/// 正是 `Isolated` 会误伤的形状：它的真值从此被 `RejectedNonTarget` 拒在
/// 外面，它又不进 expected 集合，那条 latch 于是**永远**停在 `flattening`，
/// 人工 reset 也永远满足不了判据。R19 用来判"不是 target"的证据
/// （此刻不是成员）与被判的命题（arm 那一刻是不是成员）根本不是同一件事。
///
/// 复核在 `Store::connect` 里跑（迁移之后、任何对账/重试任务之前），
/// 逐个 armed scope、各自一个事务 + state 行锁：
///
/// * 本代没有待复核的行 → 健康，一行都不动（重跑即空集）；
/// * 有 → 逐行按**证据强度**判定：
///   * 带着 arm 事务的**冻结签名**（`(reconciled_at_ms, reconcile_clean,
///     flatten_complete) = (0, 0, 0)`——0028 之前只有 `reset_reconcile_targets_in_tx`
///     写得出这个元组，投影路径绑的 `reconciled_at_ms` 是 `now_ms`）
///     → `Restored`：**可证明**是 arm 冻结进来的 target，与此刻是不是成员无关；
///   * 此刻仍是成员（`reconcile_targets_in_tx`，也就是
///     `reset_reconcile_targets_in_tx` 当初冻结成员时用的**同一个来源**）
///     → `Adopted`：收进 `arm_snapshot`，并把 `reconcile_clean` /
///     `flatten_complete` 复位成 0。**复位是判据的一部分**——那张 clean 票
///     来自一份来源不可证明的行，留着它就等于让 regression 的自授权穿过
///     升级活下来（engine safety contract）；
///   * 两条都不成立 → `Quarantined`：`legacy_unprovable`。它**不**声称
///     "这一行不是 target"（那正是 R19 证不出来的那一步），只声称来源答不
///     出来：不投票、不阻塞、留着可审计，并让这个 scope 进入**可重新冻结**
///     状态——人工 `SetKillSwitch(manual, flatten)` 会为它开一轮新 revision，
///     按当前成员资格重新冻结（`arm_in_tx_with_round` 的 re-freeze 分支）。
///   * 此刻是成员、本代却一行都没有 → `Anchored`：补一行未收敛的成员行。
///
/// R19 已经判成 `legacy_isolated` 的行同样要重判（`needs_revalidation`）：
/// 那一版的判据本身就是这条缺陷。
///
/// 幂等：复核只处理"本代存在待复核行"的 armed scope，
/// 跑过一次之后是空集；健康库上零写入。
pub(crate) async fn revalidate_legacy_reconcile_targets(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<usize> {
    // 与 `revalidate_legacy_event_generations` 同一条纪律：外面这一趟只取
    // **scope 名单**（可能过期没关系），每个 scope 的代际、成员与判定都在
    // 它自己的事务里、拿着 state 行锁重读。
    let scope_keys = sqlx::query(
        "SELECT scope_key FROM kill_switch_state WHERE level <> 'none' ORDER BY scope_key",
    )
    .fetch_all(store.pool())
    .await
    .context("load armed kill switch scopes for reconcile target revalidation")?
    .into_iter()
    .map(|row| row.get::<String, _>("scope_key"))
    .collect::<Vec<_>>();

    let mut written = 0usize;
    for scope_key in scope_keys {
        written += revalidate_one_scope_reconcile_targets(store, &scope_key, now_ms).await?;
    }
    Ok(written)
}

async fn revalidate_one_scope_reconcile_targets(
    store: &Store,
    scope_key: &str,
    now_ms: i64,
) -> anyhow::Result<usize> {
    let mut tx = store.pool().begin().await?;
    let locked = match store.kind() {
        DbKind::MySql => {
            "SELECT level, flatten_generation FROM kill_switch_state WHERE scope_key=? FOR UPDATE"
        }
        _ => "SELECT level, flatten_generation FROM kill_switch_state WHERE scope_key=?",
    };
    let Some(state_row) = sqlx::query(locked)
        .bind(scope_key)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .next()
    else {
        // 候选集过期：拿锁之前这个 scope 已经没了。
        tx.rollback().await?;
        return Ok(0);
    };
    if state_row.get::<String, _>("level").trim() == Level::None.as_str() {
        // 候选集过期：拿锁之前它已经被 reset 掉了。
        tx.rollback().await?;
        return Ok(0);
    }
    let generation = state_row.get::<i64, _>("flatten_generation");

    let rows = reconcile_status_rows_in_tx(&mut tx, scope_key, generation).await?;
    if !rows.iter().any(|row| row.needs_revalidation()) {
        // 健康（含"跑过一次"）：纯读，零写入。
        tx.rollback().await?;
        return Ok(0);
    }
    let membership = reconcile_targets_in_tx(&mut tx, scope_key)
        .await?
        .into_iter()
        .collect::<BTreeSet<String>>();
    let evidence = format!(
        "migration 0028 left {} of this revision's {} status rows without a provable \
         provenance; each was re-judged from the arm transaction's freeze signature and, \
         failing that, from the scope's current membership ({} credentials) at flatten \
         generation {generation}",
        rows.iter().filter(|row| row.needs_revalidation()).count(),
        rows.len(),
        membership.len(),
    );

    let mut written = 0usize;
    for row in rows.iter().filter(|row| row.needs_revalidation()) {
        // **先找可证明来源，
        // 再退回成员资格；「当前不是成员」不再被用来推导「arm 时不是 snapshot」。**
        //
        // R19 的二分是 `membership.contains(cid) ? Adopted : Isolated`。
        // 「arm 那一刻有敞口、升级时敞口已经消失」的**真实** frozen target
        // 恰好落在 `Isolated` 那一侧：它的真值从此被 `RejectedNonTarget` 拒在
        // 外面，而它又不进 expected 集合，那条 latch 于是永远收敛不了
        // （R19 自己的源码注释就承认这条形状存在——`a_frozen_target_whose_\
        // exposure_disappeared_*` 钉的正是它）。
        //
        // 顺序因此是：
        //
        // 1. **可证明**：这一行带着 arm 事务的冻结签名（`(reconciled_at_ms,
        //    reconcile_clean, flatten_complete) = (0, 0, 0)`，投影路径写不出来）
        //    ⇒ `Restored`，与成员资格无关；
        // 2. **此刻仍是成员** ⇒ `Adopted`（R19 的那一档，原样保留）；
        // 3. **都不成立** ⇒ `Quarantined`：不声称它不是 target，只声称
        //    来源答不出来——不投票、不阻塞、留着可审计，并让这个 scope
        //    进入可重新冻结状态。
        let verdict = if row.bears_arm_freeze_signature() {
            ReconcileTargetVerdict::Restored
        } else if membership.contains(&row.credential_id) {
            ReconcileTargetVerdict::Adopted
        } else {
            ReconcileTargetVerdict::Quarantined
        };
        let updated = if verdict.resets_votes() {
            sqlx::query(
                "UPDATE kill_switch_reconcile_status SET expected_provenance=?, \
                 reconcile_clean=0, flatten_complete=0, reconciled_at_ms=0 \
                 WHERE scope_key=? AND credential_id=? AND flatten_generation=? \
                 AND (expected_provenance IS NULL OR expected_provenance=?)",
            )
        } else {
            sqlx::query(
                "UPDATE kill_switch_reconcile_status SET expected_provenance=? \
                 WHERE scope_key=? AND credential_id=? AND flatten_generation=? \
                 AND (expected_provenance IS NULL OR expected_provenance=?)",
            )
        }
        .bind(verdict.provenance())
        .bind(scope_key)
        .bind(&row.credential_id)
        .bind(generation)
        .bind(TARGET_LEGACY_ISOLATED)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated != 1 {
            tx.rollback().await?;
            return Err(anyhow!(
                "revalidating legacy reconcile target {} of {scope_key} at generation \
                 {generation} affected {updated} rows",
                row.credential_id
            ));
        }
        record_reconcile_target_revalidation(
            &mut tx,
            store.kind(),
            scope_key,
            &row.credential_id,
            generation,
            verdict,
            &evidence,
            now_ms,
        )
        .await?;
        written += 1;
    }

    let present = rows
        .iter()
        .map(|row| row.credential_id.clone())
        .collect::<BTreeSet<String>>();
    let insert = match store.kind() {
        DbKind::MySql => {
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, ?, 0, 0, 0, ?, ?) ON DUPLICATE KEY UPDATE \
             reconcile_clean=0, flatten_complete=0, reconciled_at_ms=0, \
             flatten_generation=VALUES(flatten_generation), \
             expected_provenance=VALUES(expected_provenance)"
        }
        _ => {
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, ?, 0, 0, 0, ?, ?) \
             ON CONFLICT(scope_key, credential_id) DO UPDATE SET reconcile_clean=0, \
             flatten_complete=0, reconciled_at_ms=0, \
             flatten_generation=excluded.flatten_generation, \
             expected_provenance=excluded.expected_provenance"
        }
    };
    for credential_id in membership.difference(&present) {
        sqlx::query(insert)
            .bind(scope_key)
            .bind(credential_id)
            .bind(generation)
            .bind(TARGET_FROM_ARM_SNAPSHOT)
            .execute(&mut *tx)
            .await?;
        record_reconcile_target_revalidation(
            &mut tx,
            store.kind(),
            scope_key,
            credential_id,
            generation,
            ReconcileTargetVerdict::Anchored,
            &evidence,
            now_ms,
        )
        .await?;
        written += 1;
    }

    tx.commit().await?;
    tracing::warn!(
        %scope_key,
        generation,
        revalidated = written,
        "an armed kill switch revision carried reconcile status rows written before migration \
         0028; its expected target set was rebuilt from the arm transaction's freeze signature \
         and the scope's current membership, and every adopted row must report a fresh \
         reconcile truth before the revision can converge"
    );
    Ok(written)
}

async fn record_reconcile_target_revalidation(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    scope_key: &str,
    credential_id: &str,
    generation: i64,
    verdict: ReconcileTargetVerdict,
    evidence: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    let upsert = match kind {
        DbKind::MySql => {
            "INSERT INTO kill_switch_reconcile_target_revalidations (scope_key, credential_id, \
             generation, verdict, evidence, revalidated_at_ms) VALUES (?, ?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE verdict=VALUES(verdict), evidence=VALUES(evidence), \
             revalidated_at_ms=VALUES(revalidated_at_ms)"
        }
        _ => {
            "INSERT INTO kill_switch_reconcile_target_revalidations (scope_key, credential_id, \
             generation, verdict, evidence, revalidated_at_ms) VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(scope_key, credential_id, generation) DO UPDATE SET \
             verdict=excluded.verdict, evidence=excluded.evidence, \
             revalidated_at_ms=excluded.revalidated_at_ms"
        }
    };
    sqlx::query(upsert)
        .bind(scope_key)
        .bind(credential_id)
        .bind(generation)
        .bind(verdict.as_str())
        .bind(evidence)
        .bind(now_ms)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn record_event_generation_revalidation(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    scope_key: &str,
    event_id: &str,
    previous_generation: i64,
    resolved_generation: i64,
    state_generation: i64,
    verdict: EventGenerationVerdict,
    evidence: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    let upsert = match kind {
        DbKind::MySql => {
            "INSERT INTO kill_switch_event_generation_revalidations (scope_key, event_id, \
             previous_generation, resolved_generation, state_generation, verdict, evidence, \
             revalidated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE \
             previous_generation=VALUES(previous_generation), \
             resolved_generation=VALUES(resolved_generation), \
             state_generation=VALUES(state_generation), verdict=VALUES(verdict), \
             evidence=VALUES(evidence), revalidated_at_ms=VALUES(revalidated_at_ms)"
        }
        _ => {
            "INSERT INTO kill_switch_event_generation_revalidations (scope_key, event_id, \
             previous_generation, resolved_generation, state_generation, verdict, evidence, \
             revalidated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(scope_key, event_id) DO UPDATE SET \
             previous_generation=excluded.previous_generation, \
             resolved_generation=excluded.resolved_generation, \
             state_generation=excluded.state_generation, verdict=excluded.verdict, \
             evidence=excluded.evidence, revalidated_at_ms=excluded.revalidated_at_ms"
        }
    };
    sqlx::query(upsert)
        .bind(scope_key)
        .bind(event_id)
        .bind(previous_generation)
        .bind(resolved_generation)
        .bind(state_generation)
        .bind(verdict.as_str())
        .bind(evidence)
        .bind(now_ms)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub struct ReconcileTruth {
    pub has_diff: bool,
    pub all_positions_zero: bool,
    pub all_open_orders_empty: bool,
    pub all_open_algo_orders_empty: bool,
    pub local_position_zero_diff: bool,
}

#[cfg(test)]
#[tonic::async_trait]
pub trait FlattenAdapter: Send + Sync {
    async fn cancel_all_open_algo_orders(&self) -> anyhow::Result<()>;
    async fn cancel_all_open_orders(&self) -> anyhow::Result<()>;
    async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()>;
    async fn force_reconcile(&self) -> anyhow::Result<ReconcileTruth>;
}

#[cfg(test)]
pub async fn drive_flatten_once<A: FlattenAdapter>(
    store: &Store,
    scope_key: &str,
    credential_id: &str,
    adapter: &A,
    now_ms: i64,
) -> anyhow::Result<ReconcileTruth> {
    // Request acknowledgement is never completion. Completion is recorded only
    // after forced reconcile returns every truth condition. Cancellation
    // failures are recorded by the production driver but never short-circuit
    // the reduce-only flatten attempt.
    let _algo_cancel_error = adapter.cancel_all_open_algo_orders().await.err();
    let _ordinary_cancel_error = adapter.cancel_all_open_orders().await.err();
    let _flatten_error = adapter.submit_reduce_only_flatten().await.err();
    // 观察点与生产同形：先取代际快照，再去读交易所真值。
    let observed = observe_flatten_generations(store).await?;
    let truth = adapter.force_reconcile().await?;
    record_reconcile_for_scope(store, scope_key, credential_id, truth, now_ms, &observed).await?;
    Ok(truth)
}

impl ReconcileTruth {
    pub fn flatten_complete(self) -> bool {
        !self.has_diff
            && self.all_positions_zero
            && self.all_open_orders_empty
            && self.all_open_algo_orders_empty
            && self.local_position_zero_diff
    }
}

/// 一条 `kill_switch_reconcile_status` 行的判据部分。
struct ReconcileStatusRow {
    credential_id: String,
    reconcile_clean: bool,
    flatten_complete: bool,
    /// 这一行最后一次被投影的墙钟。**arm 事务写下的冻结行取 0**
    /// （`reset_reconcile_targets_in_tx` 的 `VALUES (?, ?, 0, 0, 0, …)`），
    /// 投影路径写下的行取 `now_ms`（生产里必然为正）。见
    /// `bears_arm_freeze_signature`。
    reconciled_at_ms: i64,
    /// **这一行是怎么来的**。
    /// `None` = 0028 之前写下、尚未经启动期复核的存量行。
    expected_provenance: Option<String>,
}

impl ReconcileStatusRow {
    /// 只有 arm 事务写下的行才是 frozen expected target。
    fn is_arm_snapshot(&self) -> bool {
        self.expected_provenance.as_deref() == Some(TARGET_FROM_ARM_SNAPSHOT)
    }

    /// **这一行的来源证不出来，等一次结构化人工处置**
    /// 。它**一票都不投**——赞成票不算，
    /// 反对票也不算（`evaluate_expected_convergence` 的 `counted` 谓词）。
    ///
    /// **"不投票"必须是谓词，
    /// 不能只是注释。** R20 只在 quarantine 那一刻改了 provenance，
    /// **旧的 clean/flat 票原样留在行上**，而 `counted` 谓词当时只问
    /// "这个 credential 在不在 expected 里"。于是这个 credential 一旦
    /// 重新进入 `reconcile_targets_in_tx` 认得的 live 成员集合（敞口回来了、
    /// 孤儿仓位被记回 `position_state`……），它那张**升级之前**、来源
    /// 证不出来的 clean/flat 票就地复活，替**当前这一代** flatten latch
    /// 投了一张赞成票——一份陈旧真值可以把一条一次都没平过的 latch
    /// 收敛成 `flattened`（engine safety contract「对账失败后继续交易」一律 High）。
    ///
    /// 现在这条判据真的落在 `counted` 上：quarantine 行在本代拿到
    /// **新的 reconcile 真值**之前一票不算。拿到新真值时，投影 upsert 会
    /// 把它的 provenance 从 quarantine 提升成 `reconcile_discovery`
    /// （见 `record_reconcile_for_scope` 的 upsert），于是它凭**新真值**
    /// 参与收敛，而不是凭那张旧票。若它此刻是 live 成员却还没报新真值，
    /// 它会落进 `unreported`——本代不得收敛，直到新真值到来。
    fn is_unprovable(&self) -> bool {
        matches!(
            self.expected_provenance.as_deref(),
            Some(TARGET_LEGACY_UNPROVABLE) | Some(TARGET_LEGACY_ISOLATED)
        )
    }

    /// 启动期复核还没看过这一行（`NULL`，或 R19 判过 `legacy_isolated`
    /// ——那一版的判据本身就是 regression 的缺陷，必须重判）。
    fn needs_revalidation(&self) -> bool {
        self.expected_provenance.is_none()
            || self.expected_provenance.as_deref() == Some(TARGET_LEGACY_ISOLATED)
    }

    /// **arm 冻结行的可证明签名**。
    ///
    /// `(reconciled_at_ms, reconcile_clean, flatten_complete) = (0, 0, 0)` 是
    /// `reset_reconcile_targets_in_tx` 写出来的**逐字**元组，而这张表在 0028
    /// 之前只有两个写者：那个 arm 事务，和 `record_reconcile_for_scope` 的
    /// 投影 upsert。投影 upsert 绑的 `reconciled_at_ms` 是 `now_ms`
    /// （`crate::svc::now_ms()`，epoch 毫秒，生产里恒为正），**写不出 0**。
    /// 因此这个元组反向证明了"这一行是 arm 事务冻结进来的成员"——它不依赖
    /// "此刻还是不是成员"这个与 arm 时点无关的事实。
    ///
    /// 这条判据的强度与 `expected_provenance` 这一列本身相同：两者都可以被
    /// 直接改库的人伪造（伪造 `'arm_snapshot'` 更省事），它没有引入新的
    /// 攻击面；它能挡住的是**代码路径**能产生的每一种行。
    fn bears_arm_freeze_signature(&self) -> bool {
        self.reconciled_at_ms == 0 && !self.reconcile_clean && !self.flatten_complete
    }
}

/// **一次「本 revision 收敛到什么程度」的判定**
/// 。
///
/// R19 有**两份**过滤：聚合（`record_reconcile_for_scope`）只数 expected
/// 成员的行，`reset` 却数**本代的全部行**。两份判据于是能各说各话——一条
/// 聚合明确排除掉的行（`legacy_isolated`、或任何一条 outsider dirty 行）
/// 照样把一个**已经完整收敛**的 scope 永久钉在"reset 拒绝"上：那个 scope
/// 的 latch 写着 `flattened`、退出了重试集合，人工却清不掉它。
///
/// 现在两条路径都走这一个函数：**谁是 expected 成员**、**够不够收敛**、
/// **哪些行不算数**全部只有一份定义。
struct ExpectedConvergence {
    /// `arm` 冻结的成员 ∪ 此刻 `reconcile_targets_in_tx` 认得的成员。
    expected: BTreeSet<String>,
    frozen_len: usize,
    live_len: usize,
    /// expected 成员里**已经报过**本代真值的行数。
    total: i64,
    dirty: usize,
    incomplete: usize,
    /// 本代有行、却不在 expected 里的 credential（一票不算）。
    unexpected: Vec<String>,
    /// expected 里本代**一行都没有**的 credential。
    unreported: Vec<String>,
    /// 来源证不出来、等人工处置的行（既不投票也不阻塞）。
    quarantined: Vec<String>,
}

impl ExpectedConvergence {
    /// 成员集合逐字完整：至少一个成员报过，且没有成员缺席。
    fn complete_membership(&self) -> bool {
        self.total > 0 && self.unreported.is_empty()
    }

    fn clean(&self) -> bool {
        self.complete_membership() && self.dirty == 0
    }

    fn flatten_complete(&self) -> bool {
        self.complete_membership() && self.incomplete == 0
    }
}

/// 读一次本代的 expected 判据（成员资格与收敛完整性同源）。
async fn evaluate_expected_convergence(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    scope_key: &str,
    generation: i64,
) -> anyhow::Result<ExpectedConvergence> {
    let live_targets = reconcile_targets_in_tx(tx, scope_key)
        .await?
        .into_iter()
        .collect::<BTreeSet<String>>();
    let rows = reconcile_status_rows_in_tx(tx, scope_key, generation).await?;
    let frozen_targets = rows
        .iter()
        .filter(|row| row.is_arm_snapshot())
        .map(|row| row.credential_id.clone())
        .collect::<BTreeSet<String>>();
    let expected = frozen_targets
        .union(&live_targets)
        .cloned()
        .collect::<BTreeSet<String>>();
    let quarantined = rows
        .iter()
        .filter(|row| row.is_unprovable())
        .map(|row| row.credential_id.clone())
        .collect::<Vec<_>>();
    let unexpected = rows
        .iter()
        .filter(|row| !expected.contains(&row.credential_id))
        .map(|row| row.credential_id.clone())
        .collect::<Vec<_>>();
    // **quarantine 行一票不算，
    // 哪怕它此刻又是 live 成员。**
    //
    // R20 这里只有 `expected.contains(...)`。`expected` 是「arm 冻结的成员
    // ∪ 此刻 `reconcile_targets_in_tx` 认得的成员」——第二项与 provenance
    // 无关，于是一条 quarantine 行（来源证不出来、R20 明确写着"不投票"）
    // 只要它的 credential 重新回到 live targets，就**带着升级之前那张旧
    // clean/flat 票**被数进 `total`/`dirty`/`incomplete`。旧票没有任何新
    // 真值支撑，却能把**当前这一代**的 flatten latch 收敛成 `flattened`。
    //
    // 排除它之后，这个 credential 若确实是 live 成员，就落进 `unreported`
    // （`reported` 由 `counted` 导出）——本代不收敛，直到它真的报一份新
    // 真值；那次投影会把 provenance 提升出 quarantine，这一行随即恢复投票。
    // 若它不是 live 成员也不是冻结成员，它连 `expected` 都进不去，
    // 既不投票也不阻塞，R20 的语义逐字不变。
    let counted = rows
        .iter()
        .filter(|row| expected.contains(&row.credential_id) && !row.is_unprovable())
        .collect::<Vec<_>>();
    let reported = counted
        .iter()
        .map(|row| row.credential_id.clone())
        .collect::<BTreeSet<String>>();
    let unreported = expected.difference(&reported).cloned().collect::<Vec<_>>();
    Ok(ExpectedConvergence {
        frozen_len: frozen_targets.len(),
        live_len: live_targets.len(),
        total: counted.len() as i64,
        dirty: counted.iter().filter(|row| !row.reconcile_clean).count(),
        incomplete: counted.iter().filter(|row| !row.flatten_complete).count(),
        expected,
        unexpected,
        unreported,
        quarantined,
    })
}

/// 读**某个 revision** 的逐 credential 行。
///
/// 成员资格与收敛判定读的是**同一份**行，
/// 免得"谁能写"与"够不够收敛"各查各的、各自漂移。
async fn reconcile_status_rows_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    scope_key: &str,
    generation: i64,
) -> Result<Vec<ReconcileStatusRow>, sqlx::Error> {
    Ok(sqlx::query(
        r#"
        SELECT credential_id, reconcile_clean, flatten_complete,
               COALESCE(CAST(reconciled_at_ms AS CHAR), '') AS reconciled_at_ms_text,
               COALESCE(expected_provenance, '') AS expected_provenance
        FROM kill_switch_reconcile_status
        WHERE scope_key=? AND flatten_generation=?
        ORDER BY credential_id
        "#,
    )
    .bind(scope_key)
    .bind(generation)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|row| {
        let provenance = row.get::<String, _>("expected_provenance");
        ReconcileStatusRow {
            credential_id: row.get::<String, _>("credential_id"),
            reconcile_clean: row.get::<i64, _>("reconcile_clean") != 0,
            flatten_complete: row.get::<i64, _>("flatten_complete") != 0,
            reconciled_at_ms: row
                .get::<String, _>("reconciled_at_ms_text")
                .parse::<i64>()
                .unwrap_or_default(),
            expected_provenance: (!provenance.is_empty()).then_some(provenance),
        }
    })
    .collect())
}

/// **把一次对账真值投影到某个 kill-switch scope。**
///
/// `observed` 必须是**读交易所之前**取的代际快照
/// （`observe_flatten_generations`）。/// 没有这个参数时，一次在旧一轮开始、落库时才回来的 flat truth
/// 会把中途新开的 latch 写成 `flattened`——新敞口于是离开重试集合，
/// 而它一次都没被平过。
///
/// 全过程在**一个事务**里：读代际、CAS 判定、写逐 credential 行、聚合、
/// 写 `kill_switch_state` / `kill_switch_events`。分成几条独立语句时，
/// "读到的代际"与"写下去的那一刻"之间照样能插进一次 latch。
pub async fn record_reconcile_for_scope(
    store: &Store,
    scope_key: &str,
    credential_id: &str,
    truth: ReconcileTruth,
    now_ms: i64,
    observed: &FlattenGenerations,
) -> anyhow::Result<ReconcileProjection> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Err(anyhow!("credential_id is required for reconcile status"));
    }
    let mut tx = store.pool().begin().await?;
    let current =
        sqlx::query("SELECT level, flatten_generation FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .next()
            .map(|row| {
                (
                    row.get::<String, _>("level"),
                    row.get::<i64, _>("flatten_generation"),
                )
            });
    let current_generation = current
        .as_ref()
        .map(|(_, generation)| *generation)
        .unwrap_or(0);
    // 这个 scope 上有没有一条**活着的** latch 要被这份真值收敛。
    let has_live_latch = current
        .as_ref()
        .is_some_and(|(level, _)| level.trim() != Level::None.as_str());
    let observed_generation = observed.observed_for(scope_key);
    if observed_generation < current_generation {
        tx.rollback().await?;
        tracing::warn!(
            %scope_key,
            %credential_id,
            observed_generation,
            current_generation,
            "discarding a reconcile truth observed before the current flatten latch; \
             it cannot converge an exposure it never looked at"
        );
        return Ok(ReconcileProjection::DiscardedStale {
            observed: observed_generation,
            current: current_generation,
        });
    }
    // **没有活 latch = 没有要收敛的 revision。**
    //
    // R17 在这条路径上照写一行逐 credential"事实记录"。那行没有读者——聚合与
    // `reset` 都只数**本 revision** 的行，而下一次 arm 的
    // `reset_reconcile_targets_in_tx` 会把这个 scope 的行整批删掉重建——
    // 却正是"任意 credential 都能凭空建行"的那条通路。现在这里什么都不写。
    if !has_live_latch {
        tx.rollback().await?;
        return Ok(ReconcileProjection::NoActiveLatch);
    }

    // **收敛判据必须是一个闭世界。**
    //
    // R17 这里是一条**无条件 upsert**（任何非空 `credential_id` 都能给这个
    // scope 添一行）+ 一个只看"本 revision 已经出现的行"的聚合
    // （`total > 0 && dirty = 0`）。两条合起来是：**第一个任意 credential
    // 报一次 flat，整个 scope 就收敛**——latch 被写成 `flattened`、退出
    // `scopes_needing_action_retry` 的重试集合，而它真正的敞口一次都没被平过
    // （engine safety contract「对账失败后继续交易」一律 High）。最裸露的形状是
    // target 集合为空的 scope（`bot:` 下还没有任何本地单或仓位时
    // `reconcile_targets_in_tx` 返回空集）：一条与它毫无关系的 credential
    // 的 flat truth 就能把它收敛掉。
    //
    // 闭世界由**两个方向**合起来构成，缺一条都不成立：
    //
    // * **成员资格（谁能写）**——`expected` = 本 revision 在 arm 那一刻冻结
    //   下来的行（`arm_in_tx_with_round` 在同一个事务里调
    //   `reset_reconcile_targets_in_tx` 写的）**并上**此刻 `reconcile_targets_in_tx`
    //   仍然认得的成员。两者都不认的 credential 一行都写不进去
    //   （`RejectedNonTarget`）。**并上"此刻的成员"是必须的**：一次
    //   reconcile 可能**先发现**一个孤儿仓位、把它记进 `position_state`，
    //   然后才把真值投影到那条早就闩着的 `bot:` latch 上
    //   （`reconcile::reconcile_binance_futures_truth_with_client` 的孤儿恢复
    //   就是这个顺序）。只认冻结集合会把那条 latch 永远钉死在 `flattening`。
    //
    // * **完整性（够不够收敛）**——聚合不再只问"已经出现的行干不干净"，
    //   还要问"**该出现的都出现了吗**"：`expected` 里任何一个成员在本
    //   revision 一行都没有，就是"这个敞口一次都没被对账过"，
    //   不得收敛（engine safety contract：缺失不得被当成正常）。
    //
    // 两条合起来，收敛的充要条件是**成员集合逐字相等且全部 clean/flat**：
    // 非成员进不来，成员少一个就不收敛。空集依旧收敛不了（`total > 0`
    // 为假）——一次交易所真值都没看过就宣布"已平"正是这条判据要拦的东西。
    // **frozen expected target
    // 必须有独立的 provenance，不能是「本代的全部 status 行」。**
    //
    // R18 这里读的是本代**所有** status 行。可是这张表有两个写者：arm 事务
    // 里的 `reset_reconcile_targets_in_tx`，和下面那条投影 upsert。读回来时
    // 两者写的行**逐字无法区分**，于是"谁是本 revision 的 expected 成员"
    // 这个判据的输入包含了它自己的输出——一条同代的 outsider clean/flat 行
    // （0028 之前的 R17 存量行；或任何一次在成员资格之外落进来的行）会把
    // 自己算进 expected，随后凭这一行完成收敛。评审给的形状是：预置 active
    // latch + 同代 active event + **空 live targets** + 一条同代 outsider
    // clean/flat 行 ⇒ 它自己就构成了完整的 expected 集合，`total=1 /
    // dirty=0 / unreported=∅`，latch 被写成 `flattened` 并退出重试集合，
    // 而真正的敞口一次都没被平过（engine safety contract「对账失败后继续交易」）。
    //
    // 现在成员身份只认 `expected_provenance='arm_snapshot'`——迁移 0028 加的
    // 那一列，**只有** arm 事务写得出它。投影路径写下的行拿的是
    // `reconcile_discovery`，它凭"此刻 `reconcile_targets_in_tx` 还认不认得
    // 它"参与收敛，而不是凭自己这一行。0028 之前的存量行 provenance 为
    // `NULL`（来源不可证明），一律不算 expected；把它们收敛回可用状态的是
    // 启动期的 `revalidate_legacy_reconcile_targets`。
    //
    // **成员资格与收敛完整性
    // 现在读的是同一个 `evaluate_expected_convergence`**，`reset` 也读它。
    // R19 是"聚合一份过滤、reset 另一份过滤"，两份判据能各说各话。
    let before = evaluate_expected_convergence(&mut tx, scope_key, current_generation).await?;
    if !before.expected.contains(credential_id) {
        tx.rollback().await?;
        tracing::warn!(
            %scope_key,
            %credential_id,
            current_generation,
            frozen_targets = before.frozen_len,
            live_targets = before.live_len,
            "a reconcile truth arrived for a credential that belongs to neither this kill \
             switch revision's frozen target set nor the scope's current membership; it \
             cannot converge an exposure this revision never claimed, so nothing was applied"
        );
        return Ok(ReconcileProjection::RejectedNonTarget {
            generation: current_generation,
        });
    }
    let per_credential_clean = !truth.has_diff && truth.local_position_zero_diff;
    // 投影**从不**写 `arm_snapshot`：新建的行一律是 `reconcile_discovery`，
    // 已经存在的行**原样保留**它的 provenance——arm 冻结进来的成员不会因为
    // 报了一次真值就把自己降级，存量 `NULL` 行也不会因为被投影过就"洗白"
    // 成成员（`NULL` 的归宿只有启动期的 `revalidate_legacy_reconcile_targets`）。
    //
    // **唯一的例外：quarantine
    // 行拿到一份新真值时必须离开 quarantine。**
    //
    // `evaluate_expected_convergence` 现在把 quarantine 行排除在 `counted`
    // 之外（旧票不得复活）。若 provenance 在这里一动不动，那个 credential
    // 即便重新成为 live 成员、即便老老实实报了新真值，也会永远停在
    // `unreported` 上——本代再也收敛不了，而这正是"排除旧票"不该带来的
    // 副作用。这条 `CASE` 因此只做一件事：**已经存在的 quarantine 行**
    // （`legacy_unprovable` / `legacy_isolated`）在一次**通过成员资格闸门**
    // 的投影里升级成 `reconcile_discovery`。
    //
    // 它不放宽任何成员资格：`reconcile_discovery` 不进 frozen 集合，
    // 这一行此后仍然只凭"此刻 `reconcile_targets_in_tx` 还认不认得它"
    // 参与收敛；能走到这条语句本身就已经被 `before.expected` 闸门放行过。
    // `arm_snapshot` 与 `NULL` 两个方向逐字不变（`ELSE` 分支原样写回）。
    let promote_quarantined = |current: &str, incoming: &str| {
        format!(
            "CASE WHEN {current} IN ('{TARGET_LEGACY_UNPROVABLE}', '{TARGET_LEGACY_ISOLATED}') \
             THEN {incoming} ELSE {current} END"
        )
    };
    // **P4b（0038）· 五真位同事务写入**：投影路径持有完整 `ReconcileTruth`，
    // 两个聚合布尔反推不出它们；这里逐位落库，读侧以 TriState 透出，
    // legacy 行（NULL）呈现 UNKNOWN（r2 §4.3，变异 M5 的零写边界之外）。
    let upsert = match store.kind() {
        DbKind::MySql => {
            let provenance =
                promote_quarantined("expected_provenance", "VALUES(expected_provenance)");
            format!("INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, expected_provenance, truth_positions_zero, truth_orders_empty, truth_algo_orders_empty, truth_local_zero_diff, truth_no_diff) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE reconcile_clean=VALUES(reconcile_clean), flatten_complete=VALUES(flatten_complete), reconciled_at_ms=VALUES(reconciled_at_ms), flatten_generation=VALUES(flatten_generation), expected_provenance={provenance}, truth_positions_zero=VALUES(truth_positions_zero), truth_orders_empty=VALUES(truth_orders_empty), truth_algo_orders_empty=VALUES(truth_algo_orders_empty), truth_local_zero_diff=VALUES(truth_local_zero_diff), truth_no_diff=VALUES(truth_no_diff)")
        }
        _ => {
            let provenance = promote_quarantined(
                "kill_switch_reconcile_status.expected_provenance",
                "excluded.expected_provenance",
            );
            format!("INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, expected_provenance, truth_positions_zero, truth_orders_empty, truth_algo_orders_empty, truth_local_zero_diff, truth_no_diff) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(scope_key, credential_id) DO UPDATE SET reconcile_clean=excluded.reconcile_clean, flatten_complete=excluded.flatten_complete, reconciled_at_ms=excluded.reconciled_at_ms, flatten_generation=excluded.flatten_generation, expected_provenance={provenance}, truth_positions_zero=excluded.truth_positions_zero, truth_orders_empty=excluded.truth_orders_empty, truth_algo_orders_empty=excluded.truth_algo_orders_empty, truth_local_zero_diff=excluded.truth_local_zero_diff, truth_no_diff=excluded.truth_no_diff")
        }
    };
    sqlx::query(&upsert)
        .bind(scope_key)
        .bind(credential_id)
        .bind(if per_credential_clean { 1i64 } else { 0i64 })
        .bind(if truth.flatten_complete() { 1i64 } else { 0i64 })
        .bind(now_ms.to_string())
        .bind(current_generation)
        .bind(TARGET_FROM_RECONCILE_DISCOVERY)
        .bind(if truth.all_positions_zero { 1i64 } else { 0i64 })
        .bind(if truth.all_open_orders_empty {
            1i64
        } else {
            0i64
        })
        .bind(if truth.all_open_algo_orders_empty {
            1i64
        } else {
            0i64
        })
        .bind(if truth.local_position_zero_diff {
            1i64
        } else {
            0i64
        })
        .bind(if !truth.has_diff { 1i64 } else { 0i64 })
        .execute(&mut *tx)
        .await?;
    // 聚合只数**本代**的行：上一轮遗留的行既不能算收敛，也不能算脏。
    //
    // **聚合必须
    // 用 `COUNT`，不能用 `SUM`。** MySQL 的 `SUM()` 返回 `DECIMAL`
    // （`NewDecimal`），而 `sqlx` 的 `Any` 驱动没有该类型的映射：整条
    // `record_reconcile_for_scope` 在 MySQL 上**必然**以
    // 「Any driver does not support MySql type … NewDecimal」失败。也就是说
    // flatten latch 的收敛判定在 MySQL 后端上从来没有跑通过——这条路径此前
    // 只在 SQLite 上被观察过（R15R §10.③/§10.⑧ 如实登记过"未经真库验证"）。
    // `COUNT(CASE WHEN … THEN 1 END)` 语义与原式等价（不满足条件时 CASE 为
    // NULL、不计数），两个后端都返回整型。
    //
    // **聚合改成逐行读，因为它还要判
    // 「该出现的都出现了吗」。** 计数式聚合只答得出"已经出现的行干不干净"，
    // 答不出"expected 集合里还有谁一行都没有"——而后者正是"一个成员从未被
    // 对账过"这件事本身。行数与凭据数同阶（一个 scope 至多几个 credential），
    // 逐行读的代价可以忽略。
    // **聚合只数 expected 成员
    // 的行。**
    //
    // 成员资格挡住了非成员**写入**，但 R18 的聚合读的仍是本代的全部行：
    // 一条早已躺在库里的同代 outsider clean/flat 行照样进 `total`，替这一轮
    // 投了一张它没资格投的赞成票（反过来，一条 outsider dirty 行能把一个
    // 真正已经平掉的 scope 永远钉死在 `flattening`）。expected 集合是
    // 「arm 冻结的成员 ∪ 此刻 `reconcile_targets_in_tx` 认得的成员」——
    // 闭世界的两个方向都只在这个集合上判：**非成员的行一票不算**，
    // **成员少报一个就不收敛**。
    let convergence = evaluate_expected_convergence(&mut tx, scope_key, current_generation).await?;
    if !convergence.unexpected.is_empty() {
        tracing::warn!(
            %scope_key,
            current_generation,
            unexpected = convergence.unexpected.join(","),
            "this kill switch revision carries reconcile status rows for credentials it never \
             claimed; they are not counted toward convergence"
        );
    }
    if !convergence.quarantined.is_empty() {
        tracing::warn!(
            %scope_key,
            current_generation,
            quarantined = convergence.quarantined.join(","),
            "this kill switch revision carries reconcile status rows whose origin cannot be \
             proven; they neither vote nor block. If any of them is a real target of this \
             revision, re-arm this scope manually (SetKillSwitch manual flatten) to refreeze \
             its expected target set from the scope's current membership"
        );
    }
    if !convergence.unreported.is_empty() {
        tracing::warn!(
            %scope_key,
            current_generation,
            unreported = convergence.unreported.join(","),
            "this kill switch revision still has target credentials that have not reported a \
             reconcile truth; the revision cannot be considered converged"
        );
    }
    let clean = convergence.clean();
    let flatten_complete = convergence.flatten_complete();
    let flatten_state = if flatten_complete {
        Some(FLATTEN_CONVERGED)
    } else {
        Some(FLATTEN_PENDING)
    };
    // **绑代写入必须查命中数。**
    //
    // R14 已经把 `flatten_generation=?` 写进了这条 UPDATE 的谓词，却从不看
    // `rows_affected()`：读代际与写状态之间插进来的一次 latch 会让谓词
    // 一行都不命中，而函数照样返回 `Applied`——调用方（以及审计面）看到的是
    // "这份真值已经投影进去了"。0 行不是"没什么可写"，它就是代际错位本身。
    let state_rows = sqlx::query(
        r#"
        UPDATE kill_switch_state
        SET last_reconcile_clean=?, last_reconcile_at_ms=?,
            flatten_state=CASE WHEN level='flatten' THEN ? ELSE flatten_state END,
            updated_at_ms=?
        WHERE scope_key=? AND level <> 'none' AND flatten_generation=?
        "#,
    )
    .bind(if clean { 1i64 } else { 0i64 })
    .bind(now_ms.to_string())
    .bind(flatten_state)
    .bind(now_ms.to_string())
    .bind(scope_key)
    .bind(current_generation)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    // `has_live_latch` 在上面已经是必要条件（否则早已返回 `NoActiveLatch`），
    // 因此 0 行只可能是代际错位本身。
    if state_rows == 0 {
        tx.rollback().await?;
        tracing::warn!(
            %scope_key,
            %credential_id,
            current_generation,
            "the flatten latch moved between reading its generation and writing this \
             reconcile truth; nothing from this projection was applied"
        );
        return Ok(ReconcileProjection::DiscardedRaced {
            generation: current_generation,
        });
    }
    // 审计事件同样绑代（迁移 0026）。R14 这里只按 `armed_at_ms DESC` 取"最新
    // 一条未 reset 的事件"，一个代际条件都没有：一份早于当前 latch 的真值
    // 即便被上面的 state 谓词挡住，照样会把 `reconcile_clean` /
    // `flatten_state` 盖到**新一轮**的事件行上，于是审计面显示这一轮已经
    // clean/flattened，而 state 里根本没发生过这件事。
    let active_event = sqlx::query(
        r#"
        SELECT event_id FROM kill_switch_events
        WHERE scope_key=? AND reset_at_ms IS NULL AND flatten_generation=?
        ORDER BY armed_at_ms DESC LIMIT 1
        "#,
    )
    .bind(scope_key)
    .bind(current_generation)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .next();
    match active_event {
        Some(active_event) => {
            let event_rows = sqlx::query(
                r#"
            UPDATE kill_switch_events
            SET reconcile_clean=?, flatten_state=CASE
                  WHEN level='flatten' THEN ? ELSE flatten_state END,
                updated_at_ms=?
            WHERE event_id=? AND reset_at_ms IS NULL AND flatten_generation=?
            "#,
            )
            .bind(if clean { 1i64 } else { 0i64 })
            .bind(flatten_state)
            .bind(now_ms.to_string())
            .bind(active_event.get::<String, _>("event_id"))
            .bind(current_generation)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if event_rows == 0 {
                tx.rollback().await?;
                tracing::warn!(
                    %scope_key,
                    %credential_id,
                    current_generation,
                    "the active kill switch event moved between selecting it and writing this \
                     reconcile truth; nothing from this projection was applied"
                );
                return Ok(ReconcileProjection::DiscardedRaced {
                    generation: current_generation,
                });
            }
        }
        None => {
            tx.rollback().await?;
            tracing::warn!(
                %scope_key,
                %credential_id,
                current_generation,
                "an armed kill switch scope has no active event at this generation; this \
                 reconcile truth has nothing to converge and is not applied"
            );
            return Ok(ReconcileProjection::DiscardedRaced {
                generation: current_generation,
            });
        }
    }
    tx.commit().await?;
    Ok(ReconcileProjection::Applied)
}

/// **测试专用捷径**：在落库前一刻取代际快照。
///
/// 语义等于"这次真值与当前 latch 同代"——也就是**没有并发 latch** 的那种
/// 观察。生产路径一律不得使用：观察点必须早于交易所读取，否则代际 CAS
/// 就退化成了自己跟自己比。
#[cfg(test)]
pub(crate) async fn record_reconcile_for_scope_now(
    store: &Store,
    scope_key: &str,
    credential_id: &str,
    truth: ReconcileTruth,
    now_ms: i64,
) -> anyhow::Result<ReconcileProjection> {
    let observed = observe_flatten_generations(store).await?;
    record_reconcile_for_scope(store, scope_key, credential_id, truth, now_ms, &observed).await
}

pub async fn apply_local_order_semantics(
    store: &Store,
    scope_key: &str,
    level: Level,
    now_ms: i64,
) -> anyhow::Result<u64> {
    // 候选查询本身失败是整轮失败（`?`）；逐单隔离只发生在候选已经读出之后。
    let candidates = local_cancel_candidates(store, scope_key, level).await?;
    cancel_local_candidates(store, scope_key, &candidates, now_ms).await
}

/// 本轮「本地状态即真值」的待撤单，按候选 SELECT 返回的顺序（**未指定**）。
pub(crate) async fn local_cancel_candidates(
    store: &Store,
    scope_key: &str,
    level: Level,
) -> anyhow::Result<Vec<LocalCancelCandidate>> {
    let (scope_sql, scope_value) = if let Some(value) = scope_key.strip_prefix("cred:") {
        ("credential_id = ?", value)
    } else if let Some(value) = scope_key.strip_prefix("bot:") {
        ("bot_uid = ?", value)
    } else {
        ("1 = 1 AND ? = ?", "global")
    };
    let role_filter = if level == Level::ManageOnly {
        "AND order_role = 'entry'"
    } else if level == Level::Flatten {
        ""
    } else {
        return Ok(Vec::new());
    };

    // ****
    //
    // 此前这里一条 UPDATE 把所有活跃单直接写成 `canceled`，而紧随其后的
    // `drive_live_scope_once` 的交易所撤单 SELECT 只取
    // `('pending_submit','submitted','acked','partial_filled','unknown')`
    // ——刚被改掉的行选不出来，于是**真实撤单请求根本不发**：本地写着"已撤"，
    // 交易所挂单还活着，还能成交。这正是「伪造撤单成功」。
    //
    // 修法：**只有在"这张单不可能存在于交易所"时，本地状态才是真值**：
    // ①`reduce_only_exit_reaches_exchange_for` 为假（模拟凭据、或未配 venue 且
    //   live 未开启）——引擎根本不会去交易所，本地即真值。D39：显式配置了 venue
    //   的凭据在总开关关闭时仍由 kill switch 走交易所撤单 / flatten，那里的单
    //   不是本地真值；
    // ②`pending_risk` ——提交路径在真正提交前必先写 `pending_submit`
    //   （`svc.rs`），因此 `pending_risk` 可证明尚未到达交易所。
    // 其余一律**保持原状态**，交给交易所撤单 + `reconcile` 按**交易所真值**收敛
    // （`reconcile.rs` 的 `binance_order_status_to_engine_status`）。
    // 写库一律带 CAS（`AND status = <读到的那个>`），防止与提交路径竞态。
    let live_enabled = crate::svc::is_binance_live_enabled();
    let select = format!(
        "SELECT engine_order_id, credential_id, status FROM engine_orders WHERE {scope_sql} {role_filter} AND status IN ('pending_risk','pending_submit','submitted','acked','partial_filled','unknown')"
    );
    let mut statement = sqlx::query(&select);
    if scope_key == "global" {
        statement = statement.bind(scope_value).bind(scope_value);
    } else {
        statement = statement.bind(scope_value);
    }
    let rows = statement.fetch_all(store.pool()).await?;

    let mut candidates = Vec::new();
    for row in rows {
        let engine_order_id = row.get::<String, _>("engine_order_id");
        let credential_id = row.get::<String, _>("credential_id");
        let status = row.get::<String, _>("status");
        let kind = crate::venue::credential_kind(&credential_id);
        let venue = crate::venue::venue_for_credential(&credential_id)
            .ok()
            .flatten();
        let reaches_exchange =
            crate::svc::reduce_only_exit_reaches_exchange_for(kind, live_enabled, venue);
        let local_is_truth = !reaches_exchange || status == "pending_risk";
        if !local_is_truth {
            continue;
        }
        candidates.push(LocalCancelCandidate {
            engine_order_id,
            status,
        });
    }
    Ok(candidates)
}

/// 一张「本地状态即真值」的待撤单：候选 SELECT 选出、且通过了
/// `local_is_truth` 判据的那一行。
pub(crate) struct LocalCancelCandidate {
    pub(crate) engine_order_id: String,
    pub(crate) status: String,
}

/// 错误摘要里最多保留多少张失败单的明细；其余只计数。
const LOCAL_CANCEL_ERROR_DETAIL_LIMIT: usize = 8;
/// 单张失败单的原因在摘要里最多保留多少个字符。
const LOCAL_CANCEL_ERROR_REASON_CHARS: usize = 240;

/// 数据库本身不可用（连接池超时/已关闭/IO）——不是「这一张单」的问题。
///
/// 这类错误继续逐单重试只会让每张单各等一次连接超时，把紧随其后的
/// `drive_live_scope_once`（交易所侧撤单/平仓）拖住，所以本轮立即如实失败。
fn is_store_unavailable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<sqlx::Error>(),
            Some(
                sqlx::Error::PoolTimedOut
                    | sqlx::Error::PoolClosed
                    | sqlx::Error::Io(_)
                    | sqlx::Error::WorkerCrashed
            )
        )
    })
}

/// 逐单本地撤销，**逐单隔离错误**。
///
/// 一张不可回收的 `pending_risk` 行（0009 之前 `order_role=NULL` 的滞留行、
/// 权威不一致的 Entry）此前经循环内的 `?` 中止整个 scope：排在它后面的合法单
/// 每一轮都轮不到。现在这张单原样保留（每单仍走自己的原子 CAS/事务回滚，
/// 失败单不撤、不释放任何资源），其余候选照常处置；只要有任何一张失败，
/// **整轮仍返回 `Err`**——调用方据此保留重试与失败可见，绝不把它记成成功。
/// 摘要有界：明细至多 `LOCAL_CANCEL_ERROR_DETAIL_LIMIT` 条、每条原因截断。
async fn cancel_local_candidates(
    store: &Store,
    scope_key: &str,
    candidates: &[LocalCancelCandidate],
    now_ms: i64,
) -> anyhow::Result<u64> {
    let mut canceled: u64 = 0;
    let mut failed: u64 = 0;
    let mut details: Vec<String> = Vec::new();
    let mut not_attempted: usize = 0;
    for (index, candidate) in candidates.iter().enumerate() {
        let error = match cancel_order_if_status(
            store,
            &candidate.engine_order_id,
            &candidate.status,
            now_ms,
        )
        .await
        {
            Ok(rows) => {
                canceled += rows;
                continue;
            }
            Err(error) => error,
        };
        failed += 1;
        let store_unavailable = is_store_unavailable(&error);
        if details.len() < LOCAL_CANCEL_ERROR_DETAIL_LIMIT {
            let reason: String = error
                .to_string()
                .chars()
                .take(LOCAL_CANCEL_ERROR_REASON_CHARS)
                .collect();
            tracing::error!(
                %scope_key,
                engine_order_id = %candidate.engine_order_id,
                status = %candidate.status,
                error = %reason,
                error_code = "KILL_SWITCH_LOCAL_CANCEL_ORDER_FAILED",
                "kill-switch local cancel failed for one order; it is left untouched and the rest continue"
            );
            details.push(format!(
                "{}[{}]: {reason}",
                candidate.engine_order_id, candidate.status
            ));
        }
        if store_unavailable {
            not_attempted = candidates.len() - index - 1;
            break;
        }
    }
    if failed == 0 {
        return Ok(canceled);
    }
    let aborted = if not_attempted > 0 {
        " (store unavailable)"
    } else {
        ""
    };
    Err(anyhow!(
        "kill-switch scope {scope_key} local cancel incomplete: {failed} order(s) failed, \
         {canceled} canceled, {not_attempted} not attempted{aborted}; first {} failure(s): {}",
        details.len(),
        details.join(" | ")
    ))
}

pub fn parse_file_level(contents: &str) -> Level {
    contents
        .split_whitespace()
        .next()
        .and_then(Level::parse)
        .filter(|level| *level != Level::None)
        .unwrap_or(Level::Flatten)
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

pub fn spawn_manual_trigger_task(
    store: Store,
    kill_file: PathBuf,
    signal_flag: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut file_seen = false;
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if signal_flag.swap(false, Ordering::SeqCst) {
                let _ = arm(
                    &store,
                    ArmRequest {
                        scope_key: "global",
                        level: Level::Flatten,
                        trigger_source: "signal",
                        reason: "SIGUSR1",
                        now_ms: crate::svc::now_ms(),
                    },
                )
                .await;
            }
            let exists = kill_file.is_file();
            if exists && !file_seen {
                let contents = std::fs::read_to_string(&kill_file).unwrap_or_default();
                let level = parse_file_level(&contents);
                let reason = contents
                    .trim()
                    .split_once(char::is_whitespace)
                    .map(|(_, reason)| reason.trim())
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or("KILL_SWITCH file present");
                let _ = arm(
                    &store,
                    ArmRequest {
                        scope_key: "global",
                        level,
                        trigger_source: "manual_file",
                        reason,
                        now_ms: crate::svc::now_ms(),
                    },
                )
                .await;
            }
            // Deleting the file merely allows a future edge to be detected; it
            // never resets the durable switch state.
            file_seen = exists;
        }
    })
}

async fn credentials_for_scope(store: &Store, scope_key: &str) -> anyhow::Result<Vec<String>> {
    if let Some(credential_id) = scope_key.strip_prefix("cred:") {
        return Ok(vec![credential_id.to_string()]);
    }
    let rows = if let Some(bot_uid) = scope_key.strip_prefix("bot:") {
        sqlx::query(
            "SELECT DISTINCT credential_id FROM position_state WHERE bot_uid=? UNION SELECT DISTINCT credential_id FROM engine_orders WHERE bot_uid=?",
        )
        .bind(bot_uid)
        .bind(bot_uid)
        .fetch_all(store.pool())
        .await?
    } else if scope_key == "global" {
        sqlx::query(
            "SELECT credential_id FROM risk_budget UNION SELECT credential_id FROM engine_orders UNION SELECT credential_id FROM position_state",
        )
        .fetch_all(store.pool())
        .await?
    } else {
        return Err(anyhow!("invalid scope_key"));
    };
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("credential_id"))
        .collect())
}

async fn cancel_live_orders(
    store: &Store,
    client: &crate::binance_futures::BinanceFuturesClient,
    credential_id: &str,
    scope_key: &str,
    level: Level,
    use_local_truth: bool,
) -> anyhow::Result<()> {
    // L3 always cancels Algo protection first.  A bot scope uses local
    // ownership; credential/global scopes enumerate exchange truth.  L2 never
    // cancels protection.
    let algo_cancel_error = if level == Level::Flatten {
        async {
            let limiter = kill_switch_limiter_for_credential(credential_id)?;
            // (label, typed identity) of every owned obligation to cancel.
            let mut targets: Vec<(String, AlgoCancelIdentity)> = Vec::new();
            let mut cancel_errors = Vec::new();
            if scope_key.starts_with("bot:") || use_local_truth {
                // K04: an interrupted submit leaves only `client_algo_id`; it is
                // still an owned obligation and is cancelled by that typed
                // identity. Position state is deliberately not consulted — a
                // locally closed position is not authority to skip it.
                let (query, bot_uid) = if let Some(bot_uid) = scope_key.strip_prefix("bot:") {
                    (
                        r#"
                SELECT po.protection_order_id, po.exchange_algo_id, po.client_algo_id,
                       po.exchange_order_id
                FROM protection_orders po
                JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id
                WHERE po.credential_id=? AND eo.bot_uid=?
                  AND po.status NOT IN ('canceled','expired','failed')
                ORDER BY po.protection_order_id
                "#,
                        Some(bot_uid),
                    )
                } else {
                    (
                        r#"
                SELECT protection_order_id, exchange_algo_id, client_algo_id,
                       exchange_order_id
                FROM protection_orders
                WHERE credential_id=?
                  AND status NOT IN ('canceled','expired','failed')
                ORDER BY protection_order_id
                "#,
                        None,
                    )
                };
                let mut statement = sqlx::query(query).bind(credential_id);
                if let Some(bot_uid) = bot_uid {
                    statement = statement.bind(bot_uid);
                }
                let rows = statement.fetch_all(store.pool()).await?;
                for row in rows {
                    let protection_order_id = row.get::<String, _>("protection_order_id");
                    let nonblank = |column: &str| -> anyhow::Result<Option<String>> {
                        Ok(crate::store::try_get_nullable_string(&row, column)?
                            .map(|value| value.trim().to_string())
                            .filter(|value| !value.is_empty()))
                    };
                    let exchange_algo_id = nonblank("exchange_algo_id")?;
                    let client_algo_id = nonblank("client_algo_id")?;
                    let legacy_order_id = nonblank("exchange_order_id")?;
                    match (exchange_algo_id, client_algo_id) {
                        (Some(algo_id), _) => targets.push((
                            format!("protection {protection_order_id} algoId {algo_id}"),
                            AlgoCancelIdentity::AlgoId(algo_id),
                        )),
                        (None, Some(client_algo_id)) => targets.push((
                            format!(
                                "protection {protection_order_id} clientAlgoId {client_algo_id}"
                            ),
                            AlgoCancelIdentity::ClientAlgoId(client_algo_id),
                        )),
                        // Pre-0013 conditional order: its legacy ordinary id
                        // stays in the ordinary domain, never an Algo parent.
                        (None, None) if legacy_order_id.is_some() => {}
                        (None, None) => cancel_errors.push(format!(
                            "protection {protection_order_id}: missing Algo identity; \
                             obligation left unresolved"
                        )),
                    }
                }
            } else {
                let open_algos = client
                    .get_all_open_algo_orders_json(&limiter, RequestClass::EventTruth)
                    .await?;
                let open_algos: Vec<crate::binance_futures::BinanceAlgoOrderResponse> =
                    serde_json::from_value(open_algos).context("decode L3 openAlgoOrders truth")?;
                for algo in open_algos {
                    let algo_id = algo
                        .algo_id
                        .ok_or_else(|| anyhow!("L3 openAlgoOrders truth omitted algoId"))?;
                    targets.push((
                        format!("algoId {algo_id}"),
                        AlgoCancelIdentity::AlgoId(algo_id.to_string()),
                    ));
                }
            }
            // One failing obligation never starves an independently
            // identifiable sibling; failures are aggregated below.
            for (label, identity) in targets {
                let (algo_id, client_algo_id) = match identity {
                    AlgoCancelIdentity::AlgoId(algo_id) => match algo_id.parse::<i64>() {
                        Ok(parsed) => (
                            Some(crate::binance_futures::BinanceAlgoId::new(parsed)),
                            None,
                        ),
                        Err(error) => {
                            cancel_errors.push(format!("{label}: invalid algoId: {error}"));
                            continue;
                        }
                    },
                    AlgoCancelIdentity::ClientAlgoId(client_algo_id) => (
                        None,
                        Some(crate::binance_futures::BinanceClientAlgoId::new(
                            client_algo_id,
                        )),
                    ),
                };
                if let Err(error) = client
                    .cancel_algo_order_by_identity_json(
                        &limiter,
                        algo_id.as_ref(),
                        client_algo_id.as_ref(),
                        RequestClass::Protection,
                    )
                    .await
                {
                    cancel_errors.push(format!("{label}: {error}"));
                }
            }
            if cancel_errors.is_empty() {
                Ok(())
            } else {
                Err(anyhow!(
                    "L3 algo cancellation failures: {}",
                    cancel_errors.join("; ")
                ))
            }
        }
        .await
        .err()
    } else {
        None
    };

    // After Algo cancellation, cancel ordinary exchange orders. Bot scope and
    // L2 use local ownership so one bot cannot cancel another bot's orders.
    // L2 selects entry roles only and leaves protection untouched.
    let ordinary_cancel_result: anyhow::Result<()> = async {
        if level == Level::Flatten && !scope_key.starts_with("bot:") && !use_local_truth {
            let orders = client.open_orders(None, RequestClass::EventTruth).await?;
            let mut ordinary_errors = Vec::new();
            for order in orders.as_array().into_iter().flatten() {
                let symbol = order
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let order_id = order.get("orderId").and_then(|value| value.as_i64());
                let client_id = order.get("clientOrderId").and_then(|value| value.as_str());
                if !symbol.is_empty() {
                    let order_id = order_id.map(|value| value.to_string());
                    if let Err(error) = client
                        .cancel_order_json(
                            symbol,
                            order_id.as_deref(),
                            client_id,
                            RequestClass::Exit,
                        )
                        .await
                    {
                        ordinary_errors.push(format!(
                            "{symbol} orderId={order_id:?} clientOrderId={client_id:?}: {error}"
                        ));
                    }
                }
            }
            // A failed item must not starve later orders from this successful
            // truth read. Keep every failure visible to the scope driver.
            return if ordinary_errors.is_empty() {
                Ok(())
            } else {
                Err(anyhow!(
                    "ordinary cancellation failures: {}",
                    ordinary_errors.join("; ")
                ))
            };
        }

        // K04: a triggered child that already finished answers "unknown
        // order"; that must not starve the owned orders after it.
        let mut ordinary_errors = Vec::new();
        for row in cancellable_orders_for_scope(store, scope_key, credential_id, level).await? {
            let symbol_key = row.symbol_key;
            // regression：撤单也是交易所边界，闭世界身份认不出就不发请求。
            let Some(symbol) = crate::binance_futures::usdm_symbol(&symbol_key) else {
                tracing::error!(
                    credential_id,
                    scope_key,
                    %symbol_key,
                    "kill-switch cannot resolve a USDⓈ-M identity for this order; \
                     it is not cancellable through this venue"
                );
                continue;
            };
            let order_id = row.exchange_order_id;
            let client_id = row.client_order_id_short;
            if let Err(error) = client
                .cancel_order_json(
                    &symbol,
                    (!order_id.is_empty()).then_some(order_id.as_str()),
                    (!client_id.is_empty()).then_some(client_id.as_str()),
                    RequestClass::Exit,
                )
                .await
            {
                ordinary_errors.push(format!("{} {symbol}: {error}", row.engine_order_id));
            }
        }
        if ordinary_errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "ordinary cancellation failures: {}",
                ordinary_errors.join("; ")
            ))
        }
    }
    .await;
    match (algo_cancel_error, ordinary_cancel_result) {
        (None, Ok(())) => Ok(()),
        (Some(algo), Ok(())) => Err(algo),
        (None, Err(ordinary)) => Err(ordinary),
        (Some(algo), Err(ordinary)) => Err(anyhow!(
            "algo cancellation failed: {algo}; ordinary cancellation failed: {ordinary}"
        )),
    }
}

/// The typed venue identity an L3 Algo cancellation is sent with. A
/// clientAlgoId-only obligation is cancelled by `clientAlgoId`, never by an
/// invented algoId and never through ordinary-order cancellation.
enum AlgoCancelIdentity {
    AlgoId(String),
    ClientAlgoId(String),
}

/// 一条待撤单的交易所标识。
#[derive(Clone, Debug)]
pub(crate) struct CancellableOrder {
    pub engine_order_id: String,
    pub symbol_key: String,
    pub exchange_order_id: String,
    pub client_order_id_short: String,
}

/// **生产的撤单选取查询**（唯一一份）。
///
/// 段级复评证明 R4 的用例是**测试自己重写了一遍这段 SQL**，
/// 于是"把本地 `canceled` 放回选取集"的变异照样全绿——测的是测试自己的字符串。
/// 抽成函数后生产与测试逐字同源，读侧任何漂移都会被用例看见。
pub(crate) async fn cancellable_orders_for_scope(
    store: &Store,
    scope_key: &str,
    credential_id: &str,
    level: Level,
) -> Result<Vec<CancellableOrder>, sqlx::Error> {
    let bot_uid = scope_key.strip_prefix("bot:");
    let bot_filter = if bot_uid.is_some() {
        "AND bot_uid=?"
    } else {
        ""
    };
    let entry_only = if level == Level::ManageOnly {
        "AND order_role='entry'"
    } else {
        ""
    };
    // K04: Algo truth projects a triggered protection as `triggered` with its
    // actualOrderId in `exchange_order_id` — an ordinary child that may still
    // be working. Only that authoritative ordinary id selects it (the
    // clientAlgoId is blanked above and never reaches ordinary cancellation);
    // no other terminal status is added.
    let query = format!(
        "SELECT engine_order_id, symbol_key, COALESCE(exchange_order_id, '') AS exchange_order_id, CASE WHEN order_role LIKE 'protection_%' THEN '' ELSE COALESCE(client_order_id_short, '') END AS client_order_id_short FROM engine_orders WHERE credential_id=? {bot_filter} {entry_only} AND (order_role NOT LIKE 'protection_%' OR exchange_order_id IS NOT NULL) AND (status IN ('pending_submit','submitted','acked','partial_filled','unknown') OR (status='triggered' AND order_role LIKE 'protection_%' AND exchange_order_id IS NOT NULL AND exchange_order_id <> '')) ORDER BY engine_order_id"
    );
    // The client is bound to one credential even when the bot's history spans
    // several credentials. Never send another credential's ordinary order id.
    let mut statement = sqlx::query(&query).bind(credential_id);
    if let Some(bot_uid) = bot_uid {
        statement = statement.bind(bot_uid);
    }
    Ok(statement
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .map(|row| CancellableOrder {
            engine_order_id: row.get::<String, _>("engine_order_id"),
            symbol_key: row.get::<String, _>("symbol_key"),
            exchange_order_id: row.get::<String, _>("exchange_order_id"),
            client_order_id_short: row.get::<String, _>("client_order_id_short"),
        })
        .collect())
}

pub(crate) async fn submit_live_flatten(
    store: &Store,
    client: &crate::binance_futures::BinanceFuturesClient,
    credential_id: &str,
    scope_key: &str,
    filter_cache: &crate::exchange_filters::ExchangeFilterCache,
) -> anyhow::Result<()> {
    submit_live_flatten_with_truth_mode(
        store,
        client,
        credential_id,
        scope_key,
        filter_cache,
        false,
    )
    .await
}

async fn submit_live_flatten_with_truth_mode(
    store: &Store,
    client: &crate::binance_futures::BinanceFuturesClient,
    credential_id: &str,
    scope_key: &str,
    filter_cache: &crate::exchange_filters::ExchangeFilterCache,
    use_local_truth: bool,
) -> anyhow::Result<()> {
    // **认不出身份的持仓不得静默丢弃。**
    //
    // 这里此前是 `filter_map(|row| { … usdm_symbol(&symbol_key)? … })`：
    // `?` 把"这一行的 symbol_key 不是 Binance USDⓈ-M 身份"折成"这一行不存在"。
    // 于是一条 `okx|spot|BTC|USDT|BTC` 形状的**未平仓**记录会被整行丢掉——
    // flatten 一张都不发，调用方拿到 `Ok(())`，而库里那条敞口原样留着。
    // 现在它们被逐条登记（审计事件 + 结构化告警），并让 `submit_live_flatten`
    // 以错误返回：latch 保持 `flattening`，重试任务继续驱动，
    // 谁也不会把"没处置"当成"已平"。
    let mut unresolved_positions: Vec<String> = Vec::new();
    let positions: Vec<(String, String, String)> = if scope_key.starts_with("bot:")
        || use_local_truth
    {
        let (query, bot_uid) = if let Some(bot_uid) = scope_key.strip_prefix("bot:") {
            (
                "SELECT symbol_key, direction, qty FROM position_state WHERE credential_id=? AND bot_uid=? AND state <> 'closed'",
                Some(bot_uid),
            )
        } else {
            (
                "SELECT symbol_key, direction, qty FROM position_state WHERE credential_id=? AND state <> 'closed'",
                None,
            )
        };
        let mut statement = sqlx::query(query).bind(credential_id);
        if let Some(bot_uid) = bot_uid {
            statement = statement.bind(bot_uid);
        }
        statement
            .fetch_all(store.pool())
            .await?
            .into_iter()
            .filter_map(|row| {
                let symbol_key = row.get::<String, _>("symbol_key");
                match crate::binance_futures::usdm_symbol(&symbol_key) {
                    Some(symbol) => Some((symbol, row.get("direction"), row.get("qty"))),
                    None => {
                        unresolved_positions.push(symbol_key);
                        None
                    }
                }
            })
            .collect()
    } else {
        client
            .positions(RequestClass::EventTruth)
            .await?
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|position| {
                let symbol = position.get("symbol")?.as_str()?.to_string();
                let raw_qty = position.get("positionAmt")?.as_str()?;
                if crate::position_state::is_decimal_zero(raw_qty) {
                    return None;
                }
                let direction = if raw_qty.trim().starts_with('-') {
                    "short"
                } else {
                    "long"
                };
                Some((
                    symbol,
                    direction.to_string(),
                    crate::position_state::decimal_abs(raw_qty).to_string(),
                ))
            })
            .collect()
    };
    let venue = crate::exchange_filters::venue_for_filter_fetch(credential_id).ok();
    for (symbol, direction, qty) in positions {
        if crate::position_state::is_decimal_zero(&qty) {
            continue;
        }
        let side = if direction == "short" {
            crate::exchange_filters::OrderSide::Buy
        } else {
            crate::exchange_filters::OrderSide::Sell
        };
        let constraints = match venue {
            Some(venue) => {
                filter_cache
                    .close_constraints(venue, &symbol, crate::svc::now_ms())
                    .await
            }
            None => Err("credential venue is not configured".to_string()),
        };
        let prepared = crate::exchange_filters::apply_exchange_filters(
            crate::protection::OrderRole::Emergency,
            true,
            side,
            &qty,
            "",
            constraints.as_ref().map_err(String::as_str),
        );
        if matches!(
            prepared.outcome,
            crate::exchange_filters::FilterAuditOutcome::Skipped
        ) {
            tracing::warn!(
                %scope_key,
                %credential_id,
                %symbol,
                detail = %prepared.detail,
                "kill-switch flatten filter normalization skipped; submitting reduce-only close"
            );
            let event_id = crate::svc::sha256_hex(&format!(
                "exchange-filter-skip|{scope_key}|{credential_id}|{symbol}"
            ));
            let audit_result = async {
                let mut audit_tx = store.pool().begin().await?;
                crate::svc::emit_event(
                    &mut audit_tx,
                    store.kind(),
                    credential_id,
                    "filter_skip",
                    &crate::svc::EventContext {
                        bot_uid: scope_key
                            .strip_prefix("bot:")
                            .unwrap_or_default()
                            .to_string(),
                        run_id: "kill_switch".to_string(),
                        cycle_id: scope_key.to_string(),
                        idempotency_key: format!("flatten|{scope_key}|{symbol}"),
                        client_order_id_short: String::new(),
                        exchange: "binance".to_string(),
                        market_kind: "futures".to_string(),
                        symbol_key: symbol.clone(),
                    },
                    "",
                    None,
                    Some(serde_json::json!({
                        "orderRole": "emergency",
                        "rule": "exchange_filters",
                        "result": "skipped",
                        "reason": "not_applicable_to_close_or_hold",
                        "detail": prepared.detail.as_str(),
                    })),
                    Some(event_id),
                )
                .await
                .map_err(|error| anyhow!("persist flatten filter skip audit failed: {error}"))?;
                audit_tx.commit().await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = audit_result {
                // Audit failure must never become a new flatten blocker.
                tracing::error!(
                    %scope_key,
                    %credential_id,
                    %symbol,
                    %error,
                    "kill-switch filter skip audit failed; reduce-only close remains allowed"
                );
            }
        }
        let qty = prepared.quantity;
        let side = side.as_binance();
        let seed = crate::svc::sha256_hex(&format!(
            "kill-switch|{scope_key}|{credential_id}|{symbol}|{side}"
        ));
        let client_order_id = format!("ks_{}", &seed[..26]);
        client
            .place_market_order(&symbol, side, &qty, true, &client_order_id)
            .await?;
    }
    if !unresolved_positions.is_empty() {
        register_unresolved_flatten_positions(
            store,
            credential_id,
            scope_key,
            &unresolved_positions,
        )
        .await;
        return Err(anyhow!(
            "kill-switch flatten could not resolve {} open position row(s) to a {} {} \
             USDⓈ-M identity ({}); no close order was sent for them",
            unresolved_positions.len(),
            crate::binance_futures::USDM_VENUE,
            crate::binance_futures::USDM_MARKET_KIND,
            unresolved_positions.join(", "),
        ));
    }
    Ok(())
}

/// 把「有敞口但认不出交易身份」逐条**登记**成审计事件。
///
/// 登记失败绝不成为新的 flatten 阻塞点（与既有 `filter_skip` 审计同一条纪律）：
/// 处置路径由调用方返回的错误 + 仍然 `flattening` 的 latch 保证，
/// 审计只负责让这件事在库里留痕。
async fn register_unresolved_flatten_positions(
    store: &Store,
    credential_id: &str,
    scope_key: &str,
    symbol_keys: &[String],
) {
    for symbol_key in symbol_keys {
        tracing::error!(
            %scope_key,
            %credential_id,
            %symbol_key,
            "kill-switch flatten found an open position whose symbol_key is not a binance \
             USDⓈ-M identity; it cannot be closed by this engine and the scope stays latched"
        );
        let event_id = crate::svc::sha256_hex(&format!(
            "flatten-identity-unresolved|{scope_key}|{credential_id}|{symbol_key}"
        ));
        let audit = async {
            let mut tx = store.pool().begin().await?;
            crate::svc::emit_event(
                &mut tx,
                store.kind(),
                credential_id,
                "flatten_identity_unresolved",
                &crate::svc::EventContext {
                    bot_uid: scope_key
                        .strip_prefix("bot:")
                        .unwrap_or_default()
                        .to_string(),
                    run_id: "kill_switch".to_string(),
                    cycle_id: scope_key.to_string(),
                    idempotency_key: format!("flatten-unresolved|{scope_key}|{symbol_key}"),
                    client_order_id_short: String::new(),
                    exchange: String::new(),
                    market_kind: String::new(),
                    symbol_key: symbol_key.clone(),
                },
                "",
                None,
                Some(serde_json::json!({
                    "rule": "usdm_identity",
                    "result": "unresolved",
                    "reason": "open_position_symbol_key_is_not_a_usdm_identity",
                    "scopeKey": scope_key,
                    "symbolKey": symbol_key,
                })),
                Some(event_id),
            )
            .await
            .map_err(|error| {
                anyhow!("persist unresolved flatten identity audit failed: {error}")
            })?;
            tx.commit().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = audit {
            tracing::error!(
                %scope_key,
                %credential_id,
                %symbol_key,
                %error,
                "unresolved flatten identity audit failed; the scope remains latched regardless"
            );
        }
    }
}

fn kill_switch_limiter_for_credential(
    credential_id: &str,
) -> anyhow::Result<crate::rate_limiter::CredentialRateLimiter> {
    let venue = crate::venue::venue_for_credential(credential_id).map_err(anyhow::Error::msg)?;
    Ok(crate::rate_limiter::CredentialRateLimiter::shared(
        crate::rate_limiter::TokenBucketConfig::from_env(venue),
    ))
}

/// Decide which credentials in a kill-switch scope may take exchange-side
/// action. Extracted as a pure function so the call site has no polarity to
/// invert: it only iterates the returned list.
fn credentials_to_act_on(
    candidates: &[(String, Option<crate::venue::Venue>)],
    live_enabled: bool,
) -> (Vec<String>, Vec<(String, Option<crate::venue::Venue>)>) {
    let mut act_on = Vec::new();
    let mut skipped = Vec::new();
    for (credential_id, venue) in candidates {
        let kind = crate::venue::credential_kind(credential_id);
        if crate::svc::reduce_only_exit_reaches_exchange_for(kind, live_enabled, *venue) {
            act_on.push(credential_id.clone());
        } else {
            skipped.push((credential_id.clone(), *venue));
        }
    }
    (act_on, skipped)
}

async fn drive_live_scope_once(
    store: &Store,
    scope_key: &str,
    level: Level,
    filter_cache: &crate::exchange_filters::ExchangeFilterCache,
) -> anyhow::Result<()> {
    let live_enabled = crate::svc::is_binance_live_enabled();
    // 逐 credential 隔离：一个 venue 配置坏掉的凭据**不得**让整个 scope 收不到
    // 撤单/平仓。此前这里是 `collect::<Result<Vec<_>>>()?`，第一个坏凭据就会让
    // 后面所有持仓账户永远轮不到处置——而 SQL 的返回顺序不构成任何安全保证。
    let mut candidates = Vec::new();
    let mut per_credential_errors: Vec<String> = Vec::new();
    for credential_id in credentials_for_scope(store, scope_key).await? {
        match crate::venue::venue_for_credential(&credential_id) {
            Ok(venue) => candidates.push((credential_id, venue)),
            Err(error) => {
                tracing::error!(
                    %scope_key,
                    %credential_id,
                    level = level.as_str(),
                    %error,
                    "kill-switch skipped a credential with unreadable venue configuration; \
                     remaining credentials in this scope are still processed"
                );
                per_credential_errors.push(format!("{credential_id}: venue config: {error}"));
            }
        }
    }
    let (act_on_credentials, skipped_credentials) =
        credentials_to_act_on(&candidates, live_enabled);
    for (credential_id, venue) in skipped_credentials {
        let venue = venue
            .map(|venue| venue.to_string())
            .unwrap_or_else(|| "unconfigured".to_string());
        tracing::warn!(
            %scope_key,
            %credential_id,
            level = level.as_str(),
            %venue,
            live_enabled,
            "kill-switch exchange action skipped; gateway disabled"
        );
    }
    for credential_id in act_on_credentials {
        // 每个 credential 独立成败：本轮内把错误收集起来，跑完全部 credential
        // 之后再一并返回，绝不早退。早退 = 后面的账户不撤单不平仓。
        //
        // ****：
        // 「收集错误而不早退」只在对方**会返回**时成立。一个永久 pending 的
        // 交易所请求既不会 Ok 也不会 Err，`await` 就停在这里——后面的账户同样
        // 永远轮不到。所以每个 credential 还要一道**有界 deadline**：
        // 到点就把这个账户记成 failure/unknown 并继续下一个，
        // **绝不把超时当成撤单/平仓成功**。
        let per_credential_deadline_ms = std::env::var("KILL_SWITCH_CREDENTIAL_DEADLINE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(60_000);
        let outcome = with_credential_deadline(
            per_credential_deadline_ms,
            drive_one_credential(store, scope_key, level, filter_cache, &credential_id),
        )
        .await;
        if let Err(error) = outcome {
            tracing::error!(
                %scope_key,
                %credential_id,
                level = level.as_str(),
                %error,
                "kill-switch action failed for one credential; continuing with the rest"
            );
            per_credential_errors.push(format!("{credential_id}: {error}"));
        }
    }
    match scope_error_summary(scope_key, &per_credential_errors) {
        Some(summary) => Err(anyhow!(summary)),
        None => Ok(()),
    }
}

/// 把一张单标记为本地 `canceled`——**只有当它仍处于读取时那个状态**。
///
/// 抽成函数是为了让 CAS 本身可被直接钉住（段级复评的 N4 变异把
/// CAS 谓词改成恒真，23 个用例全绿——因为没有任何用例喂过"读到的状态已经变了"
/// 这种输入）。返回受影响行数：**0 表示这张单在读与写之间被别人动过**，
/// 此时绝不能计入"已撤"。
async fn cancel_order_status_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    engine_order_id: &str,
    expected_status: &str,
    now_ms: i64,
) -> Result<u64, sqlx::Error> {
    let status_lhs = crate::svc::exact_engine_order_status_lhs(db_kind)?;
    let cancel = format!(
        "UPDATE engine_orders SET status='canceled', updated_at_ms=? \
         WHERE engine_order_id=? AND {status_lhs}=?"
    );
    Ok(sqlx::query(&cancel)
        .bind(now_ms.to_string())
        .bind(engine_order_id)
        .bind(expected_status)
        .execute(&mut **tx)
        .await?
        .rows_affected())
}

#[derive(Debug)]
struct PendingRiskEntryCleanupCandidate {
    credential_id: String,
    bot_uid: String,
    symbol_key: String,
    action: String,
    direction: String,
    idempotency_key: String,
    exchange: String,
    market_kind: String,
    position_id: String,
    risk_verdict_id: String,
}

enum PendingRiskCancelTarget {
    MissingOrChanged,
    CancelOnly,
    Entry(Box<PendingRiskEntryCleanupCandidate>),
}

async fn cancel_order_status(
    store: &Store,
    engine_order_id: &str,
    expected_status: &str,
    now_ms: i64,
) -> anyhow::Result<u64> {
    let mut tx = store.pool().begin().await?;
    let canceled = cancel_order_status_in_tx(
        &mut tx,
        store.kind(),
        engine_order_id,
        expected_status,
        now_ms,
    )
    .await?;
    if canceled > 1 {
        tx.rollback().await?;
        return Err(anyhow!(
            "engine-order cancel CAS affected {canceled} rows; expected at most one"
        ));
    }
    tx.commit().await?;
    Ok(canceled)
}

async fn classify_pending_risk_cancel_target(
    store: &Store,
    engine_order_id: &str,
) -> anyhow::Result<PendingRiskCancelTarget> {
    // Read only enough identity to acquire locks in Entry creation order:
    // reservation -> position -> engine order. The exact CAS remains the sole
    // cleanup authority and every identity is re-read after it succeeds.
    let row = sqlx::query(
        "SELECT status, credential_id, bot_uid, symbol_key, action, idempotency_key, \
                exchange, market_kind, reduce_only, order_role, exchange_order_id, \
                position_id, risk_verdict_id \
         FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(engine_order_id)
    .fetch_one(store.pool())
    .await;
    let row = match row {
        Ok(row) => row,
        Err(sqlx::Error::RowNotFound) => return Ok(PendingRiskCancelTarget::MissingOrChanged),
        Err(error) => return Err(error.into()),
    };
    if row.get::<String, _>("status") != "pending_risk" {
        return Ok(PendingRiskCancelTarget::MissingOrChanged);
    }
    let order_role = crate::store::try_get_nullable_string(&row, "order_role")?;
    if matches!(
        order_role.as_deref(),
        Some("exit" | "protection_sl" | "protection_tp" | "emergency")
    ) {
        return Ok(PendingRiskCancelTarget::CancelOnly);
    }
    if order_role.as_deref() != Some("entry") {
        return Err(anyhow!(
            "pending_risk order has an unknown or noncanonical durable role"
        ));
    }

    let action = row.get::<String, _>("action");
    let direction = match action.as_str() {
        "open_long" => "long",
        "open_short" => "short",
        _ => {
            return Err(anyhow!(
                "pending_risk Entry has a noncanonical durable open action"
            ));
        }
    };
    if row.get::<i64, _>("reduce_only") != 0 {
        return Err(anyhow!("pending_risk Entry is unexpectedly reduce-only"));
    }
    let exchange_order_id = crate::store::try_get_nullable_string(&row, "exchange_order_id")?;
    if !exchange_order_id.as_deref().is_none_or(str::is_empty) {
        return Err(anyhow!(
            "pending_risk Entry already has an exchange order identity"
        ));
    }
    let position_id = crate::store::try_get_nullable_string(&row, "position_id")?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("pending_risk Entry has no durable position binding"))?;
    let risk_verdict_id = crate::store::try_get_nullable_string(&row, "risk_verdict_id")?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("pending_risk Entry has no durable risk-verdict binding"))?;
    Ok(PendingRiskCancelTarget::Entry(Box::new(
        PendingRiskEntryCleanupCandidate {
            credential_id: row.get("credential_id"),
            bot_uid: row.get("bot_uid"),
            symbol_key: row.get("symbol_key"),
            action,
            direction: direction.to_string(),
            idempotency_key: row.get("idempotency_key"),
            exchange: row.get("exchange"),
            market_kind: row.get("market_kind"),
            position_id,
            risk_verdict_id,
        },
    )))
}

async fn validate_canceled_entry_identity(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    engine_order_id: &str,
    candidate: &PendingRiskEntryCleanupCandidate,
) -> anyhow::Result<()> {
    let order = sqlx::query(
        "SELECT credential_id, bot_uid, symbol_key, action, idempotency_key, exchange, \
                market_kind, reduce_only, order_role, exchange_order_id, position_id, \
                risk_verdict_id FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(engine_order_id)
    .fetch_one(&mut **tx)
    .await?;
    let final_role = crate::store::try_get_nullable_string(&order, "order_role")?;
    let final_exchange_order_id =
        crate::store::try_get_nullable_string(&order, "exchange_order_id")?;
    let final_position_id = crate::store::try_get_nullable_string(&order, "position_id")?;
    let final_verdict_id = crate::store::try_get_nullable_string(&order, "risk_verdict_id")?;
    if order.get::<String, _>("credential_id") != candidate.credential_id
        || order.get::<String, _>("bot_uid") != candidate.bot_uid
        || order.get::<String, _>("symbol_key") != candidate.symbol_key
        || order.get::<String, _>("action") != candidate.action
        || order.get::<String, _>("idempotency_key") != candidate.idempotency_key
        || order.get::<String, _>("exchange") != candidate.exchange
        || order.get::<String, _>("market_kind") != candidate.market_kind
        || order.get::<i64, _>("reduce_only") != 0
        || final_role.as_deref() != Some("entry")
        || !final_exchange_order_id.as_deref().is_none_or(str::is_empty)
        || final_position_id.as_deref() != Some(candidate.position_id.as_str())
        || final_verdict_id.as_deref() != Some(candidate.risk_verdict_id.as_str())
    {
        return Err(anyhow!(
            "pending_risk Entry identity changed before cancellation ownership was acquired"
        ));
    }
    Ok(())
}

async fn validated_entry_notional(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    candidate: &PendingRiskEntryCleanupCandidate,
) -> anyhow::Result<String> {
    let verdict_select = if db_kind == DbKind::MySql {
        "SELECT risk_verdict_id, intent_id, approved_intent_key, credential_id, bot_uid, \
                symbol_key, decision, sizing_json FROM risk_verdicts \
         WHERE risk_verdict_id=? FOR UPDATE"
    } else {
        "SELECT risk_verdict_id, intent_id, approved_intent_key, credential_id, bot_uid, \
                symbol_key, decision, sizing_json FROM risk_verdicts WHERE risk_verdict_id=?"
    };
    let verdict = sqlx::query(verdict_select)
        .bind(&candidate.risk_verdict_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| match error {
            sqlx::Error::RowNotFound => anyhow!("pending_risk Entry risk verdict is missing"),
            other => other.into(),
        })?;
    let verdict_intent_id = verdict.get::<String, _>("intent_id");
    let approved_intent_key =
        crate::store::try_get_nullable_string(&verdict, "approved_intent_key")?;
    let verdict_decision = verdict.get::<String, _>("decision");
    if verdict.get::<String, _>("risk_verdict_id") != candidate.risk_verdict_id
        || verdict.get::<String, _>("credential_id") != candidate.credential_id
        || verdict.get::<String, _>("bot_uid") != candidate.bot_uid
        || verdict.get::<String, _>("symbol_key") != candidate.symbol_key
        || !matches!(verdict_decision.as_str(), "approve" | "resize")
        || approved_intent_key.as_deref() != Some(verdict_intent_id.as_str())
    {
        return Err(anyhow!(
            "pending_risk Entry risk verdict does not match durable order ownership"
        ));
    }
    let sizing_json = crate::store::try_get_text(&verdict, "sizing_json")?
        .ok_or_else(|| anyhow!("durable Entry sizing is missing"))?;
    let notional = crate::svc::final_notional_from_sizing_json(&sizing_json)?;
    let verdict_references = sqlx::query(
        "SELECT COUNT(*) AS reference_count FROM engine_orders WHERE risk_verdict_id=?",
    )
    .bind(&candidate.risk_verdict_id)
    .fetch_one(&mut **tx)
    .await?
    .get::<i64, _>("reference_count");
    if verdict_references != 1 {
        return Err(anyhow!(
            "pending_risk Entry risk verdict has {verdict_references} engine-order references"
        ));
    }
    Ok(notional)
}

async fn validated_entry_plan_presence(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    candidate: &PendingRiskEntryCleanupCandidate,
) -> anyhow::Result<bool> {
    let plan_select = if db_kind == DbKind::MySql {
        "SELECT credential_id, bot_uid, symbol_key, direction, exchange, market_kind \
         FROM position_protection_plans WHERE position_id=? FOR UPDATE"
    } else {
        "SELECT credential_id, bot_uid, symbol_key, direction, exchange, market_kind \
         FROM position_protection_plans WHERE position_id=?"
    };
    let plans = sqlx::query(plan_select)
        .bind(&candidate.position_id)
        .fetch_all(&mut **tx)
        .await?;
    if plans.len() > 1 {
        return Err(anyhow!(
            "pending_risk Entry has multiple durable protection plans"
        ));
    }
    let plan = plans.into_iter().next();
    if plan.as_ref().is_some_and(|plan| {
        plan.get::<String, _>("credential_id") != candidate.credential_id
            || plan.get::<String, _>("bot_uid") != candidate.bot_uid
            || plan.get::<String, _>("symbol_key") != candidate.symbol_key
            || plan.get::<String, _>("direction") != candidate.direction
            || plan.get::<String, _>("exchange") != candidate.exchange
            || plan.get::<String, _>("market_kind") != candidate.market_kind
    }) {
        return Err(anyhow!(
            "pending_risk Entry protection plan does not match durable order ownership"
        ));
    }
    Ok(plan.is_some())
}

async fn cancel_pending_risk_entry_and_release(
    store: &Store,
    engine_order_id: &str,
    expected_status: &str,
    now_ms: i64,
    candidate: Box<PendingRiskEntryCleanupCandidate>,
) -> anyhow::Result<u64> {
    let mut tx = store.pool().begin().await?;
    let locked = async {
        crate::risk::lock_notional_reservation(&mut tx, store.kind(), &candidate.credential_id)
            .await?;
        crate::position_state::lock_entry_claim(&mut tx, store.kind(), &candidate.position_id).await
    }
    .await;
    let claim = match locked {
        Ok(claim) => claim,
        Err(error) => {
            tx.rollback().await?;
            return Err(anyhow!(error));
        }
    };
    let canceled = cancel_order_status_in_tx(
        &mut tx,
        store.kind(),
        engine_order_id,
        expected_status,
        now_ms,
    )
    .await?;
    if canceled == 0 {
        tx.rollback().await?;
        return Ok(0);
    }
    if canceled != 1 {
        tx.rollback().await?;
        return Err(anyhow!(
            "engine-order cancel CAS affected {canceled} rows; expected at most one"
        ));
    }

    let cleanup: anyhow::Result<()> = async {
        validate_canceled_entry_identity(&mut tx, engine_order_id, &candidate).await?;
        let fills =
            sqlx::query("SELECT COUNT(*) AS fill_count FROM engine_fills WHERE engine_order_id=?")
                .bind(engine_order_id)
                .fetch_one(&mut *tx)
                .await?
                .get::<i64, _>("fill_count");
        if fills != 0 {
            return Err(anyhow!(
                "pending_risk Entry has {fills} durable fill rows; cancellation cleanup refused"
            ));
        }
        let notional = validated_entry_notional(&mut tx, store.kind(), &candidate).await?;
        let plan_exists = validated_entry_plan_presence(&mut tx, store.kind(), &candidate).await?;
        crate::risk::release_notional(&mut tx, &candidate.credential_id, &notional, now_ms).await?;
        let deleted_plans = sqlx::query(
            "DELETE FROM position_protection_plans \
             WHERE position_id=? AND credential_id=? AND bot_uid=? AND symbol_key=?",
        )
        .bind(&candidate.position_id)
        .bind(&candidate.credential_id)
        .bind(&candidate.bot_uid)
        .bind(&candidate.symbol_key)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if deleted_plans != u64::from(plan_exists) {
            return Err(anyhow!(
                "pending_risk Entry protection-plan cleanup affected an unexpected row count"
            ));
        }
        crate::position_state::retire_locked_zero_pending_entry_claim(
            &mut tx,
            store.kind(),
            crate::position_state::PendingEntryClaimRetirement {
                claim: &claim,
                credential_id: &candidate.credential_id,
                bot_uid: &candidate.bot_uid,
                symbol_key: &candidate.symbol_key,
                direction: &candidate.direction,
                idempotency_key: &candidate.idempotency_key,
                ts_ms: now_ms,
            },
        )
        .await?;
        Ok(())
    }
    .await;
    if let Err(error) = cleanup {
        tx.rollback().await?;
        return Err(error);
    }
    tx.commit().await?;
    Ok(1)
}

pub(crate) async fn cancel_order_if_status(
    store: &Store,
    engine_order_id: &str,
    expected_status: &str,
    now_ms: i64,
) -> anyhow::Result<u64> {
    if expected_status != "pending_risk" {
        return cancel_order_status(store, engine_order_id, expected_status, now_ms).await;
    }
    match classify_pending_risk_cancel_target(store, engine_order_id).await? {
        PendingRiskCancelTarget::MissingOrChanged => Ok(0),
        PendingRiskCancelTarget::CancelOnly => {
            cancel_order_status(store, engine_order_id, expected_status, now_ms).await
        }
        PendingRiskCancelTarget::Entry(candidate) => {
            cancel_pending_risk_entry_and_release(
                store,
                engine_order_id,
                expected_status,
                now_ms,
                candidate,
            )
            .await
        }
    }
}

/// 给单个 credential 的处置套一道**有界 deadline**。
///
/// 抽成独立函数是为了让"超时到底落成什么"可被直接断言：**必须是 Err**
/// （随后被调用方收集并继续下一个 credential），**绝不能**是 `Ok(())`——
/// 后者等于把"不知道"记成"已撤单/已平仓"，正是裁决明令禁止的伪造成功。
async fn with_credential_deadline<F>(deadline_ms: u64, work: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    match tokio::time::timeout(Duration::from_millis(deadline_ms), work).await {
        Ok(outcome) => outcome,
        Err(_elapsed) => Err(anyhow!(
            "exchange action unknown: deadline {deadline_ms}ms exceeded; orders and positions \
             for this credential are NOT confirmed cancelled or flat"
        )),
    }
}

/// 把本轮收集到的逐 credential 失败折成一条 scope 级错误。
///
/// 抽成纯函数是为了让「**跑完全部 credential 之后**才报错」这件事可被直接断言：
/// 只要有实现改回「第一个失败就早退」，摘要里就只会出现一个 credential，本函数
/// 的测试随即变红。这条不变量的代价是真实的——早退意味着排在后面的账户
/// 不撤单、不平仓、不收敛交易所真值。
fn scope_error_summary(scope_key: &str, errors: &[String]) -> Option<String> {
    if errors.is_empty() {
        return None;
    }
    Some(format!(
        "kill-switch scope {scope_key} had {} failing credential(s): {}",
        errors.len(),
        errors.join(" | ")
    ))
}

/// 单个 credential 的 kill-switch 处置。抽出来是为了让调用方能逐个隔离错误——
/// 内部保留原有的 `?` 早退语义（对**这一个** credential 而言早退是正确的）。
async fn drive_one_credential(
    store: &Store,
    scope_key: &str,
    level: Level,
    filter_cache: &crate::exchange_filters::ExchangeFilterCache,
    credential_id: &str,
) -> anyhow::Result<()> {
    let credential_id = credential_id.to_string();
    let keys = crate::binance_futures::keys_from_env(&credential_id)
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow!("no exchange credentials available for kill-switch action"))?;
    let client = crate::binance_futures::BinanceFuturesClient::new(keys, &credential_id)?;
    drive_one_credential_with_client(
        store,
        scope_key,
        level,
        filter_cache,
        &credential_id,
        &client,
    )
    .await
}

async fn drive_one_credential_with_client(
    store: &Store,
    scope_key: &str,
    level: Level,
    filter_cache: &crate::exchange_filters::ExchangeFilterCache,
    credential_id: &str,
    client: &crate::binance_futures::BinanceFuturesClient,
) -> anyhow::Result<()> {
    let use_local_429_truth = level == Level::Flatten
        && !scope_key.starts_with("bot:")
        && egress_rate_limited_now(client).await;
    let cancel_error = cancel_live_orders(
        store,
        client,
        credential_id,
        scope_key,
        level,
        use_local_429_truth,
    )
    .await
    .err();
    if level == Level::Flatten {
        // Even if cancel or submit reports an error (including an idempotent
        // duplicate), force exchange reconciliation. Only truth convergence,
        // never an endpoint acknowledgement, completes L3.
        let submit_error = submit_live_flatten_with_truth_mode(
            store,
            client,
            credential_id,
            scope_key,
            filter_cache,
            use_local_429_truth,
        )
        .await
        .err();
        if use_local_429_truth {
            let until_ms = client
                .active_429_cooldown_until_ms()
                .await?
                .unwrap_or_default();
            // A reduce-only order cannot increase exposure. During a 429
            // park, bounded local cancels plus local-position flattening are
            // strictly better than today's zero-action path. Any orphan the
            // local database cannot know remains for the unchanged
            // exchange-truth convergence after the window.
            tracing::warn!(
                %scope_key,
                %credential_id,
                until_ms,
                cancel_failed = cancel_error.is_some(),
                submit_failed = submit_error.is_some(),
                error_code = "KILL_SWITCH_L3_DEFERRED_BY_429",
                "L3 used bounded local truth during a 429 window; convergence is deferred"
            );
            return Err(anyhow!(
                "L3 truth convergence deferred: 429 egress cooldown active until_ms={until_ms}"
            ));
        }
        let limiter = kill_switch_limiter_for_credential(credential_id)?;
        let truth = crate::reconcile::reconcile_binance_futures_truth_with_client(
            credential_id,
            store.clone(),
            limiter,
            client,
        )
        .await?;
        if !truth.flatten_complete() {
            return Err(anyhow!(
                    "flatten incomplete: positions_zero={} orders_empty={} algo_orders_empty={} local_zero_diff={} has_diff={} cancel_failed={} submit_failed={}{}",
                    truth.all_positions_zero,
                    truth.all_open_orders_empty,
                    truth.all_open_algo_orders_empty,
                    truth.local_position_zero_diff,
                    truth.has_diff,
                    cancel_error.is_some(),
                    submit_error.is_some(),
                    cancel_error
                        .as_ref()
                        .map(|error| format!("; cancel errors: {error:#}"))
                        .unwrap_or_default(),
                ));
        }
        if let Some(error) = cancel_error.as_ref() {
            // Only exchange-truth convergence completes L3; cancellation
            // failures that remain unresolved locally stay visible.
            tracing::warn!(
                %scope_key,
                %credential_id,
                error = %format!("{error:#}"),
                error_code = "KILL_SWITCH_L3_CANCEL_UNRESOLVED",
                "L3 exchange truth converged while some cancellation attempts failed"
            );
        }
    } else if let Some(error) = cancel_error {
        return Err(error);
    }
    Ok(())
}

async fn egress_rate_limited_now(client: &crate::binance_futures::BinanceFuturesClient) -> bool {
    client
        .active_429_cooldown_until_ms()
        .await
        .ok()
        .flatten()
        .is_some()
}

/// 测试入口：后台重试任务每轮调用的同一个生产 scope 驱动（调用方传入固定
/// 真值的过滤器缓存，避免公开 exchangeInfo 读取触网）。
#[cfg(test)]
pub(crate) async fn drive_live_scope_once_for_tests(
    store: &Store,
    scope_key: &str,
    level: Level,
    filter_cache: &crate::exchange_filters::ExchangeFilterCache,
) -> anyhow::Result<()> {
    drive_live_scope_once(store, scope_key, level, filter_cache).await
}

pub fn spawn_action_retry_task(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let filter_cache = crate::exchange_filters::ExchangeFilterCache::shared();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // 1 Hz 打点保留，好让刚闩上的 scope 一秒内被处置；反复失败的 scope 由
        // pacer 按退避跳过，否则单个卡住的 flatten 就要 10500 权重/分钟。
        let mut pacer = ActionRetryPacer::new();
        loop {
            ticker.tick().await;
            let targets = action_retry_targets(&store).await.unwrap_or_default();
            pacer.retain_only(&targets.iter().map(|t| t.scope_key.clone()).collect());
            for ActionRetryTarget {
                scope_key,
                level,
                generation,
                invalid_level,
            } in targets
            {
                let now_ms = crate::svc::now_ms();
                if !pacer.is_due(&scope_key, generation, now_ms) {
                    continue;
                }
                // A conservative flatten may prove the exchange safe, but it
                // cannot repair or explain the invalid durable authority row.
                // Keep retrying with the normal bounded backoff until an
                // operator repairs the persisted token explicitly.
                let mut succeeded = invalid_level.is_none();
                if let Some(invalid) = &invalid_level {
                    tracing::error!(
                        scope_key = %invalid.scope_key,
                        raw_level = ?invalid.raw_level,
                        conservative_level = Level::Flatten.as_str(),
                        error_code = "KILL_SWITCH_INVALID_PERSISTED_LEVEL",
                        "invalid durable kill-switch level; forcing flatten and retaining retry"
                    );
                }
                if let Err(error) =
                    apply_local_order_semantics(&store, &scope_key, level, now_ms).await
                {
                    succeeded = false;
                    tracing::error!(%scope_key, level=level.as_str(), %error, "kill-switch local action incomplete; retrying");
                }
                if let Err(error) =
                    drive_live_scope_once(&store, &scope_key, level, &filter_cache).await
                {
                    // Remain latched and retry. Request acknowledgements alone
                    // never move `flattening` to `flattened`.
                    succeeded = false;
                    tracing::error!(%scope_key, level=level.as_str(), %error, "kill-switch action incomplete; retrying");
                }
                pacer.record(&scope_key, generation, succeeded, crate::svc::now_ms());
            }
        }
    })
}

pub fn default_kill_file() -> PathBuf {
    std::env::var("KILL_SWITCH_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new("./KILL_SWITCH").to_path_buf())
}

#[cfg(test)]
mod tests {
    include!("kill_switch_authority_tests.rs");
    include!("kill_switch_offline_test_env.rs");

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex, Once};

    use sqlx::Row;

    use super::*;

    static INSTALL_DRIVERS: Once = Once::new();

    #[test]
    fn action_retry_backoff_climbs_then_stops_at_the_cap() {
        // 未失败过 = 立即到期，闩上就动作。
        assert_eq!(action_retry_backoff_ms(0), 0);
        assert_eq!(action_retry_backoff_ms(1), 1_000);
        assert_eq!(action_retry_backoff_ms(2), 2_000);
        assert_eq!(action_retry_backoff_ms(6), 32_000);
        // 上限存在，且不会被后续失败推高——没有上限的退避等于悄悄放弃 L3。
        assert_eq!(action_retry_backoff_ms(7), ACTION_RETRY_BACKOFF_CAP_MS);
        assert_eq!(action_retry_backoff_ms(40), ACTION_RETRY_BACKOFF_CAP_MS);
        assert_eq!(
            action_retry_backoff_ms(u32::MAX),
            ACTION_RETRY_BACKOFF_CAP_MS
        );
    }

    #[test]
    fn a_newly_latched_scope_is_due_immediately() {
        let pacer = ActionRetryPacer::new();
        assert!(pacer.is_due("bot:fresh", 1, 0));
        assert!(pacer.is_due("bot:fresh", 1, 1_788_000_000_000));
    }

    #[test]
    fn a_failing_scope_is_skipped_until_its_backoff_elapses() {
        let mut pacer = ActionRetryPacer::new();
        let now = 1_788_000_000_000_i64;
        pacer.record("cred:1", 1, false, now);

        // 退避窗内不得重入——这一条就是 10500 权重/分钟与 175 权重/分钟的差别。
        assert!(!pacer.is_due("cred:1", 1, now));
        assert!(!pacer.is_due("cred:1", 1, now + 999));
        // 抖动只会把到期时间往后推，绝不会提前，所以基线之后总有一刻到期。
        assert!(pacer.is_due("cred:1", 1, now + 1_000 + 2_000));

        // 连续失败继续拉长，但拉不过上限。
        for _ in 0..20 {
            pacer.record("cred:1", 1, false, now);
        }
        assert!(!pacer.is_due("cred:1", 1, now + ACTION_RETRY_BACKOFF_CAP_MS as i64 - 1));
    }

    #[test]
    fn saturated_pacer_due_never_exceeds_the_final_cap_even_with_maximum_jitter() {
        let retry_number = 7_u32;
        let scope = (0..100_000)
            .map(|suffix| format!("cred:max-jitter-{suffix}"))
            .find(|scope| {
                crate::rate_limiter::deterministic_jitter_ms(scope, retry_number) == 1_000
            })
            .expect("the deterministic jitter range contains its maximum");
        assert_eq!(
            crate::rate_limiter::deterministic_jitter_ms(&scope, retry_number),
            1_000
        );
        let mut pacer = ActionRetryPacer::new();
        let now = 1_788_000_000_000_i64;
        for _ in 0..retry_number {
            pacer.record(&scope, 1, false, now);
        }
        assert!(
            pacer.is_due(&scope, 1, now + ACTION_RETRY_BACKOFF_CAP_MS as i64),
            "the final pacer due time, including jitter, must stay within 60 seconds"
        );
    }

    #[test]
    fn a_successful_attempt_restores_full_1hz_responsiveness() {
        let mut pacer = ActionRetryPacer::new();
        let now = 1_788_000_000_000_i64;
        for _ in 0..8 {
            pacer.record("cred:1", 1, false, now);
        }
        assert!(!pacer.is_due("cred:1", 1, now + 1_000));

        // 收敛之后必须立刻恢复 1 Hz 响应力：下一次闩上不能继承上一轮的退避。
        pacer.record("cred:1", 1, true, now);
        assert!(pacer.is_due("cred:1", 1, now));
    }

    #[test]
    fn a_new_generation_on_a_backed_off_scope_is_due_immediately() {
        let mut pacer = ActionRetryPacer::new();
        let now = 1_788_000_000_000_i64;
        for _ in 0..8 {
            pacer.record("cred:1", 7, false, now);
        }
        assert!(!pacer.is_due("cred:1", 7, now + 1_000), "第 7 代仍在退避");

        // 同一 scope 上出现新一代裸敞口（latch 推进了 flatten_generation）。
        // 它从未离开扫描集合，retain_only 救不了它；不看代际就会继承 60 秒退避，
        // 新敞口的首次重试被拖到一分钟后。
        assert!(pacer.is_due("cred:1", 8, now), "新一代必须立即到期");

        // 新一代自己的失败从第一次重新计数，而不是接着第 8 次。
        pacer.record("cred:1", 8, false, now);
        assert!(!pacer.is_due("cred:1", 8, now + 999));
        assert!(
            pacer.is_due("cred:1", 8, now + 1_000 + 2_000),
            "只退了第一档，不是上限"
        );
    }

    #[test]
    fn a_scope_that_leaves_the_scan_set_keeps_no_backoff() {
        let mut pacer = ActionRetryPacer::new();
        let now = 1_788_000_000_000_i64;
        for _ in 0..8 {
            pacer.record("cred:1", 1, false, now);
        }
        assert!(!pacer.is_due("cred:1", 1, now + 1_000));

        // reset 之后 scope 离开扫描集合；再次闩上时必须立即动作，而不是
        // 继承一个最长 60 秒的退避——那会把 L3 的首次响应推迟一分钟。
        let live: HashSet<String> = ["cred:2".to_string()].into_iter().collect();
        pacer.retain_only(&live);
        assert!(pacer.is_due("cred:1", 1, now));
    }

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-kill-switch-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect kill-switch sqlite store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn seed_pending_entry_cleanup_fixture(store: &Store) {
        let authority_key = crate::position_state::semantic_idempotency_key(
            "entry|bot-cleanup|binance|futures|BTC|USDT|USDT|long|1",
        );
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin Entry cleanup fixture");
        sqlx::query(
            "INSERT INTO notional_reservations \
             (credential_id, reserved_usdt, cap_usdt, updated_at_ms) \
             VALUES ('cred-cleanup', '100', '5000', 1)",
        )
        .execute(&mut *tx)
        .await
        .expect("seed cleanup reservation");
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES ('pos-cleanup', 'cred-cleanup', 'bot-cleanup',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'pending',
              'bot-cleanup|binance|futures|BTC|USDT|USDT|long', ?, '0', 1)
            "#,
        )
        .bind(&authority_key)
        .execute(&mut *tx)
        .await
        .expect("seed cleanup position");
        sqlx::query(
            r#"
            INSERT INTO risk_verdicts (
              risk_verdict_id, intent_id, approved_intent_key, credential_id,
              bot_uid, symbol_key, created_at_ms, policy_version, policy_hash,
              decision, sizing_json, rules_json, evaluator_version
            ) VALUES ('rv-cleanup', 'intent-cleanup', 'intent-cleanup', 'cred-cleanup',
              'bot-cleanup', 'binance|futures|BTC|USDT|USDT', 1, 'test', 'hash',
              'approve', '{"final_notional_usdt":"100"}', '[]', 'test')
            "#,
        )
        .execute(&mut *tx)
        .await
        .expect("seed cleanup verdict");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, risk_verdict_id,
              position_id, created_at_ms, updated_at_ms
            ) VALUES ('ord-cleanup', 'cred-cleanup', 'bot-cleanup', ?,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'open_long', 'long', 'market', '1', 0, 'entry', 'pending_risk',
              'rv-cleanup', 'pos-cleanup', 1, 1)
            "#,
        )
        .bind(&authority_key)
        .execute(&mut *tx)
        .await
        .expect("seed cleanup order");
        sqlx::query(
            r#"
            INSERT INTO position_protection_plans (
              position_id, credential_id, bot_uid, exchange, market_kind,
              symbol_key, direction, stop_loss_price, take_profit_price,
              required, confirm_timeout_ms, generation, created_at_ms, updated_at_ms
            ) VALUES ('pos-cleanup', 'cred-cleanup', 'bot-cleanup', 'binance',
              'futures', 'binance|futures|BTC|USDT|USDT', 'long', '95', '110',
              1, 3000, 1, 1, 1)
            "#,
        )
        .execute(&mut *tx)
        .await
        .expect("seed cleanup plan");
        tx.commit().await.expect("commit Entry cleanup fixture");
    }

    async fn bind_existing_pending_risk_entry_authority(
        store: &Store,
        engine_order_id: &str,
        credential_id: &str,
        bot_uid: &str,
    ) {
        let symbol_key = "binance|futures|BTC|USDT|USDT";
        let position_id = format!("pos-{engine_order_id}");
        let verdict_id = format!("rv-{engine_order_id}");
        let intent_id = format!("intent-{engine_order_id}");
        let authority_key = crate::position_state::semantic_idempotency_key(&format!(
            "entry|{bot_uid}|{symbol_key}|long|1"
        ));
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin pending-risk authority fixture");
        sqlx::query(
            "INSERT INTO notional_reservations \
             (credential_id, reserved_usdt, cap_usdt, updated_at_ms) VALUES (?, '100', '5000', 1)",
        )
        .bind(credential_id)
        .execute(&mut *tx)
        .await
        .expect("seed pending-risk reservation");
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES (?, ?, ?, ?, 'long', 1, 'pending', ?, ?, '0', 1)
            "#,
        )
        .bind(&position_id)
        .bind(credential_id)
        .bind(bot_uid)
        .bind(symbol_key)
        .bind(crate::position_state::active_key(
            bot_uid, symbol_key, "long",
        ))
        .bind(&authority_key)
        .execute(&mut *tx)
        .await
        .expect("seed pending-risk position claim");
        sqlx::query(
            r#"
            INSERT INTO risk_verdicts (
              risk_verdict_id, intent_id, approved_intent_key, credential_id,
              bot_uid, symbol_key, created_at_ms, policy_version, policy_hash,
              decision, sizing_json, rules_json, evaluator_version
            ) VALUES (?, ?, ?, ?, ?, ?, 1, 'test', 'hash', 'approve',
              '{"final_notional_usdt":"100"}', '[]', 'test')
            "#,
        )
        .bind(&verdict_id)
        .bind(&intent_id)
        .bind(&intent_id)
        .bind(credential_id)
        .bind(bot_uid)
        .bind(symbol_key)
        .execute(&mut *tx)
        .await
        .expect("seed pending-risk verdict");
        let rows = sqlx::query(
            "UPDATE engine_orders SET idempotency_key=?, position_id=?, risk_verdict_id=? \
             WHERE engine_order_id=? AND status='pending_risk' AND order_role='entry'",
        )
        .bind(&authority_key)
        .bind(&position_id)
        .bind(&verdict_id)
        .bind(engine_order_id)
        .execute(&mut *tx)
        .await
        .expect("bind pending-risk order authority")
        .rows_affected();
        assert_eq!(rows, 1, "fixture must bind one pending-risk Entry");
        tx.commit()
            .await
            .expect("commit pending-risk authority fixture");
    }

    async fn assert_entry_cleanup_resources_unchanged(store: &Store, order_status: &str) {
        let status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='ord-cleanup'")
                .fetch_one(store.pool())
                .await
                .expect("load cleanup order")
                .get::<String, _>("status");
        assert_eq!(status, order_status);
        let reserved = sqlx::query(
            "SELECT reserved_usdt FROM notional_reservations WHERE credential_id='cred-cleanup'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load cleanup reservation")
        .get::<String, _>("reserved_usdt")
        .parse::<f64>()
        .expect("numeric cleanup reservation");
        assert_eq!(reserved, 100.0);
        let position = sqlx::query(
            "SELECT state, active_key, pending_idem_key, entry_epoch \
             FROM position_state WHERE position_id='pos-cleanup'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load cleanup position");
        assert_eq!(position.get::<String, _>("state"), "pending");
        assert!(!position.get::<String, _>("active_key").is_empty());
        assert!(!position.get::<String, _>("pending_idem_key").is_empty());
        assert_eq!(position.get::<i64, _>("entry_epoch"), 1);
        let plans = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans \
             WHERE position_id='pos-cleanup'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count cleanup plans")
        .get::<i64, _>("count");
        assert_eq!(plans, 1);
    }

    async fn state(store: &Store, scope_key: &str) -> (String, Option<String>) {
        let row = sqlx::query(
            "SELECT level, COALESCE(flatten_state, '') AS flatten_state FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("load switch state");
        (
            row.get("level"),
            nonempty(row.get::<String, _>("flatten_state")),
        )
    }

    async fn seed_raw_switch_state(
        store: &Store,
        scope_key: &str,
        raw_level: &str,
        flatten_state: Option<&str>,
        generation: i64,
    ) {
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, last_reconcile_clean, last_reconcile_at_ms, \
             updated_at_ms, flatten_generation) VALUES (?, ?, 'raw fixture', \
             'test_fixture', 10, ?, 1, 11, 12, ?)",
        )
        .bind(scope_key)
        .bind(raw_level)
        .bind(flatten_state)
        .bind(generation)
        .execute(store.pool())
        .await
        .expect("seed raw kill-switch state");
    }

    #[test]
    fn persisted_level_decoder_accepts_only_the_four_canonical_tokens() {
        for (raw, expected) in [
            ("none", Level::None),
            ("pause_new", Level::PauseNew),
            ("manage_only", Level::ManageOnly),
            ("flatten", Level::Flatten),
        ] {
            assert_eq!(parse_persisted_level(raw), Some(expected));
        }

        for raw in [
            " none",
            "none ",
            "NONE",
            "Pause_New",
            "MANAGE_ONLY",
            "FLATTEN",
            "flatten\n",
            "",
        ] {
            assert_eq!(
                parse_persisted_level(raw),
                None,
                "durable token {raw:?} must remain invalid"
            );
        }

        assert_eq!(
            Level::parse(" \nflatten\t"),
            Some(Level::Flatten),
            "API/file parsing must retain its existing trim semantics"
        );
    }

    #[test]
    fn exchange_gateway_policy_is_applied_inside_the_credential_loop() {
        let source = include_str!("kill_switch.rs");
        let live_driver = source
            .split("async fn drive_live_scope_once")
            .nth(1)
            .expect("live driver")
            .split("pub fn spawn_action_retry_task")
            .next()
            .expect("live driver body");
        let selection_offset = live_driver
            .find("credentials_to_act_on(&candidates, live_enabled)")
            .expect("shared credential selection");
        let action_loop_offset = live_driver
            .find("for credential_id in act_on_credentials")
            .expect("selected credential loop");
        assert!(selection_offset < action_loop_offset);
        assert!(
            !live_driver.contains("exchange_gateway_allowed_for"),
            "live driver must iterate the pure selection result without reapplying policy"
        );

        let retry_worker = source
            .split("pub fn spawn_action_retry_task")
            .nth(1)
            .expect("retry worker")
            .split("pub fn default_kill_file")
            .next()
            .expect("retry worker body");
        assert!(
            !retry_worker.contains("is_binance_live_enabled()"),
            "scope-wide live guard would bypass testnet credentials"
        );
        assert!(
            !source.contains("\"ENGINE_LIVE_ENABLED\""),
            "kill_switch must not parse ENGINE_LIVE_ENABLED independently"
        );
    }

    fn mixed_gateway_candidates() -> Vec<(String, Option<crate::venue::Venue>)> {
        vec![
            ("testnet-a".to_string(), Some(crate::venue::Venue::Testnet)),
            ("mainnet-a".to_string(), Some(crate::venue::Venue::Mainnet)),
            ("unconfigured-a".to_string(), None),
            ("testnet-b".to_string(), Some(crate::venue::Venue::Testnet)),
        ]
    }

    /// D39（WO-M6）：实盘总开关只拦新增风险。kill switch 的交易所动作（撤单、
    /// reduce-only flatten）在总开关关闭时仍作用于**显式配置了 venue** 的交易所
    /// 凭据（testnet 与 mainnet）；未配置 venue 的旧式凭据维持「不出站」。
    #[test]
    fn credentials_to_act_on_selects_configured_venues_when_live_is_disabled() {
        let (act_on, skipped) = credentials_to_act_on(&mixed_gateway_candidates(), false);

        assert_eq!(act_on.len(), 3);
        assert_eq!(
            act_on,
            vec![
                "testnet-a".to_string(),
                "mainnet-a".to_string(),
                "testnet-b".to_string()
            ]
        );
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped, vec![("unconfigured-a".to_string(), None)]);
    }

    #[test]
    fn credentials_to_act_on_selects_every_candidate_when_live_is_enabled() {
        let (act_on, skipped) = credentials_to_act_on(&mixed_gateway_candidates(), true);

        assert_eq!(act_on.len(), 4);
        assert_eq!(
            act_on,
            vec![
                "testnet-a".to_string(),
                "mainnet-a".to_string(),
                "unconfigured-a".to_string(),
                "testnet-b".to_string(),
            ]
        );
        assert_eq!(skipped.len(), 0);
        assert_eq!(skipped, Vec::<(String, Option<crate::venue::Venue>)>::new());
    }

    /// D39 · 紧急 flatten 与单仓 reduce-only 平仓同一判据：凭据选择逐个等于
    /// `reduce_only_exit_reaches_exchange_for`（档位闸门的 LiveDisabled 豁免读的
    /// 也是它）。
    #[test]
    fn credentials_to_act_on_follows_the_reduce_only_exit_criterion() {
        let candidates = mixed_gateway_candidates();
        for live_enabled in [false, true] {
            let (act_on, skipped) = credentials_to_act_on(&candidates, live_enabled);
            for (credential_id, venue) in &candidates {
                let expected = crate::svc::reduce_only_exit_reaches_exchange_for(
                    crate::venue::credential_kind(credential_id),
                    live_enabled,
                    *venue,
                );
                assert_eq!(
                    act_on.contains(credential_id),
                    expected,
                    "{credential_id} live_enabled={live_enabled}"
                );
                assert_eq!(skipped.iter().any(|(id, _)| id == credential_id), !expected);
            }
        }
    }

    #[tokio::test]
    async fn migration_seeds_determinate_global_none() {
        let (store, path) = test_store("seed").await;
        assert_eq!(state(&store, "global").await, ("none".to_string(), None));
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn l1_l2_l3_each_arm_and_manual_reset_three_rounds() {
        let _offline = offline_order_test_env("rounds");
        let (store, path) = test_store("rounds").await;
        for (level, expected_flatten) in [
            (Level::PauseNew, None),
            (Level::ManageOnly, None),
            (Level::Flatten, Some("flattening".to_string())),
        ] {
            for round in 0..3 {
                let armed_at = 1_000 + i64::from(round) * 10;
                let entry_id = format!("{}-{round}-entry", level.as_str());
                let protection_id = format!("{}-{round}-protection", level.as_str());
                for (order_id, action, order_type, reduce_only, order_role) in [
                    (&entry_id, "open_long", "limit", 0i64, "entry"),
                    (
                        &protection_id,
                        "close_long",
                        "stop_market",
                        1i64,
                        "protection_sl",
                    ),
                ] {
                    sqlx::query(
                        r#"
                        INSERT INTO engine_orders (
                          engine_order_id, credential_id, bot_uid, idempotency_key,
                          exchange, market_kind, symbol_key, action, side, order_type,
                          quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
                        ) VALUES (?, 'rounds', 'bot-rounds', ?, 'binance', 'futures',
                          'binance|futures|BTC|USDT|USDT', ?, 'long', ?, '1', ?,
                          ?, 'acked', ?, ?)
                        "#,
                    )
                    .bind(order_id)
                    .bind(format!("idem-{order_id}"))
                    .bind(action)
                    .bind(order_type)
                    .bind(reduce_only)
                    .bind(order_role)
                    .bind(armed_at)
                    .bind(armed_at)
                    .execute(store.pool())
                    .await
                    .expect("insert three-round action fixture");
                }
                arm(
                    &store,
                    ArmRequest {
                        scope_key: "cred:rounds",
                        level,
                        trigger_source: "manual_api",
                        reason: "offline three-round exercise",
                        now_ms: armed_at,
                    },
                )
                .await
                .expect("arm layer");
                assert_eq!(
                    state(&store, "cred:rounds").await,
                    (level.as_str().to_string(), expected_flatten.clone())
                );
                apply_local_order_semantics(&store, "cred:rounds", level, armed_at + 1)
                    .await
                    .expect("apply layer order semantics");
                let order_rows = sqlx::query(
                    "SELECT engine_order_id, status FROM engine_orders WHERE engine_order_id=? OR engine_order_id=?",
                )
                .bind(&entry_id)
                .bind(&protection_id)
                .fetch_all(store.pool())
                .await
                .expect("read three-round action result");
                let order_status = |wanted: &str| {
                    order_rows
                        .iter()
                        .find(|row| row.get::<String, _>("engine_order_id") == wanted)
                        .map(|row| row.get::<String, _>("status"))
                        .expect("three-round fixture status")
                };
                match level {
                    Level::PauseNew => {
                        assert_eq!(order_status(&entry_id), "acked");
                        assert_eq!(order_status(&protection_id), "acked");
                    }
                    Level::ManageOnly => {
                        assert_eq!(order_status(&entry_id), "canceled");
                        assert_eq!(order_status(&protection_id), "acked");
                    }
                    Level::Flatten => {
                        assert_eq!(order_status(&entry_id), "canceled");
                        assert_eq!(order_status(&protection_id), "canceled");
                    }
                    Level::None => unreachable!(),
                }

                let clean_truth = ReconcileTruth {
                    has_diff: false,
                    all_positions_zero: true,
                    all_open_orders_empty: true,
                    all_open_algo_orders_empty: true,
                    local_position_zero_diff: true,
                };
                if level == Level::Flatten {
                    let fake = FakeExchange {
                        reconciles: Mutex::new(VecDeque::from([clean_truth])),
                        algo_cancels: AtomicUsize::new(0),
                        cancels: AtomicUsize::new(0),
                        flattens: AtomicUsize::new(0),
                    };
                    let truth =
                        drive_flatten_once(&store, "cred:rounds", "rounds", &fake, armed_at + 1)
                            .await
                            .expect("drive offline L3 flatten");
                    assert!(truth.flatten_complete());
                    assert_eq!(fake.algo_cancels.load(AtomicOrdering::SeqCst), 1);
                    assert_eq!(fake.cancels.load(AtomicOrdering::SeqCst), 1);
                    assert_eq!(fake.flattens.load(AtomicOrdering::SeqCst), 1);
                } else {
                    record_reconcile_for_scope_now(
                        &store,
                        "cred:rounds",
                        "rounds",
                        clean_truth,
                        armed_at + 1,
                    )
                    .await
                    .expect("record clean reconcile");
                }
                reset(&store, "cred:rounds", "test-admin", None, armed_at + 2)
                    .await
                    .expect("manual reset");
                assert_eq!(state(&store, "cred:rounds").await.0, "none");
            }
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn effective_scope_uses_strictest_level_and_global_tiebreak() {
        let (store, path) = test_store("scope").await;
        for (scope_key, level, now_ms) in [
            ("bot:bot-a", Level::ManageOnly, 100),
            ("cred:cred-a", Level::Flatten, 101),
            ("global", Level::PauseNew, 102),
        ] {
            arm(
                &store,
                ArmRequest {
                    scope_key,
                    level,
                    trigger_source: "manual_api",
                    reason: "scope test",
                    now_ms,
                },
            )
            .await
            .expect("arm scope");
        }
        let mut tx = store.pool().begin().await.expect("begin scope read");
        let effective = effective_in_tx(&mut tx, "cred-a", "bot-a")
            .await
            .expect("read effective");
        assert!(effective.invalid_levels.is_empty());
        let effective = effective.effective.expect("determinate state");
        tx.rollback().await.expect("rollback read");
        assert_eq!(effective.level, Level::Flatten);
        assert_eq!(effective.scope_key, "cred:cred-a");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persisted_level_effective_read_reports_invalid_without_losing_valid_sibling() {
        let (store, path) = test_store("strict-effective-sibling").await;
        arm(
            &store,
            ArmRequest {
                scope_key: "cred:strict-effective",
                level: Level::ManageOnly,
                trigger_source: "manual_api",
                reason: "valid sibling",
                now_ms: 100,
            },
        )
        .await
        .expect("arm the valid credential sibling");
        sqlx::query("UPDATE kill_switch_state SET level=' flatten ' WHERE scope_key='global'")
            .execute(store.pool())
            .await
            .expect("corrupt the global durable token");

        let mut tx = store.pool().begin().await.expect("begin effective read");
        let read = effective_in_tx(&mut tx, "strict-effective", "unrelated-bot")
            .await
            .expect("read all applicable scopes");
        tx.rollback().await.expect("rollback effective read");

        assert_eq!(
            read.effective,
            Some(EffectiveState {
                scope_key: "cred:strict-effective".to_string(),
                level: Level::ManageOnly,
                reason: Some("valid sibling".to_string()),
                flatten_state: None,
            }),
            "the malformed global row must not discard a valid sibling"
        );
        assert_eq!(
            read.invalid_levels,
            vec![InvalidPersistedLevel {
                scope_key: "global".to_string(),
                raw_level: " flatten ".to_string(),
            }]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persisted_level_arm_refuses_to_overwrite_invalid_raw_authority() {
        let (store, path) = test_store("strict-arm-preserves-raw").await;
        let scope_key = "cred:strict-arm";
        seed_raw_switch_state(&store, scope_key, " manage_only ", None, 7).await;

        let mut tx = store.pool().begin().await.expect("begin arm");
        let error = arm_in_tx(
            &mut tx,
            store.kind(),
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "must not replace corrupt state",
                now_ms: 200,
            },
        )
        .await
        .expect_err("an invalid durable token must block the write");
        assert_eq!(
            error.downcast_ref::<InvalidPersistedLevel>(),
            Some(&InvalidPersistedLevel {
                scope_key: scope_key.to_string(),
                raw_level: " manage_only ".to_string(),
            }),
            "the arm error must retain its typed diagnostic"
        );
        tx.rollback().await.expect("rollback rejected arm");

        let persisted: (String, i64) = sqlx::query_as(
            "SELECT level, flatten_generation FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("reload rejected arm state");
        assert_eq!(persisted, (" manage_only ".to_string(), 7));

        let force_new_error = latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "force-new must also preserve corrupt state",
            201,
        )
        .await
        .expect_err("the force-new writer must use the same strict decoder");
        assert!(force_new_error
            .downcast_ref::<InvalidPersistedLevel>()
            .is_some());
        assert_eq!(state(&store, scope_key).await.0, " manage_only ");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persisted_level_reset_uses_strict_decoder_and_preserves_raw_authority() {
        let (store, path) = test_store("strict-reset-preserves-raw").await;
        let scope_key = "cred:strict-reset";
        seed_raw_switch_state(&store, scope_key, " flatten ", Some(FLATTEN_CONVERGED), 3).await;

        let error = reset(&store, scope_key, "operator", None, 300)
            .await
            .expect_err("reset must reject a noncanonical durable level");
        assert_eq!(
            error.downcast_ref::<InvalidPersistedLevel>(),
            Some(&InvalidPersistedLevel {
                scope_key: scope_key.to_string(),
                raw_level: " flatten ".to_string(),
            })
        );
        assert_eq!(state(&store, scope_key).await.0, " flatten ");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persisted_level_retry_keeps_invalid_and_valid_siblings_despite_flattened() {
        let (store, path) = test_store("strict-retry-siblings").await;
        seed_raw_switch_state(
            &store,
            "cred:invalid-retry",
            "MANAGE_ONLY",
            Some(FLATTEN_CONVERGED),
            9,
        )
        .await;
        seed_raw_switch_state(&store, "cred:valid-retry", "manage_only", None, 4).await;
        seed_raw_switch_state(
            &store,
            "cred:converged-retry",
            "flatten",
            Some(FLATTEN_CONVERGED),
            5,
        )
        .await;

        let targets = action_retry_targets(&store)
            .await
            .expect("scan every durable state row");
        assert_eq!(
            targets.len(),
            2,
            "only the invalid and manage-only rows retry"
        );

        let invalid = targets
            .iter()
            .find(|target| target.scope_key == "cred:invalid-retry")
            .expect("invalid row must remain in the retry set");
        assert_eq!(invalid.level, Level::Flatten);
        assert_eq!(invalid.generation, 9);
        assert_eq!(
            invalid.invalid_level,
            Some(InvalidPersistedLevel {
                scope_key: "cred:invalid-retry".to_string(),
                raw_level: "MANAGE_ONLY".to_string(),
            }),
            "invalid authority is carried alongside the conservative action"
        );

        let valid = targets
            .iter()
            .find(|target| target.scope_key == "cred:valid-retry")
            .expect("valid sibling must not be lost");
        assert_eq!(valid.level, Level::ManageOnly);
        assert_eq!(valid.generation, 4);
        assert_eq!(valid.invalid_level, None);
        assert!(targets
            .iter()
            .all(|target| target.scope_key != "cred:converged-retry"));
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn reset_rejects_diff_and_automatic_trigger_never_self_recovers() {
        let (store, path) = test_store("reset").await;
        arm(
            &store,
            ArmRequest {
                scope_key: "cred:auto",
                level: Level::ManageOnly,
                trigger_source: "auto_daily_loss",
                reason: "daily loss",
                now_ms: 100,
            },
        )
        .await
        .expect("automatic arm");
        record_reconcile_for_scope_now(
            &store,
            "cred:auto",
            "auto",
            ReconcileTruth {
                has_diff: true,
                all_positions_zero: false,
                all_open_orders_empty: false,
                all_open_algo_orders_empty: false,
                local_position_zero_diff: false,
            },
            101,
        )
        .await
        .expect("record diff");
        let error = reset(&store, "cred:auto", "admin", None, 102)
            .await
            .expect_err("diff must reject reset");
        assert!(error.to_string().contains("zero diff"));
        assert_eq!(state(&store, "cred:auto").await.0, "manage_only");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn global_flatten_and_reset_require_every_credential_to_converge() {
        let (store, path) = test_store("global-aggregate").await;
        for credential_id in ["cred-a", "cred-b"] {
            sqlx::query(
                r#"
                INSERT INTO engine_orders (
                  engine_order_id, credential_id, bot_uid, idempotency_key,
                  exchange, market_kind, symbol_key, action, side, order_type,
                  quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
                ) VALUES (?, ?, 'bot-global', ?, 'binance', 'futures',
                  'binance|futures|BTC|USDT|USDT', 'close_long', 'long',
                  'market', '1', 1, 'exit', 'canceled', 1, 1)
                "#,
            )
            .bind(format!("order-{credential_id}"))
            .bind(credential_id)
            .bind(format!("idem-{credential_id}"))
            .execute(store.pool())
            .await
            .expect("insert global credential fixture");
        }
        arm(
            &store,
            ArmRequest {
                scope_key: "global",
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "global aggregate test",
                now_ms: 100,
            },
        )
        .await
        .expect("arm global L3");
        let clean = ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };
        record_reconcile_for_scope_now(&store, "global", "cred-a", clean, 101)
            .await
            .expect("record first clean credential");
        assert_eq!(
            state(&store, "global").await.1.as_deref(),
            Some("flattening")
        );
        assert!(reset(&store, "global", "admin", None, 102).await.is_err());

        record_reconcile_for_scope_now(
            &store,
            "global",
            "cred-b",
            ReconcileTruth {
                has_diff: true,
                all_positions_zero: false,
                all_open_orders_empty: false,
                all_open_algo_orders_empty: false,
                local_position_zero_diff: false,
            },
            103,
        )
        .await
        .expect("record dirty second credential");
        assert_eq!(
            state(&store, "global").await.1.as_deref(),
            Some("flattening")
        );

        record_reconcile_for_scope_now(&store, "global", "cred-b", clean, 104)
            .await
            .expect("record second clean credential");
        assert_eq!(
            state(&store, "global").await.1.as_deref(),
            Some("flattened")
        );
        reset(&store, "global", "admin", None, 105)
            .await
            .expect("reset only after every credential converges");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn l1_keeps_orders_l2_cancels_entries_only_and_l3_cancels_every_order() {
        let _offline = offline_order_test_env("actions");
        let (store, path) = test_store("actions").await;
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
                ) VALUES (?, 'actions', 'bot-actions', ?, 'binance', 'futures',
                  'binance|futures|BTC|USDT|USDT', ?, 'long', ?, '1', ?, ?, 'acked', 1, 1)
                "#,
            )
            .bind(id)
            .bind(format!("idem-{id}"))
            .bind(action)
            .bind(order_type)
            .bind(reduce_only)
            .bind(order_role)
            .execute(store.pool())
            .await
            .expect("insert order fixture");
        }

        assert_eq!(
            apply_local_order_semantics(&store, "cred:actions", Level::PauseNew, 2)
                .await
                .expect("apply L1"),
            0
        );
        assert_eq!(
            apply_local_order_semantics(&store, "cred:actions", Level::ManageOnly, 3)
                .await
                .expect("apply L2"),
            1
        );
        let statuses = sqlx::query(
            "SELECT engine_order_id, status FROM engine_orders ORDER BY engine_order_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("read L2 statuses");
        let status = |id: &str| {
            statuses
                .iter()
                .find(|row| row.get::<String, _>("engine_order_id") == id)
                .map(|row| row.get::<String, _>("status"))
                .expect("fixture status")
        };
        assert_eq!(status("entry"), "canceled");
        assert_eq!(status("sl"), "acked");
        assert_eq!(status("exit"), "acked");

        assert_eq!(
            apply_local_order_semantics(&store, "cred:actions", Level::Flatten, 4)
                .await
                .expect("apply L3"),
            2
        );
        let remaining =
            sqlx::query("SELECT COUNT(*) AS n FROM engine_orders WHERE status <> 'canceled'")
                .fetch_one(store.pool())
                .await
                .expect("count uncanceled orders")
                .get::<i64, _>("n");
        assert_eq!(remaining, 0);
        cleanup(store, path).await;
    }

    struct FakeExchange {
        reconciles: Mutex<VecDeque<ReconcileTruth>>,
        algo_cancels: AtomicUsize,
        cancels: AtomicUsize,
        flattens: AtomicUsize,
    }

    struct OrderedFlattenAdapter {
        calls: Mutex<Vec<&'static str>>,
        fail_algo_cancel: bool,
        truth: ReconcileTruth,
    }

    #[tonic::async_trait]
    impl FlattenAdapter for OrderedFlattenAdapter {
        async fn cancel_all_open_algo_orders(&self) -> anyhow::Result<()> {
            self.calls.lock().expect("calls").push("algo_cancel");
            if self.fail_algo_cancel {
                Err(anyhow!("injected algo cancel failure"))
            } else {
                Ok(())
            }
        }

        async fn cancel_all_open_orders(&self) -> anyhow::Result<()> {
            self.calls.lock().expect("calls").push("ordinary_cancel");
            Ok(())
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.calls
                .lock()
                .expect("calls")
                .push("reduce_only_flatten");
            Ok(())
        }

        async fn force_reconcile(&self) -> anyhow::Result<ReconcileTruth> {
            self.calls.lock().expect("calls").push("reconcile");
            Ok(self.truth)
        }
    }

    #[tonic::async_trait]
    impl FlattenAdapter for FakeExchange {
        async fn cancel_all_open_algo_orders(&self) -> anyhow::Result<()> {
            self.algo_cancels.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        async fn cancel_all_open_orders(&self) -> anyhow::Result<()> {
            self.cancels.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.flattens.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        async fn force_reconcile(&self) -> anyhow::Result<ReconcileTruth> {
            self.reconciles
                .lock()
                .expect("fake reconcile lock")
                .pop_front()
                .ok_or_else(|| anyhow!("missing fake reconcile result"))
        }
    }

    /// **交易所可达的单不得被本地伪造成 canceled。**
    ///
    /// 此前所有 `apply_local_order_semantics` 的用例都用**未配 venue** 的凭据，
    /// 于是 `exchange_gateway_allowed_for` 恒为假、走的全是"本地即真值"那条分支，
    /// **交易所可达这条路径一次都没被测过**——缺陷正是活在那里。
    #[tokio::test]
    async fn l2_does_not_locally_cancel_orders_that_can_still_live_at_the_exchange() {
        let (store, path) = test_store("exchange-truth").await;
        let credential_id = "cred_venue_l2";
        let env_name = crate::venue::venue_env_name(credential_id);
        let previous = std::env::var(&env_name).ok();
        // testnet ⇒ exchange_gateway_allowed_for 为真（即使 live 未开启）。
        std::env::set_var(&env_name, "testnet");

        for (id, status, role) in [
            ("acked_entry", "acked", "entry"),
            ("pending_risk_entry", "pending_risk", "entry"),
            ("acked_sl", "acked", "protection_sl"),
        ] {
            sqlx::query(
                r#"
                INSERT INTO engine_orders (
                  engine_order_id, credential_id, bot_uid, idempotency_key,
                  exchange, market_kind, symbol_key, action, side, order_type,
                  quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
                ) VALUES (?, ?, 'bot-venue', ?, 'binance', 'futures',
                  'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'limit',
                  '1', 0, ?, ?, 1, 1)
                "#,
            )
            .bind(id)
            .bind(credential_id)
            .bind(format!("idem-{id}"))
            .bind(role)
            .bind(status)
            .execute(store.pool())
            .await
            .expect("insert exchange-backed order");
        }
        bind_existing_pending_risk_entry_authority(
            &store,
            "pending_risk_entry",
            credential_id,
            "bot-venue",
        )
        .await;

        let canceled = apply_local_order_semantics(
            &store,
            &format!("cred:{credential_id}"),
            Level::ManageOnly,
            2,
        )
        .await
        .expect("apply L2");

        let status_of = |id: &'static str| {
            let store = store.clone();
            async move {
                sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
                    .bind(id)
                    .fetch_one(store.pool())
                    .await
                    .expect("read status")
                    .get::<String, _>("status")
            }
        };

        // 已在交易所的 entry **不得**被本地改成 canceled——那是伪造撤单成功。
        assert_eq!(status_of("acked_entry").await, "acked");
        // 从未到达交易所的 pending_risk 可以本地撤（提交前必先写 pending_submit）。
        assert_eq!(status_of("pending_risk_entry").await, "canceled");
        // 保护单在 L2 下原样不动。
        assert_eq!(status_of("acked_sl").await, "acked");
        assert_eq!(
            canceled, 1,
            "only the pre-exchange order may be canceled locally"
        );

        // 而且这张 acked entry 必须仍然落在**生产撤单查询**的选取集里，
        // 否则真实撤单永远不会被发出（这正是本条缺陷的机理）。
        //
        // ****：R4 这里是测试**自己
        // 重写了一遍 SQL**——等于在测我自己写的字符串，把 `canceled` 放回
        // 生产查询的变异照样全绿。现在调用**生产函数**
        // `cancellable_orders_for_scope`，读侧与生产逐字同源。
        let selectable = cancellable_orders_for_scope(
            &store,
            &format!("cred:{credential_id}"),
            credential_id,
            Level::ManageOnly,
        )
        .await
        .expect("read production cancel selection");
        assert_eq!(
            selectable.len(),
            1,
            "the acked entry must still be picked up by the production cancel query"
        );
        assert!(selectable
            .iter()
            .any(|row| row.engine_order_id == "acked_entry"));

        match previous {
            Some(value) => std::env::set_var(&env_name, value),
            None => std::env::remove_var(&env_name),
        }
        cleanup(store, path).await;
    }

    /// D39（WO-M6）· 总开关关闭时，显式配置 mainnet 的凭据上**可能仍活在交易所**
    /// 的单同样不是本地真值：kill switch 对该凭据走交易所撤单 / flatten（与单仓
    /// reduce-only 平仓同一个判据），本地只撤从未出站的 pending_risk。此前总开关
    /// 一关，mainnet 凭据整体被当作「不出站」，交易所上活着的 entry 被本地写成
    /// canceled——伪造撤单成功，且真实撤单永远不会发出。
    #[tokio::test]
    async fn live_disabled_mainnet_orders_are_not_locally_canceled_and_stay_selectable() {
        let credential_id = "cred_mainnet_d39";
        // 总开关关闭（env 串行锁内）；字段逆序析构：先还原 venue，再释放锁。
        let _offline = offline_order_test_env(credential_id);
        let _mainnet = crate::binance_futures::tests::TestEnvVar::set(
            crate::venue::venue_env_name(credential_id),
            "mainnet",
        );
        assert!(!crate::svc::is_binance_live_enabled());
        assert_eq!(
            crate::venue::venue_for_credential(credential_id),
            Ok(Some(crate::venue::Venue::Mainnet))
        );
        let (store, path) = test_store("live-disabled-mainnet").await;
        for (id, status, role) in [
            ("acked_entry", "acked", "entry"),
            ("pending_risk_entry", "pending_risk", "entry"),
            ("acked_sl", "acked", "protection_sl"),
        ] {
            sqlx::query(
                r#"
                INSERT INTO engine_orders (
                  engine_order_id, credential_id, bot_uid, idempotency_key,
                  exchange, market_kind, symbol_key, action, side, order_type,
                  quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
                ) VALUES (?, ?, 'bot-mainnet', ?, 'binance', 'futures',
                  'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'limit',
                  '1', 0, ?, ?, 1, 1)
                "#,
            )
            .bind(id)
            .bind(credential_id)
            .bind(format!("idem-{id}"))
            .bind(role)
            .bind(status)
            .execute(store.pool())
            .await
            .expect("insert mainnet order");
        }
        bind_existing_pending_risk_entry_authority(
            &store,
            "pending_risk_entry",
            credential_id,
            "bot-mainnet",
        )
        .await;

        let canceled = apply_local_order_semantics(
            &store,
            &format!("cred:{credential_id}"),
            Level::ManageOnly,
            2,
        )
        .await
        .expect("apply L2");
        let status_of = |id: &'static str| {
            let store = store.clone();
            async move {
                sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
                    .bind(id)
                    .fetch_one(store.pool())
                    .await
                    .expect("read status")
                    .get::<String, _>("status")
            }
        };
        assert_eq!(status_of("acked_entry").await, "acked");
        assert_eq!(status_of("pending_risk_entry").await, "canceled");
        assert_eq!(status_of("acked_sl").await, "acked");
        assert_eq!(canceled, 1, "only the never-sent order is local truth");
        let selectable = cancellable_orders_for_scope(
            &store,
            &format!("cred:{credential_id}"),
            credential_id,
            Level::ManageOnly,
        )
        .await
        .expect("read production cancel selection");
        assert_eq!(
            selectable
                .iter()
                .map(|row| row.engine_order_id.as_str())
                .collect::<Vec<_>>(),
            vec!["acked_entry"]
        );
        let (act_on, skipped) = credentials_to_act_on(
            &[(
                credential_id.to_string(),
                Some(crate::venue::Venue::Mainnet),
            )],
            false,
        );
        assert_eq!(act_on, vec![credential_id.to_string()]);
        assert!(skipped.is_empty());
        cleanup(store, path).await;
    }

    /// **撤单必须是持久的。**
    ///
    /// 取消侧加了 CAS，但提交侧此前**无 CAS 且丢弃执行结果**，可以把
    /// 刚被撤掉的行直接翻回 `pending_submit` 继续提交。这条用真实的交错顺序
    /// 钉住：先 L2 撤掉 `pending_risk`，再模拟提交路径的推进 UPDATE——
    /// 带 CAS 的推进必须**影响 0 行**，撤销状态不得被复活。
    #[tokio::test]
    async fn a_cancelled_order_cannot_be_revived_by_the_submit_path() {
        let (store, path) = test_store("cancel-durability").await;
        let credential_id = "cred_durable";
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
            ) VALUES ('ord_durable', ?, 'bot-durable', 'idem-durable', 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'limit',
              '1', 0, 'entry', 'pending_risk', 1, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("insert pre-submit order");
        bind_existing_pending_risk_entry_authority(
            &store,
            "ord_durable",
            credential_id,
            "bot-durable",
        )
        .await;

        // L2：`pending_risk` 可证明尚未到达交易所，本地撤销即真值。
        let canceled = apply_local_order_semantics(
            &store,
            &format!("cred:{credential_id}"),
            Level::ManageOnly,
            2,
        )
        .await
        .expect("apply L2");
        assert_eq!(canceled, 1);

        // 提交路径随后试图推进这张单——**必须影响 0 行**。
        let advanced = sqlx::query(
            r#"
            UPDATE engine_orders
            SET status = 'pending_submit', updated_at_ms = ?
            WHERE credential_id = ? AND engine_order_id = ?
              AND status IN ('pending_risk', 'pending_submit')
            "#,
        )
        .bind(3_i64)
        .bind(credential_id)
        .bind("ord_durable")
        .execute(store.pool())
        .await
        .expect("submit-side advance")
        .rows_affected();
        assert_eq!(advanced, 0, "a cancelled order must not be revived");

        let status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='ord_durable'")
                .fetch_one(store.pool())
                .await
                .expect("read status")
                .get::<String, _>("status");
        assert_eq!(status, "canceled");
        cleanup(store, path).await;
    }

    /// **取消侧的 CAS 必须真的比对状态。**
    ///
    /// R4 之前没有任何用例喂过"读到的状态与写入时不一致"这种输入，
    /// 于是把 CAS 谓词改成恒真照样 23/23 全绿。这条直接对判据函数施压。
    #[tokio::test]
    async fn cancel_only_applies_when_the_status_still_matches() {
        let (store, path) = test_store("cancel-cas").await;
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
            ) VALUES ('ord_cas', 'cred_cas', 'bot-cas', 'idem-cas', 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'limit',
              '1', 0, 'entry', 'acked', 1, 1)
            "#,
        )
        .execute(store.pool())
        .await
        .expect("insert order");

        // 读到的状态与库里不一致（模拟读与写之间被别人改过）→ 必须 0 行。
        let stale = cancel_order_if_status(&store, "ord_cas", "pending_risk", 2)
            .await
            .expect("stale CAS");
        assert_eq!(stale, 0, "a stale expected-status must not cancel anything");
        let status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='ord_cas'")
                .fetch_one(store.pool())
                .await
                .expect("read status")
                .get::<String, _>("status");
        assert_eq!(status, "acked", "the row must be untouched by a stale CAS");

        // 状态一致 → 1 行（放行面：CAS 不得严到把正常撤销也挡掉）。
        let fresh = cancel_order_if_status(&store, "ord_cas", "acked", 3)
            .await
            .expect("fresh CAS");
        assert_eq!(fresh, 1);
        let status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='ord_cas'")
                .fetch_one(store.pool())
                .await
                .expect("read status")
                .get::<String, _>("status");
        assert_eq!(status, "canceled");

        // 已经撤过的单再撤一次 → 0 行（幂等，不重复计数）。
        let again = cancel_order_if_status(&store, "ord_cas", "acked", 4)
            .await
            .expect("repeat CAS");
        assert_eq!(again, 0);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn entry_cleanup_non_exact_statuses_cancel_without_releasing_resources() {
        for status in ["pending_submit", "acked", "unknown", "PENDING_RISK"] {
            let (store, path) = test_store(&format!("cleanup-status-{status}")).await;
            seed_pending_entry_cleanup_fixture(&store).await;
            sqlx::query("UPDATE engine_orders SET status=? WHERE engine_order_id='ord-cleanup'")
                .bind(status)
                .execute(store.pool())
                .await
                .expect("change cleanup status");
            assert_eq!(
                cancel_order_if_status(&store, "ord-cleanup", status, 2)
                    .await
                    .expect("non-exact pre-submit cancellation"),
                1,
                "status={status}"
            );
            assert_entry_cleanup_resources_unchanged(&store, "canceled").await;
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn entry_cleanup_zero_row_cas_never_releases_resources() {
        let (store, path) = test_store("cleanup-cas-zero").await;
        seed_pending_entry_cleanup_fixture(&store).await;
        sqlx::query(
            "UPDATE engine_orders SET status='pending_submit' \
             WHERE engine_order_id='ord-cleanup'",
        )
        .execute(store.pool())
        .await
        .expect("advance cleanup fixture before stale cancel");
        assert_eq!(
            cancel_order_if_status(&store, "ord-cleanup", "pending_risk", 2)
                .await
                .expect("stale pending_risk cancel"),
            0
        );
        assert_entry_cleanup_resources_unchanged(&store, "pending_submit").await;
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn entry_cleanup_malformed_or_cross_bound_authority_rolls_back() {
        for case in [
            "dirty-role",
            "exchange-id",
            "foreign-fill",
            "nonzero-qty",
            "wrong-position-owner",
            "wrong-verdict-owner",
            "rejected-verdict",
            "shared-verdict",
            "wrong-pending-key",
            "wrong-plan-market",
        ] {
            let (store, path) = test_store(&format!("cleanup-negative-{case}")).await;
            seed_pending_entry_cleanup_fixture(&store).await;
            match case {
                "dirty-role" => {
                    sqlx::query(
                        "UPDATE engine_orders SET order_role='ENTRY' \
                         WHERE engine_order_id='ord-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("dirty order role");
                }
                "exchange-id" => {
                    sqlx::query(
                        "UPDATE engine_orders SET exchange_order_id='venue-1' \
                         WHERE engine_order_id='ord-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("seed exchange identity");
                }
                "foreign-fill" => {
                    sqlx::query(
                        "INSERT INTO engine_fills \
                         (credential_id, exchange_trade_id, engine_order_id, exchange, \
                          market_kind, created_at_ms) \
                         VALUES ('foreign-credential', 'trade-1', 'ord-cleanup', \
                          'binance', 'futures', 1)",
                    )
                    .execute(store.pool())
                    .await
                    .expect("seed cross-credential fill");
                }
                "nonzero-qty" => {
                    sqlx::query(
                        "UPDATE position_state SET qty='0.001' WHERE position_id='pos-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("seed nonzero pending position");
                }
                "wrong-position-owner" => {
                    sqlx::query(
                        "UPDATE position_state SET credential_id='other' \
                         WHERE position_id='pos-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("cross-bind position owner");
                }
                "wrong-verdict-owner" => {
                    sqlx::query(
                        "UPDATE risk_verdicts SET bot_uid='other' \
                         WHERE risk_verdict_id='rv-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("cross-bind verdict owner");
                }
                "rejected-verdict" => {
                    sqlx::query(
                        "UPDATE risk_verdicts SET decision='reject' \
                         WHERE risk_verdict_id='rv-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("seed non-authorizing verdict");
                }
                "shared-verdict" => {
                    sqlx::query(
                        r#"
                        INSERT INTO engine_orders (
                          engine_order_id, credential_id, bot_uid, idempotency_key,
                          exchange, market_kind, symbol_key, action, side, order_type,
                          quantity, reduce_only, order_role, status, risk_verdict_id,
                          created_at_ms, updated_at_ms
                        ) VALUES ('ord-shared', 'cred-cleanup', 'bot-cleanup', 'idem-shared',
                          'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
                          'close_long', 'long', 'market', '1', 1, 'exit', 'acked',
                          'rv-cleanup', 1, 1)
                        "#,
                    )
                    .execute(store.pool())
                    .await
                    .expect("share verdict with another engine order");
                }
                "wrong-pending-key" => {
                    sqlx::query(
                        "UPDATE position_state SET pending_idem_key='other' \
                         WHERE position_id='pos-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("cross-bind pending authority key");
                }
                "wrong-plan-market" => {
                    sqlx::query(
                        "UPDATE position_protection_plans SET market_kind='spot' \
                         WHERE position_id='pos-cleanup'",
                    )
                    .execute(store.pool())
                    .await
                    .expect("cross-bind protection plan market");
                }
                _ => unreachable!(),
            }
            assert!(
                cancel_order_if_status(&store, "ord-cleanup", "pending_risk", 2)
                    .await
                    .is_err(),
                "case={case} must fail closed"
            );
            assert_entry_cleanup_resources_unchanged(&store, "pending_risk").await;
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn entry_cleanup_known_non_entry_roles_never_release_resources() {
        for role in ["exit", "protection_sl", "protection_tp", "emergency"] {
            let (store, path) = test_store(&format!("cleanup-role-{role}")).await;
            seed_pending_entry_cleanup_fixture(&store).await;
            sqlx::query(
                "UPDATE engine_orders SET order_role=? WHERE engine_order_id='ord-cleanup'",
            )
            .bind(role)
            .execute(store.pool())
            .await
            .expect("change cleanup fixture role");
            assert_eq!(
                cancel_order_if_status(&store, "ord-cleanup", "pending_risk", 2)
                    .await
                    .expect("known non-Entry cancellation"),
                1,
                "role={role}"
            );
            assert_entry_cleanup_resources_unchanged(&store, "canceled").await;
            cleanup(store, path).await;
        }
    }

    /// 源码守卫（第二层）：提交侧的推进 UPDATE 必须带 CAS 且**不得丢弃结果**。
    #[test]
    fn submit_side_advance_keeps_its_cas_and_result() {
        let source = include_str!("svc.rs");
        let advance = source
            .split_once("pub(crate) async fn advance_then_submit")
            .expect("advance_then_submit must exist")
            .1
            .split_once("pub(crate) async fn finalize_status_if_in_flight")
            .expect("finalize_status_if_in_flight must follow advance_then_submit")
            .0;
        assert!(
            advance.contains("let status_lhs = exact_engine_order_status_lhs(db_kind)?;")
                && advance.contains("AND {status_lhs} = ?")
                && advance.contains(".bind(\"pending_risk\")"),
            "submit-side advance must carry the dialect-bound exact pending_risk CAS"
        );
        assert!(
            advance.contains(".rows_affected();")
                && advance.contains("match advanced")
                && advance.contains("AdvanceThenSubmitOutcome::Blocked"),
            "submit-side advance must inspect the CAS result and block the submit future on zero rows"
        );
    }

    /// 放行面：交易所**不可达**时（模拟凭据 / 未配 venue / live 未开启），
    /// 本地状态就是真值，L2 必须照旧撤掉未成交入场单——否则模拟盘的急停会失效。
    #[tokio::test]
    async fn l2_still_cancels_locally_when_no_exchange_can_hold_the_order() {
        let (store, path) = test_store("local-truth").await;
        let credential_id = "paper:00112233445566778899aabbccddeeff";
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
            ) VALUES ('sim_entry', ?, 'bot-sim', 'idem-sim', 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'limit',
              '1', 0, 'entry', 'acked', 1, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("insert simulation order");

        let canceled = apply_local_order_semantics(
            &store,
            &format!("cred:{credential_id}"),
            Level::ManageOnly,
            2,
        )
        .await
        .expect("apply L2");
        assert_eq!(canceled, 1);
        let status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='sim_entry'")
                .fetch_one(store.pool())
                .await
                .expect("read status")
                .get::<String, _>("status");
        assert_eq!(status, "canceled");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn l3_fake_exchange_stays_flattening_until_forced_reconcile_is_zero_diff() {
        let (store, path) = test_store("flatten").await;
        arm(
            &store,
            ArmRequest {
                scope_key: "cred:flatten",
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "test",
                now_ms: 100,
            },
        )
        .await
        .expect("arm flatten");
        let fake = FakeExchange {
            reconciles: Mutex::new(VecDeque::from([
                ReconcileTruth {
                    has_diff: true,
                    all_positions_zero: false,
                    all_open_orders_empty: false,
                    all_open_algo_orders_empty: false,
                    local_position_zero_diff: false,
                },
                ReconcileTruth {
                    has_diff: false,
                    all_positions_zero: true,
                    all_open_orders_empty: true,
                    all_open_algo_orders_empty: true,
                    local_position_zero_diff: true,
                },
            ])),
            algo_cancels: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            flattens: AtomicUsize::new(0),
        };
        let first = drive_flatten_once(&store, "cred:flatten", "flatten", &fake, 101)
            .await
            .expect("first flatten attempt");
        assert!(!first.flatten_complete());
        assert_eq!(
            state(&store, "cred:flatten").await.1.as_deref(),
            Some("flattening")
        );
        let second = drive_flatten_once(&store, "cred:flatten", "flatten", &fake, 102)
            .await
            .expect("second flatten attempt");
        assert!(second.flatten_complete());
        assert_eq!(
            state(&store, "cred:flatten").await.1.as_deref(),
            Some("flattened")
        );
        assert_eq!(fake.algo_cancels.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(fake.cancels.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(fake.flattens.load(AtomicOrdering::SeqCst), 2);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b5_l3_cancels_algo_before_ordinary_orders_and_flatten() {
        let (store, path) = test_store("b5-algo-first").await;
        let fake = OrderedFlattenAdapter {
            calls: Mutex::new(Vec::new()),
            fail_algo_cancel: false,
            truth: ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
        };
        drive_flatten_once(&store, "cred:b5", "b5", &fake, 1)
            .await
            .unwrap();
        assert_eq!(
            fake.calls.lock().expect("calls").as_slice(),
            [
                "algo_cancel",
                "ordinary_cancel",
                "reduce_only_flatten",
                "reconcile"
            ]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b6_l3_algo_set_must_converge_before_flattened_state() {
        let (store, path) = test_store("b6-algo-convergence").await;
        arm(
            &store,
            ArmRequest {
                scope_key: "cred:b6",
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "test",
                now_ms: 1,
            },
        )
        .await
        .unwrap();
        let truth = ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: true,
        };
        assert!(!truth.flatten_complete());
        record_reconcile_for_scope_now(&store, "cred:b6", "b6", truth, 2)
            .await
            .unwrap();
        assert_eq!(
            state(&store, "cred:b6").await.1.as_deref(),
            Some("flattening")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b9_algo_cancel_failure_never_blocks_reduce_only_flatten() {
        let (store, path) = test_store("b9-fail-open-close").await;
        let fake = OrderedFlattenAdapter {
            calls: Mutex::new(Vec::new()),
            fail_algo_cancel: true,
            truth: ReconcileTruth {
                has_diff: true,
                all_positions_zero: false,
                all_open_orders_empty: false,
                all_open_algo_orders_empty: false,
                local_position_zero_diff: false,
            },
        };
        let truth = drive_flatten_once(&store, "cred:b9", "b9", &fake, 1)
            .await
            .expect("cancel failure must not reject the flatten attempt");
        assert!(!truth.flatten_complete());
        assert_eq!(
            fake.calls.lock().expect("calls").as_slice(),
            [
                "algo_cancel",
                "ordinary_cancel",
                "reduce_only_flatten",
                "reconcile"
            ]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn kill_switch_file_is_latched_within_poll_window_and_delete_does_not_reset() {
        let (store, path) = test_store("file").await;
        let kill_file = std::env::temp_dir().join(format!("KILL_SWITCH-{}", ulid::Ulid::new()));
        let flag = Arc::new(AtomicBool::new(false));
        let task = spawn_manual_trigger_task(store.clone(), kill_file.clone(), flag);
        std::fs::write(&kill_file, "pause_new operator drill").expect("create kill file");
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if state(&store, "global").await.0 == "pause_new" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("file trigger must arm within 500ms");
        assert_eq!(state(&store, "global").await.0, "pause_new");
        std::fs::remove_file(&kill_file).expect("remove kill file");
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert_eq!(state(&store, "global").await.0, "pause_new");
        task.abort();
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn sigusr1_flag_is_consumed_by_background_worker_and_arms_global() {
        let (store, path) = test_store("signal").await;
        let kill_file =
            std::env::temp_dir().join(format!("missing-KILL_SWITCH-{}", ulid::Ulid::new()));
        let flag = Arc::new(AtomicBool::new(false));
        let task = spawn_manual_trigger_task(store.clone(), kill_file, flag.clone());
        flag.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_millis(750), async {
            loop {
                if state(&store, "global").await.0 == "flatten" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("SIGUSR1 must arm through the background worker");
        assert_eq!(state(&store, "global").await.0, "flatten");
        task.abort();
        cleanup(store, path).await;
    }

    /// 返修 · 逐 credential 隔离：多个失败必须**全部**出现在摘要里。
    ///
    /// 若实现回退成「第一个失败就 `?` 早退」，摘要只会含第一个 credential，
    /// 本测试变红。这正是评审指出的那条：坏凭据不得让整个 scope 收不到处置。
    #[tokio::test]
    async fn hung_exchange_call_becomes_a_bounded_unknown_not_a_success() {
        // 设计要求 裁决：timeout 只能落 failure/unknown 并继续处理其他 credential；
        // **不得伪造撤单/平仓成功**。
        let error = with_credential_deadline(20, async {
            futures_util::future::pending::<()>().await;
            Ok(())
        })
        .await
        .expect_err("a hung exchange call must not resolve to success");
        let text = error.to_string();
        assert!(text.contains("deadline"), "{text}");
        assert!(text.contains("NOT confirmed"), "{text}");
    }

    #[tokio::test]
    async fn work_that_finishes_in_time_passes_through_unchanged() {
        // 放行面：deadline 不得把正常完成的处置判失败。
        with_credential_deadline(5_000, async { Ok(()) })
            .await
            .expect("fast work must pass through");
        let error = with_credential_deadline(5_000, async { Err(anyhow!("real failure")) })
            .await
            .expect_err("real failures must still surface");
        assert!(error.to_string().contains("real failure"));
    }

    #[test]
    fn b1_scope_error_summary_reports_every_failing_credential() {
        let errors = vec![
            "cred-a: venue config: bad".to_string(),
            "cred-b: no exchange credentials available".to_string(),
            "cred-c: flatten incomplete".to_string(),
        ];
        let summary = scope_error_summary("global", &errors).expect("expected a summary");
        assert!(summary.contains("3 failing credential(s)"), "{summary}");
        for credential in ["cred-a", "cred-b", "cred-c"] {
            assert!(
                summary.contains(credential),
                "missing {credential} in {summary}"
            );
        }
    }

    /// 放行面：全部成功时不得报错（只测报错会放过「永远返回 Err」的实现）。
    #[test]
    fn b1_scope_error_summary_is_none_when_every_credential_succeeded() {
        assert_eq!(scope_error_summary("global", &[]), None);
    }

    /// 源码守卫：`drive_live_scope_once` 的 act_on 循环内不得出现 `?` 早退。
    ///
    /// 纯函数测不到「调用点有没有早退」，而这恰恰是缺陷所在的那一行。
    #[test]
    fn b1_drive_live_scope_once_collects_errors_instead_of_early_return() {
        let source = include_str!("kill_switch.rs");
        let start = source
            .find("async fn drive_live_scope_once(")
            .expect("drive_live_scope_once must exist");
        let end = source[start..]
            .find("async fn drive_one_credential(")
            .expect("drive_one_credential must follow")
            + start;
        let body = &source[start..end];
        assert!(
            body.contains("per_credential_errors.push"),
            "scope driver must accumulate per-credential failures"
        );
        assert!(
            !body.contains("collect::<anyhow::Result<Vec<_>>>()?"),
            "candidate collection must not abort the whole scope on one bad credential"
        );
        // 光「有累加」不够：实现可以照样累加，最后只报第一个（等价于早退语义，
        // 运维看到的失败面因此缺失）。必须钉住终局走的是聚合器。
        assert!(
            body.contains("scope_error_summary(scope_key, &per_credential_errors)"),
            "scope driver must report through the aggregator, not just the first failure"
        );
        assert!(
            !body.contains("per_credential_errors.first()"),
            "reporting only the first failing credential hides the rest of the blast radius"
        );
        // 评审独立抽验：把「入栈后立刻 return Err」的变异放进循环体，词法守卫
        // 若只看聚合器是否被调用就会放它过去（聚合器仍在函数末尾，只是够不到）。
        // 这里直接钉住循环体内**没有任何 return**——scope 驱动的不变量就是
        // 「跑完全部 credential 才退出」。
        let loop_start = body
            .find("for credential_id in act_on_credentials {")
            .expect("act_on loop must exist");
        let loop_body = &body[loop_start..];
        let loop_end = loop_body
            .find("\n    }\n")
            .expect("act_on loop must terminate");
        assert!(
            !loop_body[..loop_end].contains("return "),
            "the act_on loop must not return early; it must finish every credential"
        );
    }

    /// 混合 scope 下模拟凭据只进 skipped，交易所凭据照常进 act_on。
    /// 这条钉的是「kill-switch 不被模拟凭据带崩」：drive_live_scope_once 里
    /// `keys_from_env(...)?` 用了 `?`，模拟凭据一旦进 act_on，整个循环会在
    /// 它那一格中止，**排在后面的交易所凭据就不会被平仓**。
    #[test]
    fn b1_credentials_to_act_on_keeps_exchange_credentials_after_a_simulation_one() {
        // 模拟凭据**排在最前面**：如果实现让它进了 act_on，后面两个交易所
        // 凭据在真实链路上就会因为 `?` 提前返回而永远轮不到。
        let candidates = vec![
            ("paper:12345678901234567890123456789012".to_string(), None),
            ("paper-lookalike".to_string(), None),
            ("testnet-a".to_string(), Some(crate::venue::Venue::Testnet)),
            ("mainnet-a".to_string(), Some(crate::venue::Venue::Mainnet)),
        ];

        let (act_on, skipped) = credentials_to_act_on(&candidates, true);

        assert_eq!(
            act_on,
            vec!["testnet-a".to_string(), "mainnet-a".to_string()],
            "交易所凭据必须仍被处理"
        );
        assert_eq!(
            skipped
                .iter()
                .map(|(credential_id, _)| credential_id.as_str())
                .collect::<Vec<_>>(),
            vec!["paper:12345678901234567890123456789012", "paper-lookalike"]
        );
    }
}

/// **flatten latch 的两条完成纪律。**
///
/// 1. 新发现的裸敞口必须让 latch **回到未收敛**，哪怕上一轮已经收敛过；
/// 2. 认不出交易身份的未平仓位不得被静默丢弃。
#[cfg(test)]
mod r13_flatten_truth_tests {
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Mutex, Once};
    use std::time::Duration;

    use sqlx::Row;

    use super::{
        action_retry_targets, drive_one_credential_with_client, latch_flatten_pending,
        observe_flatten_generations, record_reconcile_for_scope, record_reconcile_for_scope_now,
        scopes_needing_action_retry, submit_live_flatten, ActionRetryPacer, Level,
        ReconcileProjection, ReconcileTruth,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    struct L3GovernorClock {
        now_ms: AtomicI64,
        sleeps_ms: Mutex<Vec<u64>>,
    }

    impl L3GovernorClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now_ms: AtomicI64::new(now_ms),
                sleeps_ms: Mutex::new(Vec::new()),
            }
        }

        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }
    }

    impl crate::svc::Clock for L3GovernorClock {
        fn now_ms(&self) -> i64 {
            self.now_ms()
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
                self.sleeps_ms
                    .lock()
                    .expect("lock fake sleeps")
                    .push(millis);
                self.now_ms
                    .fetch_add(i64::try_from(millis).unwrap_or(i64::MAX), Ordering::SeqCst);
                tokio::task::yield_now().await;
            })
        }
    }

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-r13ks-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect kill-switch test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn latch_state(store: &Store, scope_key: &str) -> (String, String) {
        let row = sqlx::query(
            "SELECT level, COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch");
        (
            row.get::<String, _>("level"),
            row.get::<String, _>("flatten_state"),
        )
    }

    async fn seed_position(store: &Store, position_id: &str, symbol_key: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, 'cred-r13ks', 'bot-r13ks', ?, 'long', '1', '100', 'open', 1, 1, 1)
            "#,
        )
        .bind(position_id)
        .bind(symbol_key)
        .execute(store.pool())
        .await
        .expect("seed an open position");
    }

    async fn seed_active_algo(store: &Store, position_id: &str, algo_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, position_id,
              created_at_ms, updated_at_ms
            ) VALUES ('eng_429_algo', 'cred-r13ks', 'bot-r13ks', 'idem_429_algo',
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'stop_market', '1', 1, 'protection_sl',
              'acked', ?, 1, 1)
            "#,
        )
        .bind(position_id)
        .execute(store.pool())
        .await
        .expect("seed protection engine order");
        sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind, generation,
              engine_order_id, exchange_algo_id, client_algo_id, algo_status,
              trigger_price, reduce_only, close_position, status,
              verified_from_exchange, created_at_ms, updated_at_ms
            ) VALUES ('prot_429_algo', ?, 'cred-r13ks', 'sl', 1,
              'eng_429_algo', ?, 'client_429_algo', 'NEW', '90', 1, 1,
              'confirmed', 1, 1, 1)
            "#,
        )
        .bind(position_id)
        .bind(algo_id)
        .execute(store.pool())
        .await
        .expect("seed active protection algo");
    }

    fn request_paths(client: &crate::binance_futures::BinanceFuturesClient) -> Vec<String> {
        client
            .recorded_requests()
            .iter()
            .map(|request| {
                let url = request.split_whitespace().nth(1).expect("request URL");
                url::Url::parse(url)
                    .expect("parse request URL")
                    .path()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn l3_production_request_classes_keep_truth_reads_out_of_exit_reserve() {
        let source = include_str!("kill_switch.rs");
        let production = source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production segment precedes the unit-test modules");
        assert_eq!(
            production.matches("RequestClass::Exit").count(),
            2,
            "only ordinary cancel_order_json calls may consume the Exit reserve"
        );
        assert!(
            production.matches("RequestClass::EventTruth").count() >= 3,
            "the three L3 exchange-truth reads must use the bounded Normal queue"
        );
        assert!(
            production
                .contains(".get_all_open_algo_orders_json(&limiter, RequestClass::EventTruth)"),
            "the account-wide Algo truth read must queue as EventTruth"
        );
        assert!(
            production.contains(".open_orders(None, RequestClass::EventTruth)"),
            "the account-wide ordinary-order truth read must queue as EventTruth"
        );
        assert!(
            production.contains(".positions(RequestClass::EventTruth)"),
            "the account-wide position truth read must queue as EventTruth"
        );
    }

    #[tokio::test]
    async fn l3_truth_reads_wait_for_tokens_at_production_bucket_defaults_instead_of_failing_fast()
    {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _weight_rate = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_WEIGHT_PER_SEC".to_string(),
        );
        let _legacy_rate =
            crate::binance_futures::tests::TestEnvVar::unset("RUSTCORE_BINANCE_RPS".to_string());
        let _weight_burst = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_WEIGHT_BURST".to_string(),
        );
        let _legacy_burst =
            crate::binance_futures::tests::TestEnvVar::unset("RUSTCORE_BINANCE_BURST".to_string());
        let _queue_wait = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_QUEUE_WAIT_MAX_MS".to_string(),
        );
        let _exit_reserve = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE".to_string(),
        );

        let defaults = crate::rate_limiter::TokenBucketConfig::from_env(None);
        assert_eq!(defaults.refill_per_sec, 5.0);
        assert_eq!(defaults.capacity, 10.0);

        let (store, path) = test_store("event-truth-production-defaults").await;
        let scope_key = "cred:cred-a6-r1";
        let credential_id = "cred-a6-r1";
        latch_flatten_pending(&store, scope_key, "manual_api", "A6-R1", 1_000)
            .await
            .expect("latch credential L3");

        let clock = Arc::new(L3GovernorClock::new(1_000));
        let responses = vec![
            (200, "[]".to_string()),
            (200, "[]".to_string()),
            (
                200,
                r#"[{"symbol":"BTCUSDT","positionAmt":"1"}]"#.to_string(),
            ),
            (
                200,
                r#"{"orderId":8401,"clientOrderId":"ks_a6_r1","status":"NEW","symbol":"BTCUSDT","side":"SELL","origQty":"1","executedQty":"0"}"#.to_string(),
            ),
            (200, "[]".to_string()),
            (200, "[]".to_string()),
            (200, "[]".to_string()),
            (200, "[]".to_string()),
        ];
        let (client, limiter, requests, request_times) =
            crate::binance_futures::tests::l3_governor_default_test_client(
                responses,
                credential_id,
                clock.clone(),
            );
        let egress_scope = client.egress_scope().to_string();
        let filter_cache = crate::exchange_filters::ExchangeFilterCache::new();
        let drive_store = store.clone();
        let drive_scope = scope_key.to_string();
        let drive_credential = credential_id.to_string();
        let drive = tokio::spawn(async move {
            let outcome = drive_one_credential_with_client(
                &drive_store,
                &drive_scope,
                Level::Flatten,
                &filter_cache,
                &drive_credential,
                &client,
            )
            .await;
            (outcome, client)
        });

        // TokenBucket intentionally uses a monotonic production Instant.  This
        // pump applies the exact 5 weight/s refill implied by the fake clock so
        // the test advances instantly while retaining the production 5/10
        // configuration and the real prioritized admission path.
        let started_at_ms = clock.now_ms();
        let mut refilled_through_ms = started_at_ms;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::task::yield_now().await;
                let now_ms = clock.now_ms();
                if now_ms > refilled_through_ms {
                    let elapsed_ms = now_ms - refilled_through_ms;
                    let stored = limiter
                        .stored_tokens(&egress_scope)
                        .await
                        .unwrap_or(defaults.capacity);
                    limiter
                        .set_stored_tokens(
                            &egress_scope,
                            stored + elapsed_ms as f64 * defaults.refill_per_sec / 1_000.0,
                        )
                        .await;
                    refilled_through_ms = now_ms;
                }
                if drive.is_finished() {
                    break;
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "L3 must finish under bounded fake time: now_ms={} requests={:?}",
                clock.now_ms(),
                requests.lock().expect("lock request log")
            )
        });
        let (outcome, client) = drive.await.expect("join L3 driver");
        outcome.unwrap_or_else(|error| {
            panic!("EventTruth reads must not fail through the Exit reserve: {error:#}")
        });

        let timestamped = request_times
            .lock()
            .expect("lock timestamped request log")
            .clone();
        let open_algo_at_ms = timestamped
            .iter()
            .find(|(path, _)| path == "/fapi/v1/openAlgoOrders")
            .map(|(_, at_ms)| *at_ms)
            .expect("openAlgoOrders must reach mock transport");
        let open_orders_at_ms = timestamped
            .iter()
            .find(|(path, _)| path == "/fapi/v1/openOrders")
            .map(|(_, at_ms)| *at_ms)
            .expect("openOrders must reach mock transport");
        assert!(
            open_orders_at_ms - open_algo_at_ms >= 6_000,
            "openOrders itself must wait for production-rate tokens after the preceding transport: openAlgoOrders={open_algo_at_ms} openOrders={open_orders_at_ms} timestamped={timestamped:?}"
        );
        let logged = client.recorded_requests();
        assert!(
            logged
                .iter()
                .any(|request| request.contains("/fapi/v1/openOrders")),
            "ordinary open-order truth must be read: {logged:?}"
        );
        assert!(
            logged
                .iter()
                .any(|request| request.contains("/fapi/v2/positionRisk")),
            "position truth must be read: {logged:?}"
        );
        assert!(
            logged.iter().any(|request| {
                request.starts_with("POST ")
                    && request.contains("/fapi/v1/order")
                    && request.contains("reduceOnly=true")
            }),
            "a reduce-only flatten POST must follow the truth reads: {logged:?}"
        );
        assert!(
            clock.now_ms() - started_at_ms >= 6_000,
            "the fake clock must prove waiting rather than a fast reserve failure"
        );

        client.clear_egress_for_test();
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn active_429_bot_l3_sends_algo_delete_then_reduce_only_market_flatten() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _budget = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(),
        );
        let (store, path) = test_store("bot-429-sequence").await;
        seed_position(&store, "pos_bot_429", "binance|futures|BTC|USDT|USDT").await;
        seed_active_algo(&store, "pos_bot_429", "8101").await;
        latch_flatten_pending(&store, "bot:bot-r13ks", "manual_api", "429 sequence", 10)
            .await
            .expect("latch bot L3");
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "cred-r13ks",
            vec![
                (200, "{}".to_string()),
                (
                    200,
                    r#"{"orderId":8201,"clientOrderId":"ks_bot_429","status":"NEW","symbol":"BTCUSDT","side":"SELL","origQty":"1","executedQty":"0"}"#.to_string(),
                ),
            ],
        )
        .with_egress_scope_for_test(format!("test-egress:r13-isolated-{}", ulid::Ulid::new()));
        client.park_429_for_test(i64::MAX);
        let error = drive_one_credential_with_client(
            &store,
            "bot:bot-r13ks",
            Level::Flatten,
            &crate::exchange_filters::ExchangeFilterCache::new(),
            "cred-r13ks",
            &client,
        )
        .await
        .expect_err("429 still defers final exchange-truth convergence");
        assert!(
            error.to_string().contains("rate-limit cooldown active"),
            "{error:#}"
        );
        assert_eq!(
            request_paths(&client),
            vec!["/fapi/v1/algoOrder", "/fapi/v1/order"]
        );
        client.clear_egress_for_test();
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn active_429_credential_l3_skips_all_normal_reads_flattens_locally_and_stays_latched() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _budget = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(),
        );
        let (store, path) = test_store("cred-429-local").await;
        seed_position(&store, "pos_cred_429", "binance|futures|BTC|USDT|USDT").await;
        latch_flatten_pending(&store, "cred:cred-r13ks", "manual_api", "429", 10)
            .await
            .expect("latch credential L3");
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "cred-r13ks",
            vec![(
                200,
                r#"{"orderId":8301,"clientOrderId":"ks_cred_429","status":"NEW","symbol":"BTCUSDT","side":"SELL","origQty":"1","executedQty":"0"}"#.to_string(),
            )],
        )
        .with_egress_scope_for_test(format!("test-egress:r13-isolated-{}", ulid::Ulid::new()));
        client.park_429_for_test(i64::MAX);
        let error = drive_one_credential_with_client(
            &store,
            "cred:cred-r13ks",
            Level::Flatten,
            &crate::exchange_filters::ExchangeFilterCache::new(),
            "cred-r13ks",
            &client,
        )
        .await
        .expect_err("convergence must be deferred");
        assert!(
            error.to_string().contains("L3 truth convergence deferred"),
            "{error:#}"
        );
        assert_eq!(request_paths(&client), vec!["/fapi/v1/order"]);
        assert_eq!(
            latch_state(&store, "cred:cred-r13ks").await.1,
            super::FLATTEN_PENDING
        );
        client.clear_egress_for_test();
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn active_418_l3_sends_nothing_and_never_enters_the_local_429_sequence() {
        let (store, path) = test_store("cred-418-zero-egress").await;
        seed_position(&store, "pos_cred_418", "binance|futures|BTC|USDT|USDT").await;
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "cred-r13ks",
            vec![],
        )
        .with_egress_scope_for_test(format!("test-egress:r13-isolated-{}", ulid::Ulid::new()));
        client.park_418_for_test(i64::MAX);
        let error = drive_one_credential_with_client(
            &store,
            "cred:cred-r13ks",
            Level::Flatten,
            &crate::exchange_filters::ExchangeFilterCache::new(),
            "cred-r13ks",
            &client,
        )
        .await
        .expect_err("418 blocks every egress class");
        assert!(
            error.to_string().contains("IP ban cooldown active"),
            "{error:#}"
        );
        assert!(request_paths(&client).is_empty());
        client.clear_egress_for_test();
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn expired_429_next_drive_uses_the_original_four_read_reconcile_and_converges() {
        let (store, path) = test_store("cred-429-expired").await;
        latch_flatten_pending(&store, "cred:cred-r13ks", "manual_api", "expired", 10)
            .await
            .expect("latch credential L3");
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "cred-r13ks",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
            ],
        )
        .with_egress_scope_for_test(format!("test-egress:r13-isolated-{}", ulid::Ulid::new()));
        client.park_429_for_test(1);
        drive_one_credential_with_client(
            &store,
            "cred:cred-r13ks",
            Level::Flatten,
            &crate::exchange_filters::ExchangeFilterCache::new(),
            "cred-r13ks",
            &client,
        )
        .await
        .expect("expired window returns to exchange truth");
        let paths = request_paths(&client);
        assert_eq!(
            &paths[paths.len() - 4..],
            [
                "/fapi/v1/openOrders",
                "/fapi/v1/openAlgoOrders",
                "/fapi/v2/positionRisk",
                "/fapi/v2/balance",
            ]
        );
        assert_eq!(
            latch_state(&store, "cred:cred-r13ks").await.1,
            super::FLATTEN_CONVERGED
        );
        client.clear_egress_for_test();
        cleanup(store, path).await;
    }

    /// **一条已经收敛过的 latch 再遇到新敞口，必须回到未收敛。**
    ///
    /// `arm` 有一条刻意的早返回：当前档位严重度 `>=` 请求档位时原样返回、
    /// 不写库。对 `flatten` 而言这条早返回是个安全洞——上一轮的
    /// `flatten_state='flattened'` 会被原样留下，而重试任务的扫描条件正是
    /// `level='flatten' AND flatten_state <> 'flattened'`：新敞口于是
    /// **接线证明（DB）**：重新闩上的 latch 必须带着**更高的代际**进入扫描结果，
    /// 且一个在旧代际上已退避到上限的 pacer 必须把它当成立即到期。
    ///
    /// 独立复核指出的缺陷原文：扫描只返回 scope_key + level，pacer 只按 scope_key
    /// 存状态，于是同一 scope 的新一代裸敞口会整个继承旧退避。这里走真实的
    /// latch → 收敛 → 再 latch 路径，不是只测纯 helper。
    #[tokio::test]
    async fn a_reopened_latch_reaches_the_scheduler_with_a_higher_generation() {
        let (store, path) = test_store("reopen-generation").await;
        seed_position(&store, "pos_gen", "binance|futures|BTC|USDT|USDT").await;
        let scope_key = "bot:bot-gen";

        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "first", 10)
            .await
            .expect("first latch");
        let first = action_retry_targets(&store)
            .await
            .expect("scan")
            .into_iter()
            .find(|t| t.scope_key == scope_key)
            .expect("first latch is in the retry set");
        assert_eq!(first.level, Level::Flatten);

        // 旧代际上把 pacer 退到上限：这就是复核描述的"上一轮已退避到约 60 秒"。
        let mut pacer = ActionRetryPacer::new();
        for _ in 0..8 {
            pacer.record(scope_key, first.generation, false, 1_000);
        }
        assert!(!pacer.is_due(scope_key, first.generation, 1_000 + 30_000));

        // 交易所真值收敛：latch 写成 flattened，离开重试集合。
        record_reconcile_for_scope_now(
            &store,
            scope_key,
            "cred-gen",
            ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
            11,
        )
        .await
        .expect("record a converged truth");

        // 新一代裸敞口。
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "second", 12)
            .await
            .expect("second latch");
        let second = action_retry_targets(&store)
            .await
            .expect("scan")
            .into_iter()
            .find(|t| t.scope_key == scope_key)
            .expect("reopened latch is back in the retry set");
        assert!(
            second.generation > first.generation,
            "re-latch must advance the generation: first={} second={}",
            first.generation,
            second.generation
        );
        assert!(
            pacer.is_due(scope_key, second.generation, 1_000),
            "a pacer backed off on generation {} must treat generation {} as due at once",
            first.generation,
            second.generation
        );
        cleanup(store, path).await;
    }

    /// **接线证明（scheduler）**：调度循环把扫描出的真实代际传给 pacer。
    ///
    /// 循环是 spawn 出去的任务，起不来测试进程；仓内既有做法是源码形状断言。
    /// 这条能杀掉"循环传常量而不传 `generation`"的变异。
    #[test]
    fn the_scheduler_hands_the_real_generation_to_the_pacer() {
        let source = include_str!("kill_switch.rs");
        let loop_body = source
            .split("let mut pacer = ActionRetryPacer::new();")
            .nth(1)
            .expect("scheduler loop follows the pacer construction");
        let loop_body = loop_body
            .split("pub fn default_kill_file()")
            .next()
            .expect("scheduler loop ends before default_kill_file");
        assert!(
            loop_body.contains("pacer.is_due(&scope_key, generation, now_ms)"),
            "is_due must receive the scanned generation"
        );
        assert!(
            loop_body.contains("pacer.record(&scope_key, generation, succeeded"),
            "record must receive the scanned generation"
        );
    }

    /// **一次都不会被重试**，调用方还会以为"已经平掉了"。
    #[tokio::test]
    async fn latch_flatten_pending_reopens_an_already_converged_latch() {
        let (store, path) = test_store("reopen-converged").await;
        seed_position(&store, "pos_r13ks", "binance|futures|BTC|USDT|USDT").await;

        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "first",
            10,
        )
        .await
        .expect("first latch");
        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), "flattening".to_string())
        );

        // 交易所真值收敛：latch 写成 flattened，离开重试集合。
        record_reconcile_for_scope_now(
            &store,
            "bot:bot-r13ks",
            "cred-r13ks",
            ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
            11,
        )
        .await
        .expect("record a converged truth");
        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), "flattened".to_string())
        );
        assert!(
            !scopes_needing_action_retry(&store)
                .await
                .expect("scan")
                .iter()
                .any(|(scope_key, _)| scope_key == "bot:bot-r13ks"),
            "a converged latch must not be retried"
        );

        // 又发现一个新的裸敞口。
        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "second",
            12,
        )
        .await
        .expect("second latch");

        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), "flattening".to_string()),
            "a new unprotected exposure must reopen the latch"
        );
        assert!(
            scopes_needing_action_retry(&store)
                .await
                .expect("scan")
                .contains(&("bot:bot-r13ks".to_string(), Level::Flatten)),
            "the reopened latch must be back in the retry set"
        );
        cleanup(store, path).await;
    }

    async fn generation_of(store: &Store, scope_key: &str) -> i64 {
        sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch generation")
            .get::<i64, _>("flatten_generation")
    }

    /// `(event_id, flatten_generation, reconcile_clean, flatten_state)`，按代际排序。
    async fn events_of(store: &Store, scope_key: &str) -> Vec<(String, i64, i64, String)> {
        sqlx::query(
            "SELECT event_id, flatten_generation, COALESCE(reconcile_clean, -1) AS reconcile_clean, \
             COALESCE(flatten_state, '') AS flatten_state FROM kill_switch_events \
             WHERE scope_key=? ORDER BY flatten_generation, event_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the kill switch events")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("event_id"),
                row.get::<i64, _>("flatten_generation"),
                row.get::<i64, _>("reconcile_clean"),
                row.get::<String, _>("flatten_state"),
            )
        })
        .collect()
    }

    /// **一次 latch 恰好推进一个代际，
    /// 并发两次 latch 恰好推进两个。**
    ///
    /// R14 把"重开一轮"写成了两条独立语句：`arm_in_tx`（写库时 +1）之后再
    /// `SELECT generation` + `UPDATE generation+1`。两个后果：
    ///
    /// * **算术错位**——首次 latch 一口气推进两个代际，而那条刚写下的事件行
    ///   还停在第一个号上：审计面与 state 从第一次闩就对不上；
    /// * **读改写**——两条并发 latch 同读 N 同写 N+1，两个不同的裸敞口
    ///   共用一个号，先回来的那份 flat truth 可以把两个一起写成 `flattened`。
    ///
    /// 这条用例**不靠 sleep**：`tokio::join!` 把两次 latch 同时交出去，
    /// 然后只看事实——代际号恰好是 2，两条事件行分别带 1 和 2，
    /// 没有两条共用一个号。
    #[tokio::test]
    async fn concurrent_latches_each_take_their_own_generation() {
        let (store, path) = test_store("concurrent-latch").await;
        seed_position(&store, "pos_r15ks", "binance|futures|BTC|USDT|USDT").await;

        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "first",
            10,
        )
        .await
        .expect("first latch");
        assert_eq!(
            generation_of(&store, "bot:bot-r13ks").await,
            1,
            "one latch is exactly one round"
        );

        let (second, third) = tokio::join!(
            latch_flatten_pending(
                &store,
                "bot:bot-r13ks",
                "auto_protection_timeout",
                "second",
                11
            ),
            latch_flatten_pending(
                &store,
                "bot:bot-r13ks",
                "auto_protection_timeout",
                "third",
                11
            ),
        );
        second.expect("second latch");
        third.expect("third latch");
        assert_eq!(
            generation_of(&store, "bot:bot-r13ks").await,
            3,
            "two concurrent latches must land on two distinct rounds, not one"
        );

        let events = events_of(&store, "bot:bot-r13ks").await;
        let generations = events
            .iter()
            .map(|(_, generation, _, _)| *generation)
            .collect::<Vec<_>>();
        assert_eq!(
            generations,
            vec![1, 2, 3],
            "every latch writes an event bound to its own round: {events:?}"
        );
        assert_eq!(
            generations.last().copied(),
            Some(generation_of(&store, "bot:bot-r13ks").await),
            "the newest event must carry the round the state is actually in: {events:?}"
        );
        cleanup(store, path).await;
    }

    /// **对账真值只能落在**本代**的
    /// 事件行上。**
    ///
    /// R14 的事件回写是「按 `armed_at_ms DESC` 取最新一条未 reset 的事件，
    /// 无条件写进去」——一个代际条件都没有。两轮 latch 在同一毫秒发生时
    /// （生产里就是这样：闩用的是同一个 `now_ms()`），`DESC LIMIT 1` 的
    /// 并列名次由行序决定，于是这一轮的 `reconcile_clean` / `flatten_state`
    /// 会被盖到**上一轮**那条事件行上：审计面显示上一轮已经 clean/flattened，
    /// 而它其实从未收敛过。
    ///
    /// 迁移 0026 把代际号加到事件行上，选择与写入都绑代并查命中数。
    /// 两个方向：本代事件行拿到结论；上一代事件行**一个字段都不许动**。
    ///
    /// 交错是**确定性**的，不靠 SQLite 对并列名次的处置：第一轮 latch 的
    /// `armed_at_ms` 刻意比第二轮**大**（时钟回拨是真实存在的形态，而
    /// `now_ms` 本来就是入参）。于是 `ORDER BY armed_at_ms DESC LIMIT 1`
    /// 必然选中**上一轮**那条事件行——排序不是身份，代际才是。
    #[tokio::test]
    async fn a_reconcile_truth_only_writes_the_event_of_its_own_generation() {
        let (store, path) = test_store("event-generation").await;
        seed_position(&store, "pos_r15ev", "binance|futures|BTC|USDT|USDT").await;
        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "first",
            30,
        )
        .await
        .expect("first latch");
        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "second",
            20,
        )
        .await
        .expect("second latch");
        let before = events_of(&store, "bot:bot-r13ks").await;
        assert_eq!(before.len(), 2, "{before:?}");
        assert!(
            before
                .iter()
                .all(|(_, _, reconcile_clean, _)| *reconcile_clean == -1),
            "neither round has a verdict yet: {before:?}"
        );

        assert_eq!(
            record_reconcile_for_scope_now(
                &store,
                "bot:bot-r13ks",
                "cred-r13ks",
                ReconcileTruth {
                    has_diff: false,
                    all_positions_zero: true,
                    all_open_orders_empty: true,
                    all_open_algo_orders_empty: true,
                    local_position_zero_diff: true,
                },
                31,
            )
            .await
            .expect("record a same-generation truth"),
            ReconcileProjection::Applied
        );

        let after = events_of(&store, "bot:bot-r13ks").await;
        assert_eq!(
            after[0],
            before[0].clone(),
            "the previous round's event must not receive this round's verdict: {after:?}"
        );
        assert_eq!(
            (after[1].2, after[1].3.as_str()),
            (1, super::FLATTEN_CONVERGED),
            "this round's event carries the verdict: {after:?}"
        );
        cleanup(store, path).await;
    }

    /// **绑代写入命中 0 行 = 代际错位，不是 `Applied`。**
    ///
    /// R14 的两处写入都带（或本该带）代际谓词，却从不看 `rows_affected()`：
    /// 写了 0 行与写成功返回同一个 `Applied`，调用方与审计面都会以为
    /// "这份真值已经投影进去了"。
    ///
    /// 这里把事件行放到**另一个代际**上——这正是"读到代际之后、写下去之前
    /// 又闩了一轮"在库里留下的形状（迁移 0026 之前的存量事件行也天生停在
    /// 代际 0）。判据必须整事务回滚：`DiscardedRaced`，且
    /// `kill_switch_state` 一个字段都没被写成收敛。
    #[tokio::test]
    async fn a_generation_mismatch_on_the_event_row_applies_nothing() {
        let (store, path) = test_store("event-generation-miss").await;
        seed_position(&store, "pos_r15miss", "binance|futures|BTC|USDT|USDT").await;
        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "only",
            30,
        )
        .await
        .expect("latch");
        let current = generation_of(&store, "bot:bot-r13ks").await;
        sqlx::query("UPDATE kill_switch_events SET flatten_generation=? WHERE scope_key=?")
            .bind(current - 1)
            .bind("bot:bot-r13ks")
            .execute(store.pool())
            .await
            .expect("push the active event onto another round");

        assert_eq!(
            record_reconcile_for_scope_now(
                &store,
                "bot:bot-r13ks",
                "cred-r13ks",
                ReconcileTruth {
                    has_diff: false,
                    all_positions_zero: true,
                    all_open_orders_empty: true,
                    all_open_algo_orders_empty: true,
                    local_position_zero_diff: true,
                },
                31,
            )
            .await
            .expect("a generation mismatch is a discard, not an error"),
            ReconcileProjection::DiscardedRaced {
                generation: current
            }
        );
        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), super::FLATTEN_PENDING.to_string()),
            "nothing from a raced projection may be applied"
        );
        let status_rows = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_reconcile_status \
             WHERE scope_key=? AND reconcile_clean=1",
        )
        .bind("bot:bot-r13ks")
        .fetch_one(store.pool())
        .await
        .expect("count per-credential rows")
        .get::<i64, _>("n");
        assert_eq!(
            status_rows, 0,
            "the whole transaction rolls back, including the per-credential row"
        );
        assert!(
            scopes_needing_action_retry(&store)
                .await
                .expect("scan")
                .contains(&("bot:bot-r13ks".to_string(), Level::Flatten)),
            "a scope whose projection was discarded stays in the retry set"
        );
        cleanup(store, path).await;
    }

    /// **反向**：没有任何 latch 的 scope（`level='none'` 或压根没有行）上，
    /// 同一条投影**不是错误、也不是并发**——生产对账对 `global` / `cred:*` /
    /// `bot:*` 三类 scope 无条件投影一遍，其中绝大多数根本没闩过
    /// （`reconcile::reconcile_binance_futures_truth_with_client`）。
    /// 判据若把"有没有活着的 latch"这一层丢掉，每一次普通对账都会变成
    /// `DiscardedRaced` 并让整轮对账失败。
    ///
    /// **结论从"照写一行"改成"什么都不写"。**
    ///
    /// R17 在这条路径上照写一行逐 credential"事实记录"，并在这里断言
    /// `Applied` + 两行落库。那两行没有任何读者——聚合与 `reset` 都只数
    /// **本 revision** 的行，而下一次 arm 会把整个 scope 的行删掉重建——
    /// 它们唯一的作用是充当"任意 credential 都能给任意 scope 建行"的通路，
    /// 也就是 §9.5 缺陷的载体。现在这条路径返回 `NoActiveLatch` 并且
    /// **一行都不写**；"不让普通对账变红"这条原始不变量原样保留。
    #[tokio::test]
    async fn a_scope_without_any_latch_records_nothing_and_is_not_a_race() {
        let (store, path) = test_store("unarmed-projection").await;
        let flat = ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };
        // `global` 是迁移种下的 `level='none'` 行；`cred:*` 这一行压根不存在。
        for scope_key in ["global", "cred:cred-r15none"] {
            assert_eq!(
                record_reconcile_for_scope_now(&store, scope_key, "cred-r15none", flat, 41)
                    .await
                    .expect("an unarmed scope is not an error"),
                ReconcileProjection::NoActiveLatch,
                "{scope_key} has no live latch; its projection is neither applied nor a race"
            );
        }
        let recorded = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_reconcile_status WHERE credential_id=?",
        )
        .bind("cred-r15none")
        .fetch_one(store.pool())
        .await
        .expect("count per-credential rows")
        .get::<i64, _>("n");
        assert_eq!(
            recorded, 0,
            "a scope with no live revision has nothing to converge, so nothing may be written"
        );
        cleanup(store, path).await;
    }

    /// **旧一轮的 flat truth 不得收敛
    /// 新一轮的 latch。**
    ///
    /// 交错是确定性的，一步都不靠时序：
    ///
    /// 1. 第一轮 latch（代际 1）；
    /// 2. 一次对账**动身**——按生产纪律先取代际快照（`observed`），
    ///    然后它去读交易所（这里就是它拿在手里的那一刻）；
    /// 3. **在它回来之前**又发现一个新的裸敞口，第二次 latch（代际 2）；
    /// 4. 第一轮那次对账现在才落库，而且带回来的是一份"全平"的真值。
    ///
    /// R13 没有代际号：第 4 步会把 `flatten_state` 直接写成 `flattened`，
    /// 于是这个**从未被平过**的新敞口离开重试集合，调用方还会以为已经平了。
    /// 评审点名现有用例"全串行、观察不到"——串行调用里第 2 步与第 4 步之间
    /// 没有第 3 步，代际永远相等。
    ///
    /// 两个方向都断言：旧真值被丢弃且 latch 仍在重试集合里；随后**本代**
    /// 观察到的同一份真值照常收敛（否则"一律丢弃"也能全绿，而那会让
    /// latch 永远无法完成）。
    #[tokio::test]
    async fn a_reconcile_truth_observed_before_a_new_latch_cannot_converge_it() {
        let (store, path) = test_store("stale-truth-interleave").await;
        seed_position(&store, "pos_r14ks", "binance|futures|BTC|USDT|USDT").await;
        let flat = ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };

        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "first",
            10,
        )
        .await
        .expect("first latch");

        // 这次对账动身了：它看到的是第一轮。
        let observed = observe_flatten_generations(&store)
            .await
            .expect("observe generations before reading exchange truth");

        // 它还在路上时，又发现一个新的裸敞口。
        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "second",
            11,
        )
        .await
        .expect("second latch");

        // 现在它才回来，带着一份"全平"的真值。
        let projection =
            record_reconcile_for_scope(&store, "bot:bot-r13ks", "cred-r13ks", flat, 12, &observed)
                .await
                .expect("recording a stale truth is not an error, it is a discard");
        // 代际号是**不透明的单调令牌**，用例不钉它的算术，只钉它的序：
        // 落库时的代际必须比观察时的新，这条真值因此必须被丢弃。
        match projection {
            ReconcileProjection::DiscardedStale { observed, current } => {
                assert!(
                    observed < current,
                    "a truth from an earlier round must be discarded: {observed} vs {current}"
                );
            }
            other => panic!("a stale truth must not be applied: {other:?}"),
        }
        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), "flattening".to_string()),
            "a truth observed before this exposure existed must not converge it"
        );
        assert!(
            scopes_needing_action_retry(&store)
                .await
                .expect("scan")
                .contains(&("bot:bot-r13ks".to_string(), Level::Flatten)),
            "the new exposure must stay in the retry set"
        );

        // 反向：本代观察到的同一份真值照常收敛。
        let fresh = observe_flatten_generations(&store)
            .await
            .expect("observe generations for this round");
        assert_eq!(
            record_reconcile_for_scope(&store, "bot:bot-r13ks", "cred-r13ks", flat, 13, &fresh)
                .await
                .expect("record a same-generation truth"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), "flattened".to_string()),
            "a truth that really looked at this exposure converges it"
        );
        cleanup(store, path).await;
    }

    /// **上一轮遗留的逐 credential 行不参与本轮的聚合。**
    ///
    /// 代际号同时是聚合的过滤条件（防御深度的第二层：第一层是落库时的
    /// CAS）。这里直接手写一条**上一代**的 `flatten_complete=0` 行——
    /// 没有过滤时它会把这条 latch 永远钉在 `flattening`：本轮的每个
    /// credential 都报告了真值，却因为一条属于上一轮的行而永远收不了口。
    #[tokio::test]
    async fn a_previous_generation_row_never_counts_towards_a_new_round() {
        let (store, path) = test_store("generation-aggregate").await;
        seed_position(&store, "pos_r14agg", "binance|futures|BTC|USDT|USDT").await;
        let flat = ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };

        latch_flatten_pending(
            &store,
            "bot:bot-r13ks",
            "auto_protection_timeout",
            "first",
            10,
        )
        .await
        .expect("latch");
        let generation = sqlx::query(
            "SELECT flatten_generation FROM kill_switch_state WHERE scope_key='bot:bot-r13ks'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read the current generation")
        .get::<i64, _>("flatten_generation");
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation) \
             VALUES ('bot:bot-r13ks', 'cred-from-an-older-round', 0, 0, 1, ?)",
        )
        .bind(generation - 1)
        .execute(store.pool())
        .await
        .expect("plant a leftover row from an older round");

        record_reconcile_for_scope_now(&store, "bot:bot-r13ks", "cred-r13ks", flat, 11)
            .await
            .expect("this round's only credential reports a converged truth");
        assert_eq!(
            latch_state(&store, "bot:bot-r13ks").await,
            ("flatten".to_string(), "flattened".to_string()),
            "a row belonging to an older round must not keep this round open forever"
        );
        cleanup(store, path).await;
    }

    /// **认不出交易身份的未平仓位不得被静默丢弃。**
    ///
    /// 这里此前是 `filter_map(|row| { … usdm_symbol(&symbol_key)? … })`：
    /// `?` 把"这一行不是 Binance USDⓈ-M 身份"折成"这一行不存在"。
    /// 于是一条 `okx|spot|BTC|USDT|BTC` 形状的未平仓记录被整行丢掉——
    /// 一张平仓单都不发，调用方拿到 `Ok(())`，敞口原样留着。
    ///
    /// **不联网**：这个 scope 里唯一的仓位就是那条认不出身份的行，
    /// 平仓循环体一次都不执行，交易所客户端只是被构造出来放着。
    #[tokio::test]
    async fn an_open_position_without_a_usdm_identity_is_registered_and_fails_the_flatten() {
        let (store, path) = test_store("unresolved-identity").await;
        seed_position(&store, "pos_alien", "okx|spot|BTC|USDT|BTC").await;
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "cred-r13ks",
            vec![],
        );
        let filters = crate::exchange_filters::ExchangeFilterCache::new();

        let error = submit_live_flatten(&store, &client, "cred-r13ks", "bot:bot-r13ks", &filters)
            .await
            .expect_err("a position this engine cannot close must not be reported as flattened");
        assert!(
            error.to_string().contains("okx|spot|BTC|USDT|BTC"),
            "the error must name the row it could not resolve: {error}"
        );

        let registered = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='flatten_identity_unresolved'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count audit events")
        .get::<i64, _>("n");
        assert_eq!(
            registered, 1,
            "the unresolvable exposure must be registered, not dropped"
        );
        cleanup(store, path).await;
    }

    /// **放行面（反向断言）**：闭世界内的仓位照常进入平仓循环。
    ///
    /// 没有它，"一律报错" 也能让上面那条全绿。这里用一个**空**的 mock
    /// 响应队列：真的走到 `place_market_order` 就会因为"没有可用的 mock
    /// 响应"而失败——失败信息里**不含**那条 unresolved 摘要，正好把
    /// "身份被解析出来了、代码确实往下走了" 与 "被当成认不出而拒绝"
    /// 区分开。
    #[tokio::test]
    async fn a_usdm_position_reaches_the_close_order_path() {
        let (store, path) = test_store("usdm-reaches-close").await;
        seed_position(&store, "pos_usdm", "binance|futures|BTC|USDT|USDT").await;
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "cred-r13ks",
            vec![],
        );
        let filters = crate::exchange_filters::ExchangeFilterCache::new();

        let outcome =
            submit_live_flatten(&store, &client, "cred-r13ks", "bot:bot-r13ks", &filters).await;

        let message = outcome
            .err()
            .map(|error| error.to_string())
            .expect("the close order really was attempted against the stub client");
        assert_eq!(
            message, "in-memory HTTP response queue exhausted",
            "a closed-world position must reach place_market_order, not be refused as an \
             unresolved identity"
        );
        assert_eq!(
            sqlx::query(
                "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='flatten_identity_unresolved'",
            )
            .fetch_one(store.pool())
            .await
            .expect("count audit events")
            .get::<i64, _>("n"),
            0,
            "nothing to register when the identity resolves"
        );
        cleanup(store, path).await;
    }
}

/// **generation 升级与 reset 收敛。**
///
/// 两组缺陷共用一套夹具：
///
/// * **regression（High）**：0026 把存量事件行置为代际 0 却假定
///   `kill_switch_state` 也停在 0。这里用**真实迁移 runner** 造出那个状态空间
///   ——先用一份"只到 0026"的迁移目录建库（`Migrator::new`，跑的是同一批
///   `.sql` 文件），在里面留下一条 `state.generation = N > 0` 的活 latch，
///   再让生产的 `Store::connect` 走完整迁移 + 复核。
/// * **regression（Medium，杀 R12M11）**：`reset` 按 `armed_at_ms DESC`
///   选未 reset 事件、不绑代际、不查命中数。
#[cfg(test)]
mod r16_generation_upgrade_tests {
    use std::path::{Path, PathBuf};
    use std::sync::Once;

    use sqlx::any::AnyPoolOptions;
    use sqlx::Row;

    use super::{
        arm, latch_flatten_pending, record_reconcile_for_scope_now, reset,
        revalidate_legacy_event_generations, ArmRequest, Level, ReconcileProjection,
        ReconcileTruth,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const FLAT: ReconcileTruth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    };

    fn temp_db_path(label: &str) -> PathBuf {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        std::env::temp_dir().join(format!("rustcore-r16gen-{label}-{}.db", ulid::Ulid::new()))
    }

    /// 一份**只到 0026** 的迁移目录：与仓内 `.sql` 逐字节相同（复制，不改写），
    /// 因此 sqlx 的 checksum 与后续完整迁移完全一致——这是一次真的增量升级，
    /// 不是"手写等价 DDL"。
    fn migrations_without_0027(label: &str) -> PathBuf {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let target =
            std::env::temp_dir().join(format!("rustcore-r16mig-{label}-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&target).expect("create the pre-0027 migration directory");
        let mut copied = 0usize;
        for entry in std::fs::read_dir(&source).expect("read the migration directory") {
            let path = entry.expect("read a migration entry").path();
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .expect("migration file name")
                .to_string();
            if !name.ends_with(".sql") || name.starts_with("0027_") {
                continue;
            }
            std::fs::copy(&path, target.join(&name)).expect("copy a migration file");
            copied += 1;
        }
        assert!(
            copied >= 26,
            "the pre-0027 migration set must contain the whole chain, got {copied}"
        );
        target
    }

    /// 把库推进到 **0026 时点**并留下 R14/R15 运行期才可能出现的状态：
    /// `kill_switch_state.flatten_generation = 2` 的活 flatten latch，
    /// 而所有事件行都被 0026 的 `DEFAULT 0` 置成了代际 0。
    async fn seeded_pre_0027_database(label: &str) -> (String, PathBuf, PathBuf) {
        let path = temp_db_path(label);
        let url = format!("sqlite://{}", path.display());
        std::fs::File::create(&path).expect("create the sqlite file");
        let migrations = migrations_without_0027(label);
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the pre-0027 database");
        sqlx::migrate::Migrator::new(migrations.as_path())
            .await
            .expect("load the pre-0027 migrator")
            .run(&pool)
            .await
            .expect("run migrations up to 0026");
        // 0027 之前这一列不存在，本代事件的绑定条件也就无从建立。
        assert!(
            sqlx::query("SELECT generation_proved_at_ms FROM kill_switch_events")
                .fetch_optional(&pool)
                .await
                .is_err(),
            "the pre-0027 schema must not already carry the revalidation column"
        );
        pool.close().await;
        (url, path, migrations)
    }

    /// R14 起的运行期真值：两轮 flatten latch ⇒ state 代际 2、两条事件行。
    /// 随后按 **0026 的 `DEFAULT 0`** 把事件行代际抹回 0——这正是升级后的样子。
    async fn latch_twice_then_apply_0026_defaults(
        url: &str,
        scope_key: &str,
        bot_uid: &str,
        credential_id: &str,
    ) {
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("connect for seeding");
        // ****：R14/R15 运行期的真实形状里，这条
        // `bot:` latch 之所以被闩住，就是因为这个 credential 在这个 bot 下
        // 有敞口——`reconcile_targets_in_tx` 正是从这张表认成员的。
        // R17 的夹具省掉了它，靠"任意 credential 都能建行"蒙混过关。
        sqlx::query(
            "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, \
             direction, qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms) \
             VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100', 'open', \
             1, 1, 1)",
        )
        .bind(format!("pos_{bot_uid}"))
        .bind(credential_id)
        .bind(bot_uid)
        .execute(&pool)
        .await
        .expect("seed the exposure the latch was armed for");
        for (reason, now_ms) in [("first exposure", 1_000i64), ("second exposure", 2_000)] {
            // 直接写 0026 时点的形状：事件行此时**没有** flatten_generation 列
            // 之外的任何代际语义，state 的代际由 0023 起的生产语义推进。
            sqlx::query(
                "INSERT INTO kill_switch_events (event_id, scope, scope_key, level, \
                 trigger_source, reason, armed_at_ms, flatten_state, reconcile_clean, \
                 reset_by, reset_at_ms, created_at_ms, updated_at_ms, flatten_generation) \
                 VALUES (?, 'bot', ?, 'flatten', 'auto_protection_timeout', ?, ?, \
                 'flattening', NULL, NULL, NULL, ?, ?, 0)",
            )
            .bind(ulid::Ulid::new().to_string())
            .bind(scope_key)
            .bind(reason)
            .bind(now_ms.to_string())
            .bind(now_ms.to_string())
            .bind(now_ms.to_string())
            .execute(&pool)
            .await
            .expect("seed a legacy kill switch event");
        }
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, flatten_generation, last_reconcile_clean, \
             last_reconcile_at_ms, updated_at_ms) \
             VALUES (?, 'flatten', 'second exposure', 'auto_protection_timeout', '2000', \
             'flattening', 2, NULL, NULL, '2000')",
        )
        .bind(scope_key)
        .execute(&pool)
        .await
        .expect("seed the armed latch at generation 2");
        pool.close().await;
    }

    /// **一个被闩住的 `bot:` scope 必须有敞口**。
    ///
    /// `reconcile_targets_in_tx` 是从 `position_state` / `engine_orders` 里
    /// 认成员的：一个**没有任何本地仓位或本地单**的 bot scope 成员集合为空，
    /// 而空集合按 R18 的闭世界判据永远收敛不了（一次交易所真值都没看过就
    /// 宣布"已平"正是它要拦的）。R17 的夹具靠"任意 credential 都能建行"
    /// 绕过了这一点——那恰好就是本轮修掉的缺陷。这里把夹具补成真实形状：
    /// 先有敞口，才谈得上闩住它并把它平掉。
    async fn seed_scope_exposure(store: &Store, bot_uid: &str, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100',
              'open', 1, 1, 1)
            "#,
        )
        .bind(format!("pos_{bot_uid}_{credential_id}"))
        .bind(credential_id)
        .bind(bot_uid)
        .execute(store.pool())
        .await
        .expect("seed the exposure that motivates the latch");
    }

    async fn latch_state(store: &Store, scope_key: &str) -> (String, String) {
        let row = sqlx::query(
            "SELECT level, COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch");
        (
            row.get::<String, _>("level"),
            row.get::<String, _>("flatten_state"),
        )
    }

    async fn event_rows(store: &Store, scope_key: &str) -> Vec<(String, i64, bool, String)> {
        sqlx::query(
            "SELECT event_id, flatten_generation, \
             CASE WHEN reset_at_ms IS NULL THEN 0 ELSE 1 END AS is_reset, \
             reason FROM kill_switch_events \
             WHERE scope_key=? ORDER BY created_at_ms, event_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read kill switch events")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("event_id"),
                row.get::<i64, _>("flatten_generation"),
                row.get::<i64, _>("is_reset") == 1,
                crate::store::try_get_text(&row, "reason")
                    .expect("decode the event reason")
                    .unwrap_or_default(),
            )
        })
        .collect()
    }

    async fn revalidation_verdicts(store: &Store, scope_key: &str) -> Vec<(String, i64, i64)> {
        sqlx::query(
            "SELECT verdict, previous_generation, resolved_generation \
             FROM kill_switch_event_generation_revalidations WHERE scope_key=? \
             ORDER BY verdict, event_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read revalidation verdicts")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("verdict"),
                row.get::<i64, _>("previous_generation"),
                row.get::<i64, _>("resolved_generation"),
            )
        })
        .collect()
    }

    /// **新装**：全新库跑完整迁移链，复核是空集，latch → 对账 → reset 全程正常。
    #[tokio::test]
    async fn a_fresh_install_needs_no_event_generation_revalidation() {
        let path = temp_db_path("fresh");
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect a fresh store");
        let scope_key = "bot:bot-r16fresh";
        assert!(revalidation_verdicts(&store, scope_key).await.is_empty());
        seed_scope_exposure(&store, "bot-r16fresh", "cred-r16fresh").await;

        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "fresh", 10)
            .await
            .expect("latch a fresh scope");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r16fresh", FLAT, 20)
                .await
                .expect("project a flat truth"),
            ReconcileProjection::Applied,
            "a fresh install must converge its latch on the first flat truth"
        );
        reset(&store, scope_key, "operator", None, 30)
            .await
            .expect("reset a converged latch");
        assert_eq!(
            revalidate_legacy_event_generations(&store, 40)
                .await
                .expect("revalidate a healthy database"),
            0,
            "a healthy database must not produce any revalidation rows"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **非零代升级（增量）**：0026 时点的库里留着 `state.generation=2` 的活
    /// latch 与两条代际 0 的事件行。
    ///
    /// 先证明缺陷本身：这样的 latch 在 `record_reconcile_for_scope` 里
    /// **每一次**都返回 `DiscardedRaced`——永远收敛不了，重试任务会一直
    /// 重复撤单 + reduce-only flatten。再让生产的 `Store::connect` 跑完整
    /// 迁移 + 复核，同一份真值必须变成 `Applied`。
    #[tokio::test]
    async fn a_nonzero_generation_upgrade_adopts_the_matching_legacy_event() {
        let scope_key = "bot:bot-r16adopt";
        let (url, path, migrations) = seeded_pre_0027_database("adopt").await;
        latch_twice_then_apply_0026_defaults(&url, scope_key, "bot-r16adopt", "cred-r16adopt")
            .await;

        // ---- 缺陷复现（0027 之前的行为）----------------------------------
        {
            let pool = AnyPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("connect the pre-0027 database");
            let live = sqlx::query(
                "SELECT COUNT(*) AS n FROM kill_switch_events \
                 WHERE scope_key=? AND reset_at_ms IS NULL AND flatten_generation=2",
            )
            .bind(scope_key)
            .fetch_one(&pool)
            .await
            .expect("count current-generation events")
            .get::<i64, _>("n");
            assert_eq!(
                live, 0,
                "the 0026 upgrade must leave this latch without a current-generation event"
            );
            pool.close().await;
        }

        // ---- 生产升级路径：完整迁移 + 启动复核 -----------------------------
        let store = Store::connect(&url).await.expect("upgrade through 0027");
        let verdicts = revalidation_verdicts(&store, scope_key).await;
        assert_eq!(
            verdicts,
            vec![("adopted".to_string(), 0, 2)],
            "the second-round event carries the arm identity of the live latch"
        );
        let events = event_rows(&store, scope_key).await;
        assert_eq!(events.len(), 2, "no event row may be invented on adoption");
        let adopted = events
            .iter()
            .find(|(_, _, _, reason)| reason == "second exposure")
            .expect("the second-round event still exists");
        assert_eq!(
            adopted.1, 2,
            "the live round's event must join generation 2"
        );
        let untouched = events
            .iter()
            .find(|(_, _, _, reason)| reason == "first exposure")
            .expect("the first-round event still exists");
        assert_eq!(
            untouched.1, 0,
            "an earlier round's event must not be pulled into the live generation"
        );

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r16adopt", FLAT, 3_000)
                .await
                .expect("project a flat truth after the upgrade"),
            ReconcileProjection::Applied,
            "after revalidation the live latch must be able to converge"
        );
        assert_eq!(
            latch_state(&store, scope_key).await,
            ("flatten".to_string(), "flattened".to_string())
        );

        // **重跑**：再连一次，复核必须是空操作。
        store.pool().close().await;
        let store = Store::connect(&url).await.expect("reconnect");
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await.len(),
            1,
            "rerunning the revalidation must not add rows"
        );
        assert_eq!(event_rows(&store, scope_key).await.len(), 2);
        assert_eq!(
            revalidate_legacy_event_generations(&store, 4_000)
                .await
                .expect("rerun the revalidation directly"),
            0
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(migrations);
    }

    /// **证不出归属 ⇒ 隔离 + 锚点**（而不是猜一条旧事件顶上）。
    ///
    /// 这里把 state 的 arm 身份改成与任何存量事件都不同（模拟 0026 之前
    /// 事件行与 state 已经各自演化过的库）：复核必须把候选行原样隔离在旧代际，
    /// 并在当前代际补一条 `generation_revalidation` 锚事件，让 latch 重新
    /// 可以被对账收敛。
    #[tokio::test]
    async fn an_unprovable_legacy_generation_is_isolated_and_anchored() {
        let scope_key = "bot:bot-r16anchor";
        let (url, path, migrations) = seeded_pre_0027_database("anchor").await;
        latch_twice_then_apply_0026_defaults(&url, scope_key, "bot-r16anchor", "cred-r16anchor")
            .await;
        {
            let pool = AnyPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("connect for identity drift");
            sqlx::query("UPDATE kill_switch_state SET armed_at_ms='2500' WHERE scope_key=?")
                .bind(scope_key)
                .execute(&pool)
                .await
                .expect("drift the arm identity");
            pool.close().await;
        }

        let store = Store::connect(&url).await.expect("upgrade through 0027");
        let verdicts = revalidation_verdicts(&store, scope_key).await;
        assert_eq!(
            verdicts,
            vec![
                ("anchored".to_string(), 2, 2),
                ("isolated".to_string(), 0, 0),
                ("isolated".to_string(), 0, 0),
            ],
            "unprovable legacy events stay in their own generation; a new anchor carries the \
             live one"
        );
        let events = event_rows(&store, scope_key).await;
        assert_eq!(events.len(), 3, "exactly one anchor event may be added");
        assert_eq!(
            events
                .iter()
                .filter(|(_, generation, _, _)| *generation == 0)
                .count(),
            2,
            "no legacy event may be moved into the live generation"
        );
        let anchor = events
            .iter()
            .find(|(_, generation, _, _)| *generation == 2)
            .expect("an anchor event exists at the live generation");
        assert!(
            anchor.3.contains("generation revalidation anchor"),
            "the anchor must say what it is: {}",
            anchor.3
        );

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r16anchor", FLAT, 3_000)
                .await
                .expect("project a flat truth after anchoring"),
            ReconcileProjection::Applied,
            "the anchor is the recoverable path: the latch must converge again"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(migrations);
    }

    /// **regression（杀 R12M11）· 墙钟回拨下 reset 必须绑当前代际。**
    ///
    /// 第一轮 latch 的 `armed_at_ms` 是 9000，时钟回拨之后第二轮是 1000。
    /// 旧实现按 `armed_at_ms DESC LIMIT 1` 选未 reset 事件 ⇒ 选中**第一轮**
    /// 那条：它被标成 reset，而当前这一轮的事件仍然挂着未 reset，
    /// state 却已经被清成 `none`。
    #[tokio::test]
    async fn reset_binds_to_the_current_generation_under_a_clock_rollback() {
        let path = temp_db_path("rollback");
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect a fresh store");
        let scope_key = "bot:bot-r16reset";
        seed_scope_exposure(&store, "bot-r16reset", "cred-r16reset").await;

        latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "round one",
            9_000,
        )
        .await
        .expect("first latch");
        // 墙钟回拨：新一轮的 armed_at_ms 比上一轮**小**。
        latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "round two",
            1_000,
        )
        .await
        .expect("second latch after a clock rollback");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r16reset", FLAT, 1_100)
                .await
                .expect("converge the live round"),
            ReconcileProjection::Applied
        );

        reset(&store, scope_key, "operator", None, 1_200)
            .await
            .expect("reset the converged latch");

        let events = event_rows(&store, scope_key).await;
        assert_eq!(events.len(), 2);
        let first = events
            .iter()
            .find(|(_, _, _, reason)| reason == "round one")
            .expect("round one event");
        let second = events
            .iter()
            .find(|(_, _, _, reason)| reason == "round two")
            .expect("round two event");
        assert_eq!(
            (first.1, first.2),
            (1, false),
            "the older round must not be marked reset"
        );
        assert_eq!(
            (second.1, second.2),
            (2, true),
            "reset must land on the current generation's event, not the newest wall clock"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **多条未 reset 事件时，只有本代那条被 reset。**
    ///
    /// 上一轮的事件行在 `latch_flatten_pending` 重开一轮之后仍然是未 reset 的
    /// （它记录的是那一轮的处置），reset 不得顺手把它们一起标掉。
    #[tokio::test]
    async fn reset_only_marks_the_event_of_the_current_generation() {
        let path = temp_db_path("multi-active");
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect a fresh store");
        let scope_key = "cred:cred-r16multi";
        for (reason, now_ms) in [("r1", 10i64), ("r2", 20), ("r3", 30)] {
            latch_flatten_pending(&store, scope_key, "auto_protection_timeout", reason, now_ms)
                .await
                .expect("latch a round");
        }
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r16multi", FLAT, 40)
                .await
                .expect("converge the live round"),
            ReconcileProjection::Applied
        );
        reset(&store, scope_key, "operator", None, 50)
            .await
            .expect("reset the converged latch");

        let events = event_rows(&store, scope_key).await;
        assert_eq!(events.len(), 3);
        let reset_generations = events
            .iter()
            .filter(|(_, _, is_reset, _)| *is_reset)
            .map(|(_, generation, _, _)| *generation)
            .collect::<Vec<_>>();
        assert_eq!(
            reset_generations,
            vec![3],
            "exactly the live generation's event may be marked reset"
        );
    }

    /// **反向：本代没有 active 事件时 reset 必须失败**，不得留下一条
    /// 无从追溯的清闩（在 0027 复核之前，这就是升级后的形状）。
    #[tokio::test]
    async fn reset_is_rejected_when_the_current_generation_has_no_active_event() {
        let path = temp_db_path("no-event");
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect a fresh store");
        let scope_key = "cred:cred-r16noevent";
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::ManageOnly,
                trigger_source: "manual_api",
                reason: "operator",
                now_ms: 10,
            },
        )
        .await
        .expect("arm the scope");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r16noevent", FLAT, 20)
                .await
                .expect("record a clean truth"),
            ReconcileProjection::Applied
        );
        // 审计链断掉：本代事件被移走（升级错位、人工清表都是这个形状）。
        sqlx::query("UPDATE kill_switch_events SET flatten_generation = -1 WHERE scope_key=?")
            .bind(scope_key)
            .execute(store.pool())
            .await
            .expect("strand the audit trail");

        let error = reset(&store, scope_key, "operator", None, 30)
            .await
            .expect_err("a reset with no auditable event must fail closed");
        assert!(
            error.to_string().contains("no active kill switch event"),
            "the rejection must name the missing audit row: {error}"
        );
        let level = sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch")
            .get::<String, _>("level");
        assert_eq!(
            level, "manage_only",
            "a rejected reset must leave the latch armed"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
}

/// **单调 revision、单所有者、聚合负向。**
///
/// * **regression（High）**：只有 flatten arm 推代际，`pause_new` /
///   `manage_only` 整条生命周期停在同一个号上，代际 CAS 退化成恒真；
/// * **regression（Medium）**：代际 0 下任意一条活事件都算"健康"，
///   身份复核整段被跳过，`reset` 随后按 `LIMIT 1` 认错所有者；
/// * **R13M10（聚合负向）**：`dirty` 那一列的统计从来没被任何用例观察过。
#[cfg(test)]
mod r17_revision_tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;

    use super::{
        arm, latch_flatten_pending, record_reconcile_for_scope_now, reset,
        revalidate_legacy_event_generations, ArmRequest, Level, ReconcileProjection,
        ReconcileTruth, FLATTEN_CONVERGED, FLATTEN_PENDING,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const FLAT: ReconcileTruth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    };
    const DIRTY: ReconcileTruth = ReconcileTruth {
        has_diff: true,
        all_positions_zero: false,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: false,
    };

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-r17ks-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect kill-switch test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **一个 `bot:` scope 的成员资格来自本地敞口**。
    ///
    /// `reconcile_targets_in_tx` 从 `position_state` / `engine_orders` 认成员；
    /// 没有敞口的 bot scope 成员集合为空，按闭世界判据永远收敛不了。
    async fn seed_bot_exposure(store: &Store, bot_uid: &str, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100',
              'open', 1, 1, 1)
            "#,
        )
        .bind(format!("pos_{bot_uid}_{credential_id}"))
        .bind(credential_id)
        .bind(bot_uid)
        .execute(store.pool())
        .await
        .expect("seed the exposure that makes this credential a target of the scope");
    }

    async fn revision_of(store: &Store, scope_key: &str) -> i64 {
        sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch revision")
            .get::<i64, _>("flatten_generation")
    }

    async fn arm_at(store: &Store, scope_key: &str, level: Level, reason: &str, now_ms: i64) {
        arm(
            store,
            ArmRequest {
                scope_key,
                level,
                trigger_source: "manual_api",
                reason,
                now_ms,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("arm {level:?} must succeed: {error}"));
    }

    /// **每一次安全 level arm 都推进一格 revision——不只是 flatten。**
    ///
    /// R16 只在 flatten arm 上 +1。于是 `pause_new` → `manage_only` 这条
    /// 常见的升级路径**两次 arm 共用一个号**，代际 CAS（reset 与对账投影
    /// 都靠它）在这两档上等于恒真。
    ///
    /// 反向也钉住：幂等的重复 arm（档位不高于当前档位）走的是早返回，
    /// 一个号都不推——否则一条每轮扫描都重新 arm 的自动触发源会把 revision
    /// 推成计数器，把"这一轮"变得毫无意义。
    #[tokio::test]
    async fn every_safety_level_arm_advances_the_revision() {
        let (store, path) = test_store("revision").await;
        let scope_key = "cred:cred-r17rev";

        arm_at(&store, scope_key, Level::PauseNew, "first", 10).await;
        assert_eq!(
            revision_of(&store, scope_key).await,
            1,
            "the first arm opens revision 1"
        );

        arm_at(&store, scope_key, Level::ManageOnly, "second", 20).await;
        assert_eq!(
            revision_of(&store, scope_key).await,
            2,
            "escalating to manage_only is a new arm, so it must own a new revision"
        );

        arm_at(&store, scope_key, Level::PauseNew, "idempotent", 30).await;
        assert_eq!(
            revision_of(&store, scope_key).await,
            2,
            "an arm that does not raise the level writes nothing and must not move the revision"
        );

        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 40)
            .await
            .expect("latch flatten");
        assert_eq!(revision_of(&store, scope_key).await, 3);

        // 每一次 arm 的事件行都绑着它自己的 revision。
        let revisions = sqlx::query(
            "SELECT flatten_generation FROM kill_switch_events WHERE scope_key=? \
             ORDER BY flatten_generation",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the events")
        .into_iter()
        .map(|row| row.get::<i64, _>("flatten_generation"))
        .collect::<Vec<_>>();
        assert_eq!(
            revisions,
            vec![1, 2, 3],
            "every arm must leave exactly one event on its own revision"
        );
        cleanup(store, path).await;
    }

    /// **reset 的 clean 真值必须来自当前 revision。**
    ///
    /// 这里造的是墙钟回拨之后的形状：`kill_switch_state` 上的投影
    /// （`last_reconcile_clean=1` 且 `last_reconcile_at_ms >= armed_at_ms`）
    /// 完全成立，可那份 clean 真值属于**上一个 revision**——它观察的是
    /// 另一个敞口。旧判据只看这两个投影，会放行；新判据回到逐 credential
    /// 的真值行并只数本 revision 的，必须 fail-closed。
    #[tokio::test]
    async fn reset_rejects_a_clean_truth_from_another_revision() {
        let (store, path) = test_store("clean-revision").await;
        let scope_key = "cred:cred-r17clean";
        arm_at(&store, scope_key, Level::ManageOnly, "round one", 100).await;
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r17clean", FLAT, 110)
                .await
                .expect("converge round one"),
            ReconcileProjection::Applied
        );
        // 第二次 arm 开新 revision。墙钟回拨：新 arm 的 armed_at_ms 更小，
        // 于是上一轮那份 clean 真值在**时间上**仍然"晚于 arm"。
        latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "round two",
            50,
        )
        .await
        .expect("latch a second round after a clock rollback");
        // 把 state 上的投影按"上一轮遗留"复原（arm 会清成 NULL）。
        sqlx::query(
            "UPDATE kill_switch_state SET last_reconcile_clean=1, last_reconcile_at_ms='110' \
             WHERE scope_key=?",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("restore the stale projection a clock rollback would leave");

        let error = reset(&store, scope_key, "operator", None, 60)
            .await
            .expect_err("a clean truth from another revision must not clear this one");
        assert!(
            error.to_string().contains("reconcile rows"),
            "the rejection must name the missing current-revision truth: {error}"
        );
        assert_eq!(
            sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
                .bind(scope_key)
                .fetch_one(store.pool())
                .await
                .expect("read the latch")
                .get::<String, _>("level"),
            "flatten",
            "a rejected reset must leave the new exposure armed"
        );
        cleanup(store, path).await;
    }

    /// **上一代际遗留的 clean 行不得替这一轮背书。**
    ///
    /// 运行期 `reset_reconcile_targets_in_tx` 每次 arm 都把逐 credential 行
    /// 重置成"未 clean"，所以正常路径上不会有跨代际的残留。**升级存量**会：
    /// 0023 之前写下的 `kill_switch_reconcile_status` 行天生停在代际 0，
    /// 而 state 的代际此后被历次 latch 推到了 N——这与 0026 事件行的错位
    /// 是同一个形状（regression 就是它的事件面）。
    ///
    /// 判据里的 `flatten_generation=?` 就是挡这个的：本 revision 一行都没有
    /// ⇒ 这一轮从未被对账过 ⇒ fail-closed。摘掉它，那条代际 0 的 clean 行
    /// 会被当成本轮的收敛证据，把一个从未被处置过的 latch 清成 `none`。
    #[tokio::test]
    async fn reset_rejects_a_clean_row_left_at_an_older_revision() {
        let (store, path) = test_store("stale-clean-row").await;
        let scope_key = "cred:cred-r17stale";
        // 一条**当前 revision**的 armed latch，state 上的投影完全成立。
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, flatten_generation, last_reconcile_clean, \
             last_reconcile_at_ms, updated_at_ms) \
             VALUES (?, 'manage_only', 'exposure', 'manual', '2000', NULL, 2, 1, '2100', '2100')",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("seed an armed latch whose state projection already says clean");
        // 本 revision 有一条身份相符的活事件（审计链完好，隔离掉别的失败原因）。
        seed_event(
            &store,
            scope_key,
            "manage_only",
            "manual_api",
            "exposure",
            "2000",
            2,
        )
        .await;
        // 唯一的 clean 真值停在**代际 0**（升级存量）。
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation) \
             VALUES (?, 'cred-r17stale', 1, 1, 2100, 0)",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("seed a legacy clean row at generation 0");

        let error = reset(&store, scope_key, "operator", None, 2_200)
            .await
            .expect_err("a clean row from an older revision must not clear this one");
        assert!(
            error.to_string().contains("reconcile rows"),
            "the rejection must name the missing current-revision truth: {error}"
        );
        assert_eq!(
            sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
                .bind(scope_key)
                .fetch_one(store.pool())
                .await
                .expect("read the latch")
                .get::<String, _>("level"),
            "manage_only",
            "a rejected reset must leave the latch armed"
        );

        // 反向：本 revision 真的被对账干净之后，同一次 reset 必须放行。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r17stale", FLAT, 2_300)
                .await
                .expect("converge the current revision"),
            ReconcileProjection::Applied
        );
        reset(&store, scope_key, "operator", None, 2_400)
            .await
            .expect("a revision with its own clean truth must be clearable");
        cleanup(store, path).await;
    }

    /// **聚合负向（杀 R13M10）· 一条 dirty 的逐 credential 行必须让整个 scope
    /// 判成未收敛。**
    ///
    /// `record_reconcile_for_scope` 的收敛判据是本 revision 的聚合
    /// （`dirty = 0` 且 `incomplete = 0`）。此前**每一条**用例都只往一个
    /// scope 里放一个 credential，于是 `COUNT(CASE WHEN reconcile_clean = 0
    /// THEN 1 END)` 这一列有没有真的数到 dirty 行，一次都没被观察过——
    /// 把它改成一个永不成立的条件（dirty 恒 0）测试全绿，而那意味着
    /// **一个还没平掉的 credential 会被当成已收敛**，latch 写成 `flattened`、
    /// 退出重试集合。
    ///
    /// 这里一个 scope 两个 credential：先 clean、后 dirty。两个方向都钉住。
    ///
    /// **这条用例原本把缺陷钉成了绿。**
    ///
    /// R17 的形状是：两个 credential 都**没有任何本地敞口**（`bot:bot-r17agg`
    /// 下一条 `position_state` / `engine_orders` 都没有），却能靠
    /// `record_reconcile_for_scope` 的无条件 upsert 凭空建行；而"第一个
    /// credential 报 flat 之后整个 scope 就 `flattened`"被这里的
    /// `assert_eq!(…, (1, FLATTEN_CONVERGED))` **断言成了正确行为**。
    /// 那正是 §9.5 的缺陷本体——一条与 scope 毫无关系的 flat truth
    /// 就能让 latch 退出重试集合。
    ///
    /// 改法两处：夹具补上**真实敞口**（两个 credential 各一条仓位，
    /// 于是 `reconcile_targets_in_tx` 认得它们、成员集合是 `{a, b}`）；
    /// 断言翻过来——**只报了两个成员里的一个**时 scope 不得收敛。
    #[tokio::test]
    async fn one_dirty_credential_keeps_the_whole_scope_unconverged() {
        let (store, path) = test_store("aggregate-dirty").await;
        let scope_key = "bot:bot-r17agg";
        for credential_id in ["cred-r17agg-a", "cred-r17agg-b"] {
            seed_bot_exposure(&store, "bot-r17agg", credential_id).await;
        }
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r17agg-a", FLAT, 20)
                .await
                .expect("project the first credential"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            state_verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "one of the revision's two target credentials has not reported a single truth; \
             the scope must not converge on the other one alone"
        );

        // 同一个 scope 的第二个 credential 还没平掉。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r17agg-b", DIRTY, 30)
                .await
                .expect("project the second credential"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            state_verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "one dirty credential must make the whole scope dirty and unconverged"
        );
        assert!(
            super::scopes_needing_action_retry(&store)
                .await
                .expect("scan")
                .contains(&(scope_key.to_string(), Level::Flatten)),
            "an unconverged scope must stay in the retry set"
        );
        // 事件行上的审计结论必须与 state 一致。
        assert_eq!(
            sqlx::query(
                "SELECT COALESCE(reconcile_clean, -1) AS reconcile_clean, \
                 COALESCE(flatten_state, '') AS flatten_state FROM kill_switch_events \
                 WHERE scope_key=? AND reset_at_ms IS NULL",
            )
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .map(|row| (
                row.get::<i64, _>("reconcile_clean"),
                row.get::<String, _>("flatten_state")
            ))
            .expect("read the audit event"),
            (0, FLATTEN_PENDING.to_string()),
            "the audit face must not claim a convergence the state never reached"
        );

        // 反向：第二个 credential 也平掉之后，聚合必须重新收敛。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r17agg-b", FLAT, 40)
                .await
                .expect("project the second credential again"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            state_verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "once every credential is flat the scope converges"
        );
        cleanup(store, path).await;
    }

    async fn state_verdict(store: &Store, scope_key: &str) -> (i64, String) {
        let row = sqlx::query(
            "SELECT COALESCE(last_reconcile_clean, -1) AS clean, \
             COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch verdict");
        (
            row.get::<i64, _>("clean"),
            row.get::<String, _>("flatten_state"),
        )
    }

    /// 直接写一条 **0026 时点**形状的 armed latch（revision 0）与一条与它
    /// 毫无关系的存量活事件。生产代码在 R17 之后不可能产出 revision 0 的
    /// armed latch（每次 arm 都推号），所以这只能是升级存量。
    async fn seed_revision_zero_latch(store: &Store, scope_key: &str) {
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, flatten_generation, last_reconcile_clean, \
             last_reconcile_at_ms, updated_at_ms) \
             VALUES (?, 'manage_only', 'this round', 'manual', '900', NULL, 0, NULL, NULL, '900')",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("seed a revision-0 armed latch");
    }

    async fn seed_event(
        store: &Store,
        scope_key: &str,
        level: &str,
        trigger_source: &str,
        reason: &str,
        armed_at_ms: &str,
        generation: i64,
    ) -> String {
        let event_id = ulid::Ulid::new().to_string();
        sqlx::query(
            "INSERT INTO kill_switch_events (event_id, scope, scope_key, level, \
             trigger_source, reason, armed_at_ms, flatten_state, reconcile_clean, reset_by, \
             reset_at_ms, created_at_ms, updated_at_ms, flatten_generation) \
             VALUES (?, 'cred', ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, ?)",
        )
        .bind(&event_id)
        .bind(scope_key)
        .bind(level)
        .bind(trigger_source)
        .bind(reason)
        .bind(armed_at_ms)
        .bind(armed_at_ms)
        .bind(armed_at_ms)
        .bind(generation)
        .execute(store.pool())
        .await
        .expect("seed a legacy kill switch event");
        event_id
    }

    async fn live_events(store: &Store, scope_key: &str, generation: i64) -> Vec<(String, String)> {
        sqlx::query(
            "SELECT event_id, COALESCE(trigger_source, '') AS trigger_source \
             FROM kill_switch_events WHERE scope_key=? AND reset_at_ms IS NULL \
             AND flatten_generation=? ORDER BY event_id",
        )
        .bind(scope_key)
        .bind(generation)
        .fetch_all(store.pool())
        .await
        .expect("read the live events")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("event_id"),
                row.get::<String, _>("trigger_source"),
            )
        })
        .collect()
    }

    async fn verdicts(store: &Store, scope_key: &str) -> Vec<String> {
        sqlx::query(
            "SELECT verdict FROM kill_switch_event_generation_revalidations \
             WHERE scope_key=? ORDER BY verdict, event_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the verdicts")
        .into_iter()
        .map(|row| row.get::<String, _>("verdict"))
        .collect()
    }

    /// **revision 0 不是身份复核的豁免（regression）。**
    ///
    /// R16 的健康判据只有 `COUNT(*) > 0`：本代随便一条活事件都算数。0026 把
    /// **所有**存量事件行置为代际 0，而一个从未 flatten 过的 armed scope 的
    /// state 代际也正是 0——于是一条与这一轮毫无关系的旧事件就能让复核整段
    /// 跳过，`reset` 随后按 `LIMIT 1` 把它当成本轮的处置记录标掉，真正的
    /// 处置记录永远没有。
    ///
    /// 复核之后本 revision 必须**恰好一个所有者**：那条证不出归属的活行
    /// 被就地关掉（`superseded`，代际号与行本身都留着），锚事件接手。
    #[tokio::test]
    async fn a_revision_zero_latch_still_proves_its_single_owner() {
        let (store, path) = test_store("revision-zero").await;
        let scope_key = "cred:cred-r17zero";
        seed_revision_zero_latch(&store, scope_key).await;
        let foreign = seed_event(
            &store,
            scope_key,
            "manage_only",
            "manual_api",
            "an earlier round",
            "100",
            0,
        )
        .await;

        assert_eq!(
            revalidate_legacy_event_generations(&store, 1_000)
                .await
                .expect("revalidate the revision-0 latch"),
            2,
            "one superseded row plus one anchor must be reported"
        );
        assert_eq!(
            verdicts(&store, scope_key).await,
            vec!["anchored".to_string(), "superseded".to_string()]
        );

        let live = live_events(&store, scope_key, 0).await;
        assert_eq!(
            live.len(),
            1,
            "a revision may have exactly one owner: {live:?}"
        );
        assert_eq!(
            live[0].1, "generation_revalidation",
            "the surviving owner must be the anchor, not the foreign legacy event"
        );
        assert_ne!(
            live[0].0, foreign,
            "the foreign event must not own this revision"
        );
        // 行还在（可审计），只是不再是"这一轮未 reset 的处置"。
        assert_eq!(
            sqlx::query(
                "SELECT COALESCE(reset_by, '') AS reset_by FROM kill_switch_events \
                 WHERE event_id=?",
            )
            .bind(&foreign)
            .fetch_one(store.pool())
            .await
            .expect("the superseded row must still exist")
            .get::<String, _>("reset_by"),
            "generation_revalidation"
        );

        // 幂等：再跑一次是空操作。
        assert_eq!(
            revalidate_legacy_event_generations(&store, 2_000)
                .await
                .expect("rerun"),
            0,
            "a converged revision must not be rewritten"
        );
        assert_eq!(live_events(&store, scope_key, 0).await.len(), 1);

        // reset 现在绑得住这唯一一个所有者。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r17zero", FLAT, 3_000)
                .await
                .expect("converge the revision"),
            ReconcileProjection::Applied
        );
        reset(&store, scope_key, "operator", None, 3_100)
            .await
            .expect("reset the anchored revision");
        assert!(
            live_events(&store, scope_key, 0).await.is_empty(),
            "reset must leave no live event behind"
        );
        cleanup(store, path).await;
    }

    /// **杀 R13M07 · `level` 是身份的一部分。**
    ///
    /// 一条存量事件的 `armed_at_ms` / `trigger_source` / `reason` 与 state
    /// 逐字相同，**只有档位不同**。摘掉 `level` 的比较之后它会被 `Adopted`
    /// 进当前 revision——把一次 `manage_only` 的处置记录冒充成一次 `flatten`
    /// 的处置记录，审计面从此说不出这一轮到底做了什么。
    ///
    /// 正确处置是证不出归属：隔离在原代际 + 补锚。
    #[tokio::test]
    async fn an_event_that_only_differs_by_level_is_not_this_arms_event() {
        let (store, path) = test_store("level-identity").await;
        let scope_key = "cred:cred-r17level";
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, flatten_generation, last_reconcile_clean, \
             last_reconcile_at_ms, updated_at_ms) \
             VALUES (?, 'flatten', 'exposure', 'auto_protection_timeout', '2000', \
             'flattening', 2, NULL, NULL, '2000')",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("seed an armed latch at revision 2");
        let lookalike = seed_event(
            &store,
            scope_key,
            // 唯一的差别就是这一列。
            "manage_only",
            "auto_protection_timeout",
            "exposure",
            "2000",
            0,
        )
        .await;

        assert_eq!(
            revalidate_legacy_event_generations(&store, 1_000)
                .await
                .expect("revalidate"),
            2,
            "one isolated legacy row plus one anchor"
        );
        assert_eq!(
            verdicts(&store, scope_key).await,
            vec!["anchored".to_string(), "isolated".to_string()]
        );
        assert_eq!(
            sqlx::query("SELECT flatten_generation FROM kill_switch_events WHERE event_id=?")
                .bind(&lookalike)
                .fetch_one(store.pool())
                .await
                .expect("read the look-alike")
                .get::<i64, _>("flatten_generation"),
            0,
            "an event of another level must never be pulled into this arm's revision"
        );
        let live = live_events(&store, scope_key, 2).await;
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].1, "generation_revalidation");
        cleanup(store, path).await;
    }

    /// **身份
    /// 匹配到**两条**存量事件时，一条都不许认领。**
    ///
    /// `revalidate_one_scope_generation` 认领候选行的判据是
    /// `if let [(event_id, previous_generation)] = matches.as_slice()`——
    /// **恰好一条**才 adopt。R17 时点上这条判据没有任何行为级证据：
    /// 每一条复核用例的候选集里要么 0 条匹配、要么 1 条匹配，于是把它换成
    /// "取第一条"（`matches.first()`）测试全绿。那正是 R14M07 这条变异
    /// 存活的原因。
    ///
    /// 取第一条意味着什么：两条**逐字相同**的存量事件（同一个 arm 在
    /// 0026 升级前后留下两条记录、或者一次重复写入，都会长成这样）里
    /// 随便挑一条塞进当前 revision，**另一条原样留在库里且仍然未 reset**。
    /// 于是这个 revision 有了一个"证不出来"的所有者，而 `reset` 的
    /// `LIMIT 1` 只会标掉其中一条——审计面从此说不清这一轮的处置到底是哪条，
    /// 而且永远挂着一条清不掉的活事件。
    ///
    /// 正确处置：`matches.len() == 2` 落到"证不出归属"那一支——两条候选
    /// **原样隔离**（代际号一格都不动，只打上"已复核"），再补一条锚事件
    /// 接管这个 revision。锚是可证的（`generation_proved_at_ms` 非空 +
    /// `trigger_source='generation_revalidation'`），于是 revision 重新
    /// 回到"恰好一个所有者"。
    #[tokio::test]
    async fn two_identical_legacy_events_are_isolated_instead_of_adopted() {
        let (store, path) = test_store("twin-identity").await;
        let scope_key = "cred:cred-r18twin";
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, flatten_generation, last_reconcile_clean, \
             last_reconcile_at_ms, updated_at_ms) \
             VALUES (?, 'flatten', 'exposure', 'auto_protection_timeout', '2000', \
             'flattening', 2, NULL, NULL, '2000')",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("seed an armed latch at revision 2");
        // 两条**与 arm 身份逐字相同**的存量事件：四个字段一个不差，
        // 因此 `ArmIdentity::matches` 对两条都成立。
        let twins = [
            seed_event(
                &store,
                scope_key,
                "flatten",
                "auto_protection_timeout",
                "exposure",
                "2000",
                0,
            )
            .await,
            seed_event(
                &store,
                scope_key,
                "flatten",
                "auto_protection_timeout",
                "exposure",
                "2000",
                0,
            )
            .await,
        ];

        assert_eq!(
            revalidate_legacy_event_generations(&store, 1_000)
                .await
                .expect("revalidate a revision with two matching legacy events"),
            3,
            "two isolated legacy rows plus one anchor"
        );
        assert_eq!(
            verdicts(&store, scope_key).await,
            vec![
                "anchored".to_string(),
                "isolated".to_string(),
                "isolated".to_string()
            ],
            "neither twin may be adopted; both are isolated and an anchor takes the revision"
        );

        // **两条都留在原代际**：adopt 任何一条都会把它的 generation 改成 2。
        for event_id in &twins {
            assert_eq!(
                sqlx::query("SELECT flatten_generation FROM kill_switch_events WHERE event_id=?")
                    .bind(event_id)
                    .fetch_one(store.pool())
                    .await
                    .expect("read a twin")
                    .get::<i64, _>("flatten_generation"),
                0,
                "an ambiguous legacy event must never be pulled into this arm's revision"
            );
        }

        // **这个 revision 恰好一个所有者，而且是锚**（不是随便挑的那条 twin）。
        let live = live_events(&store, scope_key, 2).await;
        assert_eq!(
            live.len(),
            1,
            "a revision may have exactly one owner: {live:?}"
        );
        assert_eq!(
            live[0].1, "generation_revalidation",
            "the owner must be the anchor, not one of two indistinguishable legacy events"
        );
        assert!(
            !twins.contains(&live[0].0),
            "no twin may end up owning this revision: {live:?}"
        );
        // 两条 twin 仍然未 reset（它们记录的是各自那一轮的处置，可审计），
        // 只是不再声称拥有本 revision。
        assert_eq!(
            live_events(&store, scope_key, 0)
                .await
                .into_iter()
                .map(|(event_id, _)| event_id)
                .collect::<Vec<_>>(),
            twins.to_vec(),
            "the isolated twins must still exist at their own generation"
        );

        // 幂等：复核跑第二遍是空操作（锚是可证的，不会被再隔离一次）。
        assert_eq!(
            revalidate_legacy_event_generations(&store, 2_000)
                .await
                .expect("rerun the revalidation"),
            0,
            "a revision that already has a provable single owner must not be rewritten"
        );
        assert_eq!(live_events(&store, scope_key, 2).await.len(), 1);
        cleanup(store, path).await;
    }
}

/// MySQL's default case-insensitive collation can make a corrupted durable
/// token compare equal to a canonical level in SQL. Exercise the production
/// decoder and retry scan against a real MySQL scratch database so SQLite
/// cannot stand in for that dialect-specific risk.
#[cfg(test)]
mod persisted_level_mysql_tests {
    use sqlx::Row;

    use super::{
        action_retry_targets, arm_in_tx, effective_in_tx, ArmRequest, InvalidPersistedLevel, Level,
        FLATTEN_CONVERGED,
    };
    use crate::store::mysql_test_support::MySqlScratch;

    #[tokio::test]
    async fn mysql_invalid_level_preserves_valid_siblings_and_raw_authority() {
        const TEST: &str = "mysql_invalid_level_preserves_valid_siblings_and_raw_authority";
        let Some(scratch) = MySqlScratch::create("kslevel", TEST).await else {
            return;
        };
        let store = scratch.connect_store().await;

        sqlx::query(
            "UPDATE kill_switch_state SET level=?, reason='invalid global fixture', \
             flatten_state=?, flatten_generation=8 WHERE scope_key='global'",
        )
        .bind("FLATTEN")
        .bind(FLATTEN_CONVERGED)
        .execute(store.pool())
        .await
        .expect("seed the noncanonical global level");
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, last_reconcile_clean, last_reconcile_at_ms, \
             updated_at_ms, flatten_generation) VALUES (?, 'manage_only', \
             'valid credential sibling', 'test_fixture', 10, NULL, 0, NULL, 10, 4)",
        )
        .bind("cred:mysql-level")
        .execute(store.pool())
        .await
        .expect("seed the canonical credential sibling");

        let mut read_tx = store.pool().begin().await.expect("begin effective read");
        let read = effective_in_tx(&mut read_tx, "mysql-level", "unrelated-bot")
            .await
            .expect("read every applicable MySQL authority row");
        read_tx.rollback().await.expect("rollback effective read");
        let effective = read
            .effective
            .expect("the valid credential sibling remains effective");
        assert_eq!(effective.scope_key, "cred:mysql-level");
        assert_eq!(effective.level, Level::ManageOnly);
        assert_eq!(
            read.invalid_levels,
            vec![InvalidPersistedLevel {
                scope_key: "global".to_string(),
                raw_level: "FLATTEN".to_string(),
            }]
        );

        let targets = action_retry_targets(&store)
            .await
            .expect("scan every MySQL authority row");
        assert_eq!(targets.len(), 2);
        let invalid = targets
            .iter()
            .find(|target| target.scope_key == "global")
            .expect("invalid global row remains retryable despite flattened marker");
        assert_eq!(invalid.level, Level::Flatten);
        assert_eq!(invalid.generation, 8);
        assert_eq!(
            invalid.invalid_level,
            Some(InvalidPersistedLevel {
                scope_key: "global".to_string(),
                raw_level: "FLATTEN".to_string(),
            })
        );
        let valid = targets
            .iter()
            .find(|target| target.scope_key == "cred:mysql-level")
            .expect("canonical manage-only sibling remains retryable");
        assert_eq!(valid.level, Level::ManageOnly);
        assert_eq!(valid.generation, 4);
        assert_eq!(valid.invalid_level, None);

        let mut arm_tx = store.pool().begin().await.expect("begin rejected arm");
        let error = arm_in_tx(
            &mut arm_tx,
            store.kind(),
            ArmRequest {
                scope_key: "global",
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "must not canonicalize corrupted authority",
                now_ms: 20,
            },
        )
        .await
        .expect_err("MySQL arm must reject a collation-equal noncanonical token");
        assert_eq!(
            error.downcast_ref::<InvalidPersistedLevel>(),
            Some(&InvalidPersistedLevel {
                scope_key: "global".to_string(),
                raw_level: "FLATTEN".to_string(),
            })
        );
        arm_tx.rollback().await.expect("rollback rejected arm");

        let persisted = sqlx::query(
            "SELECT level, flatten_generation FROM kill_switch_state WHERE scope_key='global'",
        )
        .fetch_one(store.pool())
        .await
        .expect("reload rejected MySQL arm state");
        assert_eq!(persisted.get::<String, _>("level"), "FLATTEN");
        assert_eq!(persisted.get::<i64, _>("flatten_generation"), 8);

        store.pool().close().await;
        scratch.cleanup().await;
    }
}

#[cfg(test)]
#[path = "kill_switch_local_cancel_isolation_tests.rs"]
mod local_cancel_isolation_tests;

#[cfg(test)]
#[path = "kill_switch_k04_tests.rs"]
mod k04_exchange_cancel_tests;

/// Entry cancellation owns reservation/claim cleanup only after its exact
/// `pending_risk -> canceled` CAS succeeds. Real MySQL is required here: the
/// status/state columns use the server's default case-insensitive collation,
/// and concurrent cleaners serialize through `FOR UPDATE` row locks.
#[cfg(test)]
mod entry_cleanup_mysql_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use sqlx::Row;
    use tokio::sync::Barrier;

    use super::cancel_order_if_status;
    use crate::store::{mysql_test_support::MySqlScratch, Store};

    const ORDER_ID: &str = "ord-mysql-entry-cleanup";
    const CREDENTIAL_ID: &str = "cred-mysql-entry-cleanup";
    const BOT_UID: &str = "bot-mysql-entry-cleanup";
    const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";
    const POSITION_ID: &str = "pos-mysql-entry-cleanup";
    const VERDICT_ID: &str = "rv-mysql-entry-cleanup";

    async fn seed_pending_entry(store: &Store) {
        let authority_key = crate::position_state::semantic_idempotency_key(&format!(
            "entry|{BOT_UID}|{SYMBOL_KEY}|long|1"
        ));
        let active_key = crate::position_state::active_key(BOT_UID, SYMBOL_KEY, "long");
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin MySQL Entry cleanup fixture");
        sqlx::query(
            "INSERT INTO notional_reservations \
             (credential_id, reserved_usdt, cap_usdt, updated_at_ms) \
             VALUES (?, '100', '5000', 1)",
        )
        .bind(CREDENTIAL_ID)
        .execute(&mut *tx)
        .await
        .expect("seed MySQL Entry reservation");
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES (?, ?, ?, ?, 'long', 1, 'pending', ?, ?, '0.000', 1)
            "#,
        )
        .bind(POSITION_ID)
        .bind(CREDENTIAL_ID)
        .bind(BOT_UID)
        .bind(SYMBOL_KEY)
        .bind(&active_key)
        .bind(&authority_key)
        .execute(&mut *tx)
        .await
        .expect("seed MySQL pending Entry claim");
        sqlx::query(
            r#"
            INSERT INTO risk_verdicts (
              risk_verdict_id, intent_id, approved_intent_key, credential_id,
              bot_uid, symbol_key, created_at_ms, policy_version, policy_hash,
              decision, sizing_json, rules_json, evaluator_version
            ) VALUES (?, 'intent-mysql-entry-cleanup', 'intent-mysql-entry-cleanup', ?,
              ?, ?, 1, 'test', 'hash', 'approve',
              '{"final_notional_usdt":"100"}', '[]', 'test')
            "#,
        )
        .bind(VERDICT_ID)
        .bind(CREDENTIAL_ID)
        .bind(BOT_UID)
        .bind(SYMBOL_KEY)
        .execute(&mut *tx)
        .await
        .expect("seed MySQL Entry risk verdict");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, risk_verdict_id,
              position_id, created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, 'binance', 'futures', ?, 'open_long', 'long',
              'market', '1', 0, 'entry', 'pending_risk', ?, ?, 1, 1)
            "#,
        )
        .bind(ORDER_ID)
        .bind(CREDENTIAL_ID)
        .bind(BOT_UID)
        .bind(&authority_key)
        .bind(SYMBOL_KEY)
        .bind(VERDICT_ID)
        .bind(POSITION_ID)
        .execute(&mut *tx)
        .await
        .expect("seed MySQL pending_risk Entry order");
        sqlx::query(
            r#"
            INSERT INTO position_protection_plans (
              position_id, credential_id, bot_uid, exchange, market_kind,
              symbol_key, direction, stop_loss_price, take_profit_price,
              required, confirm_timeout_ms, generation, created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance', 'futures', ?, 'long', '95', '110',
              1, 3000, 1, 1, 1)
            "#,
        )
        .bind(POSITION_ID)
        .bind(CREDENTIAL_ID)
        .bind(BOT_UID)
        .bind(SYMBOL_KEY)
        .execute(&mut *tx)
        .await
        .expect("seed MySQL Entry protection plan");
        tx.commit()
            .await
            .expect("commit MySQL Entry cleanup fixture");
    }

    async fn close_fixture(store: Store, scratch: MySqlScratch) {
        store.pool().close().await;
        scratch.cleanup().await;
    }

    async fn reserved_usdt(store: &Store) -> f64 {
        sqlx::query("SELECT reserved_usdt FROM notional_reservations WHERE credential_id=?")
            .bind(CREDENTIAL_ID)
            .fetch_one(store.pool())
            .await
            .expect("load MySQL Entry reservation")
            .get::<String, _>("reserved_usdt")
            .parse::<f64>()
            .expect("parse MySQL Entry reservation")
    }

    async fn assert_pending_resources_unchanged(
        store: &Store,
        expected_status: &str,
        expected_state: &str,
    ) {
        let order =
            sqlx::query("SELECT status, updated_at_ms FROM engine_orders WHERE engine_order_id=?")
                .bind(ORDER_ID)
                .fetch_one(store.pool())
                .await
                .expect("load unchanged MySQL Entry order");
        assert_eq!(order.get::<String, _>("status"), expected_status);
        assert_eq!(order.get::<i64, _>("updated_at_ms"), 1);
        assert_eq!(reserved_usdt(store).await, 100.0);

        let claim = sqlx::query(
            "SELECT state, entry_epoch, active_key IS NOT NULL AS has_active_key, \
                    pending_idem_key IS NOT NULL AS has_pending_idem_key \
             FROM position_state WHERE position_id=?",
        )
        .bind(POSITION_ID)
        .fetch_one(store.pool())
        .await
        .expect("load unchanged MySQL Entry claim");
        assert_eq!(claim.get::<String, _>("state"), expected_state);
        assert_eq!(claim.get::<i64, _>("entry_epoch"), 1);
        assert_eq!(claim.get::<i64, _>("has_active_key"), 1);
        assert_eq!(claim.get::<i64, _>("has_pending_idem_key"), 1);

        let plan_count = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans WHERE position_id=?",
        )
        .bind(POSITION_ID)
        .fetch_one(store.pool())
        .await
        .expect("count unchanged MySQL Entry protection plan")
        .get::<i64, _>("count");
        assert_eq!(plan_count, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mysql_canonical_pending_entry_cleanup_releases_all_owned_resources() {
        const TEST: &str = "mysql_canonical_pending_entry_cleanup_releases_all_owned_resources";
        let Some(scratch) = MySqlScratch::create("entryclean", TEST).await else {
            return;
        };
        let store = scratch.connect_store().await;
        seed_pending_entry(&store).await;

        assert_eq!(
            cancel_order_if_status(&store, ORDER_ID, "pending_risk", 2)
                .await
                .expect("cancel canonical MySQL pending_risk Entry"),
            1
        );
        let order_status = sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
            .bind(ORDER_ID)
            .fetch_one(store.pool())
            .await
            .expect("load canceled MySQL Entry order")
            .get::<String, _>("status");
        assert_eq!(order_status, "canceled");
        assert_eq!(reserved_usdt(&store).await, 0.0);

        let claim = sqlx::query(
            "SELECT state, entry_epoch, active_key IS NULL AS active_key_is_null, \
                    pending_intent_id IS NULL AS pending_intent_is_null, \
                    pending_idem_key IS NULL AS pending_idem_is_null \
             FROM position_state WHERE position_id=?",
        )
        .bind(POSITION_ID)
        .fetch_one(store.pool())
        .await
        .expect("load retired MySQL Entry claim");
        assert_eq!(claim.get::<String, _>("state"), "closed");
        assert_eq!(claim.get::<i64, _>("entry_epoch"), 1);
        assert_eq!(claim.get::<i64, _>("active_key_is_null"), 1);
        assert_eq!(claim.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(claim.get::<i64, _>("pending_idem_is_null"), 1);

        let plan_count = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans WHERE position_id=?",
        )
        .bind(POSITION_ID)
        .fetch_one(store.pool())
        .await
        .expect("count retired MySQL Entry protection plans")
        .get::<i64, _>("count");
        assert_eq!(plan_count, 0);
        let verdict_count =
            sqlx::query("SELECT COUNT(*) AS count FROM risk_verdicts WHERE risk_verdict_id=?")
                .bind(VERDICT_ID)
                .fetch_one(store.pool())
                .await
                .expect("count retained MySQL Entry verdict")
                .get::<i64, _>("count");
        assert_eq!(verdict_count, 1, "the audit verdict remains durable");

        close_fixture(store, scratch).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mysql_two_concurrent_entry_cleaners_release_exactly_once_without_deadlock() {
        const TEST: &str =
            "mysql_two_concurrent_entry_cleaners_release_exactly_once_without_deadlock";
        let Some(scratch) = MySqlScratch::create("entryrace", TEST).await else {
            return;
        };
        let store = scratch.connect_store().await;
        seed_pending_entry(&store).await;

        // Hold the first lock in the production ordering so both cleaners are
        // live concurrently before either can acquire cleanup ownership.
        let mut blocker = store
            .pool()
            .begin()
            .await
            .expect("begin MySQL race blocker");
        sqlx::query(
            "SELECT credential_id FROM notional_reservations \
             WHERE credential_id=? FOR UPDATE",
        )
        .bind(CREDENTIAL_ID)
        .fetch_one(&mut *blocker)
        .await
        .expect("lock MySQL race reservation");

        let start = Arc::new(Barrier::new(3));
        let left_store = store.clone();
        let left_start = start.clone();
        let left = tokio::spawn(async move {
            left_start.wait().await;
            cancel_order_if_status(&left_store, ORDER_ID, "pending_risk", 2).await
        });
        let right_store = store.clone();
        let right_start = start.clone();
        let right = tokio::spawn(async move {
            right_start.wait().await;
            cancel_order_if_status(&right_store, ORDER_ID, "pending_risk", 3).await
        });
        start.wait().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        blocker
            .commit()
            .await
            .expect("release MySQL race reservation lock");

        let (left, right) =
            tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(left, right) })
                .await
                .expect("concurrent MySQL Entry cleaners deadlocked or exceeded 10s");
        let mut outcomes = [
            left.expect("left MySQL cleaner task panicked")
                .expect("left MySQL cleaner failed"),
            right
                .expect("right MySQL cleaner task panicked")
                .expect("right MySQL cleaner failed"),
        ];
        outcomes.sort_unstable();
        assert_eq!(outcomes, [0, 1]);
        assert_eq!(reserved_usdt(&store).await, 0.0);
        let closed_claims = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_state \
             WHERE position_id=? AND BINARY state='closed' AND entry_epoch=1 \
               AND active_key IS NULL AND pending_idem_key IS NULL",
        )
        .bind(POSITION_ID)
        .fetch_one(store.pool())
        .await
        .expect("count exactly-once retired MySQL Entry claim")
        .get::<i64, _>("count");
        assert_eq!(closed_claims, 1);
        let remaining_plans = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans WHERE position_id=?",
        )
        .bind(POSITION_ID)
        .fetch_one(store.pool())
        .await
        .expect("count concurrently retired MySQL Entry plans")
        .get::<i64, _>("count");
        assert_eq!(remaining_plans, 0);

        close_fixture(store, scratch).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mysql_case_dirty_status_or_state_never_releases_entry_resources() {
        const TEST: &str = "mysql_case_dirty_status_or_state_never_releases_entry_resources";

        let Some(status_scratch) = MySqlScratch::create("entrystatus", TEST).await else {
            return;
        };
        let status_store = status_scratch.connect_store().await;
        seed_pending_entry(&status_store).await;
        sqlx::query("UPDATE engine_orders SET status='PENDING_RISK' WHERE engine_order_id=?")
            .bind(ORDER_ID)
            .execute(status_store.pool())
            .await
            .expect("seed case-dirty MySQL Entry status");
        assert_eq!(
            cancel_order_if_status(&status_store, ORDER_ID, "pending_risk", 2)
                .await
                .expect("case-dirty MySQL status is a zero-row ownership result"),
            0
        );
        assert_pending_resources_unchanged(&status_store, "PENDING_RISK", "pending").await;
        close_fixture(status_store, status_scratch).await;

        let Some(state_scratch) = MySqlScratch::create("entrystate", TEST).await else {
            return;
        };
        let state_store = state_scratch.connect_store().await;
        seed_pending_entry(&state_store).await;
        sqlx::query("UPDATE position_state SET state='PENDING' WHERE position_id=?")
            .bind(POSITION_ID)
            .execute(state_store.pool())
            .await
            .expect("seed case-dirty MySQL Entry state");
        cancel_order_if_status(&state_store, ORDER_ID, "pending_risk", 2)
            .await
            .expect_err("case-dirty MySQL state must roll the cancellation transaction back");
        assert_pending_resources_unchanged(&state_store, "pending_risk", "PENDING").await;
        close_fixture(state_store, state_scratch).await;
    }
}

/// **两条绑代写入的命中数判定。**
///
/// R15 把这两条判定（state 侧与事件侧的 `rows_affected() == 0`）登记为
/// **SQLite 上不可达**：`Store` 对 SQLite 只开一条连接，整个
/// `record_reconcile_for_scope` 跑在一个事务里，读代际与写状态之间没有第二个
/// 写者——R15M15b / R15M18b 因此双双存活，**完全没有行为级证据**。
///
/// 这里用**真实 MySQL 行锁**把那个窗口造出来：另一条连接先拿住目标行的排他锁
/// 且不提交，被测函数走到绑代写入时**真的被阻塞**，等对方提交之后再回来，
/// 于是谓词一行都不命中。SQLite 造不出这个交错，这是它必须在真库上跑的原因。
///
/// 真库缺失时**跳过并说明**（不拿 SQLite 冒充）：
/// `RUSTCORE_TEST_MYSQL_URL=mysql://root:<pw>@127.0.0.1:3307/mysql`；
/// 设 `RUSTCORE_REQUIRE_MYSQL_TESTS=1` 可把跳过变成失败。
///
/// **追加一条**：
/// `two_concurrent_manual_refreezes_advance_one_generation`。它要的同样是
/// SQLite 造不出的东西——两条连接同时走人工 re-freeze 的决策读。夹具沿用
/// 本模块既有的 scratch-DB + 未提交事务持锁的形状，因此放在这里而不是
/// `r21_convergence_tests`（那一模块是 SQLite 的）。
#[cfg(test)]
mod r16_mysql_concurrency_tests {
    use std::sync::Once;
    use std::time::Duration;

    use sqlx::any::AnyPoolOptions;
    use sqlx::Row;

    use super::{
        arm_in_tx_with_round, observe_flatten_generations, record_reconcile_for_scope, ArmRequest,
        LatchRound, Level, ReconcileProjection, ReconcileTruth, FLATTEN_PENDING,
    };
    use crate::store::{DbKind, Store};

    static INSTALL_DRIVERS: Once = Once::new();

    const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
    const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";

    const FLAT: ReconcileTruth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    };

    /// 建一个本次运行专用的 scratch 库并在其中跑**完整迁移链**
    /// （0025 / 0026 / 0027 的 MySQL 增量序列因此是真的跑过的）。
    ///
    /// 返回 `None` = 环境没有真库，用例按 engine safety contract 如实跳过。
    async fn mysql_store(label: &str) -> Option<(Store, String, String)> {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let admin_url = match std::env::var(MYSQL_URL_ENV) {
            Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => {
                skip_without_mysql(label, &format!("{MYSQL_URL_ENV} is not set"));
                return None;
            }
        };
        let admin = match AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => {
                skip_without_mysql(label, &format!("cannot connect: {error}"));
                return None;
            }
        };
        let database = format!(
            "rustcore_r16_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("create the scratch database");
        admin.close().await;
        let scratch_url = swap_database(&admin_url, &database);
        let store = Store::connect(&scratch_url)
            .await
            .expect("run the whole rustcore migration chain on MySQL");
        assert_eq!(store.kind(), DbKind::MySql, "the fixture must be on MySQL");
        Some((store, admin_url, database))
    }

    fn swap_database(url: &str, database: &str) -> String {
        let (prefix, _) = url
            .rsplit_once('/')
            .expect("the MySQL URL must carry a database path");
        format!("{prefix}/{database}")
    }

    async fn drop_scratch(store: Store, admin_url: String, database: String) {
        store.pool().close().await;
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("reconnect to drop the scratch database");
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
        admin.close().await;
    }

    fn skip_without_mysql(test: &str, why: &str) {
        if std::env::var(REQUIRE_MYSQL_ENV)
            .map(|value| value.trim() == "1")
            .unwrap_or(false)
        {
            panic!("{test}: {REQUIRE_MYSQL_ENV}=1 要求真实 MySQL，但 {why}");
        }
        eprintln!(
            "SKIPPED {test}: 没有可用的真实 MySQL（{why}）。\n\
             这两条命中数判定只在 MySQL 行锁并发下可达，SQLite 单连接造不出该交错；\n\
             本用例不以降级替身冒充该证据。设 {MYSQL_URL_ENV} 后可实跑，\n\
             设 {REQUIRE_MYSQL_ENV}=1 可把跳过变成失败。"
        );
    }

    /// **事务里一次空结果读
    /// 之后，这条连接还能不能继续用。**
    ///
    /// 根因见 `store.rs` 的 SQLx Any 空结果约束：旧版 MySQL 后端在
    /// `fetch_optional` 读到**空结果集**时提前返回、把结果流丢在半路，
    /// 事务里那条连接的下一条语句于是**永远不返回**。
    ///
    /// 后果落在生产入口上：`kill_switch::arm`（手动 `SetKillSwitch` 与全部
    /// 自动触发源用的就是它）第一次给某个 scope 上闩时，档位早返回那次读
    /// 命中的正是一张还没有这一行的表——**整个 arm 挂死**，kill switch 在
    /// MySQL 后端上一次都闩不上。R16 的五条真库用例全绿是因为它们走的都是
    /// `latch_flatten_pending`（`LatchRound::ForceNew`，跳过那次读）。
    ///
    /// 两个方向都在这里：**空结果**之后必须还能继续（修法），
    /// 以及紧随其后的**生产入口** `arm` 必须真的完成。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_empty_single_row_read_does_not_wedge_the_transaction() {
        let Some((store, admin_url, database)) =
            mysql_store("an_empty_single_row_read_does_not_wedge_the_transaction").await
        else {
            return;
        };
        let scope_key = "cred:cred-r17mysql-wedge";

        {
            let mut tx = store.pool().begin().await.expect("begin");
            let missing = sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key = ?")
                .bind(scope_key)
                .fetch_all(&mut *tx)
                .await
                .expect("an empty single-row read must succeed")
                .into_iter()
                .next();
            assert!(missing.is_none(), "the fixture must start without this row");
            // 判据就在这一句：R16 的读法之后它永远不返回。
            let next = tokio::time::timeout(
                Duration::from_secs(10),
                sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_state WHERE scope_key = ?")
                    .bind(scope_key)
                    .fetch_one(&mut *tx),
            )
            .await
            .expect("the connection must still answer after an empty single-row read")
            .expect("count");
            assert_eq!(next.get::<i64, _>("n"), 0);
            tx.rollback().await.expect("rollback");
        }

        // 生产入口：第一次 arm 一个全新的 scope。
        tokio::time::timeout(
            Duration::from_secs(20),
            super::arm(
                &store,
                ArmRequest {
                    scope_key,
                    level: Level::PauseNew,
                    trigger_source: "manual_api",
                    reason: "first arm on a MySQL backend",
                    now_ms: 10,
                },
            ),
        )
        .await
        .expect("arm must not hang on the MySQL backend")
        .expect("arm pause_new");
        assert_eq!(
            sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
                .bind(scope_key)
                .fetch_one(store.pool())
                .await
                .expect("read the latch")
                .get::<String, _>("level"),
            "pause_new",
            "the kill switch must actually latch on MySQL"
        );
        drop_scratch(store, admin_url, database).await;
    }

    async fn state_row(store: &Store, scope_key: &str) -> (String, i64, String) {
        let row = sqlx::query(
            "SELECT COALESCE(flatten_state,'') AS flatten_state, flatten_generation, \
             COALESCE(CAST(last_reconcile_clean AS CHAR),'') AS clean \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch");
        (
            row.get::<String, _>("flatten_state"),
            row.get::<i64, _>("flatten_generation"),
            row.get::<String, _>("clean"),
        )
    }

    /// **杀 R15M15b**：state 侧绑代 UPDATE 的 `rows_affected() == 0` 判定。
    ///
    /// 另一条连接在**未提交的事务**里跑生产的 `arm_in_tx_with_round(ForceNew)`
    /// ——代际 +1 且持有 `kill_switch_state` 那一行的排他锁。被测的
    /// `record_reconcile_for_scope` 先读到旧代际（一致性读不阻塞），
    /// 随后那条 `WHERE ... flatten_generation=<旧代际>` 的 UPDATE **真的被行锁
    /// 挡住**；对方提交之后它回来，谓词一行都不命中。
    ///
    /// 判据摘掉时这份真值会被当成"已投影"返回 `Applied`，而库里一个字节
    /// 都没写进去。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_latch_that_commits_inside_the_write_window_discards_the_projection() {
        let Some((store, admin_url, database)) =
            mysql_store("a_latch_that_commits_inside_the_write_window_discards_the_projection")
                .await
        else {
            return;
        };
        let scope_key = "cred:cred-r16mysql-state";
        super::latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "round one",
            10,
        )
        .await
        .expect("first latch");
        let observed = observe_flatten_generations(&store)
            .await
            .expect("snapshot the generation before reading exchange truth");
        assert_eq!(observed.observed_for(scope_key), 1);

        // 第二轮 latch：写完不提交，握着 state 行的排他锁。
        let mut blocking = store
            .pool()
            .begin()
            .await
            .expect("begin the blocking latch");
        arm_in_tx_with_round(
            &mut blocking,
            store.kind(),
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "auto_protection_timeout",
                reason: "round two",
                now_ms: 20,
            },
            LatchRound::ForceNew,
            None,
        )
        .await
        .expect("latch a second round without committing");

        let projection = {
            let store = store.clone();
            let handle = tokio::spawn(async move {
                // ****：credential 必须真的属于这个
                // scope——`cred:` scope 的成员集合恒为它自己那个 credential。
                // R17 的无条件 upsert 让"随便一个名字"也能走到绑代写入，
                // 这条夹具因此一直用着一个**不属于本 scope** 的 credential；
                // 闭世界判据下它会先被 `RejectedNonTarget` 挡在门外，测不到
                // 本用例真正要测的那个绑代窗口。
                record_reconcile_for_scope(
                    &store,
                    scope_key,
                    "cred-r16mysql-state",
                    FLAT,
                    30,
                    &observed,
                )
                .await
            });
            // 被测事务此时已经读完代际、正卡在绑代 UPDATE 上。
            tokio::time::sleep(Duration::from_millis(400)).await;
            blocking.commit().await.expect("commit the second latch");
            tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .expect("the blocked projection must finish once the latch commits")
                .expect("join the projection task")
                .expect("record a reconcile truth")
        };

        assert_eq!(
            projection,
            ReconcileProjection::DiscardedRaced { generation: 1 },
            "a truth whose generation stopped matching mid-write applies nothing"
        );
        let (flatten_state, generation, clean) = state_row(&store, scope_key).await;
        assert_eq!(generation, 2, "the second latch owns the scope now");
        assert_eq!(
            flatten_state, FLATTEN_PENDING,
            "the new exposure must stay unconverged"
        );
        assert_eq!(clean, "", "no reconcile verdict may be recorded");
        let recorded = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_reconcile_status \
             WHERE scope_key=? AND credential_id=? AND reconciled_at_ms=30",
        )
        .bind(scope_key)
        .bind("cred-r16mysql-state")
        .fetch_one(store.pool())
        .await
        .expect("count per-credential rows")
        .get::<i64, _>("n");
        assert_eq!(recorded, 0, "the whole projection must be rolled back");
        drop_scratch(store, admin_url, database).await;
    }

    /// **杀 R15M18b**：事件侧绑代 UPDATE 的 `rows_affected() == 0` 判定。
    ///
    /// 事件行是**另一张表**，它可以在被测事务已经拿到 `kill_switch_state`
    /// 行锁之后仍被并发改动（这里模拟一次与本轮处置竞争的事件行写入：
    /// 该事件被标成 reset）。被测函数选中它时看到的是未 reset 的快照，
    /// 写下去时谓词已经不成立——0 行命中就是代际/生命周期错位本身。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_event_row_that_moves_inside_the_write_window_discards_the_projection() {
        let Some((store, admin_url, database)) =
            mysql_store("an_event_row_that_moves_inside_the_write_window_discards_the_projection")
                .await
        else {
            return;
        };
        let scope_key = "cred:cred-r16mysql-event";
        super::latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        let observed = observe_flatten_generations(&store)
            .await
            .expect("snapshot the generation");
        let event_id = sqlx::query(
            "SELECT event_id FROM kill_switch_events WHERE scope_key=? AND reset_at_ms IS NULL",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the active event")
        .get::<String, _>("event_id");

        // 并发写者握住事件行：未提交。
        let mut blocking = store.pool().begin().await.expect("begin the event writer");
        sqlx::query(
            "UPDATE kill_switch_events SET reset_by='concurrent', reset_at_ms='15', \
             updated_at_ms='15' WHERE event_id=?",
        )
        .bind(&event_id)
        .execute(&mut *blocking)
        .await
        .expect("take the event row lock");

        let projection = {
            let store = store.clone();
            let handle = tokio::spawn(async move {
                // 同上：credential 必须真的属于这个 `cred:` scope。
                record_reconcile_for_scope(
                    &store,
                    scope_key,
                    "cred-r16mysql-event",
                    FLAT,
                    30,
                    &observed,
                )
                .await
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
            blocking.commit().await.expect("commit the event writer");
            tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .expect("the blocked projection must finish once the event writer commits")
                .expect("join the projection task")
                .expect("record a reconcile truth")
        };

        assert_eq!(
            projection,
            ReconcileProjection::DiscardedRaced { generation: 1 },
            "an event row that moved mid-write applies nothing"
        );
        let (flatten_state, _, clean) = state_row(&store, scope_key).await;
        assert_eq!(
            flatten_state, FLATTEN_PENDING,
            "the latch must stay unconverged when the audit write missed"
        );
        assert_eq!(clean, "", "the state verdict must be rolled back too");
        drop_scratch(store, admin_url, database).await;
    }

    /// **reset 的 state 侧命中数判定**（regression 的并发半边）。
    ///
    /// reset 的前置校验（`last_reconcile_clean` + `reconcile_at >= armed_at`）
    /// 读的是**上一轮**的真值。读完之后、清闩之前插进来一次
    /// `latch_flatten_pending`（新敞口、代际 +1）时，无谓词的 UPDATE 会把
    /// **新一轮**的 latch 一起清成 `none`——那个敞口一次都没被平过。
    /// 这里用真实行锁把那个窗口造出来：绑代 UPDATE 被新 latch 的行锁挡住，
    /// 对方提交后谓词一行都不命中，整个 reset 必须失败且什么都不改。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_latch_that_commits_inside_the_reset_window_rejects_the_reset() {
        let Some((store, admin_url, database)) =
            mysql_store("a_latch_that_commits_inside_the_reset_window_rejects_the_reset").await
        else {
            return;
        };
        let scope_key = "cred:cred-r16mysql-reset";
        super::latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "round one",
            10,
        )
        .await
        .expect("first latch");
        assert_eq!(
            super::record_reconcile_for_scope_now(
                &store,
                scope_key,
                "cred-r16mysql-reset",
                FLAT,
                20,
            )
            .await
            .expect("converge round one"),
            ReconcileProjection::Applied
        );

        let mut blocking = store
            .pool()
            .begin()
            .await
            .expect("begin the blocking latch");
        arm_in_tx_with_round(
            &mut blocking,
            store.kind(),
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "auto_protection_timeout",
                reason: "round two",
                now_ms: 30,
            },
            LatchRound::ForceNew,
            None,
        )
        .await
        .expect("latch a second round without committing");

        let outcome = {
            let store = store.clone();
            let handle =
                tokio::spawn(
                    async move { super::reset(&store, scope_key, "operator", None, 40).await },
                );
            tokio::time::sleep(Duration::from_millis(400)).await;
            blocking.commit().await.expect("commit the second latch");
            tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .expect("the blocked reset must finish once the latch commits")
                .expect("join the reset task")
        };
        let error = outcome.expect_err("a reset whose state moved must not report success");
        assert!(
            error.to_string().contains("kill switch state moved"),
            "the rejection must name the moved state: {error}"
        );
        let (flatten_state, generation, _) = state_row(&store, scope_key).await;
        assert_eq!(generation, 2, "the second latch owns the scope now");
        assert_eq!(
            flatten_state, FLATTEN_PENDING,
            "a reset validated against the previous round must never clear a new exposure"
        );
        let level = sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch")
            .get::<String, _>("level");
        assert_eq!(level, "flatten", "the new exposure must stay armed");
        drop_scratch(store, admin_url, database).await;
    }

    /// **reset 的事件侧命中数判定**（regression 的另一半）。
    ///
    /// 选中一条未 reset 的本代事件、写下去之前它被并发改动（这里是被另一条
    /// 连接标成 reset）：0 行命中意味着这次 reset 没有留下可审计的事件行，
    /// 整个事务必须回滚——engine safety contract「不知道写没写成功不得宣称成功」。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_event_row_that_moves_inside_the_reset_window_rejects_the_reset() {
        let Some((store, admin_url, database)) =
            mysql_store("an_event_row_that_moves_inside_the_reset_window_rejects_the_reset").await
        else {
            return;
        };
        let scope_key = "cred:cred-r16mysql-resetevt";
        super::latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        assert_eq!(
            super::record_reconcile_for_scope_now(
                &store,
                scope_key,
                "cred-r16mysql-resetevt",
                FLAT,
                20,
            )
            .await
            .expect("converge the round"),
            ReconcileProjection::Applied
        );
        let event_id = sqlx::query(
            "SELECT event_id FROM kill_switch_events WHERE scope_key=? AND reset_at_ms IS NULL",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the active event")
        .get::<String, _>("event_id");

        let mut blocking = store.pool().begin().await.expect("begin the event writer");
        sqlx::query(
            "UPDATE kill_switch_events SET reset_by='concurrent', reset_at_ms='25', \
             updated_at_ms='25' WHERE event_id=?",
        )
        .bind(&event_id)
        .execute(&mut *blocking)
        .await
        .expect("take the event row lock");

        let outcome = {
            let store = store.clone();
            let handle =
                tokio::spawn(
                    async move { super::reset(&store, scope_key, "operator", None, 40).await },
                );
            tokio::time::sleep(Duration::from_millis(400)).await;
            blocking.commit().await.expect("commit the event writer");
            tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .expect("the blocked reset must finish once the event writer commits")
                .expect("join the reset task")
        };
        let error = outcome.expect_err("a reset whose event row moved must not report success");
        assert!(
            error.to_string().contains("active kill switch event moved"),
            "the rejection must name the moved event: {error}"
        );
        let level = sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch")
            .get::<String, _>("level");
        assert_eq!(
            level, "flatten",
            "a rejected reset must roll back the state write as well"
        );
        drop_scratch(store, admin_url, database).await;
    }

    /// **非 flatten arm 的并发 reset 反例。**
    ///
    /// R16 只在 flatten arm 上推代际。于是 `pause_new` / `manage_only` 这两档
    /// 的整条生命周期共用一个号，reset 的两条绑代 CAS 就退化成了恒真谓词：
    ///
    /// 1. 一轮 `pause_new` 被闩上并对账干净；
    /// 2. reset 读到 `(level=pause_new, generation=N)` 与那份 clean 真值；
    /// 3. **读完之后**并发来一次 `arm(manage_only)`——一个刚刚才发现、一次都
    ///    没被处置过的新风险；R16 下它的号还是 N；
    /// 4. reset 的 `WHERE ... flatten_generation=N` 全部命中，新 arm 被清成
    ///    `none`。
    ///
    /// 这里用**真实 MySQL 行锁**把第 3 步塞进第 2 步与第 4 步之间：并发 arm
    /// 在未提交的事务里握住 `kill_switch_state` 那一行，被测的 reset 走到绑代
    /// UPDATE 时**真的被阻塞**，对方提交之后它回来，谓词必须一行都不命中。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_non_flatten_arm_that_commits_inside_the_reset_window_rejects_the_reset() {
        let Some((store, admin_url, database)) =
            mysql_store("a_non_flatten_arm_that_commits_inside_the_reset_window_rejects_the_reset")
                .await
        else {
            return;
        };
        let scope_key = "cred:cred-r17mysql-nonflatten";
        super::arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::PauseNew,
                trigger_source: "manual_api",
                reason: "round one",
                now_ms: 10,
            },
        )
        .await
        .expect("arm pause_new");
        let armed_revision = revision_of(&store, scope_key).await;
        assert_eq!(
            armed_revision, 1,
            "a non-flatten arm must own a revision of its own"
        );
        assert_eq!(
            super::record_reconcile_for_scope_now(
                &store,
                scope_key,
                "cred-r17mysql-nonflatten",
                FLAT,
                20,
            )
            .await
            .expect("converge round one"),
            ReconcileProjection::Applied
        );

        // 并发的**非 flatten** 升级：写完不提交，握着 state 行的排他锁。
        let mut blocking = store.pool().begin().await.expect("begin the blocking arm");
        super::arm_in_tx(
            &mut blocking,
            store.kind(),
            ArmRequest {
                scope_key,
                level: Level::ManageOnly,
                trigger_source: "manual_api",
                reason: "a brand new risk",
                now_ms: 30,
            },
        )
        .await
        .expect("escalate to manage_only without committing");

        let outcome = {
            let store = store.clone();
            let handle =
                tokio::spawn(
                    async move { super::reset(&store, scope_key, "operator", None, 40).await },
                );
            tokio::time::sleep(Duration::from_millis(400)).await;
            blocking.commit().await.expect("commit the escalation");
            tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .expect("the blocked reset must finish once the arm commits")
                .expect("join the reset task")
        };
        let error = outcome.expect_err("a reset validated against another revision must fail");
        assert!(
            error.to_string().contains("kill switch state moved"),
            "the rejection must name the moved state: {error}"
        );
        assert_eq!(
            revision_of(&store, scope_key).await,
            armed_revision + 1,
            "the escalation must own a new revision"
        );
        let level = sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch")
            .get::<String, _>("level");
        assert_eq!(
            level, "manage_only",
            "a reset validated against the previous revision must never clear a new arm"
        );
        drop_scratch(store, admin_url, database).await;
    }

    async fn revision_of(store: &Store, scope_key: &str) -> i64 {
        sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the latch revision")
            .get::<i64, _>("flatten_generation")
    }

    /// **双启动只留一个 anchor。**
    ///
    /// R16 的启动复核在**事务外**读整份 armed 状态，然后逐 scope 开事务。
    /// 两个同时启动的进程读到同一份快照、同时判"本代没有 active 事件"、
    /// 各写一条锚——同一个 `(scope_key, revision)` 于是有两个所有者，而
    /// `reset` 的 `ORDER BY armed_at_ms DESC LIMIT 1` 只会标掉其中一条，
    /// 另一条永远挂着未 reset（审计面从此说不清这一轮被清过没有）。
    ///
    /// 这条用例在**真实 MySQL** 上把两次复核真的并发跑起来（SQLite 只开一条
    /// 连接，造不出这个交错）。判据：无论谁先拿到 state 行锁，本 revision
    /// 最后**恰好一条**活事件，锚**恰好一条**。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_concurrent_startup_revalidations_leave_a_single_anchor() {
        let Some((store, admin_url, database)) =
            mysql_store("two_concurrent_startup_revalidations_leave_a_single_anchor").await
        else {
            return;
        };
        let scope_key = "bot:bot-r17mysql-anchor";
        // 0026 时点的错位形状：state 在 revision 2，事件行都停在 0，
        // 且 arm 身份与任何一条都对不上 ⇒ 复核只能走「隔离 + 锚」。
        seed_0026_shaped_latch(&swap_database(&admin_url, &database), scope_key).await;
        sqlx::query("UPDATE kill_switch_state SET armed_at_ms='2500' WHERE scope_key=?")
            .bind(scope_key)
            .execute(store.pool())
            .await
            .expect("drift the arm identity");

        let (first, second) = {
            let a = store.clone();
            let b = store.clone();
            tokio::join!(
                tokio::spawn(
                    async move { super::revalidate_legacy_event_generations(&a, 5_000).await }
                ),
                tokio::spawn(
                    async move { super::revalidate_legacy_event_generations(&b, 5_001).await }
                ),
            )
        };
        let first = first.expect("join").expect("first revalidation");
        let second = second.expect("join").expect("second revalidation");
        assert_eq!(
            first + second,
            3,
            "exactly one of the two startups may do the work (2 isolated + 1 anchor); \
             got {first} and {second}"
        );

        let anchors = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=? \
             AND trigger_source='generation_revalidation'",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("count anchors")
        .get::<i64, _>("n");
        assert_eq!(
            anchors, 1,
            "two concurrent startups must not write two anchors"
        );

        let live = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=? \
             AND reset_at_ms IS NULL AND flatten_generation=2",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("count live events at the current revision")
        .get::<i64, _>("n");
        assert_eq!(live, 1, "the revision must end up with exactly one owner");

        // 单所有者之后 reset 才是无歧义的。
        assert_eq!(
            record_reconcile_for_scope(
                &store,
                scope_key,
                "cred-r17mysql-anchor",
                FLAT,
                6_000,
                &observe_flatten_generations(&store)
                    .await
                    .expect("snapshot the revision"),
            )
            .await
            .expect("converge the anchored revision"),
            ReconcileProjection::Applied
        );
        super::reset(&store, scope_key, "operator", None, 6_100)
            .await
            .expect("reset the anchored revision");
        let still_live = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=? \
             AND reset_at_ms IS NULL AND flatten_generation=2",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("count live events after the reset")
        .get::<i64, _>("n");
        assert_eq!(
            still_live, 0,
            "a reset must not leave a second owner of this revision behind"
        );
        drop_scratch(store, admin_url, database).await;
    }

    /// **迁移链的 MySQL 增量序列**（0025 / 0026 / 0027 之前只在 SQLite 实跑过；
    /// R15R §10.⑧ 如实登记为"未经真库验证"）。
    ///
    /// 三序列都在真库上跑：
    /// * **新装**：`mysql_store` 已经把整条链跑完（含 0027）；
    /// * **重跑**：同一个库再 `Store::connect` 一次，迁移与启动复核都是空操作；
    /// * **非零代升级**：另建一个库，用**只到 0026** 的迁移目录（同一批 `.sql`
    ///   文件）跑到 0026 时点，在里面留下 R14/R15 运行期才会出现的形状
    ///   （state 代际 2、事件行代际 0），再让生产的 `Store::connect` 走 0027
    ///   与启动复核收敛它。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_migration_chain_converges_on_mysql() {
        let Some((store, admin_url, database)) =
            mysql_store("the_migration_chain_converges_on_mysql").await
        else {
            return;
        };
        let scratch_url = swap_database(&admin_url, &database);
        let applied = sqlx::query("SELECT COUNT(*) AS n FROM _sqlx_migrations")
            .fetch_one(store.pool())
            .await
            .expect("read the migration ledger")
            .get::<i64, _>("n");
        assert!(
            applied >= 27,
            "the whole chain including 0027 must be applied on MySQL, got {applied}"
        );
        assert_eq!(
            sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_event_generation_revalidations")
                .fetch_one(store.pool())
                .await
                .expect("0027 must have created its audit table")
                .get::<i64, _>("n"),
            0,
            "a fresh MySQL install has nothing to revalidate"
        );
        store.pool().close().await;

        // 重跑
        let store = Store::connect(&scratch_url)
            .await
            .expect("rerun the migration chain on MySQL");
        assert_eq!(
            sqlx::query("SELECT COUNT(*) AS n FROM _sqlx_migrations")
                .fetch_one(store.pool())
                .await
                .expect("read the migration ledger")
                .get::<i64, _>("n"),
            applied,
            "a rerun must not append migrations"
        );
        drop_scratch(store, admin_url.clone(), database).await;

        // 非零代升级
        let Some((upgrade_url, upgrade_db, migrations)) =
            mysql_pre_0027_database(&admin_url, "upgrade").await
        else {
            return;
        };
        let scope_key = "bot:bot-r16mysqlup";
        seed_0026_shaped_latch(&upgrade_url, scope_key).await;
        let store = Store::connect(&upgrade_url)
            .await
            .expect("upgrade a 0026-era MySQL database through 0027");
        let verdict = sqlx::query(
            "SELECT verdict, previous_generation, resolved_generation \
             FROM kill_switch_event_generation_revalidations WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the revalidation verdicts")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("verdict"),
                row.get::<i64, _>("previous_generation"),
                row.get::<i64, _>("resolved_generation"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            verdict,
            vec![("adopted".to_string(), 0, 2)],
            "the live round's legacy event must be adopted into generation 2 on MySQL too"
        );
        assert_eq!(
            record_reconcile_for_scope(
                &store,
                scope_key,
                "cred-r16mysqlup",
                FLAT,
                40,
                &observe_flatten_generations(&store)
                    .await
                    .expect("snapshot the generation"),
            )
            .await
            .expect("project a flat truth after the revalidation"),
            ReconcileProjection::Applied,
            "after the 0027 revalidation the latch must converge again"
        );
        drop_scratch(store, admin_url, upgrade_db).await;
        let _ = std::fs::remove_dir_all(migrations);
    }

    /// 建一个 scratch 库并只把迁移跑到 **0026**（同一批 `.sql` 文件的副本）。
    async fn mysql_pre_0027_database(
        admin_url: &str,
        label: &str,
    ) -> Option<(String, String, std::path::PathBuf)> {
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(admin_url)
            .await
            .ok()?;
        let database = format!(
            "rustcore_r16pre_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("create the pre-0027 scratch database");
        admin.close().await;
        let url = swap_database(admin_url, &database);
        let migrations = copy_migrations_without_0027(label);
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the pre-0027 MySQL database");
        sqlx::migrate::Migrator::new(migrations.as_path())
            .await
            .expect("load the pre-0027 migrator")
            .run(&pool)
            .await
            .expect("run migrations up to 0026 on MySQL");
        pool.close().await;
        Some((url, database, migrations))
    }

    fn copy_migrations_without_0027(label: &str) -> std::path::PathBuf {
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let target = std::env::temp_dir().join(format!(
            "rustcore-r16mysqlmig-{label}-{}",
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(&target).expect("create the pre-0027 migration directory");
        for entry in std::fs::read_dir(&source).expect("read the migration directory") {
            let path = entry.expect("read a migration entry").path();
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .expect("migration file name")
                .to_string();
            if !name.ends_with(".sql") || name.starts_with("0027_") {
                continue;
            }
            std::fs::copy(&path, target.join(&name)).expect("copy a migration file");
        }
        target
    }

    /// 0026 时点的形状：`kill_switch_state` 代际 2 的活 flatten latch，
    /// 两条事件行都取 0026 的 `DEFAULT 0`。
    async fn seed_0026_shaped_latch(url: &str, scope_key: &str) {
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("connect for seeding");
        // ****：R14/R15 运行期的真实形状里，这条
        // `bot:` latch 之所以被闩住，就是因为这个 credential 在这个 bot 下
        // 有敞口——`reconcile_targets_in_tx` 正是从这张表认成员的。
        // R17 的夹具省掉了它，靠"任意 credential 都能建行"蒙混过关。
        if let Some(bot_uid) = scope_key.strip_prefix("bot:") {
            sqlx::query(
                "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, \
                 direction, qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms) \
                 VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100', 'open', \
                 1, 1, 1)",
            )
            .bind(format!("pos_{bot_uid}"))
            .bind(bot_uid.replace("bot-", "cred-"))
            .bind(bot_uid)
            .execute(&pool)
            .await
            .expect("seed the exposure the latch was armed for");
        }
        for (reason, now_ms) in [("first exposure", 1_000i64), ("second exposure", 2_000)] {
            sqlx::query(
                "INSERT INTO kill_switch_events (event_id, scope, scope_key, level, \
                 trigger_source, reason, armed_at_ms, flatten_state, reconcile_clean, \
                 reset_by, reset_at_ms, created_at_ms, updated_at_ms, flatten_generation) \
                 VALUES (?, 'bot', ?, 'flatten', 'auto_protection_timeout', ?, ?, \
                 'flattening', NULL, NULL, NULL, ?, ?, 0)",
            )
            .bind(ulid::Ulid::new().to_string())
            .bind(scope_key)
            .bind(reason)
            .bind(now_ms.to_string())
            .bind(now_ms.to_string())
            .bind(now_ms.to_string())
            .execute(&pool)
            .await
            .expect("seed a legacy kill switch event");
        }
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, flatten_generation, last_reconcile_clean, \
             last_reconcile_at_ms, updated_at_ms) \
             VALUES (?, 'flatten', 'second exposure', 'auto_protection_timeout', '2000', \
             'flattening', 2, NULL, NULL, '2000')",
        )
        .bind(scope_key)
        .execute(&pool)
        .await
        .expect("seed the armed latch at generation 2");
        pool.close().await;
    }

    /// **两路并发人工 re-freeze
    /// 只许推进一代、只许写一份新 snapshot。**
    ///
    /// **缺陷本体**：re-freeze 的决策架在两次**不加锁**的读上——
    /// `arm_in_tx_with_round` 用普通 SELECT 读 `(level, flatten_generation)`，
    /// `scope_awaits_refreeze` 再用普通 SELECT 读本代有没有 quarantine 行；
    /// 而"推一代"落在后面那条原子自增里。两路并发的人工处置因此**同读
    /// 一份 quarantine**、同判"这一轮卡着"，各自走完自增：代际从 N 跳到
    /// N+2，`reset_reconcile_targets_in_tx` 跑两遍（第一份 snapshot 当场被
    /// 第二份删掉重建），审计面留下两条 arm 事件。运维点两下、或前端重试
    /// 一次，就凭空多出一代——而人工处置正是这条路径上**唯一**的出口，
    /// 它必须幂等。
    ///
    /// **为什么必须真库**：`Store` 对 SQLite 只开一条连接，两路 re-freeze
    /// 在那里天然串行，这个交错造不出来（与本模块另外几条同一条理由）。
    /// 这里用 MySQL 的真实行锁把它造出来：第一路在**未提交的事务**里走完
    /// 生产的 re-freeze 分支、握着 `kill_switch_state` 那一行；第二路走
    /// **生产入口** `kill_switch::arm`（`svc_v2::set_kill_switch` 调的就是
    /// 它）并真的被挡住；第一路提交之后它回来重读，那时 quarantine 行已经
    /// 被第一路清掉，窄门自己关上，它落回档位幂等早返回。
    ///
    /// 反向可断言：代际停在 2（缺陷版本是 3）、本代只有一份 arm 冻结的
    /// snapshot、`flatten_generation > 1` 的审计事件只有一条。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_concurrent_manual_refreezes_advance_one_generation() {
        let Some((store, admin_url, database)) =
            mysql_store("two_concurrent_manual_refreezes_advance_one_generation").await
        else {
            return;
        };
        let scope_key = "cred:cred-r21mysql-refreeze";
        super::latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // 启动期复核判成 quarantine 的那种行：它是 re-freeze 窄门唯一的钥匙
        // （`scope_awaits_refreeze` 恰以"本代存在证不出来的行"为条件）。
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, 'cred-r21mysql-ghost', 1, 1, 5, 1, ?)",
        )
        .bind(scope_key)
        .bind(super::TARGET_LEGACY_UNPROVABLE)
        .execute(store.pool())
        .await
        .expect("plant the quarantined row that opens the refreeze door");

        // 第一路：走完生产的 re-freeze 分支，**不提交**。
        let mut first = store
            .pool()
            .begin()
            .await
            .expect("begin the first refreeze");
        arm_in_tx_with_round(
            &mut first,
            store.kind(),
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "operator refreeze",
                now_ms: 20,
            },
            LatchRound::Existing,
            None,
        )
        .await
        .expect("the first manual refreeze opens a new revision");

        // 第二路：生产入口，真的并发。
        let second = {
            let store = store.clone();
            let handle = tokio::spawn(async move {
                super::arm(
                    &store,
                    ArmRequest {
                        scope_key,
                        level: Level::Flatten,
                        trigger_source: "manual_api",
                        reason: "operator refreeze (double click)",
                        now_ms: 21,
                    },
                )
                .await
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
            first.commit().await.expect("commit the first refreeze");
            tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .expect("the blocked refreeze must finish once the first one commits")
                .expect("join the second refreeze task")
                .expect("a concurrent manual refreeze is not an error, it is a no-op")
        };
        assert_eq!(
            second.level,
            Level::Flatten,
            "the second call must report the latch it found, not a new one"
        );

        let (_, generation, _) = state_row(&store, scope_key).await;
        assert_eq!(
            generation, 2,
            "two concurrent manual refreezes must advance exactly one generation"
        );
        let snapshot = sqlx::query(
            "SELECT credential_id, flatten_generation, \
             COALESCE(expected_provenance,'<null>') AS expected_provenance \
             FROM kill_switch_reconcile_status WHERE scope_key=? ORDER BY credential_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the refrozen snapshot")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("credential_id"),
                row.get::<i64, _>("flatten_generation"),
                row.get::<String, _>("expected_provenance"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            snapshot,
            vec![(
                "cred-r21mysql-refreeze".to_string(),
                2,
                super::TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "exactly one new snapshot may be written; the second refreeze must not rebuild it"
        );
        let refreeze_events = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=? \
             AND flatten_generation > 1",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("count the refreeze audit events")
        .get::<i64, _>("n");
        assert_eq!(
            refreeze_events, 1,
            "a double-clicked manual refreeze must leave one audit event, not two"
        );
        drop_scratch(store, admin_url, database).await;
    }
}

/// **收敛判据的闭世界。**
///
/// R17 的 `record_reconcile_for_scope` 是「任何非空 `credential_id` 都能给
/// 这个 scope 添一行」+「聚合只看已经出现的行」。合起来是：**第一个任意
/// credential 报一次 flat，整个 scope 就 `flattened`**——latch 退出
/// `scopes_needing_action_retry` 的重试集合，而它真正的敞口一次都没被平过
/// （engine safety contract「对账失败后继续交易」一律 High）。
///
/// 判据现在由两半构成，这五条夹具一半各钉两条、加一条并发 revision：
///
/// | 夹具 | 钉的是哪一半 |
/// | --- | --- |
/// | 空集 | 成员集合为空时**没有任何 credential** 能收敛它 |
/// | 缺一员 | expected 里有人一行都没报 ⇒ 不收敛（完整性） |
/// | 额外一员 | 非成员一行都写不进去（成员资格） |
/// | 晚出现 dirty | 收敛之后成员再报 dirty ⇒ 退回未收敛并重回重试集合 |
/// | 并发 revision | 新一轮重新冻结 target，旧一轮的 clean 行一格都不算 |
#[cfg(test)]
mod r18_scope_target_tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;

    use super::{
        latch_flatten_pending, observe_flatten_generations, record_reconcile_for_scope,
        record_reconcile_for_scope_now, reset, scopes_needing_action_retry, Level,
        ReconcileProjection, ReconcileTruth, FLATTEN_CONVERGED, FLATTEN_PENDING,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const FLAT: ReconcileTruth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    };
    const DIRTY: ReconcileTruth = ReconcileTruth {
        has_diff: true,
        all_positions_zero: false,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: false,
    };

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-r18ks-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect kill-switch test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 一条本地敞口——这就是 `reconcile_targets_in_tx` 认成员的依据。
    async fn seed_exposure(store: &Store, bot_uid: &str, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100',
              'open', 1, 1, 1)
            "#,
        )
        .bind(format!("pos_{bot_uid}_{credential_id}"))
        .bind(credential_id)
        .bind(bot_uid)
        .execute(store.pool())
        .await
        .expect("seed the exposure that makes this credential a target of the scope");
    }

    async fn verdict(store: &Store, scope_key: &str) -> (i64, String) {
        let row = sqlx::query(
            "SELECT COALESCE(last_reconcile_clean, -1) AS clean, \
             COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch verdict");
        (
            row.get::<i64, _>("clean"),
            row.get::<String, _>("flatten_state"),
        )
    }

    async fn status_rows(store: &Store, scope_key: &str) -> Vec<(String, i64, i64)> {
        sqlx::query(
            "SELECT credential_id, reconcile_clean, flatten_complete, flatten_generation \
             FROM kill_switch_reconcile_status WHERE scope_key=? ORDER BY credential_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the per-credential rows")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("credential_id"),
                row.get::<i64, _>("reconcile_clean"),
                row.get::<i64, _>("flatten_generation"),
            )
        })
        .collect()
    }

    async fn in_retry_set(store: &Store, scope_key: &str) -> bool {
        scopes_needing_action_retry(store)
            .await
            .expect("scan the retry set")
            .contains(&(scope_key.to_string(), Level::Flatten))
    }

    // -----------------------------------------------------------------------
    // ① 空集
    // -----------------------------------------------------------------------

    /// **成员集合为空的 scope 不得被任何 credential 收敛。**
    ///
    /// 这是 R17 最裸露的那个形状：`bot:` 下还没有任何本地仓位或本地单时
    /// `reconcile_targets_in_tx` 返回空集，而 R17 的无条件 upsert 让**第一条
    /// 到达的 flat truth**（哪怕来自一个与这个 bot 毫无关系的凭据）把
    /// `total` 变成 1、`dirty` 变成 0，于是 latch 立刻写成 `flattened` 并退出
    /// 重试集合。
    ///
    /// 正确处置是 fail-closed：一次交易所真值都没看过就宣布"已平"，
    /// 正是 engine safety contract 点名的"把缺失数据转换成正常"。
    #[tokio::test]
    async fn an_empty_target_set_cannot_be_converged_by_any_credential() {
        let (store, path) = test_store("empty-target").await;
        let scope_key = "bot:bot-r18empty";
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch a scope that has no local exposure yet");

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18stranger", FLAT, 20)
                .await
                .expect("a foreign credential is not an error"),
            ReconcileProjection::RejectedNonTarget { generation: 1 },
            "a credential this revision never claimed must not be able to write into it"
        );
        assert!(
            status_rows(&store, scope_key).await.is_empty(),
            "the rejected truth must not leave a row behind"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "a scope with no proven target must stay unconverged"
        );
        assert!(
            in_retry_set(&store, scope_key).await,
            "an unconverged scope must stay in the retry set"
        );
        assert!(
            reset(&store, scope_key, "operator", None, 30)
                .await
                .is_err(),
            "a revision without a single reconciled target may not be cleared"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ② 缺一员
    // -----------------------------------------------------------------------

    /// **expected 集合里有人一行都没报 ⇒ 不收敛。**
    ///
    /// 这一半是"完整性"：R17 的聚合只问"已经出现的行干不干净"，答不出
    /// "该出现的都出现了吗"。两个成员里只有一个报过 flat 时，R17 会数出
    /// `total=1, dirty=0` 并收敛——另一个成员的敞口一次都没被对账过。
    ///
    /// （`reset_reconcile_targets_in_tx` 在 arm 时会把两个成员都写成
    /// `reconcile_clean=0`，因此 `dirty` 那一列**也**能挡住这一条。用例
    /// 因此额外把那两行删掉，只留"成员在但没有行"这一种形状——完整性判据
    /// 是这里唯一的守卫。）
    #[tokio::test]
    async fn a_revision_missing_one_target_never_converges() {
        let (store, path) = test_store("missing-target").await;
        let scope_key = "bot:bot-r18missing";
        for credential_id in ["cred-r18miss-a", "cred-r18miss-b"] {
            seed_exposure(&store, "bot-r18missing", credential_id).await;
        }
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // 只留"成员存在、但本 revision 一行都没有"这一种形状。
        sqlx::query("DELETE FROM kill_switch_reconcile_status WHERE scope_key=?")
            .bind(scope_key)
            .execute(store.pool())
            .await
            .expect("clear the frozen rows so completeness is the only guard left");

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18miss-a", FLAT, 20)
                .await
                .expect("project the first target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "one of two targets has never reported; the revision is not converged"
        );
        assert!(in_retry_set(&store, scope_key).await);

        // 反向：另一个成员也报过之后，同一条聚合必须收敛。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18miss-b", FLAT, 30)
                .await
                .expect("project the second target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "every target reported a flat truth, so the revision converges"
        );
        assert!(!in_retry_set(&store, scope_key).await);
        reset(&store, scope_key, "operator", None, 40)
            .await
            .expect("a fully reconciled revision may be cleared");
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ③ 额外一员
    // -----------------------------------------------------------------------

    /// **非成员一行都写不进去。**
    ///
    /// 这一半是"成员资格"。反向面同样钉住：真正的成员照常写得进去，
    /// 否则"一律拒绝"这种过度处置也能让上面那条断言全绿。
    #[tokio::test]
    async fn a_credential_outside_the_target_set_is_rejected() {
        let (store, path) = test_store("extra-target").await;
        let scope_key = "bot:bot-r18extra";
        seed_exposure(&store, "bot-r18extra", "cred-r18extra-a").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");

        // 额外一员：与这个 bot 毫无关系的凭据报一份"全平"。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18outsider", FLAT, 20)
                .await
                .expect("a foreign credential is not an error"),
            ReconcileProjection::RejectedNonTarget { generation: 1 },
        );
        assert_eq!(
            status_rows(&store, scope_key)
                .await
                .into_iter()
                .map(|(credential_id, ..)| credential_id)
                .collect::<Vec<_>>(),
            vec!["cred-r18extra-a".to_string()],
            "the outsider must not appear among this revision's rows"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "a foreign flat truth must not converge this revision"
        );

        // 反向：真正的成员写得进去并收敛。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18extra-a", FLAT, 30)
                .await
                .expect("project the real target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        cleanup(store, path).await;
    }

    /// **成员资格看的是「此刻还认不认得它」，不只是 arm 那一刻冻结的名单。**
    ///
    /// 生产里有一条真实顺序：一次对账**先发现**一个孤儿仓位、把它写进
    /// `position_state`，**然后**才把真值投影到那条早就闩着的 `bot:` latch 上
    /// （`reconcile::reconcile_binance_futures_truth_with_client` 的孤儿恢复，
    /// 回归见 `binance_futures::tests::t310_*`）。这个 credential 在 arm 的
    /// 那一刻**还不存在**，只认冻结名单会把那条 latch 永远钉死在
    /// `flattening`——一个可用性故障，而且掩盖真正的敞口。
    ///
    /// 新成员进来时是**未收敛**的（它必须自己报一份真值），所以这条放行
    /// 不会把 §9.5 的缺陷放回来。
    #[tokio::test]
    async fn a_target_discovered_after_the_arm_still_has_to_converge() {
        let (store, path) = test_store("late-target").await;
        let scope_key = "bot:bot-r18late";
        seed_exposure(&store, "bot-r18late", "cred-r18late-a").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18late-a", FLAT, 20)
                .await
                .expect("project the only target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "the revision's only target reported flat"
        );

        // 对账随后发现一个孤儿仓位：这个 bot 下多了一个 credential。
        seed_exposure(&store, "bot-r18late", "cred-r18late-orphan").await;
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18late-orphan", DIRTY, 30)
                .await
                .expect("a newly discovered member is a target, not an outsider"),
            ReconcileProjection::Applied,
            "a credential the scope now owns must be able to report its truth"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "the newly discovered exposure is not flat, so the revision reopens"
        );
        assert!(in_retry_set(&store, scope_key).await);

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18late-orphan", FLAT, 40)
                .await
                .expect("project the orphan once it is flat"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "with every member flat the revision converges again"
        );
        cleanup(store, path).await;
    }

    /// **反向：arm 时冻结下来的 target，敞口后来消失了也仍然是 target。**
    ///
    /// 成员资格是「arm 冻结的集合 **∪** 此刻 `reconcile_targets_in_tx` 认得的
    /// 成员」。只认后者会漏掉一个可达形状：这个 credential 在 arm 那一刻
    /// **有**敞口（于是被冻结进本 revision），随后那条 `position_state` 行
    /// 被删掉了——`svc.rs` / `svc_v2.rs` 都会 `DELETE FROM position_state …
    /// state='pending'` 回滚一条没落地的仓位。此时它不再是"此刻的成员"，
    /// 但它**仍然是本 revision 要收敛的那个敞口**：拒绝它的真值，这条 latch
    /// 就永远停在 `flattening`，而 `reset` 的判据（本 revision 全 clean）
    /// 也永远满足不了。
    ///
    /// 反过来这条放行也不会把 §9.5 的缺陷放回来：它认的是**本 revision
    /// 自己冻结的那一行**，不是任意 credential（那条由
    /// `a_credential_outside_the_target_set_is_rejected` 钉住）。
    #[tokio::test]
    async fn a_frozen_target_whose_exposure_disappeared_can_still_converge() {
        let (store, path) = test_store("vanished-exposure").await;
        let scope_key = "bot:bot-r18vanish";
        seed_exposure(&store, "bot-r18vanish", "cred-r18vanish").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope while the exposure still exists");
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![("cred-r18vanish".to_string(), 0, 1)],
            "the arm must have frozen this credential as a target of revision 1"
        );

        // 敞口消失：这条 pending 仓位被回滚掉了。
        sqlx::query("DELETE FROM position_state WHERE bot_uid=?")
            .bind("bot-r18vanish")
            .execute(store.pool())
            .await
            .expect("roll the pending position back");

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18vanish", FLAT, 20)
                .await
                .expect("a frozen target is still a target"),
            ReconcileProjection::Applied,
            "this revision froze this credential; its truth must still be able to converge it"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "every frozen target reported flat, so the revision converges"
        );
        reset(&store, scope_key, "operator", None, 30)
            .await
            .expect("a fully reconciled revision may be cleared");
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ④ 晚出现 dirty
    // -----------------------------------------------------------------------

    /// **已经收敛的 revision 里，一个成员再报 dirty 必须让它退回未收敛。**
    ///
    /// 收敛不是一次性闸门：`spawn_action_retry_task` 每一轮都会重新对账，
    /// 而交易所真值随时可能翻回"还有敞口"。判据若只在"第一次全 clean"
    /// 时写一次 `flattened`，这个 scope 就再也不会回到重试集合。
    #[tokio::test]
    async fn a_target_that_turns_dirty_reopens_a_converged_revision() {
        let (store, path) = test_store("late-dirty").await;
        let scope_key = "bot:bot-r18dirty";
        for credential_id in ["cred-r18dirty-a", "cred-r18dirty-b"] {
            seed_exposure(&store, "bot-r18dirty", credential_id).await;
        }
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        for (credential_id, now_ms) in [("cred-r18dirty-a", 20), ("cred-r18dirty-b", 30)] {
            assert_eq!(
                record_reconcile_for_scope_now(&store, scope_key, credential_id, FLAT, now_ms)
                    .await
                    .expect("project a flat target"),
                ReconcileProjection::Applied
            );
        }
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "both targets reported flat"
        );
        assert!(!in_retry_set(&store, scope_key).await);

        // 下一轮对账在同一个成员上看到了新的分歧。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18dirty-b", DIRTY, 40)
                .await
                .expect("project the dirty truth"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "a target that turned dirty must reopen the revision"
        );
        assert!(
            in_retry_set(&store, scope_key).await,
            "a reopened revision must go back into the retry set"
        );
        assert!(
            reset(&store, scope_key, "operator", None, 50)
                .await
                .is_err(),
            "a reopened revision may not be cleared"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ⑤ 并发 revision
    // -----------------------------------------------------------------------

    /// **新一轮重新冻结 target，上一轮的 clean 行一格都不算。**
    ///
    /// 交错是确定性的：
    ///
    /// 1. 第一轮 latch（revision 1），唯一的成员报 flat ⇒ 收敛；
    /// 2. 一次对账**动身**（按生产纪律先取代际快照）；
    /// 3. 在它回来之前又发现一个新的裸敞口 ⇒ 第二次 latch（revision 2）；
    /// 4. 第 2 步那次对账现在才落库，带回来的是一份"全平"的真值。
    ///
    /// 第 4 步必须被丢弃（`DiscardedStale`），而且第 1 轮那条 clean 行
    /// 不得替第 2 轮背书——`reset_reconcile_targets_in_tx` 已经把成员行
    /// 重置成"未 clean"并绑上新号，聚合又只数本 revision 的行。
    #[tokio::test]
    async fn a_new_revision_refreezes_its_targets_and_discards_the_older_truth() {
        let (store, path) = test_store("concurrent-revision").await;
        let scope_key = "bot:bot-r18rev";
        seed_exposure(&store, "bot-r18rev", "cred-r18rev").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "first", 10)
            .await
            .expect("first latch");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18rev", FLAT, 20)
                .await
                .expect("converge the first revision"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );

        // 一次对账动身：快照取在**读交易所之前**。
        let observed = observe_flatten_generations(&store)
            .await
            .expect("observe the generations this truth belongs to");
        // 它还在路上时又发现一个新的裸敞口。
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "second", 30)
            .await
            .expect("second latch while the reconcile is in flight");
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "a new exposure must reopen the latch"
        );
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![("cred-r18rev".to_string(), 0, 2)],
            "the new revision must refreeze its targets as unconverged"
        );

        assert_eq!(
            record_reconcile_for_scope(&store, scope_key, "cred-r18rev", FLAT, 40, &observed)
                .await
                .expect("a stale truth is not an error"),
            ReconcileProjection::DiscardedStale {
                observed: 1,
                current: 2
            },
            "a truth that never looked at this exposure must not converge it"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "the discarded truth must leave the new revision untouched"
        );
        assert!(in_retry_set(&store, scope_key).await);

        // 反向：本 revision 自己的真值照常收敛它。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r18rev", FLAT, 50)
                .await
                .expect("converge the current revision"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        cleanup(store, path).await;
    }
}

/// **/// regression。**
///
/// R18 的 frozen expected target 是「本代的**全部** `kill_switch_reconcile_status`
/// 行」。这张表有两个写者（arm 事务的 `reset_reconcile_targets_in_tx`、
/// 投影路径的 upsert），读回来时两者写下的行逐字无法区分——于是"谁是本
/// revision 的 expected 成员"这个判据的输入**包含它自己的输出**。评审给的
/// 形状：预置 active latch + 同代 active event + 空 live targets + 一条同代
/// outsider clean/flat 行 ⇒ 那条 outsider 自己就构成了完整的 expected 集合，
/// latch 被写成 `flattened` 并退出重试集合，而真正的敞口一次都没被平过。
///
/// 修法是迁移 0028 的 `expected_provenance` 一列：只有 arm 事务写得出
/// `'arm_snapshot'`。这一组夹具钉的是**来源**与**集合完整性**两件事：
///
/// | 夹具 | 钉的是什么 |
/// | --- | --- |
/// | 同代历史 outsider | 来源不可证明的同代行不得自授权（成员资格） |
/// | 不速之客的票 | 非 expected 的行既不能投赞成票、也不能把 scope 钉死（聚合） |
/// | 真实新发现 orphan | 孤儿靠**此刻的成员资格**进来，拿的是 discovery 来源 |
/// | 消失的 discovery 行 | 敞口没了的 discovery 行不再是成员（对照冻结成员） |
/// | revision rollover | 旧代的行——哪怕带 `arm_snapshot`——一格都不算 |
/// | 存量行启动复核 | `NULL` 来源按此刻的成员资格重建，clean 票一律复位 |
#[cfg(test)]
mod r19_target_provenance_tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;

    use super::{
        arm, latch_flatten_pending, record_reconcile_for_scope_now, reset,
        revalidate_legacy_reconcile_targets, scopes_needing_action_retry, ArmRequest, Level,
        ReconcileProjection, ReconcileTruth, FLATTEN_CONVERGED, FLATTEN_PENDING,
        TARGET_FROM_ARM_SNAPSHOT, TARGET_FROM_RECONCILE_DISCOVERY, TARGET_LEGACY_UNPROVABLE,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const FLAT: ReconcileTruth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    };

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-r19ks-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect kill-switch test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 一条本地敞口——`reconcile_targets_in_tx` 认成员的依据。
    async fn seed_exposure(store: &Store, bot_uid: &str, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100',
              'open', 1, 1, 1)
            "#,
        )
        .bind(format!("pos_{bot_uid}_{credential_id}"))
        .bind(credential_id)
        .bind(bot_uid)
        .execute(store.pool())
        .await
        .expect("seed the exposure that makes this credential a target of the scope");
    }

    /// 直接往库里塞一行 status——**绕过**投影路径，模拟 0028 之前写下的
    /// 存量行（`expected_provenance IS NULL`），或任何一次在成员资格之外
    /// 落进来的行。
    async fn plant_status_row(
        store: &Store,
        scope_key: &str,
        credential_id: &str,
        generation: i64,
        clean: bool,
        provenance: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(scope_key)
        .bind(credential_id)
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(generation)
        .bind(provenance)
        .execute(store.pool())
        .await
        .expect("plant a reconcile status row");
    }

    /// **一条 0028 之前由投影路径写下的行**。
    ///
    /// 与 `plant_status_row` 的差别只有一处，而这一处正是判据：
    /// `reconciled_at_ms` 是一个**真实墙钟**。R17 的
    /// `record_reconcile_for_scope` 绑的是 `crate::svc::now_ms()`——epoch
    /// 毫秒，生产里恒为正——因此投影路径写下的行**写不出 0**。
    /// `plant_status_row` 那个 `reconciled_at_ms=0` 的形状在 0028 之前
    /// 只有 arm 事务产生得出来；拿它当 "outsider" 会让夹具自己造出一个
    /// 生产不可达的行，把 R20 的可证明签名判据测成假的。
    async fn plant_projected_status_row(
        store: &Store,
        scope_key: &str,
        credential_id: &str,
        generation: i64,
        clean: bool,
        reconciled_at_ms: i64,
    ) {
        assert!(
            reconciled_at_ms > 0,
            "a row the projection wrote always carries a real wall clock; a zero here would \
             forge the arm transaction's freeze signature and make the fixture unreachable"
        );
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, ?, ?, ?, ?, ?, NULL)",
        )
        .bind(scope_key)
        .bind(credential_id)
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(reconciled_at_ms)
        .bind(generation)
        .execute(store.pool())
        .await
        .expect("plant a reconcile status row the pre-0028 projection could have written");
    }

    /// 把本代的行退回 0028 之前的形状（来源被抹掉，其余原样）。
    async fn age_rows_to_pre_0028(store: &Store, scope_key: &str) {
        sqlx::query(
            "UPDATE kill_switch_reconcile_status SET expected_provenance=NULL WHERE scope_key=?",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("age the frozen rows back to the pre-0028 shape");
    }

    async fn generation_of(store: &Store, scope_key: &str) -> i64 {
        sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the flatten generation")
            .get::<i64, _>("flatten_generation")
    }
    async fn verdict(store: &Store, scope_key: &str) -> (i64, String) {
        let row = sqlx::query(
            "SELECT COALESCE(last_reconcile_clean, -1) AS clean, \
             COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch verdict");
        (
            row.get::<i64, _>("clean"),
            row.get::<String, _>("flatten_state"),
        )
    }

    /// `(credential_id, reconcile_clean, flatten_generation, expected_provenance)`。
    async fn status_rows(store: &Store, scope_key: &str) -> Vec<(String, i64, i64, String)> {
        sqlx::query(
            "SELECT credential_id, reconcile_clean, flatten_generation, \
             COALESCE(expected_provenance, '<null>') AS expected_provenance \
             FROM kill_switch_reconcile_status WHERE scope_key=? \
             ORDER BY credential_id, flatten_generation",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the reconcile status rows")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("credential_id"),
                row.get::<i64, _>("reconcile_clean"),
                row.get::<i64, _>("flatten_generation"),
                row.get::<String, _>("expected_provenance"),
            )
        })
        .collect()
    }

    async fn revalidation_verdicts(store: &Store, scope_key: &str) -> Vec<(String, String)> {
        sqlx::query(
            "SELECT credential_id, verdict FROM kill_switch_reconcile_target_revalidations \
             WHERE scope_key=? ORDER BY credential_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("read the revalidation evidence")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("credential_id"),
                row.get::<String, _>("verdict"),
            )
        })
        .collect()
    }

    async fn in_retry_set(store: &Store, scope_key: &str) -> bool {
        scopes_needing_action_retry(store)
            .await
            .expect("scan the retry set")
            .contains(&(scope_key.to_string(), Level::Flatten))
    }

    // -----------------------------------------------------------------------
    // ① 同代历史 outsider（成员资格）
    // -----------------------------------------------------------------------

    /// **一条同代的、来源不可证明的 status 行不得让自己成为 expected 成员。**
    ///
    /// 这是第十五轮 §8.5 给的形状，逐字复现：active flatten latch（`arm_in_tx`
    /// 在同一个事务里写了本代的 state 与事件行）+ **空 live targets**
    /// （这个 bot 下还没有任何本地敞口，`reconcile_targets_in_tx` 返回空集）
    /// + 一条**同代**的 outsider clean/flat 行。
    ///
    /// R18 把这条 outsider 行读成 frozen expected target：它于是通过成员资格
    /// 检查、写进本代、并且是本代**唯一**的行 ⇒ `total=1 / dirty=0 /
    /// unreported=∅` ⇒ latch 写成 `flattened`、退出重试集合。整条链上没有
    /// 任何一份交易所真值属于这个 scope 的敞口（engine safety contract
    /// 「对账失败后继续交易」）。
    ///
    /// 修法后 expected 只认 `arm_snapshot`：这条行的 provenance 是 `NULL`，
    /// 既不是成员、也不参与聚合。
    #[tokio::test]
    async fn a_same_generation_outsider_row_cannot_authorise_itself() {
        let (store, path) = test_store("self-authorised").await;
        let scope_key = "bot:bot-r19self";
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch a scope that has no local exposure yet");
        assert!(
            status_rows(&store, scope_key).await.is_empty(),
            "the fixture must start with an empty frozen target set, otherwise the outsider \
             is not the only row and the assertion is vacuous"
        );

        // 同代的 outsider clean/flat 行：R17 存量行的形状（来源不可证明）。
        plant_status_row(&store, scope_key, "cred-r19outsider", 1, true, None).await;

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19outsider", FLAT, 20)
                .await
                .expect("a foreign credential is not an error"),
            ReconcileProjection::RejectedNonTarget { generation: 1 },
            "a status row of unprovable provenance must not make its own credential a target \
             of this revision"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "a self-authorised flat truth must not converge this revision"
        );
        assert!(
            in_retry_set(&store, scope_key).await,
            "an unconverged scope must stay in the retry set"
        );
        assert!(
            reset(&store, scope_key, "operator", None, 30)
                .await
                .is_err(),
            "a revision without a single proven target may not be cleared"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ② 不速之客的票（聚合完整性）
    // -----------------------------------------------------------------------

    /// **非 expected 的行一票不算——赞成票不算，反对票也不算。**
    ///
    /// 成员资格挡住的是**写入**；R18 的聚合读的仍是本代的全部行。一条本
    /// revision 从未认下的 dirty 行因此能把一个真正已经平掉的 scope 永远
    /// 钉死在 `flattening`（重试任务每一轮重复撤单 + reduce-only flatten），
    /// 而一条 clean 行则替这一轮投了一张它没资格投的赞成票（①）。
    ///
    /// 这条用例走"反对票"那一侧：唯一的真实成员报了 flat，判据必须收敛，
    /// 旁边那条不速之客的 dirty 行一格都不算。
    #[tokio::test]
    async fn an_unexpected_row_does_not_vote_in_the_aggregate() {
        let (store, path) = test_store("stranger-vote").await;
        let scope_key = "bot:bot-r19vote";
        seed_exposure(&store, "bot-r19vote", "cred-r19vote").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // 同代的不速之客：dirty，而且来源不可证明。
        plant_status_row(&store, scope_key, "cred-r19stranger", 1, false, None).await;

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19vote", FLAT, 20)
                .await
                .expect("project the only real target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "every credential this revision actually claimed reported flat; a row it never \
             claimed must not keep it pending"
        );
        assert!(!in_retry_set(&store, scope_key).await);
        // 那条行还在（可审计），只是不参与判据。
        assert!(
            status_rows(&store, scope_key)
                .await
                .iter()
                .any(|(credential_id, ..)| credential_id == "cred-r19stranger"),
            "the unexpected row must be kept for audit, not silently deleted"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ③ 真实新发现 orphan / ④ 消失的 discovery 行
    // -----------------------------------------------------------------------

    /// **孤儿仓位靠「此刻的成员资格」进来，写下的是 discovery 来源；
    /// 敞口消失之后它就不再是成员。**
    ///
    /// 两个方向一条用例：
    ///
    /// * **正向**——`reconcile_binance_futures_truth_with_client` 的孤儿恢复
    ///   会先把一个 arm 时还不存在的 credential 写进 `position_state`，
    ///   再把真值投影上来。它必须写得进去（否则 latch 永远钉死），而它写下
    ///   的行来源是 `reconcile_discovery`，**不是** `arm_snapshot`。
    /// * **反向**——这条 discovery 行的敞口后来消失了，它就既不是"此刻的
    ///   成员"、也不是"arm 冻结的成员"，于是不再能写、也不再计票。这正是
    ///   它与 `r18_scope_target_tests::a_frozen_target_whose_exposure_disappeared_can_still_converge`
    ///   的分界：**冻结成员**的行留任，**discovery 行**不留任。少了
    ///   provenance 这一列，两者读回来一模一样。
    #[tokio::test]
    async fn a_discovered_orphan_is_a_target_only_while_the_scope_still_owns_it() {
        let (store, path) = test_store("orphan-provenance").await;
        let scope_key = "bot:bot-r19orphan";
        seed_exposure(&store, "bot-r19orphan", "cred-r19orphan-a").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![(
                "cred-r19orphan-a".to_string(),
                0,
                1,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "the arm must freeze its member with a provable provenance"
        );

        // 正向：对账发现一个孤儿仓位，随后把真值投上来。
        seed_exposure(&store, "bot-r19orphan", "cred-r19orphan-late").await;
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19orphan-late", FLAT, 20)
                .await
                .expect("a newly discovered member is a target, not an outsider"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            status_rows(&store, scope_key)
                .await
                .into_iter()
                .map(|(credential_id, _, _, provenance)| (credential_id, provenance))
                .collect::<Vec<_>>(),
            vec![
                (
                    "cred-r19orphan-a".to_string(),
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r19orphan-late".to_string(),
                    TARGET_FROM_RECONCILE_DISCOVERY.to_string()
                ),
            ],
            "a row the projection created must never claim the arm snapshot's provenance"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "the frozen member has not reported yet, so the revision is not converged"
        );

        // 反向：孤儿的敞口消失 ⇒ 它不再是成员，真值写不进去、也不计票。
        sqlx::query("DELETE FROM position_state WHERE credential_id=?")
            .bind("cred-r19orphan-late")
            .execute(store.pool())
            .await
            .expect("the orphan exposure is gone");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19orphan-late", FLAT, 30)
                .await
                .expect("a former discovery is not an error"),
            ReconcileProjection::RejectedNonTarget { generation: 1 },
            "a discovery row whose exposure disappeared is not a target this revision claimed"
        );

        // 冻结成员报 flat ⇒ 收敛：那条 discovery 行既没投赞成票也没挡路。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19orphan-a", FLAT, 40)
                .await
                .expect("project the frozen member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "every credential this revision still claims reported flat"
        );
        // **投影不得改写已有行的 provenance。** 冻结成员报过一次真值之后
        // 仍然是冻结成员——否则它的敞口一旦消失（`position_state` 被回滚）
        // 就连自己的 latch 都收敛不了，而那正是 R18 §9.5 放行"此刻的成员
        // ∪ 冻结成员"要救的形状。
        assert_eq!(
            status_rows(&store, scope_key)
                .await
                .into_iter()
                .find(|(credential_id, ..)| credential_id == "cred-r19orphan-a")
                .map(|(_, _, _, provenance)| provenance),
            Some(TARGET_FROM_ARM_SNAPSHOT.to_string()),
            "a frozen member must keep its arm-snapshot provenance after reporting a truth"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ⑤ revision rollover（正反）
    // -----------------------------------------------------------------------

    /// **旧一代的行——哪怕带着 `arm_snapshot`——一格都不算。**
    ///
    /// provenance 回答的是"这一行是谁写的"，代际回答的是"写给哪一轮"。
    /// 两个都要：只看 provenance，上一轮冻结下来的 clean 行就能替新一轮
    /// 背书；只看代际，就是 R18 的形状。
    ///
    /// 正向：新一轮重新冻结成员（新代、`arm_snapshot`、未收敛），它自己的
    /// 真值照常收敛它。反向：一条挂在**上一代**的 `arm_snapshot` clean 行
    /// 既不能让它的 credential 成为本代成员，也不进本代的聚合。
    #[tokio::test]
    async fn only_this_revisions_snapshot_counts_after_a_rollover() {
        let (store, path) = test_store("rollover").await;
        let scope_key = "bot:bot-r19roll";
        seed_exposure(&store, "bot-r19roll", "cred-r19roll").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "first", 10)
            .await
            .expect("first latch");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19roll", FLAT, 20)
                .await
                .expect("converge the first revision"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );

        // 新一轮：`reset_reconcile_targets_in_tx` 删掉旧行、按新代重新冻结。
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "second", 30)
            .await
            .expect("second latch");
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![(
                "cred-r19roll".to_string(),
                0,
                2,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "the new revision must refreeze its own snapshot as unconverged"
        );

        // 反向：一条挂在**上一代**的 `arm_snapshot` clean 行。
        plant_status_row(
            &store,
            scope_key,
            "cred-r19roll-stale",
            1,
            true,
            Some(TARGET_FROM_ARM_SNAPSHOT),
        )
        .await;
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19roll-stale", FLAT, 40)
                .await
                .expect("a previous revision's member is not an error"),
            ReconcileProjection::RejectedNonTarget { generation: 2 },
            "a snapshot row of the previous revision must not make its credential a target of \
             this one"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "the new revision has not been converged by anything yet"
        );

        // 正向：本代自己的成员报 flat ⇒ 收敛。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r19roll", FLAT, 50)
                .await
                .expect("converge the current revision"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ⑥ 0028 存量行的启动期复核
    // -----------------------------------------------------------------------

    /// **升级复核按「此刻的成员资格」重建本代快照，并把来源不可证明的
    /// clean 票一律复位。**
    ///
    /// 形状是一个**升级时正闩着 flatten** 的部署：本代的行全是 0028 之前
    /// 写下的（`expected_provenance IS NULL`）。三种归宿各一条：
    ///
    /// * 仍是成员 ⇒ `adopted`（收进 `arm_snapshot`，clean 复位成 0）；
    /// * 不是成员、也没有 arm 冻结签名 ⇒ `quarantined`
    ///   ；
    /// * 此刻是成员、本代却一行都没有 ⇒ `anchored`（补一行未收敛的成员行）。
    ///
    /// 反向同样钉住：复核之后那条被 adopt 的行**不能**直接收敛这一轮——
    /// 它必须重新报一次真值（复核把它的 clean 票复位了）。
    #[tokio::test]
    async fn legacy_rows_are_rebuilt_from_the_scopes_membership_at_startup() {
        let (store, path) = test_store("legacy-upgrade").await;
        let scope_key = "bot:bot-r19legacy";
        seed_exposure(&store, "bot-r19legacy", "cred-r19legacy-a").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // 0028 之前的形状：成员行的来源被抹掉，clean 票留在库里。
        sqlx::query(
            "UPDATE kill_switch_reconcile_status SET expected_provenance=NULL, \
             reconcile_clean=1, flatten_complete=1 WHERE scope_key=?",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("age the frozen rows back to the pre-0028 shape");
        // 一条同代的存量 outsider 行，以及一个"此刻是成员、本代没有行"的
        // credential。
        plant_status_row(&store, scope_key, "cred-r19legacy-x", 1, true, None).await;
        seed_exposure(&store, "bot-r19legacy", "cred-r19legacy-b").await;

        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy rows"),
            3,
            "two legacy rows plus one anchored member"
        );
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![
                (
                    "cred-r19legacy-a".to_string(),
                    0,
                    1,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r19legacy-b".to_string(),
                    0,
                    1,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r19legacy-x".to_string(),
                    1,
                    1,
                    TARGET_LEGACY_UNPROVABLE.to_string()
                ),
            ],
            "members are adopted and reset to unconverged; the row whose origin cannot be \
             proven is quarantined as-is"
        );
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await,
            vec![
                ("cred-r19legacy-a".to_string(), "adopted".to_string()),
                ("cred-r19legacy-b".to_string(), "anchored".to_string()),
                ("cred-r19legacy-x".to_string(), "quarantined".to_string()),
            ],
            "every legacy row must leave a per-row verdict behind"
        );

        // 幂等：再跑一次是空集、零写入。
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 200)
                .await
                .expect("re-running the revalidation must be a no-op"),
            0
        );

        // 反向：复核之后这一轮**没有**被那张存量 clean 票收敛。
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "an upgraded revision must not inherit a clean vote of unprovable provenance"
        );
        assert!(in_retry_set(&store, scope_key).await);

        // 正向：两个成员各自重新报一次真值之后照常收敛。
        for (credential_id, now_ms) in [("cred-r19legacy-a", 300), ("cred-r19legacy-b", 310)] {
            assert_eq!(
                record_reconcile_for_scope_now(&store, scope_key, credential_id, FLAT, now_ms)
                    .await
                    .expect("project an adopted member"),
                ReconcileProjection::Applied
            );
        }
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "with every adopted member reporting a fresh flat truth the revision converges"
        );
        cleanup(store, path).await;
    }

    /// **复核真的挂在启动路径上。**
    ///
    /// 上一条用例直接调 `revalidate_legacy_reconcile_targets`——它证明的是
    /// "这个函数做对了事"，不是"进程起来的时候它会跑"。把调用点从
    /// `Store::connect` 里摘掉，上一条用例照样全绿，而真实部署升级之后
    /// 库里那些 `NULL` 来源的行一辈子不会被复核：本代 frozen expected 为空，
    /// 敞口已消失的冻结成员连自己的 latch 都收敛不了
    /// （latch 永远停在 `flattening`，重试任务无限重复处置）。
    ///
    /// 这条用例因此从**同一个入口**进：拿一个已经带着存量行的库
    /// 重新 `Store::connect`（生产里 `main()` 唯一的那条建库路径，
    /// 迁移与全部启动期复核都在它里面），再看库里的行。
    #[tokio::test]
    async fn the_startup_path_revalidates_legacy_reconcile_targets() {
        let (store, path) = test_store("startup-revalidation").await;
        let scope_key = "bot:bot-r19boot";
        seed_exposure(&store, "bot-r19boot", "cred-r19boot").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        sqlx::query(
            "UPDATE kill_switch_reconcile_status SET expected_provenance=NULL,              reconcile_clean=1, flatten_complete=1 WHERE scope_key=?",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("age the frozen rows back to the pre-0028 shape");
        plant_status_row(&store, scope_key, "cred-r19boot-x", 1, true, None).await;
        store.pool().close().await;

        // **同一个入口**：生产里 `main()` 就是这样建 `Store` 的。
        let restarted = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("restart against the upgraded database");
        assert_eq!(
            status_rows(&restarted, scope_key).await,
            vec![
                (
                    "cred-r19boot".to_string(),
                    0,
                    1,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r19boot-x".to_string(),
                    1,
                    1,
                    TARGET_LEGACY_UNPROVABLE.to_string()
                ),
            ],
            "starting the store must rebuild this revision's expected target set"
        );
        assert_eq!(
            revalidation_verdicts(&restarted, scope_key).await,
            vec![
                ("cred-r19boot".to_string(), "adopted".to_string()),
                ("cred-r19boot-x".to_string(), "quarantined".to_string()),
            ],
            "the startup revalidation must leave its per-row evidence behind"
        );
        cleanup(restarted, path).await;
    }

    // -----------------------------------------------------------------------
    // ⑦ 第十六轮 §8.5（regression）·「冻结后消失」的可证明来源
    // -----------------------------------------------------------------------

    /// **正向：arm 那一刻冻结、升级时敞口已经消失的真实 target，
    /// 靠 arm 事务的冻结签名被认回来。**
    ///
    /// 这是 R19 复核会误伤的那条形状，逐字复现：一个**升级时正闩着 flatten**
    /// 的 scope，本代唯一的成员行是 arm 冻结进来的（`(reconciled_at_ms,
    /// reconcile_clean, flatten_complete) = (0, 0, 0)`），0028 把它的
    /// provenance 留成 `NULL`；随后那条 `position_state` 被回滚掉
    /// （`svc.rs` / `svc_v2.rs` 都会 `DELETE FROM position_state … state='pending'`），
    /// 于是它**此刻不是成员**。
    ///
    /// R19 的二分（`membership.contains(cid) ? Adopted : Isolated`）把它判成
    /// `isolated`：它的真值从此被 `RejectedNonTarget` 拒在外面，它又不进
    /// expected 集合 ⇒ 这条 latch 永远停在 `flattening`，人工 reset 也永远
    /// 满足不了判据。R19 用来证明"不是 target"的证据（**此刻**不是成员）
    /// 与被证的命题（**arm 那一刻**是不是成员）根本不是同一件事。
    ///
    /// R20 先找**可证明来源**：那个三元组只有 `reset_reconcile_targets_in_tx`
    /// 写得出来（投影路径绑的 `reconciled_at_ms` 是 `now_ms`，写不出 0），
    /// 因此它反向证明了这一行是 arm 冻结进来的 ⇒ `restored`，与此刻是不是
    /// 成员无关。
    #[tokio::test]
    async fn a_frozen_target_that_vanished_before_the_upgrade_is_restored_by_its_signature() {
        let (store, path) = test_store("r20-vanished-frozen").await;
        let scope_key = "bot:bot-r20vanish";
        seed_exposure(&store, "bot-r20vanish", "cred-r20vanish").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope while the exposure still exists");
        // 0028 之前的形状：来源被抹掉，冻结行其余部分原样（未被投影过）。
        age_rows_to_pre_0028(&store, scope_key).await;
        // 敞口消失：这条 pending 仓位被回滚掉了。
        sqlx::query("DELETE FROM position_state WHERE bot_uid=?")
            .bind("bot-r20vanish")
            .execute(store.pool())
            .await
            .expect("roll the pending position back");

        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy row"),
            1
        );
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![(
                "cred-r20vanish".to_string(),
                0,
                1,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "a row bearing the arm transaction's freeze signature must be restored as a \
             frozen target even though the credential is no longer a member"
        );
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await,
            vec![("cred-r20vanish".to_string(), "restored".to_string())],
            "the restoration must name the evidence it used, not hide behind 'adopted'"
        );

        // 正向：它的真值写得进去，并且真的收敛这一轮。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20vanish", FLAT, 200)
                .await
                .expect("a restored frozen target is still a target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "the revision's only frozen target reported flat, so it converges"
        );
        assert!(!in_retry_set(&store, scope_key).await);
        reset(&store, scope_key, "operator", None, 300)
            .await
            .expect("a fully reconciled revision may be cleared");

        // 幂等：复核跑第二次是空集。
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 400)
                .await
                .expect("re-running the revalidation must be a no-op"),
            0
        );
        cleanup(store, path).await;
    }

    /// **反向：投影路径能写出来的存量行不得被"证明"成 arm 冻结的成员。**
    ///
    /// 签名判据的强度全在"投影路径写不出这个元组"上。这条用例造的正是
    /// 投影路径**写得出**的那种行：一条带着真实墙钟、来源为 `NULL` 的
    /// dirty 行，credential 与这个 scope 毫无关系（R17 那条无条件 upsert
    /// 给任何 credential 都能建行）。它必须落进 `quarantined`——**不是**
    /// 因为"它此刻不是成员"，而是因为它的来源在数据里答不出来。
    ///
    /// 同时钉住 quarantine 的两条语义（第十六轮 §8.6）：**不投票**
    /// （真实成员报 flat 之后这一轮照常收敛，它那张 dirty 票一格不算）、
    /// **不阻塞**（人工 reset 不被它拒绝）。
    #[tokio::test]
    async fn a_legacy_row_the_projection_could_have_written_is_quarantined_not_restored() {
        let (store, path) = test_store("r20-unprovable").await;
        let scope_key = "bot:bot-r20quarantine";
        seed_exposure(&store, "bot-r20quarantine", "cred-r20member").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        age_rows_to_pre_0028(&store, scope_key).await;
        // 一条 R17 投影路径写得出的存量行：真实墙钟 + dirty + 非成员。
        plant_projected_status_row(&store, scope_key, "cred-r20outsider", 1, false, 2_100).await;

        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy rows"),
            2
        );
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await,
            vec![
                ("cred-r20member".to_string(), "restored".to_string()),
                ("cred-r20outsider".to_string(), "quarantined".to_string()),
            ],
            "only the row carrying the arm transaction's freeze signature may be restored"
        );
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![
                (
                    "cred-r20member".to_string(),
                    0,
                    1,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r20outsider".to_string(),
                    0,
                    1,
                    TARGET_LEGACY_UNPROVABLE.to_string()
                ),
            ],
            "a quarantined row is kept for audit, exactly as it was"
        );

        // 不投票（写侧）：它不是本 revision 认下的 target。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20outsider", FLAT, 200)
                .await
                .expect("a quarantined credential is not an error"),
            ReconcileProjection::RejectedNonTarget { generation: 1 },
            "a row whose origin cannot be proven must not authorise its own credential"
        );

        // 不投票（读侧）+ 不阻塞：真实成员报 flat 之后照常收敛，并且能被清掉。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20member", FLAT, 210)
                .await
                .expect("project the real member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "a quarantined dirty row must not keep a converged revision pending"
        );
        reset(&store, scope_key, "operator", None, 220)
            .await
            .expect("a quarantined dirty row must not permanently block a manual reset");
        cleanup(store, path).await;
    }

    /// **R19 判过的 `legacy_isolated` 行必须被新判据重判。**
    ///
    /// R19 已经上线过一版复核，它的判据（`membership.contains(cid) ? Adopted
    /// : Isolated`）本身就是 regression。一个已经跑过 R19 的库里，
    /// 那条"arm 时冻结、升级时敞口已消失"的真实 target 已经被写成
    /// `legacy_isolated`——如果 R20 的复核只看 `NULL`，它就永远停在那儿，
    /// 缺陷穿过第二次升级活下来。
    ///
    /// 正向：带着冻结签名的 `legacy_isolated` 行被重判成 `restored`。
    /// 反向：重判之后再跑一次是空集（幂等没有被"永远重判"换掉）。
    #[tokio::test]
    async fn a_row_r19_isolated_is_rejudged_by_the_new_evidence() {
        let (store, path) = test_store("r20-rejudge").await;
        let scope_key = "bot:bot-r20rejudge";
        seed_exposure(&store, "bot-r20rejudge", "cred-r20rejudge").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // R19 的复核结论：这条冻结行被判成"不是 target"。
        sqlx::query(
            "UPDATE kill_switch_reconcile_status SET expected_provenance='legacy_isolated' \
             WHERE scope_key=?",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("replay the verdict R19 would have written");
        sqlx::query("DELETE FROM position_state WHERE bot_uid=?")
            .bind("bot-r20rejudge")
            .execute(store.pool())
            .await
            .expect("the exposure disappeared before the R19 upgrade");

        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("re-judge the R19 verdict"),
            1
        );
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![(
                "cred-r20rejudge".to_string(),
                0,
                1,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "a row R19 isolated on an unprovable inference must be re-judged on evidence"
        );
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await,
            vec![("cred-r20rejudge".to_string(), "restored".to_string())]
        );
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 200)
                .await
                .expect("re-running the revalidation must be a no-op"),
            0,
            "re-judging must not turn the revalidation into a permanent rewriter"
        );
        cleanup(store, path).await;
    }

    /// **结构化人工处置：来源证不出来时，运维可以把这一轮重新冻结。**
    ///
    /// quarantine 只说"来源答不出来"，它不说"这一行不是 target"。万一其中
    /// 真有一条是本 revision 的 target，这一轮的 expected 集合就是残缺的，
    /// 而运维手上必须有一条出口——否则"不把真实 frozen target 永久拒在外"
    /// 这句话就落不了地。
    ///
    /// 出口是 `arm` 的 re-freeze 分支：**人工** `flatten` 重新 arm 一个
    /// 本代带着 quarantine 行的 flatten scope，代际 +1、按
    /// `reconcile_targets_in_tx` 重新冻结成员、写一条新的审计事件。
    /// 它不清 latch、不写任何 clean/flat 票——新一轮从"未收敛"开始，
    /// 而逐行的处置结论与证据留在 `kill_switch_reconcile_target_revalidations`
    /// 上（新一轮的快照重置会像每一次 arm 那样清掉旧的 status 行）。
    ///
    /// 反向两条同样钉住：**自动**源不得凭空推代际（幂等早返回原样保留），
    /// 且重新冻结之前这一轮确实清不掉（expected 集合为空）。
    #[tokio::test]
    async fn an_unprovable_revision_can_be_refrozen_by_a_manual_arm() {
        let (store, path) = test_store("r20-refreeze").await;
        let scope_key = "bot:bot-r20refreeze";
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch a scope that has no local exposure yet");
        assert!(
            status_rows(&store, scope_key).await.is_empty(),
            "the fixture must start from an empty frozen target set"
        );
        // R17 那条自授权行的形状：真实墙钟 + clean/flat + 来源不可证明。
        plant_projected_status_row(&store, scope_key, "cred-r20ghost", 1, true, 2_100).await;
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy row"),
            1
        );
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await,
            vec![("cred-r20ghost".to_string(), "quarantined".to_string())]
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "an unprovable clean vote must not converge this revision"
        );
        assert!(
            reset(&store, scope_key, "operator", None, 110)
                .await
                .is_err(),
            "with no provable target this revision has nothing to be reset against"
        );

        // 反向：**自动**源不得靠这条出口推代际。
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "auto_reconcile_diff",
                reason: "same exposure",
                now_ms: 120,
            },
        )
        .await
        .expect("re-arming an already latched scope is idempotent");
        assert_eq!(
            generation_of(&store, scope_key).await,
            1,
            "an automatic source must not open a new revision through the refreeze door"
        );

        // 人工处置：敞口这时已经被对账重新发现，运维重新冻结这一轮。
        seed_exposure(&store, "bot-r20refreeze", "cred-r20real").await;
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "refreeze after an unprovable upgrade",
                now_ms: 130,
            },
        )
        .await
        .expect("an operator may refreeze a revision whose target set cannot be proven");
        assert_eq!(
            generation_of(&store, scope_key).await,
            2,
            "the manual refreeze must open a new revision"
        );
        assert_eq!(
            status_rows(&store, scope_key).await,
            vec![(
                "cred-r20real".to_string(),
                0,
                2,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "the refrozen revision carries a snapshot every row of which is provable; the \
             row whose origin could not be proven is gone from the judgement input"
        );
        // 审计不靠那一行留在判据表里：逐行结论与证据在复核表上。
        assert_eq!(
            revalidation_verdicts(&store, scope_key).await,
            vec![("cred-r20ghost".to_string(), "quarantined".to_string())],
            "the refreeze must not erase the per-row evidence of what it disposed of"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string()),
            "a refreeze must not write a single clean vote of its own"
        );
        // 处置完就恢复幂等：本代已经没有证不出来的行，窄门自己关上。
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "same exposure again",
                now_ms: 135,
            },
        )
        .await
        .expect("re-arming the refrozen scope is idempotent again");
        assert_eq!(
            generation_of(&store, scope_key).await,
            2,
            "the refreeze door must close once the revision's target set is provable"
        );

        // 新一轮照常收敛并可清。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20real", FLAT, 140)
                .await
                .expect("project the refrozen member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        reset(&store, scope_key, "operator", None, 150)
            .await
            .expect("the refrozen revision may be cleared once its member is flat");
        cleanup(store, path).await;
    }

    /// **反向：健康 scope 上的重复人工 arm 仍然是幂等的。**
    ///
    /// re-freeze 是一条**窄门**：只有"本代真的带着 quarantine 行"才开。
    /// 否则每一次重复 arm 都会凭空推一格代际，把上一轮的收敛结论抹掉
    /// （`latch_flatten_pending` 的说明里那条早返回正是为此存在的）。
    /// 少了这条反向断言，"永远重新冻结"这种过度处置在其余用例上一样全绿。
    #[tokio::test]
    async fn a_provable_revision_is_not_refrozen_by_a_repeated_manual_arm() {
        let (store, path) = test_store("r20-no-refreeze").await;
        let scope_key = "bot:bot-r20idem";
        seed_exposure(&store, "bot-r20idem", "cred-r20idem").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20idem", FLAT, 20)
                .await
                .expect("converge the revision"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );

        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "same exposure again",
                now_ms: 30,
            },
        )
        .await
        .expect("re-arming a healthy latched scope is idempotent");
        assert_eq!(
            generation_of(&store, scope_key).await,
            1,
            "a revision whose target set is fully provable must not be refrozen"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "the idempotent re-arm must not touch the convergence verdict"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ⑧ 第十六轮 §8.6（regression / -007）· reset 与聚合同源
    // -----------------------------------------------------------------------

    /// **regression（High）· `level=flatten` 的 reset 必须查 flatten 真值。**
    ///
    /// `docs/SAFETY_MODEL.md` 把 L3 的完成定义写成「强制 reconcile 同时证明
    /// **零仓位、零普通挂单、零 Algo 挂单与本地零差异**」——也就是
    /// `ReconcileTruth::flatten_complete()`。R19 的 reset 只要求
    /// `reconcile_clean`（`!has_diff && local_position_zero_diff`）：一份
    /// **本地与交易所一致地都还持着仓**的真值满足它。于是人工能把一条
    /// **一次都没平掉**的 flatten latch 直接清成 `none`，敞口连同它的重试
    /// 任务一起从系统里消失。
    ///
    /// 正反两侧：clean-but-not-flat 必拒；同一个成员随后报一份真的 flat
    /// 之后同一次 reset 必须放行（收紧不得误伤）。
    #[tokio::test]
    async fn a_clean_but_unflattened_l3_revision_may_not_be_reset() {
        let (store, path) = test_store("r20-clean-not-flat").await;
        let scope_key = "bot:bot-r20notflat";
        seed_exposure(&store, "bot-r20notflat", "cred-r20notflat").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");

        // 本地与交易所**一致**，可仓位还在：clean 成立、flat 不成立。
        const CLEAN_NOT_FLAT: ReconcileTruth = ReconcileTruth {
            has_diff: false,
            all_positions_zero: false,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };
        assert!(
            // 逐字复现 `record_reconcile_for_scope` 里的 per-credential clean 判据。
            !CLEAN_NOT_FLAT.has_diff
                && CLEAN_NOT_FLAT.local_position_zero_diff
                && !CLEAN_NOT_FLAT.flatten_complete(),
            "the fixture must really be clean-but-not-flat, otherwise the assertion is vacuous"
        );
        assert_eq!(
            record_reconcile_for_scope_now(
                &store,
                scope_key,
                "cred-r20notflat",
                CLEAN_NOT_FLAT,
                20
            )
            .await
            .expect("project the clean-but-not-flat truth"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_PENDING.to_string()),
            "the aggregate already knows this revision is clean but not flat"
        );
        let error = reset(&store, scope_key, "operator", None, 30)
            .await
            .expect_err("a latch whose exposure was never flattened must not be clearable");
        assert!(
            error.to_string().contains("completed flatten"),
            "the rejection must name the L3 completion definition: {error}"
        );
        assert!(
            in_retry_set(&store, scope_key).await,
            "a rejected reset must leave the scope in the retry set"
        );

        // 反向：真的平掉之后同一次 reset 必须放行。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20notflat", FLAT, 40)
                .await
                .expect("project the flat truth"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        // 第三面：逐行真值与 state 上那一位必须**同时**成立。两者由同一个
        // 事务、同一份行算出来，不一致本身就是代际错位——此时 reset 不得
        // 只信逐行那一半。
        sqlx::query("UPDATE kill_switch_state SET flatten_state=? WHERE scope_key=?")
            .bind(FLATTEN_PENDING)
            .bind(scope_key)
            .execute(store.pool())
            .await
            .expect("desynchronise the latch bit from the per-row truth");
        assert!(
            reset(&store, scope_key, "operator", None, 45)
                .await
                .is_err(),
            "a latch whose own flatten bit still says pending must not be cleared on the \
             per-row truth alone"
        );
        sqlx::query("UPDATE kill_switch_state SET flatten_state=? WHERE scope_key=?")
            .bind(FLATTEN_CONVERGED)
            .bind(scope_key)
            .execute(store.pool())
            .await
            .expect("restore the latch bit the aggregate wrote");

        reset(&store, scope_key, "operator", None, 50)
            .await
            .expect("a revision that proved a completed flatten may be cleared");
        cleanup(store, path).await;
    }

    /// **反向（收紧不得误伤）：非 flatten 档位不受 flatten 完成定义约束。**
    ///
    /// `manage_only` / `pause_new` 从来不承诺"敞口已经平掉"，它们的 reset
    /// 判据是"本 revision 已经被干净地对账过"。把 flatten 的完成定义套到
    /// 它们头上，会让每一个 L1/L2 latch 都清不掉。
    #[tokio::test]
    async fn a_clean_but_unflattened_l2_revision_may_still_be_reset() {
        let (store, path) = test_store("r20-l2-clean").await;
        let scope_key = "cred:cred-r20l2";
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::ManageOnly,
                trigger_source: "manual_api",
                reason: "l2 exposure",
                now_ms: 10,
            },
        )
        .await
        .expect("arm L2");
        const CLEAN_NOT_FLAT: ReconcileTruth = ReconcileTruth {
            has_diff: false,
            all_positions_zero: false,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20l2", CLEAN_NOT_FLAT, 20)
                .await
                .expect("project the clean-but-not-flat truth"),
            ReconcileProjection::Applied
        );
        reset(&store, scope_key, "operator", None, 30)
            .await
            .expect("an L2 latch does not owe a completed flatten");
        cleanup(store, path).await;
    }

    /// **regression（High）· reset 的过滤与聚合是同一个 expected predicate。**
    ///
    /// R19 的 reset 数的是本代**全部**行，聚合只数 expected 成员的行。
    /// 于是一条聚合明确排除掉的行——`legacy_unprovable`（R19 是
    /// `legacy_isolated`）、或任何一条本 revision 从未认下的 outsider dirty
    /// 行——让 `dirty_rows > 0` 恒成立：一个 latch 已经写成 `flattened`、
    /// 已经退出重试集合的 scope 被**永久**钉在"reset 拒绝"上。运行态说
    /// "平完了"，人工却清不掉它。
    ///
    /// 正向：两类不算数的行都不阻塞。反向：一个**真实 expected 成员**
    /// 报 dirty 时同一次 reset 仍然必须拒绝——同源不等于放水。
    #[tokio::test]
    async fn rows_the_aggregate_excludes_do_not_block_a_converged_scopes_reset() {
        let (store, path) = test_store("r20-reset-same-predicate").await;
        let scope_key = "bot:bot-r20same";
        seed_exposure(&store, "bot-r20same", "cred-r20same").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // 两类聚合不算数的同代 dirty 行。
        plant_status_row(&store, scope_key, "cred-r20stranger", 1, false, None).await;
        plant_status_row(
            &store,
            scope_key,
            "cred-r20quarantined",
            1,
            false,
            Some(TARGET_LEGACY_UNPROVABLE),
        )
        .await;

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20same", FLAT, 20)
                .await
                .expect("project the only real target"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "rows this revision never claimed must not keep it pending"
        );
        assert!(!in_retry_set(&store, scope_key).await);
        reset(&store, scope_key, "operator", None, 30)
            .await
            .expect("a scope the aggregate calls converged must be clearable by hand");

        // 反向：真实成员报 dirty 时 reset 仍然必须拒绝。
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "again", 40)
            .await
            .expect("latch a second round");
        const DIRTY: ReconcileTruth = ReconcileTruth {
            has_diff: true,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: false,
        };
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r20same", DIRTY, 50)
                .await
                .expect("project a dirty truth"),
            ReconcileProjection::Applied
        );
        assert!(
            reset(&store, scope_key, "operator", None, 60)
                .await
                .is_err(),
            "a real expected member that still reports a diff must keep blocking the reset"
        );
        cleanup(store, path).await;
    }
}

/// **quarantine 行的旧票不得复活，
/// 人工重新冻结不得制造死锁代。**
///
/// 三条缺陷同一个根：R20 把 `legacy_unprovable` 的语义写进了注释
/// （"不投票、不阻塞"）和一条**人工出口**（`scope_awaits_refreeze`），
/// 却没有把它们各自写进**判据**里：
///
/// | 缺陷 | R20 的形状 | 后果 |
/// | --- | --- | --- |
/// | regression（High） | `counted` 只问 `expected.contains(cid)` | quarantine credential 回归 live targets 时，**升级前**那张 clean/flat 旧票就地复活，可以用陈旧真值把当前代 flatten latch 收敛掉 |
/// | regression（Medium） | re-freeze 先删旧 snapshot、再按当前 target 冻结 | 当前 target 为空时冻出一个 `total = 0` 的代（永不收敛），quarantine 行同时被删光 ⇒ 出口自己消失（死锁） |
/// | regression（Medium） | 决策读用普通 SELECT、自增在另一条语句 | 两路并发人工 re-freeze 同读同判、各推一代（N→N+2），人工处置不幂等 |
///
/// 本模块钉 003 与 004（SQLite 可达）；005 的证据在
/// `r16_mysql_concurrency_tests::two_concurrent_manual_refreezes_advance_one_generation`
/// ——它要的是**真实 MySQL 行锁**，SQLite 的单连接造不出那个交错。
#[cfg(test)]
mod r21_convergence_tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;

    use super::{
        arm, latch_flatten_pending, record_reconcile_for_scope_now, reset,
        revalidate_legacy_reconcile_targets, scopes_needing_action_retry, ArmRequest, Level,
        ReconcileProjection, ReconcileTruth, FLATTEN_CONVERGED, FLATTEN_PENDING,
        TARGET_FROM_ARM_SNAPSHOT, TARGET_FROM_RECONCILE_DISCOVERY, TARGET_LEGACY_ISOLATED,
        TARGET_LEGACY_UNPROVABLE,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const FLAT: ReconcileTruth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    };

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-r21ks-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect kill-switch test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 一条本地敞口——`reconcile_targets_in_tx` 认成员的依据。
    async fn seed_exposure(store: &Store, bot_uid: &str, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', '1', '100',
              'open', 1, 1, 1)
            "#,
        )
        .bind(format!("pos_{bot_uid}_{credential_id}"))
        .bind(credential_id)
        .bind(bot_uid)
        .execute(store.pool())
        .await
        .expect("seed the exposure that makes this credential a target of the scope");
    }

    /// **一条 0028 之前由投影路径写下的行**：`reconciled_at_ms` 是真实墙钟，
    /// 因此它**不**带 arm 事务的冻结签名（见 `bears_arm_freeze_signature`），
    /// 启动期复核会把它判成 `quarantined`。
    async fn plant_projected_status_row(
        store: &Store,
        scope_key: &str,
        credential_id: &str,
        generation: i64,
        clean: bool,
        reconciled_at_ms: i64,
    ) {
        assert!(
            reconciled_at_ms > 0,
            "a row the projection wrote always carries a real wall clock; a zero here would \
             forge the arm transaction's freeze signature and make the fixture unreachable"
        );
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, ?, ?, ?, ?, ?, NULL)",
        )
        .bind(scope_key)
        .bind(credential_id)
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(reconciled_at_ms)
        .bind(generation)
        .execute(store.pool())
        .await
        .expect("plant a reconcile status row the pre-0028 projection could have written");
    }

    /// 直接塞一行带**指定 provenance** 的 status 行——**绕过**启动期复核，
    /// 复现"库里已经躺着一条 R19/R20 判过的行、进程这一趟没重启"的形状。
    async fn plant_status_row_with_provenance(
        store: &Store,
        scope_key: &str,
        credential_id: &str,
        generation: i64,
        clean: bool,
        provenance: &str,
    ) {
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, ?, ?, ?, 2100, ?, ?)",
        )
        .bind(scope_key)
        .bind(credential_id)
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(if clean { 1i64 } else { 0i64 })
        .bind(generation)
        .bind(provenance)
        .execute(store.pool())
        .await
        .expect("plant a reconcile status row with an explicit provenance");
    }

    /// 把本代的行退回 0028 之前的形状（来源被抹掉，其余原样）。
    async fn age_rows_to_pre_0028(store: &Store, scope_key: &str) {
        sqlx::query(
            "UPDATE kill_switch_reconcile_status SET expected_provenance=NULL WHERE scope_key=?",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("age the frozen rows back to the pre-0028 shape");
    }

    async fn generation_of(store: &Store, scope_key: &str) -> i64 {
        sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("read the flatten generation")
            .get::<i64, _>("flatten_generation")
    }

    async fn verdict(store: &Store, scope_key: &str) -> (i64, String) {
        let row = sqlx::query(
            "SELECT COALESCE(last_reconcile_clean, -1) AS clean, \
             COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch verdict");
        (
            row.get::<i64, _>("clean"),
            row.get::<String, _>("flatten_state"),
        )
    }

    /// `(credential_id, reconcile_clean, flatten_complete, expected_provenance)`，
    /// 只看本代。
    async fn status_rows(
        store: &Store,
        scope_key: &str,
        generation: i64,
    ) -> Vec<(String, i64, i64, String)> {
        sqlx::query(
            "SELECT credential_id, reconcile_clean, flatten_complete, \
             COALESCE(expected_provenance, '<null>') AS expected_provenance \
             FROM kill_switch_reconcile_status WHERE scope_key=? AND flatten_generation=? \
             ORDER BY credential_id",
        )
        .bind(scope_key)
        .bind(generation)
        .fetch_all(store.pool())
        .await
        .expect("read the reconcile status rows")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("credential_id"),
                row.get::<i64, _>("reconcile_clean"),
                row.get::<i64, _>("flatten_complete"),
                row.get::<String, _>("expected_provenance"),
            )
        })
        .collect()
    }

    async fn arm_event_count(store: &Store, scope_key: &str) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("count the audit events")
            .get::<i64, _>("n")
    }

    async fn in_retry_set(store: &Store, scope_key: &str) -> bool {
        scopes_needing_action_retry(store)
            .await
            .expect("scan the retry set")
            .into_iter()
            .any(|(key, _)| key == scope_key)
    }

    // -----------------------------------------------------------------------
    // ① regression（High）· quarantine 旧票不得随 credential 回归 live 复活
    // -----------------------------------------------------------------------

    /// **缺陷回归：quarantine credential 回归 live targets 时，旧 clean/flat
    /// 票不得替当前这一代投赞成票。**
    ///
    /// R20 的 `counted` 谓词只有一句 `expected.contains(cid)`，而 `expected`
    /// 是「arm 冻结的成员 **∪ 此刻 `reconcile_targets_in_tx` 认得的成员**」。
    /// 第二项与 provenance 毫无关系——于是一条被 quarantine 的行只要它的
    /// credential **重新变成 live 成员**（敞口回来了、孤儿仓位被对账记回
    /// `position_state`……），就带着**升级之前**那张来源证不出来的
    /// clean/flat 票被数进 `total` / `dirty` / `incomplete`。
    ///
    /// 这条路径的终点是交易真值错误：本代的另一个成员报一份 flat 之后，
    /// 聚合看到"两个 expected 成员都 clean 且都 flat"，把 latch 写成
    /// `flattened` 并让它退出 `scopes_needing_action_retry` 的重试集合——
    /// 而 quarantine credential 那个**刚刚回来的真实敞口**一次都没被平过，
    /// 一次都没被对账过（engine safety contract「对账失败后继续交易」一律 High）。
    ///
    /// 反向（不得过度收紧）同在这条用例里：quarantine credential 报出一份
    /// **本代的新真值**之后，它必须重新参与收敛——投影会把它的 provenance
    /// 从 quarantine 提升成 `reconcile_discovery`，这一轮随即收敛并可以被
    /// 人工清掉。排除旧票不等于把这个 credential 永久钉死。
    #[tokio::test]
    async fn a_quarantined_credential_back_in_live_targets_does_not_revive_its_old_vote() {
        let (store, path) = test_store("r21-revive").await;
        let scope_key = "bot:bot-r21revive";
        seed_exposure(&store, "bot-r21revive", "cred-r21fresh").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // 0028 之前的库：本代成员行的来源被抹掉；另有一条投影路径写得出的
        // clean/flat 行，它的 credential 此刻不是成员。
        age_rows_to_pre_0028(&store, scope_key).await;
        plant_projected_status_row(&store, scope_key, "cred-r21ghost", 1, true, 2_100).await;
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy rows"),
            2
        );
        assert_eq!(
            status_rows(&store, scope_key, 1).await,
            vec![
                (
                    "cred-r21fresh".to_string(),
                    0,
                    0,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r21ghost".to_string(),
                    1,
                    1,
                    TARGET_LEGACY_UNPROVABLE.to_string()
                ),
            ],
            "the fixture must really carry a quarantined row whose old clean/flat vote is \
             still on it — that vote is the thing under test"
        );

        // **敞口回来了**：这个 credential 重新成为 scope 的 live 成员。
        seed_exposure(&store, "bot-r21revive", "cred-r21ghost").await;

        // 本代真正的成员报一份 flat。缺陷版本在这一步就把整轮收敛掉。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r21fresh", FLAT, 200)
                .await
                .expect("project the provable member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "a stale vote from before the upgrade must not converge this generation"
        );
        assert!(
            in_retry_set(&store, scope_key).await,
            "the scope must stay in the retry set until the returned exposure is really flat"
        );
        // 人工也清不掉它。第一道拦住它的是 `last_reconcile_clean`——那一位
        // 正是上面那次聚合按同一份行写下的 0，也就是"这一代还没收敛"这件事
        // 本身；缺陷版本里它会是 1，`reset` 会一路放行到底。
        let error = reset(&store, scope_key, "operator", None, 210)
            .await
            .expect_err("a revision one of whose targets never reported must not be clearable");
        assert!(
            error.to_string().contains("reset rejected"),
            "the rejection must be the reset gate, not an unrelated failure: {error}"
        );

        // 反向：本代的**新**真值到来之后，它照常参与收敛。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r21ghost", FLAT, 220)
                .await
                .expect("project the returned exposure's own fresh truth"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            status_rows(&store, scope_key, 1).await,
            vec![
                (
                    "cred-r21fresh".to_string(),
                    1,
                    1,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
                (
                    "cred-r21ghost".to_string(),
                    1,
                    1,
                    TARGET_FROM_RECONCILE_DISCOVERY.to_string()
                ),
            ],
            "a fresh membership-gated truth lifts the row out of quarantine so that it votes \
             on its own evidence instead of on the vote it carried in"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "excluding the stale vote must not pin the credential out of convergence forever"
        );
        assert!(!in_retry_set(&store, scope_key).await);
        reset(&store, scope_key, "operator", None, 230)
            .await
            .expect("a revision every target of which proved a fresh flat may be cleared");
        cleanup(store, path).await;
    }

    /// **`legacy_isolated` 与 `legacy_unprovable` 必须走同一条判据。**
    ///
    /// `is_unprovable()` 把两个值列在一起，但 R21 收工时只有
    /// `legacy_unprovable` 有行为级用例——于是"把 `legacy_isolated` 从
    /// quarantine 集合里漏掉"这种收紧面缺陷可以在其余用例上全绿溜走
    /// （变异 R21M04 实测存活，本用例即为补这条缺口而写）。
    ///
    /// `legacy_isolated` 是 R19 那一版复核写下的值。启动期复核会重判它
    /// （`needs_revalidation`），但一个**没有重启**的进程完全可能在重判
    /// 之前就跑到这条投影路径上——判据不得依赖"复核一定先跑过"。
    #[tokio::test]
    async fn a_legacy_isolated_row_is_judged_exactly_like_a_quarantined_one() {
        let (store, path) = test_store("r21-isolated").await;
        let scope_key = "bot:bot-r21isolated";
        seed_exposure(&store, "bot-r21isolated", "cred-r21iso-member").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        // R19 判过的行，带着它那张来源证不出来的 clean/flat 票。
        plant_status_row_with_provenance(
            &store,
            scope_key,
            "cred-r21iso-ghost",
            1,
            true,
            TARGET_LEGACY_ISOLATED,
        )
        .await;
        // 敞口回来了：这个 credential 重新是 live 成员。
        seed_exposure(&store, "bot-r21isolated", "cred-r21iso-ghost").await;

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r21iso-member", FLAT, 200)
                .await
                .expect("project the frozen member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (0, FLATTEN_PENDING.to_string()),
            "an R19-era isolated row's stale vote must not converge this generation either"
        );
        assert!(in_retry_set(&store, scope_key).await);

        // 反向：它自己的新真值到来之后照常参与收敛。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r21iso-ghost", FLAT, 210)
                .await
                .expect("project the returned exposure's own fresh truth"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            status_rows(&store, scope_key, 1).await,
            vec![
                (
                    "cred-r21iso-ghost".to_string(),
                    1,
                    1,
                    TARGET_FROM_RECONCILE_DISCOVERY.to_string()
                ),
                (
                    "cred-r21iso-member".to_string(),
                    1,
                    1,
                    TARGET_FROM_ARM_SNAPSHOT.to_string()
                ),
            ],
            "a fresh truth must lift an R19-era isolated row out of quarantine too"
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        reset(&store, scope_key, "operator", None, 220)
            .await
            .expect("the revision may be cleared once both targets proved a fresh flat");
        cleanup(store, path).await;
    }

    /// **反向：quarantine 行在它**不是** live 成员时的 R20 语义逐字不变
    /// ——既不投票，也不阻塞。**
    ///
    /// 排除旧票有一条显而易见的过度收紧写法：把 quarantine 行直接算成
    /// "缺席的成员"（进 `unreported`）。那会让每一个带 quarantine 行的
    /// scope 永久收敛不了、永久 reset 不了——正是第十六轮 §8.6
    /// （regression）刚刚修掉的那种"运行态说平完了、人工却清不掉"。
    ///
    /// 判据的边界因此是 `expected`：quarantine 行只有在它的 credential
    /// **此刻确实是成员**时才会落进 `unreported`（那时它确实欠一份真值）；
    /// 不是成员时它连 `expected` 都进不去，一格都不算。
    #[tokio::test]
    async fn a_quarantined_row_outside_live_targets_still_neither_votes_nor_blocks() {
        let (store, path) = test_store("r21-quarantine-inert").await;
        let scope_key = "bot:bot-r21inert";
        seed_exposure(&store, "bot-r21inert", "cred-r21member").await;
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch the scope");
        age_rows_to_pre_0028(&store, scope_key).await;
        // 一条 clean 的、一条 dirty 的 quarantine 行，两个 credential 都不是成员。
        plant_projected_status_row(&store, scope_key, "cred-r21inert-clean", 1, true, 2_100).await;
        plant_projected_status_row(&store, scope_key, "cred-r21inert-dirty", 1, false, 2_200).await;
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy rows"),
            3
        );

        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r21member", FLAT, 200)
                .await
                .expect("project the only real member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string()),
            "a quarantined row outside live targets must not keep a converged revision pending"
        );
        assert!(!in_retry_set(&store, scope_key).await);
        reset(&store, scope_key, "operator", None, 210)
            .await
            .expect("a quarantined row outside live targets must not block a manual reset");
        cleanup(store, path).await;
    }

    /// **反向：把行"提升出 quarantine"这条 upsert 分支必须是**窄**的。**
    ///
    /// 它只允许改动 `legacy_unprovable` / `legacy_isolated` 两个值。写成
    /// 无条件覆盖会同时干两件坏事：把 arm 冻结的成员降级成
    /// `reconcile_discovery`（frozen 集合从此漏成员，敞口消失的那种真实
    /// target 又会被拒在外面——regression 复发），以及把 0028 的存量
    /// `NULL` 行"洗白"成一条来源可证明的行，绕开启动期复核。
    #[tokio::test]
    async fn a_fresh_truth_promotes_only_quarantined_rows() {
        let (store, path) = test_store("r21-promotion-narrow").await;

        // ① arm 冻结的成员报真值之后仍然是 `arm_snapshot`。
        let frozen_scope = "bot:bot-r21frozen";
        seed_exposure(&store, "bot-r21frozen", "cred-r21frozen").await;
        latch_flatten_pending(
            &store,
            frozen_scope,
            "auto_protection_timeout",
            "exposure",
            10,
        )
        .await
        .expect("latch the frozen scope");
        assert_eq!(
            record_reconcile_for_scope_now(&store, frozen_scope, "cred-r21frozen", FLAT, 20)
                .await
                .expect("project the frozen member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            status_rows(&store, frozen_scope, 1).await,
            vec![(
                "cred-r21frozen".to_string(),
                1,
                1,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "reporting a truth must never downgrade a frozen member's provenance"
        );

        // ② 0028 的存量 `NULL` 行报真值之后仍然是 `NULL`（等启动期复核）。
        let legacy_scope = "bot:bot-r21legacy";
        seed_exposure(&store, "bot-r21legacy", "cred-r21legacy").await;
        latch_flatten_pending(
            &store,
            legacy_scope,
            "auto_protection_timeout",
            "exposure",
            10,
        )
        .await
        .expect("latch the legacy scope");
        age_rows_to_pre_0028(&store, legacy_scope).await;
        assert_eq!(
            record_reconcile_for_scope_now(&store, legacy_scope, "cred-r21legacy", FLAT, 20)
                .await
                .expect("a live target may report even while its provenance is unknown"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            status_rows(&store, legacy_scope, 1).await,
            vec![("cred-r21legacy".to_string(), 1, 1, "<null>".to_string())],
            "a pre-0028 row must not be whitewashed into a provable one by being projected"
        );
        cleanup(store, path).await;
    }

    // -----------------------------------------------------------------------
    // ② regression（Medium）· 零目标 re-freeze 必须 fail-closed
    // -----------------------------------------------------------------------

    /// **缺陷回归：当前 target 为空时的人工 re-freeze 必须被拒，
    /// 而且拒完之后那条出口还在。**
    ///
    /// R20 的 re-freeze 走的是 `arm` 的普通路径：代际 +1，然后
    /// `reset_reconcile_targets_in_tx` **先 `DELETE` 掉本 scope 的全部
    /// status 行**、再按 `reconcile_targets_in_tx` 此刻的 target 重新冻结。
    /// 当前 target 为空时这两步合起来是一个死锁代：
    ///
    /// * 新代一行都没有 ⇒ `complete_membership()`（`total > 0`）恒假 ⇒
    ///   这一代永远收敛不了，`reset` 也永远拒绝；
    /// * quarantine 行——`scope_awaits_refreeze` 唯一的触发条件——刚刚被
    ///   同一步删光 ⇒ 下一次人工 arm 落回档位幂等早返回，**再也开不出
    ///   新一轮**。出口把出口自己堵死了。
    ///
    /// 现在这一刻在写任何字节之前就被拒绝，并如实说明原因；旧 snapshot
    /// 与 quarantine 行原样留着，出口仍然可用。反向在同一条用例里：
    /// 敞口回来之后，**同一发**人工 arm 必须照常重新冻结（fail-closed
    /// 不得退化成"永远不许重新冻结"）。
    #[tokio::test]
    async fn a_refreeze_with_no_current_targets_is_refused_and_keeps_the_door_open() {
        let (store, path) = test_store("r21-empty-refreeze").await;
        let scope_key = "bot:bot-r21empty";
        latch_flatten_pending(&store, scope_key, "auto_protection_timeout", "exposure", 10)
            .await
            .expect("latch a scope that has no local exposure of its own");
        assert!(
            status_rows(&store, scope_key, 1).await.is_empty(),
            "the fixture must start from an empty frozen target set"
        );
        plant_projected_status_row(&store, scope_key, "cred-r21ghost", 1, true, 2_100).await;
        assert_eq!(
            revalidate_legacy_reconcile_targets(&store, 100)
                .await
                .expect("revalidate the legacy row"),
            1
        );
        let quarantined = vec![(
            "cred-r21ghost".to_string(),
            1,
            1,
            TARGET_LEGACY_UNPROVABLE.to_string(),
        )];
        assert_eq!(status_rows(&store, scope_key, 1).await, quarantined);
        let events_before = arm_event_count(&store, scope_key).await;

        // 人工重新冻结，而此刻这个 scope 一个 target 都没有。
        let error = arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "refreeze while the scope has no targets",
                now_ms: 110,
            },
        )
        .await
        .expect_err("refreezing an empty target set must be refused, not silently performed");
        assert!(
            error.to_string().contains("no target credentials"),
            "the refusal must say what is missing: {error}"
        );
        assert_eq!(
            generation_of(&store, scope_key).await,
            1,
            "a refused refreeze must not advance the generation"
        );
        assert_eq!(
            status_rows(&store, scope_key, 1).await,
            quarantined,
            "a refused refreeze must not drop the existing snapshot — deleting it is what \
             makes the generation unrecoverable"
        );
        assert_eq!(
            arm_event_count(&store, scope_key).await,
            events_before,
            "a refused refreeze must not write an audit event of its own"
        );

        // 反向：敞口回来之后，同一发人工 arm 必须照常重新冻结。
        seed_exposure(&store, "bot-r21empty", "cred-r21real").await;
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "refreeze once the scope has a provable target again",
                now_ms: 120,
            },
        )
        .await
        .expect("the refreeze door must still be open after a refusal");
        assert_eq!(
            generation_of(&store, scope_key).await,
            2,
            "the manual refreeze must open a new revision once there is something to freeze"
        );
        assert_eq!(
            status_rows(&store, scope_key, 2).await,
            vec![(
                "cred-r21real".to_string(),
                0,
                0,
                TARGET_FROM_ARM_SNAPSHOT.to_string()
            )],
            "the refrozen revision must carry a provable, unconverged snapshot"
        );

        // 新一轮照常收敛并可清。
        assert_eq!(
            record_reconcile_for_scope_now(&store, scope_key, "cred-r21real", FLAT, 130)
                .await
                .expect("project the refrozen member"),
            ReconcileProjection::Applied
        );
        assert_eq!(
            verdict(&store, scope_key).await,
            (1, FLATTEN_CONVERGED.to_string())
        );
        reset(&store, scope_key, "operator", None, 140)
            .await
            .expect("the refrozen revision may be cleared once its member is flat");
        cleanup(store, path).await;
    }

    /// **反向：零目标判据只挂在 re-freeze 那条窄门上。**
    ///
    /// 把"当前 target 为空就拒绝"抬到 `arm` 的入口，会让每一次对**还没有
    /// 任何本地敞口**的 scope 上闩都失败——那是 kill switch 最要紧的一种
    /// 用法（先闩住，再让对账去发现敞口；`bot:` scope 在第一笔单落库之前
    /// `reconcile_targets_in_tx` 恒为空集）。
    #[tokio::test]
    async fn arming_a_scope_that_has_no_targets_yet_is_still_allowed() {
        let (store, path) = test_store("r21-first-arm-empty").await;
        let scope_key = "bot:bot-r21firstarm";

        // 首次 arm：一个 target 都没有，照样必须闩上。
        arm(
            &store,
            ArmRequest {
                scope_key,
                level: Level::Flatten,
                trigger_source: "manual_api",
                reason: "latch before the exposure is even discovered",
                now_ms: 10,
            },
        )
        .await
        .expect("a scope with no discovered targets must still be latchable");
        assert_eq!(generation_of(&store, scope_key).await, 1);
        assert_eq!(
            verdict(&store, scope_key).await,
            (-1, FLATTEN_PENDING.to_string())
        );
        assert!(in_retry_set(&store, scope_key).await);

        // 自动源的强制重开一轮同样不受这条判据约束。
        latch_flatten_pending(
            &store,
            scope_key,
            "auto_protection_timeout",
            "new exposure",
            20,
        )
        .await
        .expect("a fresh naked exposure must always be able to reopen the latch");
        assert_eq!(
            generation_of(&store, scope_key).await,
            2,
            "the zero-target guard must not stop a new exposure from opening a new round"
        );
        cleanup(store, path).await;
    }
}
