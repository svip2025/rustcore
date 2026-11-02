use std::sync::Arc;

use anyhow::Context;

use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{Any, Pool, Row, TypeInfo, ValueRef};

// SQLx Any/MySQL empty-result invariant (High).
//
// In sqlx 0.7.4, MySQL's Any backend inspected only the first result-stream
// item in `fetch_optional`. An empty SELECT first yields a QueryResult marker,
// so the method returned None without draining the stream. Inside a transaction
// the next statement then hung; through a Pool the on-release ping hung while
// retaining its permit. `Executor::fetch_one` delegated to the same broken
// method, so changing only explicit `fetch_optional` call sites was incomplete.
//
// This crate therefore pins sqlx exactly to 0.8.6. That version loops to a row
// or EOF and covers both Pool and Transaction executors, including `fetch_one`.
// `sqlx_any_empty_result_tests` exercises both shapes against real MySQL with a
// one-connection pool. Existing `fetch_all(...).into_iter().next()` call sites
// remain valid defense in depth, but no longer define the repository-wide fix.
// Do not lower the pin or remove those driver-contract tests.

pub(crate) fn try_get_nullable_string(
    row: &AnyRow,
    column: &str,
) -> Result<Option<String>, sqlx::Error> {
    if row
        .try_get_raw(column)?
        .type_info()
        .name()
        .eq_ignore_ascii_case("NULL")
    {
        Ok(None)
    } else {
        row.try_get::<Option<String>, _>(column)
    }
}

/// **读 `TEXT` 列**。
///
/// MySQL 把 `TEXT` 列（以及任何最大长度超出 `VARCHAR` 的表达式，含
/// `CAST(text_col AS CHAR)`）按 `MYSQL_TYPE_BLOB` 返回，`sqlx::Any` 于是把它
/// 映射成 `Blob`，`row.get::<String, _>(col)` 必然报
/// 「Rust type `String` is not compatible with SQL type `BLOB`」。SQLite 上同一
/// 列是 TEXT，直接取 `String` 又是对的。两个后端共用一份 SQL 时只能在读侧
/// 归一：先按 `String` 取，失败再按字节取并**严格**按 UTF-8 解码——
/// 不用 `from_utf8_lossy`，因为这些列（`kill_switch_state.reason` 等）会参与
/// 身份比对，静默替换成 U+FFFD 等于把损坏的数据当成正常数据用
/// （engine safety contract）。
pub(crate) fn try_get_text(row: &AnyRow, column: &str) -> Result<Option<String>, sqlx::Error> {
    if row
        .try_get_raw(column)?
        .type_info()
        .name()
        .eq_ignore_ascii_case("NULL")
    {
        return Ok(None);
    }
    match row.try_get::<Option<String>, _>(column) {
        Ok(value) => Ok(value),
        Err(_) => match row.try_get::<Option<Vec<u8>>, _>(column)? {
            None => Ok(None),
            Some(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|error| sqlx::Error::Decode(Box::new(error))),
        },
    }
}

/// 读可空的 BLOB 列（如 `engine_order_events.payload_json`）。
/// 与 `try_get_nullable_string` 同形：`sqlx::Any` 在 NULL 上会把类型信息报成
/// `NULL`，直接 `try_get::<Option<Vec<u8>>>` 在部分驱动上会报解码错误。
pub(crate) fn try_get_nullable_bytes(
    row: &AnyRow,
    column: &str,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    if row
        .try_get_raw(column)?
        .type_info()
        .name()
        .eq_ignore_ascii_case("NULL")
    {
        return Ok(None);
    }
    match row.try_get::<Option<Vec<u8>>, _>(column) {
        Ok(value) => Ok(value),
        // SQLite 会把 TEXT 亲和的列取成字符串；退回按字符串读再转字节。
        Err(_) => Ok(row
            .try_get::<Option<String>, _>(column)?
            .map(|text| text.into_bytes())),
    }
}

/// **严格读取显式文本投影中的 `BIGINT`**。
///
/// 旧的 sqlx 0.7.4 Any/SQLite 会在建行时把宽整数截成 `i32`，所以仓内历史
/// 查询把关键 epoch / trade id 写成 `CAST(col AS CHAR) AS col_text`。精确锁定的
/// sqlx 0.8.6 已修复直接 `i64` 解码；`sqlx_any_bigint_width_tests` 同时守住
/// SQLite 与 MySQL。现有文本投影仍保持正确，先保留它们以免把驱动升级与约
/// 50 处业务 SQL 改写混成一个不可审计批次，后续可单独机械清理。
///
/// **严格**：解析不出来一律上抛 `sqlx::Error::Decode`，不折成 `0`。
/// 静默折 `0` 正是 R12 regression 钉死的那类「编一行真值」——
/// `now_ms - 0` 必然超期，一条读不出来的活单会被裁决成 `failed`。
pub(crate) fn try_get_optional_i64_text(
    row: &AnyRow,
    column: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let Some(text) = try_get_text(row, column)? else {
        return Ok(None);
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed.parse::<i64>().map(Some).map_err(|error| {
        sqlx::Error::Decode(
            format!("column {column} is not a base-10 i64 ({trimmed:?}): {error}").into(),
        )
    })
}

/// `try_get_optional_i64_text` 的 NOT NULL 版：列为 NULL / 空串即解码错误。
pub(crate) fn try_get_i64_text(row: &AnyRow, column: &str) -> Result<i64, sqlx::Error> {
    try_get_optional_i64_text(row, column)?.ok_or_else(|| {
        sqlx::Error::Decode(
            format!("column {column} is NULL but was read as a NOT NULL i64").into(),
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Sqlite,
    MySql,
    Unknown,
}

#[derive(Clone)]
pub struct Store {
    pool: Arc<Pool<Any>>,
    kind: DbKind,
    /// P4b：本进程的启动周期 ID（FA C1.2）。会员解除准入只在
    /// `admitted_boot_id == boot_id` 时有效；每次启动无条件失效。
    boot_id: String,
}

/// 仓内那份迁移目录（`sqlx::migrate!` 在编译期嵌进二进制）。
///
/// 抽成函数是为了让 re-entry 逻辑与用例都拿到**同一份** `Migration`——
/// 版本号、描述与 sha384 checksum 全都来自这里，没有第二份手抄的常量
/// 可以走样（写错 checksum 的账本行会让 sqlx 下一次启动直接
/// `VersionMismatch`）。
pub(crate) fn embedded_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

/// 一列的**结构身份**：基础类型、声明长度（`VARCHAR` 才有）、可空性。
///
/// 。R17 的效果探针只问
/// "这个名字在不在目录里"，于是一张**同名但结构错**的表/列会被判成
/// "这一段已经跑过"、直接跳过并**落章**：这个部署从此带着一个没人验证过的
/// schema 继续跑，而账本声称 0027 已成功（engine safety contract）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlKind {
    BigInt,
    VarChar(i64),
    Text,
    // P4b（0036/0037）：归属/准入列的整数编码。整数比较在任何 collation
    // 下都字节精确，这是「不含 COLLATE」之外真正承担字节语义的机制
    // （r2 §7：不以省略 COLLATE 冒充双库已解决）。
    SmallInt,
    Int,
}

impl SqlKind {
    /// 后端上报的基础类型名（两个后端都归一成小写、去掉 `(n)` 后缀）。
    fn base_type(self) -> &'static str {
        match self {
            Self::BigInt => "bigint",
            Self::VarChar(_) => "varchar",
            Self::Text => "text",
            Self::SmallInt => "smallint",
            Self::Int => "int",
        }
    }

    fn declared_length(self) -> Option<i64> {
        match self {
            Self::VarChar(length) => Some(length),
            _ => None,
        }
    }
}

/// 一列的期望结构。
#[derive(Debug, Clone, Copy)]
struct ColumnSpec {
    name: &'static str,
    kind: SqlKind,
    nullable: bool,
}

/// 后端目录里**实际**读到的一列。
#[derive(Debug, Clone)]
struct ObservedColumn {
    name: String,
    base_type: String,
    max_length: Option<i64>,
    nullable: bool,
}

impl ObservedColumn {
    /// 与期望逐项比对；不符时返回一句**说得出差在哪**的话。
    fn mismatch_against(&self, spec: &ColumnSpec) -> Option<String> {
        if !self.base_type.eq_ignore_ascii_case(spec.kind.base_type()) {
            return Some(format!(
                "column {} has type {} but this build expects {}",
                spec.name,
                self.base_type,
                spec.kind.base_type()
            ));
        }
        if let (Some(expected), Some(observed)) = (spec.kind.declared_length(), self.max_length) {
            if expected != observed {
                return Some(format!(
                    "column {} is declared with length {observed} but this build expects \
                     {expected}",
                    spec.name
                ));
            }
        }
        if self.nullable != spec.nullable {
            let observed = if self.nullable {
                "nullable"
            } else {
                "not null"
            };
            let expected = if spec.nullable {
                "nullable"
            } else {
                "not null"
            };
            return Some(format!(
                "column {} is {observed} but this build expects it {expected}",
                spec.name
            ));
        }
        None
    }
}

/// 一段 DDL 留下的**效果**——re-entry 判"这一段跑过没有"的依据。
///
/// 判据不是"把那条语句再跑一遍看它报不报错"：一次连接故障、一次权限问题
/// 都会被那种写法读成"没跑过"，然后去补一段其实已经在的 DDL。这里问的是
/// 后端的**目录**（MySQL 的 `information_schema`、SQLite 的 `sqlite_master` /
/// `pragma_table_info`），拿到的是一份**结构**，不是一个错误。
///
/// **名字在不等于结构对。**
/// R17 这里只比对表名/列名是否存在。一个同名错误结构（少 PK、列类型不对、
/// 该 NOT NULL 的可空）会让探针报"已在"，于是那一段被跳过、账本被落章。
/// 之后 `revalidate_legacy_event_generations` 往
/// `kill_switch_event_generation_revalidations` 写复核证据时才会撞上真相
/// ——而那时 kill-switch 的代际复核已经在启动路径上，整个部署起不来，
/// 却没有任何东西说得出"是 0027 被错误地 adopt 了"。
///
/// 现在两类效果都带**期望结构**：列比类型/长度/可空性，表还要比 PK 的
/// 列与顺序。名字在但结构不符时不是 `Absent`（那会去补跑一段必然撞
/// "already exists" 的 DDL），而是一条**独立的 fail-closed 结论**：拒绝落章。
enum DdlEffect {
    /// 0042 is a bounded MySQL 16 -> 32 widening, not a general column repair.
    /// SQLite ignores the executable comment and keeps its original declaration.
    MySqlOrderTypeWidth,
    Table {
        table: &'static str,
        columns: &'static [ColumnSpec],
        primary_key: &'static [&'static str],
    },
    Column {
        table: &'static str,
        column: ColumnSpec,
    },
    /// **P4b（TG-1 · Lead 20260925 工程确认）· 0037 的固定禁用种子行。**
    ///
    /// 迁移 0037 的最后一段是 `INSERT INTO member_reset_admission (1,0,…)`
    /// ——一段**有界 DML**：它只初始化本迁移自己定义的那一行，不补任何
    /// 历史业务行。种子没有执行就不能把该版本落章成功（初始化行未完成
    /// ≠迁移成功）；而种子的补执行也绝不能覆盖一行已存在的
    /// enabled/admission_version（那是一份准入状态，不是 schema）。
    ///
    /// 探针判据：表存在的前提下，`id=1` 行缺席 ⇒ `Absent`（补跑那一条
    /// INSERT）；行存在且 `enabled=0 ∧ admitted_boot_id=''` ⇒ `Present`
    /// （与种子逐值一致；admission_version 不参与判据——半执行的原始窗口
    /// 里它只可能是 0，后来被运营推号说明迁移早已成功落章，不会走到这里）；
    /// 其余（enabled=1、非空 admitted_boot_id、或多行）⇒ `Mismatch`，
    /// 停机不猜（engine safety contract）。
    SeedRow { table: &'static str },
    /// **M0fix（评审 claude-review-m0 P2-4）· 一条普通（非唯一）索引。**
    ///
    /// 0044 是第一个带 `CREATE INDEX` 的登记迁移。索引是独立的一段 DDL
    /// （MySQL 上各自隐式提交），必须有自己的效果探针：只探表会把「表在、
    /// 索引缺」的断点判成已完成并落章；整份文件当一段补跑又会撞 MySQL
    /// 拒绝多语句 PREPARE（sqlx 带参 `query` 一律预处理）。
    ///
    /// 探针判据：表必须在（否则这段的前提不成立 ⇒ `Mismatch`）；同名索引
    /// 缺席 ⇒ `Absent`；在且为非唯一索引、列与顺序逐项一致 ⇒ `Present`；
    /// 其余 ⇒ `Mismatch`。
    Index {
        table: &'static str,
        index: &'static str,
        columns: &'static [&'static str],
    },
    /// **M0fix · 被后续登记迁移追加过列的表。**
    ///
    /// 0044 建的义务表从 0045 起多一列 `close_revision`。0044 自身的崩溃
    /// 重入只可能发生在 0045 之前（sqlx 逐版本推进、dirty 即停机），但
    /// 「每段效果都与全新安装一致」的对账拿的是整条迁移链之后的 schema。
    /// 判据：原始列逐项符合；多出来的列要么一列没有，要么**恰好**是登记的
    /// 后续列（同样逐项符合）；PK 不变。任何别的差异仍是 `Mismatch`。
    TableLaterExtended {
        table: &'static str,
        columns: &'static [ColumnSpec],
        later_columns: &'static [ColumnSpec],
        primary_key: &'static [&'static str],
    },
}

impl DdlEffect {
    fn table(&self) -> &'static str {
        match self {
            Self::MySqlOrderTypeWidth => "engine_orders",
            Self::Table { table, .. } => table,
            Self::Column { table, .. } => table,
            Self::SeedRow { table } => table,
            Self::Index { table, .. } => table,
            Self::TableLaterExtended { table, .. } => table,
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::MySqlOrderTypeWidth => {
                "MySQL engine_orders.order_type VARCHAR(32) NOT NULL".into()
            }
            Self::Table { table, .. } => format!("table {table}"),
            Self::Column { table, column } => format!("column {table}.{}", column.name),
            Self::SeedRow { table } => format!("seed row {table}(id=1)"),
            Self::Index { table, index, .. } => format!("index {index} on {table}"),
            Self::TableLaterExtended { table, .. } => format!("table {table}"),
        }
    }
}

/// 一次结构探测的三种结论。
#[derive(Debug, Clone, PartialEq, Eq)]
enum DdlEffectState {
    /// 目录里没有这个对象：这一段还没跑过，补跑它。
    Absent,
    /// 目录里的对象**逐项符合**本次构建的期望：这一段跑过了。
    Present,
    /// 名字在、结构不符：**不许猜**，也不许补跑（会撞 already exists）。
    Mismatch(String),
}

/// 一段 DDL 的**效果 + 补跑语句**。
pub(crate) struct DdlStep {
    effect: DdlEffect,
    #[cfg(test)]
    pub(crate) apply: &'static str,
    #[cfg(not(test))]
    apply: &'static str,
}

/// 0027 那张复核证据表的期望结构（与 `migrations/0027_*.sql` 逐列同源）。
///
/// 这份清单同时被 `every_registered_reentry_column_matches_a_fresh_install`
/// 拿去与**全新安装出来的真实 schema** 对账——手抄一份期望是第二份实现，
/// 它和迁移文件漂移时探针会开始放行错误结构，而那正是 §9.6 要拦的东西。
const REVALIDATION_TABLE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "scope_key",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
    ColumnSpec {
        name: "event_id",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "previous_generation",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "resolved_generation",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "state_generation",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "verdict",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "evidence",
        kind: SqlKind::Text,
        nullable: false,
    },
    ColumnSpec {
        name: "revalidated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];

/// 需要 re-entry 保护的**多段 DDL** 迁移。
///
/// 只登记确实由**多条**语句组成、因而可能半执行的那些；单条语句的迁移在
/// 两个后端上都是原子的（MySQL 的单条 DDL 自己就是一次隐式提交）。
///
/// **不在这张表里的 dirty 版本一律 fail-closed**：我们没有它的效果判据，
/// 猜等于拿一个半成品的 schema 继续跑（engine safety contract）。
/// 0028 那张复核证据表的期望结构（与 `migrations/0028_*.sql` 逐列同源）。
const RECONCILE_TARGET_REVALIDATION_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "scope_key",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
    ColumnSpec {
        name: "credential_id",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
    ColumnSpec {
        name: "generation",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "verdict",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "evidence",
        kind: SqlKind::Text,
        nullable: false,
    },
    ColumnSpec {
        name: "revalidated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];

/// 0029 那张复核证据表的期望结构（与 `migrations/0029_*.sql` 逐列同源）。
const CREDENTIAL_RECONCILE_DIFF_REVALIDATION_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "credential_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "verdict",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "evidence",
        kind: SqlKind::Text,
        nullable: false,
    },
    ColumnSpec {
        name: "revalidated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];

const PROTECTION_DISPATCH_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "attempt_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "engine_order_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "protection_order_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "owner_token",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "binding_json",
        kind: SqlKind::Text,
        nullable: false,
    },
    ColumnSpec {
        name: "revision",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "authorized_post_count",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "phase",
        kind: SqlKind::VarChar(16),
        nullable: false,
    },
    ColumnSpec {
        name: "dispatch_closed",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "sender_finished",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "outcome",
        kind: SqlKind::VarChar(32),
        nullable: true,
    },
    ColumnSpec {
        name: "created_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "updated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];

// P4b · 0037 三张准入/运行时表（与 migrations/0037 逐列同源；表列清单也被
// `every_registered_reentry_column_matches_a_fresh_install` 对账，见其文档）。
const MEMBER_RESET_ADMISSION_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "id",
        kind: SqlKind::Int,
        nullable: false,
    },
    ColumnSpec {
        name: "enabled",
        kind: SqlKind::SmallInt,
        nullable: false,
    },
    ColumnSpec {
        name: "admitted_boot_id",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "admission_version",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "basis",
        kind: SqlKind::VarChar(200),
        nullable: false,
    },
    ColumnSpec {
        name: "admitted_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "admitted_by",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
];

const MEMBER_RESET_ADMISSION_HISTORY_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "event_id",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "boot_id",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "action",
        kind: SqlKind::VarChar(16),
        nullable: false,
    },
    ColumnSpec {
        name: "basis",
        kind: SqlKind::VarChar(200),
        nullable: false,
    },
    ColumnSpec {
        name: "at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "actor",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
];

const ENGINE_RUNTIME_PROVENANCE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "boot_id",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "protocol_version",
        kind: SqlKind::SmallInt,
        nullable: false,
    },
    ColumnSpec {
        name: "started_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];

// D30 · 0044 保护单善后义务表（与 migrations/0044 逐列同源；同样被
// fresh-install 探针对账）。
const PROTECTION_CLEANUP_OBLIGATION_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "position_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "credential_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "bot_uid",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "symbol_key",
        kind: SqlKind::VarChar(160),
        nullable: false,
    },
    ColumnSpec {
        name: "direction",
        kind: SqlKind::VarChar(16),
        nullable: false,
    },
    ColumnSpec {
        name: "state",
        kind: SqlKind::VarChar(24),
        nullable: false,
    },
    ColumnSpec {
        name: "origin",
        kind: SqlKind::VarChar(24),
        nullable: false,
    },
    ColumnSpec {
        name: "attempts",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "next_attempt_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "complete_reason",
        kind: SqlKind::VarChar(64),
        nullable: true,
    },
    ColumnSpec {
        name: "attempt_outcomes_json",
        kind: SqlKind::Text,
        nullable: true,
    },
    ColumnSpec {
        name: "created_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "updated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "completed_at_ms",
        kind: SqlKind::BigInt,
        nullable: true,
    },
];

// M0fix · 0045 追加的生命周期键列（0044 表探针的「后续列」与 0045 列探针
// 共用这一份期望）。
const PROTECTION_CLEANUP_OBLIGATION_LATER_COLUMNS: &[ColumnSpec] = &[ColumnSpec {
    name: "close_revision",
    kind: SqlKind::BigInt,
    nullable: false,
}];

const POSITION_EXECUTION_PROVENANCE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "position_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "credential_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "bot_uid",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "symbol_key",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
    ColumnSpec {
        name: "direction",
        kind: SqlKind::VarChar(16),
        nullable: false,
    },
    ColumnSpec {
        name: "entry_epoch",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "entry_engine_order_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "run_mode",
        kind: SqlKind::VarChar(16),
        nullable: false,
    },
    ColumnSpec {
        name: "execution_venue",
        kind: SqlKind::VarChar(16),
        nullable: false,
    },
    ColumnSpec {
        name: "created_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];
const POSITION_CLOSE_REQUESTS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "credential_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "request_ref",
        kind: SqlKind::VarChar(26),
        nullable: false,
    },
    ColumnSpec {
        name: "actor_member_id",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "position_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "expected_revision",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "expected_boot_id",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "params_hash",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "scope_key",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "target_json",
        kind: SqlKind::Text,
        nullable: false,
    },
    ColumnSpec {
        name: "phase",
        kind: SqlKind::VarChar(24),
        nullable: false,
    },
    ColumnSpec {
        name: "reason_code",
        kind: SqlKind::VarChar(128),
        nullable: false,
    },
    ColumnSpec {
        name: "engine_order_id",
        kind: SqlKind::VarChar(64),
        nullable: true,
    },
    ColumnSpec {
        name: "risk_verdict_id",
        kind: SqlKind::VarChar(64),
        nullable: true,
    },
    ColumnSpec {
        name: "outcome_json",
        kind: SqlKind::Text,
        nullable: true,
    },
    ColumnSpec {
        name: "created_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "updated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "finalized_at_ms",
        kind: SqlKind::BigInt,
        nullable: true,
    },
    ColumnSpec {
        name: "idempotency_key",
        kind: SqlKind::VarChar(80),
        nullable: false,
    },
    ColumnSpec {
        name: "client_order_id_short",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
];
const POSITION_CLOSE_SCOPE_LOCKS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "scope_key",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "pending_request_ref",
        kind: SqlKind::VarChar(26),
        nullable: true,
    },
];

const LIFECYCLE_CREDENTIAL_FENCES_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "credential_key",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "credential_id",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "actor_member_id",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "account_uid",
        kind: SqlKind::VarChar(32),
        nullable: false,
    },
    ColumnSpec {
        name: "revision",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "first_request_ref",
        kind: SqlKind::VarChar(26),
        nullable: false,
    },
    ColumnSpec {
        name: "updated_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
];

const LIFECYCLE_FENCE_REQUESTS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "request_ref",
        kind: SqlKind::VarChar(26),
        nullable: false,
    },
    ColumnSpec {
        name: "credential_key",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
    ColumnSpec {
        name: "identity_json",
        kind: SqlKind::Text,
        nullable: false,
    },
    ColumnSpec {
        name: "expected_boot_id",
        kind: SqlKind::VarChar(26),
        nullable: false,
    },
    ColumnSpec {
        name: "armed_boot_id",
        kind: SqlKind::VarChar(26),
        nullable: false,
    },
    ColumnSpec {
        name: "fence_revision",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "armed_at_ms",
        kind: SqlKind::BigInt,
        nullable: false,
    },
    ColumnSpec {
        name: "params_hash",
        kind: SqlKind::VarChar(64),
        nullable: false,
    },
];

const DDL_REENTRY_PLANS: &[(i64, &[DdlStep])] = &[
    // M0fix（评审 P2-4）：0044 是两段 DDL（表 + 索引），逐段登记、逐段探针；
    // 整份文件当一段在 MySQL 上是一条被拒绝的多语句 PREPARE。
    (44, &[
        DdlStep {
            effect: DdlEffect::TableLaterExtended {
                table: "protection_cleanup_obligations",
                columns: PROTECTION_CLEANUP_OBLIGATION_COLUMNS,
                later_columns: PROTECTION_CLEANUP_OBLIGATION_LATER_COLUMNS,
                primary_key: &["position_id"],
            },
            apply: r#"CREATE TABLE protection_cleanup_obligations (
  position_id VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(160) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  state VARCHAR(24) NOT NULL,
  origin VARCHAR(24) NOT NULL,
  attempts BIGINT NOT NULL DEFAULT 0,
  next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
  complete_reason VARCHAR(64),
  attempt_outcomes_json TEXT,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  completed_at_ms BIGINT,
  PRIMARY KEY (position_id)
)"#,
        },
        DdlStep {
            effect: DdlEffect::Index {
                table: "protection_cleanup_obligations",
                index: "protection_cleanup_pending_scope_idx",
                columns: &["credential_id", "symbol_key", "direction", "state"],
            },
            apply: r#"CREATE INDEX protection_cleanup_pending_scope_idx
  ON protection_cleanup_obligations (credential_id, symbol_key, direction, state)"#,
        },
    ]),
    // M0fix（评审 P0-1 / P2-5）：生命周期键列 + 到期扫描索引，各一段。
    (45, &[
        DdlStep {
            effect: DdlEffect::Column {
                table: "protection_cleanup_obligations",
                column: PROTECTION_CLEANUP_OBLIGATION_LATER_COLUMNS[0],
            },
            apply: "ALTER TABLE protection_cleanup_obligations ADD COLUMN close_revision BIGINT NOT NULL DEFAULT 0",
        },
        DdlStep {
            effect: DdlEffect::Index {
                table: "protection_cleanup_obligations",
                index: "protection_cleanup_due_scan_idx",
                columns: &["state", "next_attempt_at_ms"],
            },
            apply: r#"CREATE INDEX protection_cleanup_due_scan_idx
  ON protection_cleanup_obligations (state, next_attempt_at_ms)"#,
        },
    ]),
    (43, &[
        DdlStep { effect: DdlEffect::Table { table: "lifecycle_credential_fences", columns: LIFECYCLE_CREDENTIAL_FENCES_COLUMNS, primary_key: &["credential_key"] }, apply: r#"CREATE TABLE lifecycle_credential_fences (
  credential_key VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  actor_member_id BIGINT NOT NULL,
  account_uid VARCHAR(32) NOT NULL,
  revision BIGINT NOT NULL,
  first_request_ref VARCHAR(26) NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_key)
)"# },
        DdlStep { effect: DdlEffect::Table { table: "lifecycle_fence_requests", columns: LIFECYCLE_FENCE_REQUESTS_COLUMNS, primary_key: &["request_ref"] }, apply: r#"CREATE TABLE lifecycle_fence_requests (
  request_ref VARCHAR(26) NOT NULL,
  credential_key VARCHAR(64) NOT NULL,
  identity_json TEXT NOT NULL,
  expected_boot_id VARCHAR(26) NOT NULL,
  armed_boot_id VARCHAR(26) NOT NULL,
  fence_revision BIGINT NOT NULL,
  armed_at_ms BIGINT NOT NULL,
  params_hash VARCHAR(64) NOT NULL,
  PRIMARY KEY (request_ref)
)"# }
    ]),
    // MySQL ALTER commits before sqlx stamps success; reentry verifies the exact
    // old or new shape and never adopts a different column as already repaired.
    (42, &[DdlStep {
        effect: DdlEffect::MySqlOrderTypeWidth,
        apply: include_str!("../migrations/0042_engine_order_type_width.sql"),
    }]),
    // B3-K1: every DDL/ledger gap is structurally checked, including single DDLs.
    (39, &[DdlStep {
        effect: DdlEffect::Column { table: "position_state", column: ColumnSpec { name: "close_revision", kind: SqlKind::BigInt, nullable: false } },
        apply: "ALTER TABLE position_state ADD COLUMN close_revision BIGINT NOT NULL DEFAULT 0",
    }]),
    (40, &[
        DdlStep { effect: DdlEffect::Table { table: "position_execution_provenance", columns: POSITION_EXECUTION_PROVENANCE_COLUMNS, primary_key: &["position_id"] },
            apply: r#"CREATE TABLE position_execution_provenance (
  position_id VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  entry_epoch BIGINT NOT NULL,
  entry_engine_order_id VARCHAR(64) NOT NULL,
  run_mode VARCHAR(16) NOT NULL,
  execution_venue VARCHAR(16) NOT NULL,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (position_id)
)"#,
        },
    ]),
    (41, &[
        DdlStep { effect: DdlEffect::Table { table: "position_close_requests", columns: POSITION_CLOSE_REQUESTS_COLUMNS, primary_key: &["credential_id", "request_ref"] },
            apply: r#"CREATE TABLE position_close_requests (
  credential_id VARCHAR(64) NOT NULL,
  request_ref VARCHAR(26) NOT NULL,
  actor_member_id BIGINT NOT NULL,
  position_id VARCHAR(64) NOT NULL,
  expected_revision BIGINT NOT NULL,
  expected_boot_id VARCHAR(32) NOT NULL,
  params_hash VARCHAR(64) NOT NULL,
  scope_key VARCHAR(64) NOT NULL,
  target_json TEXT NOT NULL,
  idempotency_key VARCHAR(80) NOT NULL,
  client_order_id_short VARCHAR(64) NOT NULL,
  phase VARCHAR(24) NOT NULL,
  reason_code VARCHAR(128) NOT NULL,
  engine_order_id VARCHAR(64),
  risk_verdict_id VARCHAR(64),
  outcome_json TEXT,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  finalized_at_ms BIGINT,
  PRIMARY KEY (credential_id, request_ref)
)"#,
        },
        DdlStep { effect: DdlEffect::Table { table: "position_close_scope_locks", columns: POSITION_CLOSE_SCOPE_LOCKS_COLUMNS, primary_key: &["scope_key"] },
            apply: r#"CREATE TABLE position_close_scope_locks (
  scope_key VARCHAR(64) NOT NULL,
  pending_request_ref VARCHAR(26),
  PRIMARY KEY (scope_key)
)"#,
        },
    ]),

    // MySQL DDL is atomic, but its later _sqlx_migrations success stamp is not
    // the same transaction. 0035 also handles that single-DDL/ledger gap.
    (35, &[DdlStep {
        effect: DdlEffect::Table { table: "protection_dispatch_attempts", columns: PROTECTION_DISPATCH_COLUMNS, primary_key: &["attempt_id"] },
        apply: include_str!("../migrations/0035_protection_dispatch_attempts.sql"),
    }]),
    (
    27,
    &[
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec {
                    name: "generation_proved_at_ms",
                    kind: SqlKind::BigInt,
                    // 0027 的 `ADD COLUMN … BIGINT` 没有 NOT NULL：
                    // `NULL` 就是"这条存量事件尚未复核"（迁移注释里的判据）。
                    nullable: true,
                },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN generation_proved_at_ms BIGINT",
        },
        DdlStep {
            effect: DdlEffect::Table {
                table: "kill_switch_event_generation_revalidations",
                columns: REVALIDATION_TABLE_COLUMNS,
                primary_key: &["scope_key", "event_id"],
            },
            apply: "CREATE TABLE kill_switch_event_generation_revalidations (\n  \
                    scope_key VARCHAR(128) NOT NULL,\n  event_id VARCHAR(32) NOT NULL,\n  \
                    previous_generation BIGINT NOT NULL,\n  \
                    resolved_generation BIGINT NOT NULL,\n  \
                    state_generation BIGINT NOT NULL,\n  verdict VARCHAR(32) NOT NULL,\n  \
                    evidence TEXT NOT NULL,\n  revalidated_at_ms BIGINT NOT NULL,\n  \
                    PRIMARY KEY (scope_key, event_id)\n)",
        },
    ],
    ),
    // **0028 同样是两段 DDL。**
    //
    // `ALTER TABLE … ADD COLUMN expected_provenance` + 一张
    // `CREATE TABLE`：两个崩溃点与 0027 逐字同形。不登记的话，一次
    // 半执行的 0028 会让这个部署再也起不来（`DDL_REENTRY_PLANS`
    // 查不到 ⇒ fail-closed 停机），而它本来是可以被逐段判定收敛的。
    (
        28,
        &[
            DdlStep {
                effect: DdlEffect::Column {
                    table: "kill_switch_reconcile_status",
                    column: ColumnSpec {
                        name: "expected_provenance",
                        kind: SqlKind::VarChar(32),
                        // 0028 的 `ADD COLUMN … VARCHAR(32)` 没有 NOT NULL：
                        // `NULL` 就是"这一行的来源尚未复核"（迁移注释里的判据）。
                        nullable: true,
                    },
                },
                apply: "ALTER TABLE kill_switch_reconcile_status \
                        ADD COLUMN expected_provenance VARCHAR(32)",
            },
            DdlStep {
                effect: DdlEffect::Table {
                    table: "kill_switch_reconcile_target_revalidations",
                    columns: RECONCILE_TARGET_REVALIDATION_COLUMNS,
                    primary_key: &["scope_key", "credential_id", "generation"],
                },
                apply: "CREATE TABLE kill_switch_reconcile_target_revalidations (\n  \
                        scope_key VARCHAR(128) NOT NULL,\n  \
                        credential_id VARCHAR(128) NOT NULL,\n  \
                        generation BIGINT NOT NULL,\n  verdict VARCHAR(32) NOT NULL,\n  \
                        evidence TEXT NOT NULL,\n  revalidated_at_ms BIGINT NOT NULL,\n  \
                        PRIMARY KEY (scope_key, credential_id, generation)\n)",
            },
        ],
    ),
    // **段③ 批 0（H-01 High / M-04 Medium）· 0029 同样是两段 DDL。**
    //
    // `ALTER TABLE engine_credential_health ADD COLUMN reconcile_diff_open`
    // + 一张 `CREATE TABLE`：两个崩溃点与 0027 / 0028 逐字同形。不登记的话
    // 一次半执行的 0029 会让这个部署再也起不来（查不到计划 ⇒ fail-closed
    // 停机），而它本来是可以被逐段判定收敛的。
    (
        29,
        &[
            DdlStep {
                effect: DdlEffect::Column {
                    table: "engine_credential_health",
                    column: ColumnSpec {
                        name: "reconcile_diff_open",
                        kind: SqlKind::BigInt,
                        // 0029 的 `ADD COLUMN … BIGINT` 没有 NOT NULL：
                        // `NULL` 就是"这一行的对账差异状态从未被写过"
                        //（迁移注释里的判据），读侧一律 fail-closed。
                        nullable: true,
                    },
                },
                apply: "ALTER TABLE engine_credential_health \
                        ADD COLUMN reconcile_diff_open BIGINT",
            },
            DdlStep {
                effect: DdlEffect::Table {
                    table: "engine_credential_reconcile_diff_revalidations",
                    columns: CREDENTIAL_RECONCILE_DIFF_REVALIDATION_COLUMNS,
                    primary_key: &["credential_id"],
                },
                apply: "CREATE TABLE engine_credential_reconcile_diff_revalidations (\n  \
                        credential_id VARCHAR(64) NOT NULL,\n  \
                        verdict VARCHAR(32) NOT NULL,\n  \
                        evidence TEXT NOT NULL,\n  \
                        revalidated_at_ms BIGINT NOT NULL,\n  \
                        PRIMARY KEY (credential_id)\n)",
            },
        ],
    ),
    // **P4b（0036）· 八条单列 ALTER，八个独立崩溃点。** 每条语句单独探针、
    // 单独补跑；缺席才补，同名错结构一律停机（§9.6 纪律不变）。
    (
    36,
    &[
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_state",
                column: ColumnSpec { name: "authority_kind", kind: SqlKind::SmallInt, nullable: false },
            },
            apply: "ALTER TABLE kill_switch_state ADD COLUMN authority_kind SMALLINT NOT NULL DEFAULT 0",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_state",
                column: ColumnSpec { name: "authority_member_id", kind: SqlKind::Int, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_state ADD COLUMN authority_member_id INT",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec { name: "authority_kind", kind: SqlKind::SmallInt, nullable: false },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN authority_kind SMALLINT NOT NULL DEFAULT 0",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec { name: "authority_member_id", kind: SqlKind::Int, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN authority_member_id INT",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec { name: "arm_request_ref", kind: SqlKind::VarChar(64), nullable: true },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN arm_request_ref VARCHAR(64)",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec { name: "reset_request_ref", kind: SqlKind::VarChar(64), nullable: true },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN reset_request_ref VARCHAR(64)",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec { name: "arm_actor_kind", kind: SqlKind::SmallInt, nullable: false },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN arm_actor_kind SMALLINT NOT NULL DEFAULT 0",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_events",
                column: ColumnSpec { name: "arm_actor_member_id", kind: SqlKind::Int, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_events ADD COLUMN arm_actor_member_id INT",
        },
    ],
    ),
    // **P4b（0037）· 三张 CREATE + 固定禁用种子。** 种子是本迁移定义的唯一
    // 一行初始化 DML；它没执行完，这一版本不得落章成功（TG-1）。补执行只
    // 补这一行，不覆盖任何已存在的准入状态（探针见 `DdlEffect::SeedRow`）。
    (
    37,
    &[
        DdlStep {
            effect: DdlEffect::Table {
                table: "member_reset_admission",
                columns: MEMBER_RESET_ADMISSION_COLUMNS,
                primary_key: &["id"],
            },
            apply: "CREATE TABLE IF NOT EXISTS member_reset_admission (\n  \
                    id INT NOT NULL PRIMARY KEY,\n  \
                    enabled SMALLINT NOT NULL DEFAULT 0,\n  \
                    admitted_boot_id VARCHAR(32) NOT NULL DEFAULT '',\n  \
                    admission_version BIGINT NOT NULL DEFAULT 0,\n  \
                    basis VARCHAR(200) NOT NULL DEFAULT '',\n  \
                    admitted_at_ms BIGINT NOT NULL DEFAULT 0,\n  \
                    admitted_by VARCHAR(128) NOT NULL DEFAULT ''\n)",
        },
        DdlStep {
            effect: DdlEffect::Table {
                table: "member_reset_admission_history",
                columns: MEMBER_RESET_ADMISSION_HISTORY_COLUMNS,
                primary_key: &["event_id"],
            },
            apply: "CREATE TABLE IF NOT EXISTS member_reset_admission_history (\n  \
                    event_id VARCHAR(32) NOT NULL PRIMARY KEY,\n  \
                    boot_id VARCHAR(32) NOT NULL,\n  action VARCHAR(16) NOT NULL,\n  \
                    basis VARCHAR(200) NOT NULL,\n  at_ms BIGINT NOT NULL,\n  \
                    actor VARCHAR(128) NOT NULL\n)",
        },
        DdlStep {
            effect: DdlEffect::Table {
                table: "engine_runtime_provenance",
                columns: ENGINE_RUNTIME_PROVENANCE_COLUMNS,
                primary_key: &["boot_id"],
            },
            apply: "CREATE TABLE IF NOT EXISTS engine_runtime_provenance (\n  \
                    boot_id VARCHAR(32) NOT NULL PRIMARY KEY,\n  \
                    protocol_version SMALLINT NOT NULL,\n  \
                    started_at_ms BIGINT NOT NULL\n)",
        },
        DdlStep {
            effect: DdlEffect::SeedRow { table: "member_reset_admission" },
            apply: "INSERT INTO member_reset_admission\n  \
                    (id, enabled, admitted_boot_id, admission_version, basis, admitted_at_ms, admitted_by)\n  \
                    VALUES (1, 0, '', 0, '', 0, '')",
        },
    ],
    ),
    // **P4b（0038）· 五条单列 ALTER（五真位）。** NULL=legacy/未投影，读侧
    // 呈现 UNKNOWN，永不回填（engine safety contract）。
    (
    38,
    &[
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_reconcile_status",
                column: ColumnSpec { name: "truth_positions_zero", kind: SqlKind::BigInt, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_positions_zero BIGINT",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_reconcile_status",
                column: ColumnSpec { name: "truth_orders_empty", kind: SqlKind::BigInt, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_orders_empty BIGINT",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_reconcile_status",
                column: ColumnSpec { name: "truth_algo_orders_empty", kind: SqlKind::BigInt, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_algo_orders_empty BIGINT",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_reconcile_status",
                column: ColumnSpec { name: "truth_local_zero_diff", kind: SqlKind::BigInt, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_local_zero_diff BIGINT",
        },
        DdlStep {
            effect: DdlEffect::Column {
                table: "kill_switch_reconcile_status",
                column: ColumnSpec { name: "truth_no_diff", kind: SqlKind::BigInt, nullable: true },
            },
            apply: "ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_no_diff BIGINT",
        },
    ],
    ),
];

/// **多段 DDL 在账本落章前崩溃之后的 adopt / re-entry。**
///
/// `sqlx` 在 MySQL 上是这样跑一条迁移的（`sqlx-mysql/src/migrate.rs`）：
/// 先写一行 `_sqlx_migrations(success=FALSE)`，再执行整段 SQL，最后
/// `UPDATE ... success=TRUE`。它自己的注释就写着：MySQL 的 DDL 会造成
/// **隐式提交**，所以这三步无法真的放进一个事务。
///
/// 于是 0027 这种「`ALTER` + `CREATE TABLE`」的两段 DDL 有两个崩溃点：
///
/// * **ALTER 之后崩溃** —— 列已经加上、表还没建、账本停在 `success=0`；
/// * **CREATE 之后、落章之前崩溃** —— 两段 DDL 都在了，账本仍停在 `success=0`。
///
/// 两种在下一次启动时都撞 `Migrator::run` 的 `dirty_version()` 检查，
/// 直接 `MigrateError::Dirty(27)`：**这个部署再也起不来了**，而且重跑没用
/// ——盲目重放整段 SQL 会撞 "Duplicate column name"。R16 落 0027 时没有考虑
/// 这条路径（第十三轮 §8.4）。
///
/// 这里在 `Migrator::run` **之前**收敛它，逐段判定、只补缺的那一段：
///
/// 1. 账本表不存在 ⇒ 全新库，什么都不做；
/// 2. 逐个 dirty 版本查 `DDL_REENTRY_PLANS`。没有登记 ⇒ 报错停机
///    （不猜一个没有探针的 schema）；
/// 3. 账本行的 checksum 必须与嵌进二进制的那份**逐字节相同**——不同意味着
///    迁移文件在崩溃之后被改过，此时补跑等于执行一段没人验证过的 DDL；
/// 4. 逐段跑探针：效果已在 ⇒ 跳过；不在 ⇒ 执行该段，再探一次确认；
/// 5. 全部就位之后才把账本改成 `success=TRUE`。落章是**最后**一步，
///    因此这个函数本身崩溃在任何位置都可以再跑一次（幂等）。
async fn reenter_half_applied_migrations(
    pool: &Pool<Any>,
    kind: DbKind,
    migrator: &sqlx::migrate::Migrator,
) -> anyhow::Result<usize> {
    // 账本表不存在 = 全新库：没有半执行可言。
    if !table_exists(pool, kind, "_sqlx_migrations").await? {
        return Ok(0);
    }
    let dirty = sqlx::query("SELECT version, checksum FROM _sqlx_migrations WHERE success = 0")
        .fetch_all(pool)
        .await
        .context("list half-applied migrations")?;
    let mut repaired = 0usize;
    for row in dirty {
        let version = row.get::<i64, _>("version");
        let recorded = row
            .try_get::<Vec<u8>, _>("checksum")
            .context("read the half-applied migration checksum")?;
        let Some((_, steps)) = DDL_REENTRY_PLANS
            .iter()
            .find(|(planned, _)| *planned == version)
        else {
            return Err(anyhow::anyhow!(
                "migration {version} is recorded as half-applied but has no registered \
                 re-entry plan; completing it would mean guessing which of its statements \
                 already ran"
            ));
        };
        let expected = migrator
            .iter()
            .find(|migration| migration.version == version)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "migration {version} is recorded as half-applied but is not part of this \
                     build's migration set"
                )
            })?;
        if recorded.as_slice() != &*expected.checksum {
            return Err(anyhow::anyhow!(
                "migration {version} is recorded as half-applied with a checksum that does not \
                 match this build; the migration file changed after the interrupted run and \
                 re-entering it would execute unverified DDL"
            ));
        }
        for (index, step) in steps.iter().enumerate() {
            match probe_ddl_effect(pool, kind, &step.effect).await? {
                DdlEffectState::Present => continue,
                // **同名错误
                // 结构一律停机。** 跳过它等于把一个没人验证过的 schema 落章
                // 成"0027 已成功"；补跑它必然撞 `already exists`，错误信息
                // 还会把真正的问题盖住。两条都不做——如实说出差在哪并停机。
                DdlEffectState::Mismatch(detail) => {
                    return Err(anyhow::anyhow!(
                        "statement {} of migration {version} left an object with the right \
                         name but the wrong structure ({detail}); re-entering would stamp a \
                         schema no one verified, so this database is not started",
                        index + 1
                    ));
                }
                DdlEffectState::Absent => {}
            }
            sqlx::query(step.apply)
                .execute(pool)
                .await
                .with_context(|| {
                    format!("re-enter statement {} of migration {version}", index + 1)
                })?;
            match probe_ddl_effect(pool, kind, &step.effect).await? {
                DdlEffectState::Present => {}
                state => {
                    return Err(anyhow::anyhow!(
                        "statement {} of migration {version} reported success but its effect \
                         is still not the one this build expects: {state:?}",
                        index + 1
                    ));
                }
            }
        }
        let stamped = sqlx::query("UPDATE _sqlx_migrations SET success = 1 WHERE version = ?")
            .bind(version)
            .execute(pool)
            .await
            .context("stamp the re-entered migration")?
            .rows_affected();
        if stamped == 0 {
            return Err(anyhow::anyhow!(
                "re-entered migration {version} but its ledger row disappeared before it \
                 could be stamped"
            ));
        }
        tracing::warn!(
            migration_version = version,
            backend = ?kind,
            "completed a migration whose ledger entry was left half-applied by an interrupted \
             run; every statement was verified by its own effect probe before stamping"
        );
        repaired += 1;
    }
    Ok(repaired)
}

/// 目录里有没有这张表（**只问存在**）。
///
/// 只用在 `_sqlx_migrations` 上——那张表的结构是 `sqlx` 自己的，不归本仓
/// 定义，拿一份手抄的期望去比对只会在 sqlx 升级时误报。
async fn table_exists(pool: &Pool<Any>, kind: DbKind, table: &str) -> anyhow::Result<bool> {
    let sql = match kind {
        DbKind::MySql => {
            "SELECT COUNT(*) AS n FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_name = ?"
        }
        _ => "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = ?",
    };
    Ok(sqlx::query(sql)
        .bind(table)
        .fetch_one(pool)
        .await
        .context("probe a table's existence in the database catalog")?
        .get::<i64, _>("n")
        > 0)
}

/// 读一张表在**后端目录**里的实际列结构。表不存在时是空 `Vec`。
async fn observed_columns(
    pool: &Pool<Any>,
    kind: DbKind,
    table: &str,
) -> anyhow::Result<Vec<ObservedColumn>> {
    let rows = match kind {
        DbKind::MySql => sqlx::query(
            "SELECT column_name AS name, data_type AS base_type, \
                 COALESCE(character_maximum_length, -1) AS max_length, \
                 CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() AND table_name = ?",
        )
        .bind(table)
        .fetch_all(pool)
        .await
        .context("read a table's columns from information_schema")?,
        _ => {
            sqlx::query(
                // `notnull` 是 SQLite 的后缀运算符（`expr NOTNULL`），
                // 裸写会被解析成运算符——列名必须加引号。
                "SELECT name, type AS base_type, \
                 CASE WHEN \"notnull\" = 0 THEN 1 ELSE 0 END AS nullable \
                 FROM pragma_table_info(?)",
            )
            .bind(table)
            .fetch_all(pool)
            .await
            .context("read a table's columns from pragma_table_info")?
        }
    };
    rows.into_iter()
        .map(|row| {
            // MySQL 8 的 `information_schema` 把这些列按 `longtext` 送回来，
            // `sqlx::Any` 于是映射成 Blob——两个后端共用一份读侧归一
            // （见 `try_get_text` 的说明）。
            let name = try_get_text(&row, "name")?.unwrap_or_default();
            let declared = try_get_text(&row, "base_type")?.unwrap_or_default();
            // `VARCHAR(128)` / `BIGINT` ⇒ 基础类型 + 可选的声明长度。
            // MySQL 的 `data_type` 本来就不带 `(n)`，长度走
            // `character_maximum_length`；SQLite 的 `type` 带着 `(n)`。
            let (base, inline_length) = match declared.split_once('(') {
                Some((base, rest)) => (
                    base.trim().to_string(),
                    rest.trim_end_matches(')').trim().parse::<i64>().ok(),
                ),
                None => (declared.trim().to_string(), None),
            };
            let max_length = match kind {
                DbKind::MySql => match row.get::<i64, _>("max_length") {
                    value if value >= 0 => Some(value),
                    _ => None,
                },
                _ => inline_length,
            };
            Ok(ObservedColumn {
                name,
                base_type: base,
                max_length,
                nullable: row.get::<i64, _>("nullable") != 0,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(anyhow::Error::from)
}

/// 读一张表的主键列（**按顺序**）。没有主键时是空 `Vec`。
async fn observed_primary_key(
    pool: &Pool<Any>,
    kind: DbKind,
    table: &str,
) -> anyhow::Result<Vec<String>> {
    let rows = match kind {
        DbKind::MySql => sqlx::query(
            "SELECT column_name AS name FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() AND table_name = ? AND index_name = 'PRIMARY' \
                 ORDER BY seq_in_index",
        )
        .bind(table)
        .fetch_all(pool)
        .await
        .context("read a table's primary key from information_schema")?,
        _ => sqlx::query("SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk")
            .bind(table)
            .fetch_all(pool)
            .await
            .context("read a table's primary key from pragma_table_info")?,
    };
    rows.iter()
        .map(|row| Ok(try_get_text(row, "name")?.unwrap_or_default()))
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(anyhow::Error::from)
}

async fn dispatch_unique_indexes_match(pool: &Pool<Any>, kind: DbKind) -> anyhow::Result<bool> {
    let mut indexes = std::collections::BTreeMap::<String, Vec<String>>::new();
    if kind == DbKind::MySql {
        let rows = sqlx::query("SELECT index_name AS idx,column_name AS col FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='protection_dispatch_attempts' AND non_unique=0 ORDER BY index_name,seq_in_index").fetch_all(pool).await?;
        for row in rows {
            indexes
                .entry(try_get_text(&row, "idx")?.unwrap_or_default())
                .or_default()
                .push(try_get_text(&row, "col")?.unwrap_or_default());
        }
    } else {
        for row in sqlx::query("SELECT name FROM pragma_index_list('protection_dispatch_attempts') WHERE [unique]=1 ORDER BY name").fetch_all(pool).await? {
            let name=try_get_text(&row,"name")?.unwrap_or_default();
            let cols=sqlx::query("SELECT name FROM pragma_index_info(?) ORDER BY seqno").bind(&name).fetch_all(pool).await?;
            for col in cols { indexes.entry(name.clone()).or_default().push(try_get_text(&col,"name")?.unwrap_or_default()); }
        }
    }
    let found = indexes
        .into_values()
        .collect::<std::collections::BTreeSet<_>>();
    let expected = [
        vec!["attempt_id".to_owned()],
        vec!["engine_order_id".to_owned()],
        vec!["protection_order_id".to_owned()],
    ]
    .into_iter()
    .collect();
    Ok(found == expected)
}

/// **一段 DDL 的效果探针**。
///
/// 三种结论见 `DdlEffectState`。关键在于 `Mismatch` 不是 `Absent`：
/// 同名错误结构既不能被当成"已经跑过"（那会落章一个没人验证过的 schema），
/// 也不能被当成"还没跑过"（补跑必然撞 already exists，错误信息还会把
/// 真正的问题盖住）。
async fn probe_ddl_effect(
    pool: &Pool<Any>,
    kind: DbKind,
    effect: &DdlEffect,
) -> anyhow::Result<DdlEffectState> {
    let columns = observed_columns(pool, kind, effect.table()).await?;
    match effect {
        DdlEffect::MySqlOrderTypeWidth => {
            if kind == DbKind::Sqlite {
                return Ok(DdlEffectState::Present);
            }
            let Some(column) = columns.iter().find(|c| c.name == "order_type") else {
                return Ok(DdlEffectState::Mismatch(
                    "engine_orders.order_type missing".into(),
                ));
            };
            if kind != DbKind::MySql || column.base_type != "varchar" || column.nullable {
                return Ok(DdlEffectState::Mismatch(
                    "order_type must remain VARCHAR NOT NULL".into(),
                ));
            }
            Ok(match column.max_length {
                Some(16) => DdlEffectState::Absent,
                Some(32) => DdlEffectState::Present,
                other => DdlEffectState::Mismatch(format!(
                    "order_type length {other:?} is neither 16 nor 32"
                )),
            })
        }
        DdlEffect::Column { column, .. } => {
            if columns.is_empty() {
                // 整张表都不在：这一段 `ALTER` 的前提就不成立。
                return Ok(DdlEffectState::Mismatch(format!(
                    "table {} does not exist, so {} cannot be verified",
                    effect.table(),
                    effect.describe()
                )));
            }
            let Some(observed) = columns
                .iter()
                .find(|observed| observed.name.eq_ignore_ascii_case(column.name))
            else {
                return Ok(DdlEffectState::Absent);
            };
            match observed.mismatch_against(column) {
                Some(detail) => Ok(DdlEffectState::Mismatch(format!(
                    "{} exists but {detail}",
                    effect.describe()
                ))),
                None => Ok(DdlEffectState::Present),
            }
        }
        DdlEffect::Table {
            table,
            columns: expected,
            primary_key,
        } => probe_table(pool, kind, table, expected, &[], primary_key, &columns).await,
        DdlEffect::TableLaterExtended {
            table,
            columns: expected,
            later_columns,
            primary_key,
        } => {
            probe_table(
                pool,
                kind,
                table,
                expected,
                later_columns,
                primary_key,
                &columns,
            )
            .await
        }
        DdlEffect::Index {
            table,
            index,
            columns: expected,
        } => probe_index(pool, kind, table, index, expected).await,
        // **P4b（TG-1）· 固定种子的探针**：判据写在 `DdlEffect::SeedRow`
        // 的文档里——缺席补跑那一条 INSERT；与种子逐值一致算 Present；
        // 任何偏离（含 enabled=1 / 非空 admitted_boot_id / 多行）停机不猜。
        DdlEffect::SeedRow { table } => {
            if !table_exists(pool, kind, table).await? {
                return Ok(DdlEffectState::Mismatch(format!(
                    "table {table} does not exist, so its seed row cannot be verified"
                )));
            }
            // A2：按 id=1 精确查询（只认本迁移定义的那一行）。id≠1 的行
            // 一票不算——「只有别的行」= 效果未证明。
            let rows = sqlx::query(&format!(
                "SELECT enabled, admitted_boot_id, admission_version, basis, \
                 admitted_at_ms, admitted_by FROM {table} WHERE id = 1"
            ))
            .fetch_all(pool)
            .await
            .with_context(|| format!("probe the seed row of {table} (id=1)"))?;
            let Some(row) = rows.first() else {
                let extras = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                    .fetch_one(pool)
                    .await
                    .with_context(|| format!("count the rows of {table}"))?
                    .get::<i64, _>("n");
                if extras > 0 {
                    return Ok(DdlEffectState::Mismatch(format!(
                        "table {table} holds {extras} rows but none of them is the \
                         seed row id=1; this migration defines exactly that one row, \
                         and inserting a seed into a table someone else wrote to is \
                         not schema repair"
                    )));
                }
                return Ok(DdlEffectState::Absent);
            };
            // 已存在的 id=1 行必须逐值等于种子的初始形状；任何偏离（live
            // 准入、被运营推号、结构损坏）都是「效果未证明」——不覆盖、
            // 不清零重插、不落成功章。
            let enabled = row.get::<i64, _>("enabled");
            let admitted_boot =
                crate::store::try_get_text(row, "admitted_boot_id")?.unwrap_or_default();
            let version = row.get::<i64, _>("admission_version");
            let basis = crate::store::try_get_text(row, "basis")?.unwrap_or_default();
            let admitted_by = crate::store::try_get_text(row, "admitted_by")?.unwrap_or_default();
            if enabled == 0
                && admitted_boot.is_empty()
                && version == 0
                && basis.is_empty()
                && admitted_by.is_empty()
            {
                Ok(DdlEffectState::Present)
            } else {
                Ok(DdlEffectState::Mismatch(format!(
                    "table {table} row id=1 does not match the seed's initial shape \
                     (enabled={enabled}, admission_version={version}, basis set={}, \
                     admitted_by set={}); the seed effect is unproven, so this \
                     migration stays incomplete and the existing row is left untouched",
                    !basis.is_empty(),
                    !admitted_by.is_empty()
                )))
            }
        }
    }
}

/// 测试可见的生产单点（A2/A3 用例消费同一份事实，不手抄第二份）。
#[cfg(test)]
pub(crate) fn embedded_migrator_for_test() -> sqlx::migrate::Migrator {
    embedded_migrator()
}

/// `Table` / `TableLaterExtended` 的共用探针（`later` 为空即 `Table` 的原判据）。
async fn probe_table(
    pool: &Pool<Any>,
    kind: DbKind,
    table: &str,
    expected: &[ColumnSpec],
    later: &[ColumnSpec],
    primary_key: &[&str],
    columns: &[ObservedColumn],
) -> anyhow::Result<DdlEffectState> {
    if !table_exists(pool, kind, table).await? {
        return Ok(DdlEffectState::Absent);
    }
    for spec in expected.iter() {
        let Some(observed) = columns
            .iter()
            .find(|observed| observed.name.eq_ignore_ascii_case(spec.name))
        else {
            return Ok(DdlEffectState::Mismatch(format!(
                "table {table} exists but has no column {}",
                spec.name
            )));
        };
        if let Some(detail) = observed.mismatch_against(spec) {
            return Ok(DdlEffectState::Mismatch(format!(
                "table {table} exists but {detail}"
            )));
        }
    }
    // 多出来的列同样是"不是这一段建出来的那张表"——唯一的例外是登记过的
    // 后续迁移追加列（`TableLaterExtended`），且必须全部在、逐项符合。
    if columns.len() != expected.len() {
        let extras = columns
            .iter()
            .filter(|observed| {
                !expected
                    .iter()
                    .any(|spec| observed.name.eq_ignore_ascii_case(spec.name))
            })
            .collect::<Vec<_>>();
        let later_match = !later.is_empty()
            && extras.len() == later.len()
            && later.iter().all(|spec| {
                extras.iter().any(|observed| {
                    observed.name.eq_ignore_ascii_case(spec.name)
                        && observed.mismatch_against(spec).is_none()
                })
            });
        if !later_match {
            return Ok(DdlEffectState::Mismatch(format!(
                "table {table} has {} columns but this build expects {}",
                columns.len(),
                expected.len()
            )));
        }
    }
    if table == "protection_dispatch_attempts" && !dispatch_unique_indexes_match(pool, kind).await?
    {
        return Ok(DdlEffectState::Mismatch(
            "protection dispatch unique identity constraints differ".to_owned(),
        ));
    }
    let observed_pk = observed_primary_key(pool, kind, table).await?;
    if observed_pk.len() != primary_key.len()
        || observed_pk
            .iter()
            .zip(primary_key.iter())
            .any(|(observed, expected)| !observed.eq_ignore_ascii_case(expected))
    {
        return Ok(DdlEffectState::Mismatch(format!(
            "table {table} has primary key ({}) but this build expects ({})",
            observed_pk.join(", "),
            primary_key.join(", ")
        )));
    }
    Ok(DdlEffectState::Present)
}

/// `DdlEffect::Index` 的探针（判据见该变体的文档）。
async fn probe_index(
    pool: &Pool<Any>,
    kind: DbKind,
    table: &str,
    index: &str,
    expected: &[&str],
) -> anyhow::Result<DdlEffectState> {
    if !table_exists(pool, kind, table).await? {
        return Ok(DdlEffectState::Mismatch(format!(
            "table {table} does not exist, so index {index} cannot be verified"
        )));
    }
    let (unique, observed) = match kind {
        DbKind::MySql => {
            let rows = sqlx::query(
                "SELECT CAST(non_unique AS SIGNED) AS non_unique, column_name AS name \
                 FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? \
                 ORDER BY seq_in_index",
            )
            .bind(table)
            .bind(index)
            .fetch_all(pool)
            .await
            .context("read an index's columns from information_schema")?;
            if rows.is_empty() {
                return Ok(DdlEffectState::Absent);
            }
            let unique = rows.iter().any(|row| row.get::<i64, _>("non_unique") == 0);
            let names = rows
                .iter()
                .map(|row| Ok(try_get_text(row, "name")?.unwrap_or_default()))
                .collect::<Result<Vec<_>, sqlx::Error>>()?;
            (unique, names)
        }
        _ => {
            let listed = sqlx::query(
                "SELECT CAST([unique] AS INTEGER) AS is_unique FROM pragma_index_list(?) \
                 WHERE name = ?",
            )
            .bind(table)
            .bind(index)
            .fetch_optional(pool)
            .await
            .context("read an index from pragma_index_list")?;
            let Some(listed) = listed else {
                return Ok(DdlEffectState::Absent);
            };
            let names = sqlx::query("SELECT name FROM pragma_index_info(?) ORDER BY seqno")
                .bind(index)
                .fetch_all(pool)
                .await
                .context("read an index's columns from pragma_index_info")?
                .iter()
                .map(|row| Ok(try_get_text(row, "name")?.unwrap_or_default()))
                .collect::<Result<Vec<_>, sqlx::Error>>()?;
            (listed.get::<i64, _>("is_unique") != 0, names)
        }
    };
    if unique {
        return Ok(DdlEffectState::Mismatch(format!(
            "index {index} on {table} exists but is unique; this build expects an ordinary index"
        )));
    }
    if observed.len() != expected.len()
        || observed
            .iter()
            .zip(expected.iter())
            .any(|(observed, expected)| !observed.eq_ignore_ascii_case(expected))
    {
        return Ok(DdlEffectState::Mismatch(format!(
            "index {index} on {table} covers ({}) but this build expects ({})",
            observed.join(", "),
            expected.join(", ")
        )));
    }
    Ok(DdlEffectState::Present)
}

/// 测试可见的 re-entry 计划（按版本的 apply 语句清单）。
#[cfg(test)]
pub(crate) fn ddl_reentry_plans_for_test() -> &'static [(i64, &'static [DdlStep])] {
    DDL_REENTRY_PLANS
        .iter()
        .map(|(version, steps)| (*version, *steps))
        .collect::<Vec<_>>()
        .leak()
}

/// 进程级 boot_id（FA C1.2）：整个进程生命周期一个启动周期。测试进程会
/// 创建大量 Store，共享一个 boot_id 让 provenance 时间线与「启动周期」的
/// 语义一致，也避免每连接一行的时间线噪音。
fn process_boot_id() -> &'static str {
    static BOOT_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BOOT_ID.get_or_init(|| ulid::Ulid::new().to_string())
}

/// **P4b（FA C1.2 / FA2 §1.3）· 启动周期登记 + 旧准入无条件失效。**
///
/// 每次启动：①把本 boot 写进 `engine_runtime_provenance`（旧二进制从不写
/// 它——这是新协议写者的时间线，供准入 attestation 引用，不是排他证明）；
/// ②`member_reset_admission` **无条件** `enabled=0` 并推 `admission_version`
/// ——推号无条件，任何持有旧版本 expected 值的在途准入请求在锁内比对时
/// 必然失配；③只有本次确实关闭了一次 `enabled=1` 的准入时才写
/// `revoked_boot` 历史（推号与写历史是两件事，分开判定）。
///
/// 单例行在成功迁移后异常缺失（TG-1 的另一场景）不在此补写：UPDATE 命中
/// 0 行时如实报错日志，会员 reset/admit 的读门自行 fail-closed。
async fn register_boot_cycle(store: &Store, now_ms: i64) -> anyhow::Result<()> {
    let provenance_sql = match store.kind() {
        // 幂等登记：同一进程的后续 Store 连接不重复插行。
        DbKind::MySql => {
            "INSERT IGNORE INTO engine_runtime_provenance \
             (boot_id, protocol_version, started_at_ms) VALUES (?, ?, ?)"
        }
        _ => {
            "INSERT OR IGNORE INTO engine_runtime_provenance \
             (boot_id, protocol_version, started_at_ms) VALUES (?, ?, ?)"
        }
    };
    sqlx::query(provenance_sql)
        .bind(store.boot_id())
        .bind(2_i16)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .context("register engine runtime provenance")?;

    // SQLite 没有 FOR UPDATE（单连接天然串行——kill_switch.rs 同款分支）。
    let prior_sql = match store.kind() {
        DbKind::MySql => "SELECT enabled FROM member_reset_admission WHERE id = 1 FOR UPDATE",
        _ => "SELECT enabled FROM member_reset_admission WHERE id = 1",
    };
    let prior = sqlx::query(prior_sql).fetch_one(store.pool()).await;
    let prior_enabled = match prior {
        Ok(row) => Some(row.get::<i64, _>("enabled")),
        Err(sqlx::Error::RowNotFound) => None,
        Err(error) => return Err(error.into()),
    };
    let bumped = sqlx::query(
        "UPDATE member_reset_admission \
         SET enabled = 0, admission_version = admission_version + 1 \
         WHERE id = 1",
    )
    .execute(store.pool())
    .await
    .context("invalidate stale member-reset admission for this boot")?
    .rows_affected();
    if bumped == 0 {
        // 成功迁移后的异常缺行：不补写、不猜；会员 reset/admit 的锁内读门
        // 会按"无有效准入"拒绝并留异常日志（TG-1 第 4 条场景）。
        tracing::error!(
            boot_id = %store.boot_id(),
            error_code = "MEMBER_RESET_ADMISSION_SINGLETON_MISSING",
            "member_reset_admission singleton row is missing after a successful \
             migration; member reset/admit will fail closed until investigated"
        );
        return Ok(());
    }
    if prior_enabled == Some(1) {
        sqlx::query(
            "INSERT INTO member_reset_admission_history \
             (event_id, boot_id, action, basis, at_ms, actor) \
             VALUES (?, ?, 'revoked_boot', 'startup_cycle_reset', ?, 'system:boot')",
        )
        .bind(ulid::Ulid::new().to_string())
        .bind(store.boot_id())
        .bind(now_ms)
        .execute(store.pool())
        .await
        .context("record revoked_boot admission history")?;
        tracing::warn!(
            boot_id = %store.boot_id(),
            "an admission from a previous boot cycle was disabled by this startup; \
             re-admission requires a fresh verification against this boot"
        );
    }
    Ok(())
}

impl Store {
    pub async fn connect(db_url: &str) -> anyhow::Result<Self> {
        let store = Self::connect_unregistered(db_url).await?;
        #[cfg(test)]
        if !crate::binance_futures::test_cooldown_store_initialized() {
            // Keep this SQLite file for the whole test-process lifetime. This
            // path deliberately bypasses Store::connect, otherwise installing
            // the registry would recurse through this block.
            let path = std::env::temp_dir().join(format!(
                "rustcore-cooldown-store-{}-{}.db",
                std::process::id(),
                ulid::Ulid::new()
            ));
            let url = format!("sqlite://{}", path.display());
            let lifecycle_store = Self::connect_unregistered(&url)
                .await
                .context("initialize process-lifetime cooldown test store")?;
            crate::binance_futures::install_test_cooldown_store(lifecycle_store);
        }
        crate::binance_futures::install_cooldown_store(store.clone());
        Ok(store)
    }

    async fn connect_unregistered(db_url: &str) -> anyhow::Result<Self> {
        let db_url = db_url.trim();
        let kind = if db_url.starts_with("mysql://") {
            DbKind::MySql
        } else if db_url.starts_with("sqlite:") {
            DbKind::Sqlite
        } else {
            DbKind::Unknown
        };

        if kind == DbKind::Sqlite {
            // Best-effort: ensure sqlite database file exists so sqlx can open it.
            // Supports URLs like: sqlite://./data/rustcore.db
            if let Some(path) = db_url.strip_prefix("sqlite://") {
                let path = path.split('?').next().unwrap_or(path);
                if !path.is_empty() && path != ":memory:" {
                    let p = std::path::Path::new(path);
                    if let Some(parent) = p.parent() {
                        if !parent.as_os_str().is_empty() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                    }
                    if !p.exists() {
                        let _ = std::fs::File::create(p);
                    }
                }
            }
        }

        // SQLite permits one writer. A single pooled connection avoids
        // deferred-transaction upgrade races (`SQLITE_BUSY`) while the DB
        // conditional UPDATE remains the cross-process cap authority. MySQL
        // keeps the wider pool and row-level concurrency.
        let max_connections = if kind == DbKind::Sqlite { 1 } else { 8 };
        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .connect(db_url)
            .await
            .with_context(|| format!("connect db failed: {kind:?}"))?;

        let migrator = embedded_migrator();
        // Also guard a clean pending 0042: sqlx would otherwise run its ALTER
        // before the dirty-migration recovery probe ever sees the old shape.
        if kind == DbKind::MySql && table_exists(&pool, kind, "engine_orders").await? {
            if let DdlEffectState::Mismatch(detail) =
                probe_ddl_effect(&pool, kind, &DdlEffect::MySqlOrderTypeWidth).await?
            {
                return Err(anyhow::anyhow!("0042 preflight refused: {detail}"));
            }
        }
        // ****：多段 DDL
        // 在 MySQL 上没有事务保护，账本落章前崩溃会把这个库**永久卡死**。
        // 详见 `reenter_half_applied_migrations`。
        reenter_half_applied_migrations(&pool, kind, &migrator)
            .await
            .context("re-enter half-applied migrations failed")?;
        migrator.run(&pool).await.context("run migrations failed")?;
        if kind == DbKind::MySql
            && probe_ddl_effect(&pool, kind, &DdlEffect::MySqlOrderTypeWidth).await?
                != DdlEffectState::Present
        {
            return Err(anyhow::anyhow!(
                "0042 order_type width is not verified after migration"
            ));
        }

        // **P4b（FA C1.2）· 本进程的启动周期 ID。** 会员解除准入只在
        // `admitted_boot_id == boot_id` 时有效。boot_id 是**进程级**的
        // （一个引擎进程 = 一个启动周期）：同一进程里的多个 Store 实例
        // 共享同一个周期，provenance 每进程只登记一行。
        let boot_id = process_boot_id().to_string();
        let store = Self {
            pool: Arc::new(pool),
            kind,
            boot_id,
        };
        // ****：迁移 0024 只加了
        // "是否已复核"那一列（零 DML），复核本身在这里做——**迁移之后、
        // 任何恢复域发现之前**，用生产谓词逐行证明 0017 留下的
        // `symbol_scope_known=1` 标记，并把逐行结论写进
        // `fill_symbol_scope_revalidations`。证不出来的行降级成 unknown，
        // 不再播种 fill 恢复域。幂等：跑过一次之后是空集。
        crate::fill_replay::revalidate_legacy_fill_symbol_scopes(&store, crate::svc::now_ms())
            .await
            .context("revalidate legacy fill symbol scopes")?;
        // ****：0024 只复核了 `engine_fills`。
        // 恢复域的另外两个来源——0018 的 `fill_recovery_scopes` 与 0017 的
        // `fill_replay_cursors`——一列身份都没有，此前被无条件 UNION 进发现器、
        // 并被 `scopes_for_credential` 直接拿去驱动 REST 回补。迁移 0025 同样
        // 零 DML，复核在这里做：**必须排在上面那次 fill 复核之后**，因为
        // "有没有一条已证明的 fill 支撑这个域"正是它的第一条判据。
        crate::fill_replay::revalidate_legacy_fill_scope_origins(&store, crate::svc::now_ms())
            .await
            .context("revalidate legacy fill scope origins")?;
        // ****：迁移 0027 同样零 DML，
        // kill-switch 事件行的代际复核在这里做——**迁移之后、任何对账或
        // kill-switch 重试任务启动之前**。0026 把存量事件行置为代际 0 却假定
        // `kill_switch_state` 也停在 0；R14 起 0023 就在跑，活 latch 完全可能
        // 已经是 N > 0。不复核的话，这类部署的活 latch 在
        // `record_reconcile_for_scope` 里每一次都返回 `DiscardedRaced`，
        // 永远收敛不了、并被重试任务反复重复处置。幂等：健康库是空集。
        crate::kill_switch::revalidate_legacy_event_generations(&store, crate::svc::now_ms())
            .await
            .context("revalidate legacy kill switch event generations")?;
        // ****：迁移 0028 同样零
        // DML，`kill_switch_reconcile_status` 的**来源**复核在这里做——
        // 迁移之后、任何对账或 kill-switch 重试任务启动之前。0028 之前写下的
        // 行 `expected_provenance IS NULL`（来源不可证明），运行期判据一律
        // 不把它们算作 expected 成员；这一步按**此刻的成员资格**把本代的
        // 快照重建出来，并把被 adopt 的行复位成未收敛——来源不可证明的
        // clean 票不得穿过升级活下来。幂等：健康库是空集。
        crate::kill_switch::revalidate_legacy_reconcile_targets(&store, crate::svc::now_ms())
            .await
            .context("revalidate legacy kill switch reconcile targets")?;
        // **段③ 批 0（H-01 High / M-04 Medium）**：迁移 0029 同样零 DML，
        // `engine_credential_health.reconcile_diff_open` 的存量行复核在这里做
        // ——迁移之后、任何对账任务启动之前。0029 之前的行本列是 `NULL`
        //（"从未被写过"）；被掩盖的差异**不可从库里恢复**，所以诚实的值只有
        // "不知道"，而不知道不得被当成正常（engine safety contract）。这一步逐行
        // 给出结论（`diff_open_proven` / `unprovable`）并留证据到
        // `engine_credential_reconcile_diff_revalidations`，把 `NULL`
        // fail-closed 收敛成 1，由下一轮对账用真值收敛回 0。
        // 幂等：跑过一次之后是空集；健康库上零写入。
        crate::reconcile::revalidate_legacy_credential_reconcile_diff(&store, crate::svc::now_ms())
            .await
            .context("revalidate legacy credential reconcile diff state")?;
        // **P4b（FA C1.2 / FA2 §1.3）· 启动周期登记与准入失效。** 顺序固定：
        // 登记 `engine_runtime_provenance` → **无条件**推号并关闭旧准入
        // （准入不跨任何未证明的启动周期沿用）→ authority 一致性栅栏
        // （发现不一致时由栅栏再次推号并写 `revoked_fence`）。均发生在
        // serving 之前；保护/平仓/对账不读准入门，不受影响。
        register_boot_cycle(&store, crate::svc::now_ms())
            .await
            .context("register engine boot cycle and invalidate stale admission")?;
        crate::kill_switch::revalidate_authority_consistency(&store, crate::svc::now_ms())
            .await
            .context("revalidate kill-switch authority consistency")?;
        Ok(store)
    }

    pub fn pool(&self) -> &Pool<Any> {
        &self.pool
    }

    /// Passive observers identify a live Store without keeping its pool alive.
    pub(crate) fn weak_pool_identity(&self) -> std::sync::Weak<Pool<Any>> {
        Arc::downgrade(&self.pool)
    }

    pub fn kind(&self) -> DbKind {
        self.kind
    }

    /// P4b：本进程启动周期 ID（FA C1.2）。准入与 member reset 的锁内比对
    /// 都以它为"当前 boot"的真值。
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }
}

/// Shared scratch-database fixture for env-gated MySQL tests. Each caller gets
/// a fresh database, runs the production migration path through `Store`, and
/// must call `cleanup` after closing its stores. Keeping this under `cfg(test)`
/// ensures the fixture cannot enter the runtime binary.
#[cfg(test)]
pub(crate) mod mysql_test_support {
    use std::sync::Once;

    use sqlx::any::AnyPoolOptions;

    use super::{DbKind, Store};

    static INSTALL_DRIVERS: Once = Once::new();

    const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
    const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";

    pub(crate) struct MySqlScratch {
        admin_url: String,
        database: String,
        url: String,
    }

    impl MySqlScratch {
        pub(crate) async fn create(prefix: &str, test: &str) -> Option<Self> {
            assert!(
                !prefix.is_empty()
                    && prefix.len() <= 20
                    && prefix
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
                "MySQL scratch prefix must be 1..=20 lowercase ASCII letters/digits"
            );
            INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
            let admin_url = match std::env::var(MYSQL_URL_ENV) {
                Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
                _ => {
                    skip_without_mysql(test, &format!("{MYSQL_URL_ENV} is not set"));
                    return None;
                }
            };
            let admin = match AnyPoolOptions::new()
                .max_connections(1)
                .connect(&admin_url)
                .await
            {
                Ok(pool) => pool,
                Err(_) => {
                    skip_without_mysql(test, "the configured MySQL endpoint is unreachable");
                    return None;
                }
            };
            let database = format!(
                "rustcore_{}_{}",
                prefix,
                ulid::Ulid::new().to_string().to_ascii_lowercase()
            );
            sqlx::query(&format!("CREATE DATABASE `{database}`"))
                .execute(&admin)
                .await
                .expect("create the isolated MySQL scratch database");
            admin.close().await;
            let url = swap_database(&admin_url, &database);
            Some(Self {
                admin_url,
                database,
                url,
            })
        }

        pub(crate) fn database_url(&self) -> &str {
            &self.url
        }

        pub(crate) async fn connect_store(&self) -> Store {
            let store = Store::connect(&self.url)
                .await
                .expect("connect and migrate the isolated MySQL scratch database");
            assert_eq!(
                store.kind(),
                DbKind::MySql,
                "the env-gated fixture must use the MySQL dialect"
            );
            store
        }

        pub(crate) async fn cleanup(self) {
            let admin = AnyPoolOptions::new()
                .max_connections(1)
                .connect(&self.admin_url)
                .await
                .expect("reconnect to clean the isolated MySQL scratch database");
            sqlx::query(&format!("DROP DATABASE `{}`", self.database))
                .execute(&admin)
                .await
                .expect("drop the isolated MySQL scratch database");
            admin.close().await;
        }
    }

    fn swap_database(url: &str, database: &str) -> String {
        let (base, query) = url
            .split_once('?')
            .map_or((url, None), |(base, query)| (base, Some(query)));
        let (prefix, _) = base
            .rsplit_once('/')
            .expect("the MySQL test URL must carry a database path");
        match query {
            Some(query) => format!("{prefix}/{database}?{query}"),
            None => format!("{prefix}/{database}"),
        }
    }

    fn skip_without_mysql(test: &str, why: &str) {
        if std::env::var(REQUIRE_MYSQL_ENV)
            .map(|value| value.trim() == "1")
            .unwrap_or(false)
        {
            panic!("{test}: {REQUIRE_MYSQL_ENV}=1 requires real MySQL, but {why}");
        }
        eprintln!(
            "SKIPPED {test}: no real MySQL is available ({why}).\n\
             This test does not substitute SQLite for MySQL dialect evidence. Set \
             {MYSQL_URL_ENV} to run it; set {REQUIRE_MYSQL_ENV}=1 to forbid skips."
        );
    }
}

/// **多段 DDL 的崩溃恢复。**
///
/// 两个崩溃点各一条真库实跑（MySQL 的 DDL 会隐式提交，这是它必须在真库上
/// 跑的原因；SQLite 的 DDL 在事务里，造不出半执行）。真库缺失时按
/// engine safety contract 如实跳过，不拿 SQLite 冒充：
/// `RUSTCORE_TEST_MYSQL_URL=mysql://root:<pw>@127.0.0.1:3307/mysql`；
/// 设 `RUSTCORE_REQUIRE_MYSQL_TESTS=1` 把跳过变成失败。
#[cfg(test)]
mod r17_migration_reentry_tests {
    use std::sync::Once;

    use sqlx::any::AnyPoolOptions;
    use sqlx::Row;

    use super::{embedded_migrator, DdlStep, Store, DDL_REENTRY_PLANS};

    static INSTALL_DRIVERS: Once = Once::new();

    const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
    const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";

    fn squeeze(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// **登记的补跑语句必须就是迁移文件里的那一段。**
    ///
    /// re-entry 的补跑 SQL 是第二份实现：它和 `.sql` 文件一旦漂移，
    /// 崩溃恢复出来的 schema 就与正常安装出来的不是同一个（而且要等到
    /// 下一次真的崩溃才会发现）。这里逐段核对——空白归一之后，每一段
    /// `apply` 都必须逐字出现在那份被 `sqlx::migrate!` 嵌进来的 SQL 里。
    #[test]
    fn every_registered_reentry_statement_is_verbatim_in_its_migration() {
        let migrator = embedded_migrator();
        assert!(
            !DDL_REENTRY_PLANS.is_empty(),
            "the plan table must not be empty"
        );
        for (version, steps) in DDL_REENTRY_PLANS {
            let migration = migrator
                .iter()
                .find(|migration| migration.version == *version)
                .unwrap_or_else(|| panic!("migration {version} must exist in this build"));
            let sql = squeeze(&migration.sql);
            assert!(
                steps.len() > 1 || matches!(*version, 35 | 39 | 40 | 42),
                "migration {version} needs a bounded registered recovery reason"
            );
            for DdlStep { apply, .. } in steps.iter() {
                assert!(
                    sql.contains(&squeeze(apply)),
                    "migration {version} does not contain the registered statement {apply:?}; \
                     the re-entry plan drifted from the migration file"
                );
            }
        }
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
             多段 DDL 的半执行只在 MySQL 上可达（DDL 隐式提交），SQLite 的迁移跑在事务里；\n\
             本用例不以降级替身冒充该证据。设 {MYSQL_URL_ENV} 后可实跑。"
        );
    }

    fn swap_database(url: &str, database: &str) -> String {
        let (prefix, _) = url
            .rsplit_once('/')
            .expect("the MySQL URL must carry a database path");
        format!("{prefix}/{database}")
    }

    /// 建一个 scratch 库，并用**只到 0026** 的迁移目录（仓内 `.sql` 的逐字节
    /// 副本）把它推到 0026 时点——0027 还没跑过。
    async fn scratch_at_0026(label: &str) -> Option<(String, String, String, std::path::PathBuf)> {
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
            "rustcore_r17re_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("create the scratch database");
        admin.close().await;
        let url = swap_database(&admin_url, &database);

        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let target =
            std::env::temp_dir().join(format!("rustcore-r17mig-{label}-{}", ulid::Ulid::new()));
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
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the pre-0027 scratch database");
        sqlx::migrate::Migrator::new(target.as_path())
            .await
            .expect("load the pre-0027 migrator")
            .run(&pool)
            .await
            .expect("run migrations up to 0026 on MySQL");
        pool.close().await;
        Some((admin_url, database, url, target))
    }

    /// 把 0027 打成"崩溃在第 `statements_run` 段之后"的样子：账本行按 sqlx
    /// 的真实顺序先落 `success=FALSE`（`sqlx-mysql/src/migrate.rs` 的 apply
    /// 就是这么写的），再跑前 `statements_run` 段 DDL。
    /// 只写 sqlx 会写的那一行 `success=FALSE` 账本记录（不跑任何 DDL）。
    async fn write_dirty_ledger_row(url: &str) {
        let migrator = embedded_migrator();
        let migration = migrator
            .iter()
            .find(|migration| migration.version == 27)
            .expect("0027 exists");
        let checksum = migration
            .checksum
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("connect the scratch database");
        sqlx::query(&format!(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
             execution_time) VALUES (27, ?, FALSE, X'{checksum}', -1)"
        ))
        .bind(&*migration.description)
        .execute(&pool)
        .await
        .expect("write the half-applied ledger row sqlx would have written");
        pool.close().await;
    }

    async fn crash_0027_after(url: &str, statements_run: usize) {
        let migrator = embedded_migrator();
        let migration = migrator
            .iter()
            .find(|migration| migration.version == 27)
            .expect("0027 exists");
        let checksum = migration
            .checksum
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("connect the scratch database");
        sqlx::query(&format!(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
             execution_time) VALUES (27, ?, FALSE, X'{checksum}', -1)"
        ))
        .bind(&*migration.description)
        .execute(&pool)
        .await
        .expect("write the half-applied ledger row sqlx would have written");
        let (_, steps) = DDL_REENTRY_PLANS
            .iter()
            .find(|(version, _)| *version == 27)
            .expect("0027 has a re-entry plan");
        for step in steps.iter().take(statements_run) {
            sqlx::query(step.apply)
                .execute(&pool)
                .await
                .expect("run the statement that made it before the crash");
        }

        // **缺陷复现**：不带 re-entry 时，下一次启动连迁移都过不去。
        //
        // 这里关掉 sqlx 的迁移互斥锁（`GET_LOCK`）：`run` 在 `Dirty` 上提前返回、
        // 不走 `unlock()`，锁会跟着这条连接留在池里，把随后那次生产
        // `Store::connect` 的 `GET_LOCK(..., -1)` **无限期**挂住——那是夹具的
        // 副作用，不是被测行为。生产路径本身照常带锁。
        let mut plain = embedded_migrator();
        plain.set_locking(false);
        let dirty = plain
            .run(&pool)
            .await
            .expect_err("a half-applied ledger row must stop the plain migrator");
        assert!(
            matches!(dirty, sqlx::migrate::MigrateError::Dirty(27)),
            "the plain migrator must refuse a dirty version, got {dirty:?}"
        );
        pool.close().await;
    }

    /// 账本里**已落章**的版本号（升序）。
    async fn stamped_versions(url: &str) -> Vec<i64> {
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("connect the scratch database");
        let versions = sqlx::query(
            "SELECT version FROM _sqlx_migrations WHERE success = 1 AND version = 27 \
             ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .expect("read the ledger")
        .into_iter()
        .map(|row| row.get::<i64, _>("version"))
        .collect::<Vec<_>>();
        pool.close().await;
        versions
    }

    async fn assert_converged(url: &str) {
        let store = Store::connect(url)
            .await
            .expect("the production connect path must re-enter the half-applied migration");
        // 两段 DDL 的效果都在。
        sqlx::query("SELECT generation_proved_at_ms FROM kill_switch_events WHERE 1 = 0")
            .fetch_optional(store.pool())
            .await
            .expect("the ALTER's effect must exist");
        sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_event_generation_revalidations")
            .fetch_one(store.pool())
            .await
            .expect("the CREATE TABLE's effect must exist");
        // 账本落章，且 0027 只有一行。
        let stamped = sqlx::query(
            "SELECT COUNT(*) AS n FROM _sqlx_migrations WHERE version = 27 AND success = 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("read the ledger")
        .get::<i64, _>("n");
        assert_eq!(stamped, 1, "0027 must end up stamped exactly once");
        // 重跑：迁移与启动复核都是空操作。
        store.pool().close().await;
        let store = Store::connect(url).await.expect("reconnect after re-entry");
        assert_eq!(
            sqlx::query("SELECT COUNT(*) AS n FROM _sqlx_migrations WHERE success = 0")
                .fetch_one(store.pool())
                .await
                .expect("read the ledger")
                .get::<i64, _>("n"),
            0,
            "no ledger row may be left half-applied after a rerun"
        );
        store.pool().close().await;
    }

    async fn drop_scratch(admin_url: &str, database: &str, migrations: std::path::PathBuf) {
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(admin_url)
            .await
            .expect("reconnect to drop the scratch database");
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
        admin.close().await;
        let _ = std::fs::remove_dir_all(migrations);
    }

    /// **崩溃点一：`ALTER` 已提交、`CREATE TABLE` 还没跑。**
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crash_after_the_alter_is_re_entered_on_the_next_start() {
        let Some((admin_url, database, url, migrations)) =
            scratch_at_0026("a_crash_after_the_alter_is_re_entered_on_the_next_start").await
        else {
            return;
        };
        crash_0027_after(&url, 1).await;
        assert_converged(&url).await;
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **崩溃点二：两段 DDL 都跑完了，账本还停在 `success=FALSE`。**
    ///
    /// 这一条最容易被漏：schema 已经是对的，盲目重放整段 SQL 会撞
    /// "Duplicate column name"，而什么都不做又会被 `dirty_version()` 挡死。
    /// 正确处置是**只落章**（adopt）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crash_before_the_ledger_stamp_is_adopted_on_the_next_start() {
        let Some((admin_url, database, url, migrations)) =
            scratch_at_0026("a_crash_before_the_ledger_stamp_is_adopted_on_the_next_start").await
        else {
            return;
        };
        crash_0027_after(&url, 2).await;
        assert_converged(&url).await;
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **反向：没有登记 re-entry 方案的半执行版本必须停机，不许猜。**
    ///
    /// 补跑一段我们没有效果探针的 DDL，等于拿一个来路不明的 schema 继续跑
    /// （engine safety contract）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unregistered_half_applied_migration_fails_closed() {
        let Some((admin_url, database, url, migrations)) =
            scratch_at_0026("an_unregistered_half_applied_migration_fails_closed").await
        else {
            return;
        };
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query("UPDATE _sqlx_migrations SET success = 0 WHERE version = 26")
            .execute(&pool)
            .await
            .expect("mark 0026 half-applied");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("an unregistered half-applied migration must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("no registered re-entry plan"),
            "the failure must name the missing plan: {rendered}"
        );
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **反向：账本 checksum 与本次构建对不上时不得补跑。**
    ///
    /// 崩溃之后有人改了 `.sql`：补跑就是执行一段**没有任何人验证过**的 DDL。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_half_applied_migration_whose_file_changed_fails_closed() {
        let Some((admin_url, database, url, migrations)) =
            scratch_at_0026("a_half_applied_migration_whose_file_changed_fails_closed").await
        else {
            return;
        };
        crash_0027_after(&url, 1).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 27")
            .execute(&pool)
            .await
            .expect("drift the recorded checksum");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a drifted checksum must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("does not match this build"),
            "the failure must name the checksum drift: {rendered}"
        );
        drop_scratch(&admin_url, &database, migrations).await;
    }

    // -----------------------------------------------------------------------
    // 第十四轮 §9.6「0027 结构级 probe」（regression，High）
    // -----------------------------------------------------------------------

    /// **登记的期望结构必须与「全新安装出来的真实 schema」逐项相同。**
    ///
    /// `DDL_REENTRY_PLANS` 里的 `ColumnSpec` 是一份**手抄的期望**——第二份
    /// 实现。它和迁移文件一旦漂移，探针要么开始误报 `Mismatch`（好库起不来），
    /// 要么开始把错误结构判成 `Present`（正是 §9.6 要拦的）。这里拿**跑完
    /// 整条迁移链**的库去对账：每一段的效果都必须是 `Present`。
    ///
    /// 两个后端各一条：MySQL 与 SQLite 上报类型/可空性/主键的方式完全不同
    /// （`information_schema` vs `pragma_table_info`），只验一个等于只验一半。
    #[tokio::test]
    async fn the_registered_effects_match_a_fresh_sqlite_install() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-r18probe-{}.db", ulid::Ulid::new()));
        std::fs::File::create(&path).expect("create the sqlite file");
        // P4b R1（A2 分阶段）：探针的契约是**迁移阶段**状态；Store::connect 会
        // 追加启动登记（把 admission_version 推到 1），种子行就不再等于初始
        // 形状。这里直接用 Migrator 跑迁移链，不经过启动序列。
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("open the fresh sqlite database");
        embedded_migrator()
            .run(&pool)
            .await
            .expect("run the whole migration chain on SQLite");
        for (version, steps) in DDL_REENTRY_PLANS {
            for (index, step) in steps.iter().enumerate() {
                assert_eq!(
                    super::probe_ddl_effect(&pool, super::DbKind::Sqlite, &step.effect)
                        .await
                        .expect("probe a registered effect"),
                    super::DdlEffectState::Present,
                    "statement {} of migration {version} does not match the schema a fresh \
                     install produces; the registered expectation drifted from the migration",
                    index + 1
                );
            }
        }
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **M0fix（评审 P2-4）· 新增的两类效果探针的判据（SQLite 目录）。**
    ///
    /// 索引：缺席 ⇒ Absent；列序不符 / 唯一索引 / 表不在 ⇒ Mismatch；逐项
    /// 一致 ⇒ Present。被后续迁移追加过列的表：原始形状与「原始 + 恰好
    /// 登记的后续列」都是 Present，任何别的多余列仍是 Mismatch。
    #[tokio::test]
    async fn the_index_and_later_extended_table_probes_have_exact_verdicts_on_sqlite() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-m0fixprobe-{}.db", ulid::Ulid::new()));
        std::fs::File::create(&path).expect("create the sqlite file");
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("open the sqlite database");
        let probe = |effect: super::DdlEffect| {
            let pool = pool.clone();
            async move {
                super::probe_ddl_effect(&pool, super::DbKind::Sqlite, &effect)
                    .await
                    .expect("probe")
            }
        };
        let index = || super::DdlEffect::Index {
            table: "m0fix_probe",
            index: "m0fix_probe_idx",
            columns: &["a", "b"],
        };
        assert!(
            matches!(probe(index()).await, super::DdlEffectState::Mismatch(_)),
            "no table"
        );
        sqlx::query(
            "CREATE TABLE m0fix_probe (a VARCHAR(8) NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a))",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(probe(index()).await, super::DdlEffectState::Absent);
        for (ddl, why) in [
            (
                "CREATE INDEX m0fix_probe_idx ON m0fix_probe (b, a)",
                "column order",
            ),
            (
                "CREATE UNIQUE INDEX m0fix_probe_idx ON m0fix_probe (a, b)",
                "unique",
            ),
            (
                "CREATE INDEX m0fix_probe_idx ON m0fix_probe (a)",
                "column count",
            ),
        ] {
            sqlx::query(ddl).execute(&pool).await.unwrap();
            assert!(
                matches!(probe(index()).await, super::DdlEffectState::Mismatch(_)),
                "{why} must be a mismatch"
            );
            sqlx::query("DROP INDEX m0fix_probe_idx")
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("CREATE INDEX m0fix_probe_idx ON m0fix_probe (a, b)")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(probe(index()).await, super::DdlEffectState::Present);

        const ORIGINAL: &[super::ColumnSpec] = &[
            super::ColumnSpec {
                name: "a",
                kind: super::SqlKind::VarChar(8),
                nullable: false,
            },
            super::ColumnSpec {
                name: "b",
                kind: super::SqlKind::BigInt,
                nullable: false,
            },
        ];
        const LATER: &[super::ColumnSpec] = &[super::ColumnSpec {
            name: "c",
            kind: super::SqlKind::BigInt,
            nullable: false,
        }];
        let table = || super::DdlEffect::TableLaterExtended {
            table: "m0fix_probe",
            columns: ORIGINAL,
            later_columns: LATER,
            primary_key: &["a"],
        };
        assert_eq!(
            probe(table()).await,
            super::DdlEffectState::Present,
            "original shape"
        );
        sqlx::query("ALTER TABLE m0fix_probe ADD COLUMN c BIGINT NOT NULL DEFAULT 0")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            probe(table()).await,
            super::DdlEffectState::Present,
            "original + later column"
        );
        assert!(
            matches!(
                probe(super::DdlEffect::Table {
                    table: "m0fix_probe",
                    columns: ORIGINAL,
                    primary_key: &["a"],
                })
                .await,
                super::DdlEffectState::Mismatch(_)
            ),
            "a plain Table effect still rejects the extra column"
        );
        sqlx::query("ALTER TABLE m0fix_probe ADD COLUMN d BIGINT")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            matches!(probe(table()).await, super::DdlEffectState::Mismatch(_)),
            "an unregistered extra column is a mismatch"
        );
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 同上，在**真实 MySQL** 上。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_registered_effects_match_a_fresh_mysql_install() {
        let label = "the_registered_effects_match_a_fresh_mysql_install";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        // P4b R1（A2 分阶段）：同 sqlite 版——迁移阶段状态，不经过启动序列。
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("open the scratch mysql database");
        embedded_migrator()
            .run(&pool)
            .await
            .expect("run the whole migration chain on MySQL");
        for (version, steps) in DDL_REENTRY_PLANS {
            for (index, step) in steps.iter().enumerate() {
                assert_eq!(
                    super::probe_ddl_effect(&pool, super::DbKind::MySql, &step.effect)
                        .await
                        .expect("probe a registered effect"),
                    super::DdlEffectState::Present,
                    "statement {} of migration {version} does not match the schema a fresh \
                     MySQL install produces",
                    index + 1
                );
            }
        }
        pool.close().await;
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **regression（High）· 同名但结构错的表不得被 adopt。**
    ///
    /// 崩溃点二的形状（两段 DDL 看起来都在、账本停在 `success=0`），只是
    /// 那张表**不是 0027 建出来的那张**：列名一个不差，主键没了。
    /// R17 的探针只问名字，于是它判"这一段跑过了"、跳过、**落章**——
    /// 这个部署从此带着一张没有主键的复核证据表继续跑，
    /// `record_event_generation_revalidation` 的 upsert 语义随之失效
    /// （同一条事件可以被写进去无数遍），而账本声称 0027 已成功。
    ///
    /// 正确处置：如实说出差在哪并**停机**；账本一格都不许动。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_same_named_table_without_its_primary_key_is_never_stamped() {
        let label = "a_same_named_table_without_its_primary_key_is_never_stamped";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        // 第一段真的跑过（列在），第二段被一张**同名错误结构**顶了位置。
        crash_0027_after(&url, 1).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query(
            "CREATE TABLE kill_switch_event_generation_revalidations (\
               scope_key VARCHAR(128) NOT NULL, event_id VARCHAR(32) NOT NULL, \
               previous_generation BIGINT NOT NULL, resolved_generation BIGINT NOT NULL, \
               state_generation BIGINT NOT NULL, verdict VARCHAR(32) NOT NULL, \
               evidence TEXT NOT NULL, revalidated_at_ms BIGINT NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect("plant a same-named table without the primary key");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a same-named table with the wrong structure must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("wrong structure") && rendered.contains("primary key"),
            "the failure must name the structural difference: {rendered}"
        );
        assert_eq!(
            stamped_versions(&url).await,
            Vec::<i64>::new(),
            "a migration whose effect could not be verified must not be stamped"
        );
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **regression（High）· 同名但类型错的列不得被 adopt。**
    ///
    /// `generation_proved_at_ms` 是复核判据本身（`IS NULL` = 未复核）。
    /// 一列 `VARCHAR(32)` 顶着同一个名字时，R17 的探针判"已在"、落章；
    /// 之后写进去的复核时间戳是字符串，`revalidate_legacy_event_generations`
    /// 的判据从此在一个没人验证过的类型上运行。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_same_named_column_with_the_wrong_type_is_never_stamped() {
        let label = "a_same_named_column_with_the_wrong_type_is_never_stamped";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        write_dirty_ledger_row(&url).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query(
            "ALTER TABLE kill_switch_events ADD COLUMN generation_proved_at_ms VARCHAR(32)",
        )
        .execute(&pool)
        .await
        .expect("plant a same-named column of the wrong type");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a same-named column with the wrong type must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("wrong structure") && rendered.contains("expects bigint"),
            "the failure must name the type difference: {rendered}"
        );
        assert_eq!(
            stamped_versions(&url).await,
            Vec::<i64>::new(),
            "a migration whose effect could not be verified must not be stamped"
        );
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **regression（High）· 声明长度也在判据里。**
    ///
    /// `scope_key VARCHAR(64)` 顶着 `VARCHAR(128)` 的位置：列名、类型、
    /// 可空性、主键全都对得上，只有长度短了一半。而 `scope_key` 是
    /// `bot:<uid>` / `cred:<id>` 这种可以很长的身份串——一条被截断的
    /// scope_key 会让复核证据指向另一个 scope。只比基础类型的探针放行它。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_same_named_table_with_a_short_column_is_never_stamped() {
        let label = "a_same_named_table_with_a_short_column_is_never_stamped";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        crash_0027_after(&url, 1).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query(
            "CREATE TABLE kill_switch_event_generation_revalidations (\
               scope_key VARCHAR(64) NOT NULL, event_id VARCHAR(32) NOT NULL, \
               previous_generation BIGINT NOT NULL, resolved_generation BIGINT NOT NULL, \
               state_generation BIGINT NOT NULL, verdict VARCHAR(32) NOT NULL, \
               evidence TEXT NOT NULL, revalidated_at_ms BIGINT NOT NULL, \
               PRIMARY KEY (scope_key, event_id))",
        )
        .execute(&pool)
        .await
        .expect("plant a same-named table whose scope_key is half as long");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a same-named table with a short column must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("wrong structure") && rendered.contains("length 64"),
            "the failure must name the length difference: {rendered}"
        );
        assert_eq!(stamped_versions(&url).await, Vec::<i64>::new());
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **regression（High）· 主键比的是**哪几列**，不是几列。**
    ///
    /// 这张表的主键必须是 `(scope_key, event_id)`——`record_event_generation_revalidation`
    /// 的 upsert 语义（"同一条事件的复核结论只有一条"）全靠它。一张同名表
    /// 把主键写成 `(scope_key, verdict)` 时列数一样、类型一样、列也一个不少，
    /// 但语义整个反了：同一条事件可以被写进去无数遍，而同一个 scope 的
    /// 第二种裁决反而被拒。只数主键列数的探针放行它。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_same_named_table_keyed_on_the_wrong_columns_is_never_stamped() {
        let label = "a_same_named_table_keyed_on_the_wrong_columns_is_never_stamped";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        crash_0027_after(&url, 1).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query(
            "CREATE TABLE kill_switch_event_generation_revalidations (\
               scope_key VARCHAR(128) NOT NULL, event_id VARCHAR(32) NOT NULL, \
               previous_generation BIGINT NOT NULL, resolved_generation BIGINT NOT NULL, \
               state_generation BIGINT NOT NULL, verdict VARCHAR(32) NOT NULL, \
               evidence TEXT NOT NULL, revalidated_at_ms BIGINT NOT NULL, \
               PRIMARY KEY (scope_key, verdict))",
        )
        .execute(&pool)
        .await
        .expect("plant a same-named table keyed on the wrong columns");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a same-named table with the wrong key must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("wrong structure")
                && rendered.contains("primary key (scope_key, verdict)"),
            "the failure must name the key columns it actually found: {rendered}"
        );
        assert_eq!(stamped_versions(&url).await, Vec::<i64>::new());
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **regression（High）· 多出来的列同样是"不是这一段建出来的那张表"。**
    ///
    /// 每一列都在、类型都对、主键也对，但还多一列。它意味着这张表是**别的
    /// 东西**建的（另一个版本的迁移、手工建表、另一个应用同名占位）——
    /// 把它当成 0027 的效果落章，等于让这个部署带着一个来路不明的 schema
    /// 继续跑。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_same_named_table_with_an_extra_column_is_never_stamped() {
        let label = "a_same_named_table_with_an_extra_column_is_never_stamped";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        crash_0027_after(&url, 1).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query(
            "CREATE TABLE kill_switch_event_generation_revalidations (\
               scope_key VARCHAR(128) NOT NULL, event_id VARCHAR(32) NOT NULL, \
               previous_generation BIGINT NOT NULL, resolved_generation BIGINT NOT NULL, \
               state_generation BIGINT NOT NULL, verdict VARCHAR(32) NOT NULL, \
               evidence TEXT NOT NULL, revalidated_at_ms BIGINT NOT NULL, \
               surprise BIGINT NOT NULL, PRIMARY KEY (scope_key, event_id))",
        )
        .execute(&pool)
        .await
        .expect("plant a same-named table with one column too many");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a same-named table with an extra column must stop the process"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("wrong structure") && rendered.contains("9 columns"),
            "the failure must name the column-count difference: {rendered}"
        );
        assert_eq!(stamped_versions(&url).await, Vec::<i64>::new());
        drop_scratch(&admin_url, &database, migrations).await;
    }

    /// **反向：可空性也在判据里。**
    ///
    /// `generation_proved_at_ms` 必须**可空**——`NULL` 就是"这条存量事件
    /// 尚未复核"，也是启动复核的候选谓词（`generation_proved_at_ms IS NULL`）。
    /// 一列 `BIGINT NOT NULL DEFAULT 0` 顶着同一个名字时，全库的存量事件
    /// 会在升级的那一刻被标成"已复核"，复核于是变成一次空操作
    /// （engine safety contract：缺失数据不得被写成"正常"）。类型对、名字对，
    /// 只有可空性错——只比类型的探针放行它。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_same_named_column_that_is_not_nullable_is_never_stamped() {
        let label = "a_same_named_column_that_is_not_nullable_is_never_stamped";
        let Some((admin_url, database, url, migrations)) = scratch_at_0026(label).await else {
            return;
        };
        write_dirty_ledger_row(&url).await;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the scratch database");
        sqlx::query(
            "ALTER TABLE kill_switch_events ADD COLUMN generation_proved_at_ms BIGINT NOT NULL \
             DEFAULT 0",
        )
        .execute(&pool)
        .await
        .expect("plant a same-named column that is not nullable");
        pool.close().await;

        let error = match Store::connect(&url).await {
            Ok(_) => panic!("a same-named column with the wrong nullability must stop"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("wrong structure") && rendered.contains("nullable"),
            "the failure must name the nullability difference: {rendered}"
        );
        assert_eq!(
            stamped_versions(&url).await,
            Vec::<i64>::new(),
            "a migration whose effect could not be verified must not be stamped"
        );
        drop_scratch(&admin_url, &database, migrations).await;
    }
}

/// **驱动行为钉子**（sqlx 0.8.6）。
///
/// Any + SQLite 与 Any + MySQL 都必须无损读取宽 `i64`；现存文本投影也必须
/// 与直接读取相等。任一断言变红都表示依赖更新改变了持久真值语义。
#[cfg(test)]
mod sqlx_any_bigint_width_tests {
    use std::sync::Once;

    use sqlx::any::AnyPoolOptions;
    use sqlx::Row;

    use super::{try_get_i64_text, Store};

    static INSTALL_DRIVERS: Once = Once::new();

    /// 一个真实量级的毫秒 epoch（2026-08-25 附近）。其低 32 位与原值差
    /// 3 个数量级，因此这个探针对旧驱动截断保持敏感。
    const WIDE_MS: i64 = 1_787_429_492_942;
    const WIDE_MS_LOW_32: i64 = 723_097_806;

    #[test]
    fn the_probe_constant_is_sensitive_to_i32_truncation() {
        assert_eq!(WIDE_MS as i32 as i64, WIDE_MS_LOW_32);
        assert!(WIDE_MS > i64::from(i32::MAX));
    }

    #[tokio::test]
    async fn any_sqlite_reads_stored_bigint_intact_and_cast_char_agrees() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-i64probe-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect the probe sqlite store");

        sqlx::query(
            "INSERT INTO market_stream_health (credential_id, symbol_key, timeframe, \
             last_bar_ms, updated_at_ms) VALUES ('cred_probe', 'binance|futures|BTCUSDT', '1m', ?, ?)",
        )
        .bind(WIDE_MS)
        .bind(WIDE_MS)
        .execute(store.pool())
        .await
        .expect("store a wide millisecond epoch");

        let row = sqlx::query(
            "SELECT last_bar_ms, CAST(last_bar_ms AS CHAR) AS last_bar_ms_text \
             FROM market_stream_health WHERE credential_id='cred_probe'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read the probe row back");

        // 直接读与保守保留的文本投影都必须拿到同一个持久真值。
        assert_eq!(
            row.get::<i64, _>("last_bar_ms"),
            WIDE_MS,
            "sqlx Any+SQLite must preserve the complete stored i64"
        );
        assert_eq!(
            try_get_i64_text(&row, "last_bar_ms_text").expect("parse the text-cast truth"),
            WIDE_MS
        );

        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// MySQL 侧同一件事的对照。真库缺失时按 engine safety contract 如实跳过，
    /// **不拿 SQLite 冒充**：`RUSTCORE_TEST_MYSQL_URL=mysql://root:<pw>@127.0.0.1:3307/mysql`；
    /// 设 `RUSTCORE_REQUIRE_MYSQL_TESTS=1` 把跳过变成失败。
    #[tokio::test]
    async fn any_mysql_reads_stored_bigint_intact_and_cast_char_agrees() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
        const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";
        let require = std::env::var(REQUIRE_MYSQL_ENV)
            .map(|value| value.trim() == "1")
            .unwrap_or(false);
        let url = match std::env::var(MYSQL_URL_ENV) {
            Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => {
                assert!(
                    !require,
                    "{REQUIRE_MYSQL_ENV}=1 要求真实 MySQL，但 {MYSQL_URL_ENV} 未设"
                );
                eprintln!(
                    "SKIPPED any_mysql_reads_stored_bigint_intact_and_cast_char_agrees: \
                     没有可用的真实 MySQL（{MYSQL_URL_ENV} 未设）。\
                     两个后端的解码分叉只能在真库上对照，本用例不以 SQLite 冒充。"
                );
                return;
            }
        };
        if !url.starts_with("mysql://") {
            assert!(
                !require,
                "{REQUIRE_MYSQL_ENV}=1 requires {MYSQL_URL_ENV} to use mysql://; another backend cannot prove the MySQL width contract"
            );
            eprintln!(
                "SKIPPED any_mysql_reads_stored_bigint_intact_and_cast_char_agrees: \
                 {MYSQL_URL_ENV} is not a mysql:// URL; another backend is not a substitute"
            );
            return;
        }
        // 连接级临时表：不碰任何持久 schema，连接归还即消失（max_connections=1）。
        let pool = match AnyPoolOptions::new().max_connections(1).connect(&url).await {
            Ok(pool) => pool,
            Err(_) => {
                assert!(
                    !require,
                    "{REQUIRE_MYSQL_ENV}=1 requires a reachable real MySQL endpoint"
                );
                eprintln!(
                    "SKIPPED any_mysql_reads_stored_bigint_intact_and_cast_char_agrees: \
                     configured real MySQL endpoint is unavailable"
                );
                return;
            }
        };
        let identity = sqlx::query("SELECT CAST(CONNECTION_ID() AS SIGNED) AS mysql_connection_id")
            .fetch_one(&pool)
            .await
            .expect("configured endpoint must answer the MySQL-only identity query");
        assert!(
            identity.get::<i64, _>("mysql_connection_id") > 0,
            "configured endpoint must prove a live MySQL connection identity"
        );
        sqlx::query("CREATE TEMPORARY TABLE rustcore_i64_probe (last_bar_ms BIGINT NOT NULL)")
            .execute(&pool)
            .await
            .expect("create the temporary probe table");
        sqlx::query("INSERT INTO rustcore_i64_probe (last_bar_ms) VALUES (?)")
            .bind(WIDE_MS)
            .execute(&pool)
            .await
            .expect("store a wide millisecond epoch on MySQL");
        let row = sqlx::query(
            "SELECT last_bar_ms, CAST(last_bar_ms AS CHAR) AS last_bar_ms_text \
             FROM rustcore_i64_probe",
        )
        .fetch_one(&pool)
        .await
        .expect("read the probe row back from MySQL");

        assert_eq!(
            row.get::<i64, _>("last_bar_ms"),
            WIDE_MS,
            "MySQL 侧一直是无损的（ColumnType::LongLong => AnyTypeInfoKind::BigInt）；\
             这条红了说明升级把 MySQL 也改坏了"
        );
        assert_eq!(
            try_get_i64_text(&row, "last_bar_ms_text").expect("parse the text-cast truth"),
            WIDE_MS,
            "本批的修法在 MySQL 上必须与直读等价——否则 CAST 改写会把 MySQL 改坏"
        );
        pool.close().await;
    }
}

/// Real-MySQL contract for the SQLx Any empty-result bug fixed by the exact
/// dependency pin. These use one connection so a stranded transaction or pool
/// permit cannot be hidden by spare capacity.
#[cfg(test)]
mod sqlx_any_empty_result_tests {
    use std::sync::Once;
    use std::time::Duration;

    use sqlx::any::AnyPoolOptions;
    use sqlx::{Any, Pool, Row};

    static INSTALL_DRIVERS: Once = Once::new();
    const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
    const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";
    const STEP_TIMEOUT: Duration = Duration::from_secs(5);

    async fn mysql_pool(test: &str) -> Option<Pool<Any>> {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let required = std::env::var(REQUIRE_MYSQL_ENV)
            .map(|value| value.trim() == "1")
            .unwrap_or(false);
        let url = match std::env::var(MYSQL_URL_ENV) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                assert!(
                    !required,
                    "{test}: {REQUIRE_MYSQL_ENV}=1 requires real MySQL, but {MYSQL_URL_ENV} is unset"
                );
                eprintln!("SKIPPED {test}: {MYSQL_URL_ENV} is unset; SQLite is not a substitute");
                return None;
            }
        };
        let url = url.trim();
        if !url.starts_with("mysql://") {
            assert!(
                !required,
                "{test}: {MYSQL_URL_ENV} must use the mysql:// scheme; another backend cannot prove the MySQL driver contract"
            );
            eprintln!(
                "SKIPPED {test}: {MYSQL_URL_ENV} is not a mysql:// URL; another backend is not a substitute"
            );
            return None;
        }
        let pool = match tokio::time::timeout(
            STEP_TIMEOUT,
            AnyPoolOptions::new().max_connections(1).connect(url),
        )
        .await
        {
            Ok(Ok(pool)) => pool,
            _ if required => {
                panic!("{test}: {REQUIRE_MYSQL_ENV}=1 requires the configured real MySQL endpoint")
            }
            _ => {
                eprintln!(
                    "SKIPPED {test}: configured real MySQL endpoint is unavailable; \
                     SQLite is not a substitute"
                );
                return None;
            }
        };
        let identity = tokio::time::timeout(
            STEP_TIMEOUT,
            sqlx::query("SELECT CAST(CONNECTION_ID() AS SIGNED) AS mysql_connection_id")
                .fetch_one(&pool),
        )
        .await;
        match identity {
            Ok(Ok(row)) if row.get::<i64, _>("mysql_connection_id") > 0 => Some(pool),
            _ if required => {
                panic!("{test}: configured endpoint did not prove a live MySQL server identity")
            }
            _ => {
                eprintln!(
                    "SKIPPED {test}: configured endpoint did not prove a live MySQL server identity"
                );
                None
            }
        }
    }

    #[tokio::test]
    async fn mysql_any_empty_fetch_optional_releases_the_only_pool_permit() {
        const TEST: &str = "mysql_any_empty_fetch_optional_releases_the_only_pool_permit";
        let Some(pool) = mysql_pool(TEST).await else {
            return;
        };

        let missing = tokio::time::timeout(
            STEP_TIMEOUT,
            sqlx::query("SELECT CAST(1 AS SIGNED) AS marker WHERE 1=0").fetch_optional(&pool),
        )
        .await
        .expect("empty fetch_optional must finish within the driver budget")
        .expect("empty fetch_optional must not return a driver error");
        assert!(
            missing.is_none(),
            "the probe query must really return zero rows"
        );

        let row = tokio::time::timeout(
            STEP_TIMEOUT,
            sqlx::query("SELECT CAST(7 AS SIGNED) AS marker").fetch_one(&pool),
        )
        .await
        .expect("the only pool permit must be reusable after an empty result")
        .expect("the follow-up query must succeed");
        assert_eq!(row.get::<i64, _>("marker"), 7);

        tokio::time::timeout(STEP_TIMEOUT, pool.close())
            .await
            .expect("pool close must not wait on a stranded release ping");
    }

    #[tokio::test]
    async fn mysql_any_empty_fetch_one_keeps_the_transaction_usable() {
        const TEST: &str = "mysql_any_empty_fetch_one_keeps_the_transaction_usable";
        let Some(pool) = mysql_pool(TEST).await else {
            return;
        };
        let mut tx = tokio::time::timeout(STEP_TIMEOUT, pool.begin())
            .await
            .expect("begin must finish within the driver budget")
            .expect("begin the one-connection transaction");

        let result = tokio::time::timeout(
            STEP_TIMEOUT,
            sqlx::query("SELECT CAST(1 AS SIGNED) AS marker WHERE 1=0").fetch_one(&mut *tx),
        )
        .await
        .expect("empty fetch_one must finish within the driver budget");
        let error = match result {
            Ok(_) => panic!("fetch_one must report RowNotFound for a real empty result"),
            Err(error) => error,
        };
        assert!(
            matches!(error, sqlx::Error::RowNotFound),
            "empty fetch_one must preserve RowNotFound semantics: {error}"
        );

        let row = tokio::time::timeout(
            STEP_TIMEOUT,
            sqlx::query("SELECT CAST(9 AS SIGNED) AS marker").fetch_one(&mut *tx),
        )
        .await
        .expect("the same transaction must remain usable after RowNotFound")
        .expect("the follow-up transaction query must succeed");
        assert_eq!(row.get::<i64, _>("marker"), 9);
        tokio::time::timeout(STEP_TIMEOUT, tx.rollback())
            .await
            .expect("rollback must not hang after the empty result")
            .expect("rollback the driver-contract transaction");
        tokio::time::timeout(STEP_TIMEOUT, pool.close())
            .await
            .expect("pool close must finish after transaction rollback");
    }
}

/// K07: loopback MySQL proxy shared with `tests/k07_pool_lifecycle.rs` (test builds only).
#[cfg(test)]
#[path = "../tests/support/mysql_ping_proxy.rs"]
mod k07_store_proxy;

/// K07: production `Store::connect` wiring (MySQL max 8 / min 0, vendored patched sqlx-core)
/// under withheld release pings. Every one of the 8 pooled connections is held on return; the
/// pool must drop them at the return deadline, recover capacity, and a first close started
/// while a return is withheld must release everything. This proves the Store pool wiring only;
/// it is not an RPC, engine or VM end-to-end proof.
#[cfg(test)]
mod k07_store_tests {
    use std::collections::HashSet;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Once;
    use std::time::{Duration, Instant};

    use sqlx::any::AnyPoolOptions;
    use sqlx::pool::PoolConnection;
    use sqlx::{Any, Pool, Row};

    use super::k07_store_proxy::PingProxy;
    use super::{DbKind, Store};

    static INSTALL_DRIVERS: Once = Once::new();
    const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
    const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";
    const T_RELEASE: Duration = Duration::from_millis(5000);
    const MARGIN: Duration = Duration::from_millis(1500);
    const SERVER_WINDOW: Duration = Duration::from_millis(2000);
    const SETTLE: Duration = Duration::from_millis(300);

    async fn mysql_id(conn: &mut PoolConnection<Any>) -> u32 {
        let row = sqlx::query("SELECT CAST(CONNECTION_ID() AS SIGNED)")
            .fetch_one(&mut **conn)
            .await
            .expect("CONNECTION_ID");
        u32::try_from(row.get::<i64, _>(0)).expect("thread id fits u32")
    }

    async fn server_has(observer: &Pool<Any>, id: u32) -> bool {
        let row = sqlx::query(
            "SELECT CAST(COUNT(*) AS SIGNED) FROM information_schema.PROCESSLIST WHERE ID = ?",
        )
        .bind(i64::from(id))
        .fetch_one(observer)
        .await
        .expect("PROCESSLIST probe");
        row.get::<i64, _>(0) > 0
    }

    async fn server_gone(observer: &Pool<Any>, ids: &[u32], from: Instant) -> Duration {
        for &id in ids {
            while server_has(observer, id).await {
                assert!(
                    from.elapsed() <= SERVER_WINDOW,
                    "server thread {id} still present {SERVER_WINDOW:?} after local release"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        from.elapsed()
    }

    async fn until(what: &str, deadline: Instant, cond: impl Fn() -> bool) -> Instant {
        loop {
            if cond() {
                return Instant::now();
            }
            assert!(
                Instant::now() <= deadline,
                "{what}: not reached by its deadline"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn k07_store_production_pool_bounds_withheld_returns_and_first_close() {
        const TEST: &str = "k07_store_production_pool_bounds_withheld_returns_and_first_close";
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let required = std::env::var(REQUIRE_MYSQL_ENV).is_ok_and(|v| v.trim() == "1");
        let raw = match std::env::var(MYSQL_URL_ENV) {
            Ok(value) if !value.trim().is_empty() => value.trim().to_owned(),
            _ => {
                assert!(
                    !required,
                    "{TEST}: {REQUIRE_MYSQL_ENV}=1 but {MYSQL_URL_ENV} is unset"
                );
                eprintln!("SKIPPED {TEST}: {MYSQL_URL_ENV} is unset; SQLite is not a substitute");
                return;
            }
        };
        let admin_url = url::Url::parse(&raw).expect("MySQL test URL must parse");
        assert_eq!(admin_url.scheme(), "mysql");
        let host: IpAddr = admin_url
            .host_str()
            .and_then(|h| h.parse().ok())
            .expect("fixture host must be an IP literal");
        assert!(host.is_loopback(), "fixture must be a loopback server");
        let observer = AnyPoolOptions::new()
            .max_connections(1)
            .connect(admin_url.as_str())
            .await
            .expect("real MySQL fixture must be reachable");
        let database = format!("k07_{}", ulid::Ulid::new().to_string().to_ascii_lowercase());
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&observer)
            .await
            .expect("create k07 scratch schema");

        let proxy = PingProxy::start(SocketAddr::new(host, admin_url.port().unwrap_or(3306)))
            .await
            .expect("start loopback proxy");
        let mut store_url = admin_url.clone();
        store_url.set_host(Some("127.0.0.1")).unwrap();
        store_url.set_port(Some(proxy.addr().port())).unwrap();
        store_url.set_path(&format!("/{database}"));
        store_url.set_query(Some("ssl-mode=disabled"));

        // Production entry: the same symbol the engine calls, with its own pool configuration.
        let store = Store::connect(store_url.as_str())
            .await
            .expect("Store::connect through the proxy");
        assert_eq!(store.kind(), DbKind::MySql);
        let pool = store.pool().clone();
        assert_eq!(pool.options().get_max_connections(), 8);
        assert_eq!(pool.options().get_min_connections(), 0);

        // Phase A: all 8 pooled connections are returned with their release ping withheld.
        let mut conns = Vec::new();
        for _ in 0..8 {
            conns.push(
                tokio::time::timeout(MARGIN, pool.acquire())
                    .await
                    .expect("acquire within budget")
                    .expect("acquire"),
            );
        }
        let mut old_ids = Vec::new();
        for conn in &mut conns {
            old_ids.push(mysql_id(conn).await);
        }
        assert_eq!(
            old_ids.iter().collect::<HashSet<_>>().len(),
            8,
            "8 distinct connections"
        );
        assert_eq!(pool.size(), 8);
        for &id in &old_ids {
            proxy.hold_next_ping_reply(id);
        }
        let dropped_at = Instant::now();
        drop(conns);
        proxy
            .wait_for("8 release pings withheld", MARGIN, |c| {
                old_ids
                    .iter()
                    .all(|id| c.iter().any(|c| c.mysql_id == Some(*id) && c.held_now == 1))
            })
            .await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            (pool.size(), pool.num_idle()),
            (8, 0),
            "capacity held by pending returns"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(500), pool.acquire())
                .await
                .is_err(),
            "no capacity while all returns are withheld"
        );
        let zero_at = until(
            "all returns dropped",
            dropped_at + T_RELEASE + MARGIN,
            || pool.size() == 0,
        )
        .await;
        assert!(zero_at.duration_since(dropped_at) >= T_RELEASE);
        proxy
            .wait_for("8 relays ended by client EOF", MARGIN, |c| {
                old_ids.iter().all(|id| {
                    c.iter().any(|c| {
                        c.mysql_id == Some(*id)
                            && c.client_eof_at.is_some()
                            && c.relay_done_at.is_some()
                    })
                })
            })
            .await;
        for &id in &old_ids {
            let c = proxy.by_mysql_id(id);
            assert_eq!(
                (c.held_total, c.released_total),
                (1, 0),
                "held reply never forwarded"
            );
        }
        let gone_a = server_gone(&observer, &old_ids, zero_at).await;

        let mut fresh = tokio::time::timeout(MARGIN, pool.acquire())
            .await
            .expect("capacity recovered")
            .expect("acquire after recovery");
        let fresh_id = mysql_id(&mut fresh).await;
        assert!(!old_ids.contains(&fresh_id));
        let one: i64 = sqlx::query("SELECT CAST(1 AS SIGNED)")
            .fetch_one(&mut *fresh)
            .await
            .unwrap()
            .get(0);
        assert_eq!(one, 1);

        // Phase B: a first close started while one return is withheld releases everything.
        proxy.hold_next_ping_reply(fresh_id);
        let dropped_b = Instant::now();
        drop(fresh);
        proxy
            .wait_for("release ping withheld", MARGIN, |c| {
                c.iter()
                    .any(|c| c.mysql_id == Some(fresh_id) && c.held_now == 1)
            })
            .await;
        let close_bound =
            (dropped_b + T_RELEASE + MARGIN).saturating_duration_since(Instant::now());
        tokio::time::timeout(close_bound, pool.close())
            .await
            .expect("first close returns within T_release + margin");
        let closed_at = Instant::now();
        assert!(closed_at.duration_since(dropped_b) >= T_RELEASE);
        assert!(pool.is_closed());
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert_eq!(proxy.by_mysql_id(fresh_id).released_total, 0);
        let all_ids: Vec<u32> = proxy.snapshot().iter().filter_map(|c| c.mysql_id).collect();
        let gone_b = server_gone(&observer, &all_ids, closed_at).await;

        let cleanup = proxy.shutdown().await;
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&observer)
            .await
            .expect("drop k07 scratch schema");
        observer.close().await;
        println!(
            "K07_EVIDENCE {}",
            serde_json::json!({
                "test": TEST,
                "max_connections": 8, "min_connections": 0,
                "held_ids": old_ids, "fresh_id": fresh_id,
                "phase_a_size_zero_after_drop_ms": zero_at.duration_since(dropped_at).as_millis(),
                "phase_a_server_gone_after_size_zero_ms": gone_a.as_millis(),
                "phase_b_first_close_after_drop_ms": closed_at.duration_since(dropped_b).as_millis(),
                "phase_b_server_gone_after_close_ms": gone_b.as_millis(),
                "proxy_accepted": cleanup.accepted,
                "proxy_relays_active_after_wait": cleanup.relays_active_after_wait,
                "proxy_relays_force_aborted": cleanup.relays_force_aborted,
            })
        );
        assert_eq!(cleanup.relays_active_after_wait, 0);
        assert_eq!(cleanup.relays_force_aborted, 0);
    }
}

#[cfg(test)]
#[path = "order_type_width_tests.rs"]
mod order_type_width_tests;
