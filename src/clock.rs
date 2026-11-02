//! 引擎**时间观**的唯一完成点。
//!
//! 墙钟读取在此之前已经收敛到 `svc::SystemClock::now_ms()` 一处：自由函数
//! `svc::now_ms()`（引擎内 160+ 个时间消费点全部经它）与
//! `EngineExecutionService::clock`（决策 `now_ms`、下单时间戳、交易所客户端）
//! 在生产装配里都是同一个 `SystemClock`。本模块把那一处再拆成两半：
//!
//! * [`system_now_ms`] —— 进程的真实墙钟，**永不偏移**；
//! * [`engine_now_ms`] —— 引擎"现在几点"的观念。
//!
//! **默认构建（无 `l4-fault-injection`）里 [`engine_now_ms`] 逐字就是
//! [`system_now_ms`]**：没有运行时分支、没有 env 读取，`L4_CLOCK_SKEW_*`
//! 两个常量根本不会被编进二进制——`tests/gate/l4` 的
//! `assert_production_build` 靠扫描这两个 marker 字符串判定生产构建。
//!
//! `l4-fault-injection` 特性下，D7（promotion policy §3.3
//! 「时钟漂移 ±5s 注入不产生错误决策或重复下单」）的 fault harness 可以给
//! 引擎时间观加一个偏移量。偏移的语义边界（**故意**如此）：
//!
//! * 改的是引擎自己看到的"现在"——决策输入 `now_ms`、幂等/去重路径上写下的
//!   时间戳、订单与风控裁决的 `created_at_ms` 全部跟着走；
//! * **不改写交易所回报里的时间戳**。交易所时间是交易所真值，把它一起伪造
//!   就不是"本机时钟漂移"而是"交易所撒谎"，属于另一条判据。

use std::time::{SystemTime, UNIX_EPOCH};

/// 进程真实墙钟（epoch 毫秒）。任何注入都不影响它。
/// UTC 自然日 00:00 的毫秒值，**从传入的时间观推导**而不是各自读墙钟——
/// D7 完成的最后一处：`svc_v2` 的当日已实现盈亏窗口随引擎时间观漂移
/// （默认构建下与 `chrono::Utc::now()` 的午夜逐字等价）。
pub(crate) fn utc_day_start_ms(now_ms: i64) -> i64 {
    now_ms.div_euclid(86_400_000) * 86_400_000
}

pub(crate) fn system_now_ms() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    dur.as_millis() as i64
}

/// 引擎时间观。默认构建下与 [`system_now_ms`] 等价。
#[cfg(not(feature = "l4-fault-injection"))]
#[inline]
pub(crate) fn engine_now_ms() -> i64 {
    system_now_ms()
}

/// 引擎时间观（`l4-fault-injection`）：真实墙钟 + 已注入偏移。
#[cfg(feature = "l4-fault-injection")]
pub(crate) fn engine_now_ms() -> i64 {
    let raw = system_now_ms();
    raw.saturating_add(cached_skew_ms(raw))
}

/// 偏移的**文件**传输：内容是一个十进制整数毫秒。沿用 `l4_file_rpc` 的形态，
/// 因此 harness 可以在引擎**运行当中**改写它——真实的时钟漂移正是这样发生的。
#[cfg(feature = "l4-fault-injection")]
pub(crate) const CLOCK_SKEW_FILE: &str = "L4_CLOCK_SKEW_FILE";

/// 偏移的 **env** 传输：整段进程固定一个偏移。文件传输存在时以文件为准。
#[cfg(feature = "l4-fault-injection")]
pub(crate) const CLOCK_SKEW_MS: &str = "L4_CLOCK_SKEW_MS";

/// 单次注入的绝对上限（一天）。越界一律按 0 处理。
#[cfg(feature = "l4-fault-injection")]
const MAX_ABS_SKEW_MS: i64 = 86_400_000;

/// 文件重读节流窗口。计时用的是**未偏移**的真实墙钟，不自指。
#[cfg(feature = "l4-fault-injection")]
const REFRESH_INTERVAL_MS: i64 = 20;

#[cfg(feature = "l4-fault-injection")]
static CACHED_SKEW_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

#[cfg(feature = "l4-fault-injection")]
static CACHED_AT_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(i64::MIN);

#[cfg(feature = "l4-fault-injection")]
fn bounded_skew_ms(raw: &str) -> i64 {
    match raw.trim().parse::<i64>() {
        Ok(value) if value.abs() <= MAX_ABS_SKEW_MS => value,
        // 解析失败、越界、空文件：一律**不偏移**。harness 侧必须自己观测到
        // 漂移确实落地（见 D7 producer 的 `observedSkewMs` 判据），所以静默
        // 归零只会让 artifact 变成 `invalid`，不会让它假绿。
        _ => 0,
    }
}

/// env / 文件 → 偏移量的**纯解析**。用例直接调它，不需要改进程环境。
#[cfg(feature = "l4-fault-injection")]
pub(crate) fn resolve_skew_ms(
    enabled: bool,
    skew_file: Option<&str>,
    skew_env: Option<&str>,
) -> i64 {
    if !enabled {
        return 0;
    }
    if let Some(path) = skew_file.map(str::trim).filter(|value| !value.is_empty()) {
        if !std::path::Path::new(path).is_absolute() {
            return 0;
        }
        return match std::fs::read_to_string(path) {
            Ok(raw) => bounded_skew_ms(&raw),
            Err(_) => 0,
        };
    }
    skew_env.map(bounded_skew_ms).unwrap_or(0)
}

/// 带节流缓存的偏移读取。`raw_now_ms` 必须是未偏移的真实墙钟。
#[cfg(feature = "l4-fault-injection")]
fn cached_skew_ms(raw_now_ms: i64) -> i64 {
    use std::sync::atomic::Ordering;

    let cached_at = CACHED_AT_MS.load(Ordering::Relaxed);
    if cached_at != i64::MIN && raw_now_ms.saturating_sub(cached_at).abs() < REFRESH_INTERVAL_MS {
        return CACHED_SKEW_MS.load(Ordering::Relaxed);
    }
    let skew = resolve_skew_ms(
        crate::fault_injection::enabled(),
        std::env::var(CLOCK_SKEW_FILE).ok().as_deref(),
        std::env::var(CLOCK_SKEW_MS).ok().as_deref(),
    );
    CACHED_SKEW_MS.store(skew, Ordering::Relaxed);
    CACHED_AT_MS.store(raw_now_ms, Ordering::Relaxed);
    skew
}

#[cfg(test)]
mod tests {
    #[test]
    fn utc_day_start_matches_chrono_midnight() {
        for &ms in &[
            0i64,
            86_400_000 - 1,
            86_400_000,
            1_788_000_000_123,
            -1,
            -86_400_001,
        ] {
            let naive = chrono::DateTime::from_timestamp_millis(ms)
                .expect("valid ms")
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("UTC midnight is valid")
                .and_utc()
                .timestamp_millis();
            assert_eq!(super::utc_day_start_ms(ms), naive, "ms={ms}");
        }
    }

    use super::*;

    /// **两种构建共有的 fail-closed 不变量**：没有显式注入时，引擎时间观
    /// 就是真实墙钟。特性构建下这条同时证明"编进去 ≠ 生效"。
    #[test]
    fn engine_clock_does_not_drift_without_an_explicit_injection() {
        let before = system_now_ms();
        let observed = engine_now_ms();
        let after = system_now_ms();
        assert!(
            before <= observed && observed <= after,
            "engine_now_ms()={observed} left the real wall-clock window [{before}, {after}]"
        );
    }
}

/// **"生产构建零行为差异"的运行时证据**，只在默认构建里编译。
///
/// 这条用例把 D7 的两个注入开关全部打开，然后要求引擎时间观**纹丝不动**。
/// 默认构建里 `L4_CLOCK_SKEW_*` 根本没有读取点，所以它对同进程其他用例零
/// 影响；一旦有人把 `#[cfg(feature = ...)]` 门去掉、让偏移在生产构建里也
/// 生效，这条立刻变红。
#[cfg(all(test, not(feature = "l4-fault-injection")))]
mod production_build_tests {
    use super::*;

    #[test]
    fn production_build_ignores_every_clock_skew_switch() {
        // 进程级 `L4_*` 开关：与其余改 env 的用例互斥，避免在别的用例窗口里翻转。
        let _serial = crate::binance_futures::tests::env_serial_guard();
        std::env::set_var("L4_FAULT_INJECTION_ENABLED", "1");
        std::env::set_var("L4_CLOCK_SKEW_MS", "5000");
        let before = system_now_ms();
        let observed = engine_now_ms();
        let after = system_now_ms();
        std::env::remove_var("L4_CLOCK_SKEW_MS");
        std::env::remove_var("L4_FAULT_INJECTION_ENABLED");
        assert!(
            before <= observed && observed <= after,
            "a default production build consumed a clock skew switch: \
             engine_now_ms()={observed} left the real window [{before}, {after}]"
        );
    }
}

#[cfg(all(test, feature = "l4-fault-injection"))]
mod injection_tests {
    use super::*;

    fn skew_file(name: &str, contents: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("rustcore-clock-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).expect("create clock skew test directory");
        let path = root.join(name);
        std::fs::write(&path, contents).expect("write clock skew file");
        path
    }

    #[test]
    fn disabled_harness_never_skews_even_with_a_populated_file() {
        let path = skew_file("skew", "5000");
        assert_eq!(
            resolve_skew_ms(false, Some(path.to_str().expect("utf8 path")), Some("5000")),
            0
        );
    }

    #[test]
    fn file_transport_carries_both_drift_directions() {
        let plus = skew_file("plus", "5000\n");
        let minus = skew_file("minus", " -5000 ");
        assert_eq!(
            resolve_skew_ms(true, Some(plus.to_str().expect("utf8 path")), None),
            5_000
        );
        assert_eq!(
            resolve_skew_ms(true, Some(minus.to_str().expect("utf8 path")), None),
            -5_000
        );
    }

    #[test]
    fn file_transport_outranks_the_env_transport() {
        let path = skew_file("skew", "-5000");
        assert_eq!(
            resolve_skew_ms(true, Some(path.to_str().expect("utf8 path")), Some("5000")),
            -5_000
        );
    }

    #[test]
    fn env_transport_carries_the_skew_when_no_file_is_configured() {
        assert_eq!(resolve_skew_ms(true, None, Some("5000")), 5_000);
        assert_eq!(resolve_skew_ms(true, Some("  "), Some("-5000")), -5_000);
        assert_eq!(resolve_skew_ms(true, None, None), 0);
    }

    #[test]
    fn relative_unreadable_and_malformed_transports_fail_closed_to_zero() {
        let missing = skew_file("present", "5000")
            .parent()
            .expect("temp root")
            .join("absent");
        let malformed = skew_file("malformed", "five thousand");
        let empty = skew_file("empty", "");
        let over_range = skew_file("over", "86400001");
        for (label, value) in [
            (
                "relative path",
                resolve_skew_ms(true, Some("relative/skew"), None),
            ),
            (
                "missing file",
                resolve_skew_ms(true, Some(missing.to_str().expect("utf8 path")), None),
            ),
            (
                "malformed file",
                resolve_skew_ms(true, Some(malformed.to_str().expect("utf8 path")), None),
            ),
            (
                "empty file",
                resolve_skew_ms(true, Some(empty.to_str().expect("utf8 path")), None),
            ),
            (
                "out of range file",
                resolve_skew_ms(true, Some(over_range.to_str().expect("utf8 path")), None),
            ),
            ("malformed env", resolve_skew_ms(true, None, Some("later"))),
            (
                "out of range env",
                resolve_skew_ms(true, None, Some("-86400001")),
            ),
        ] {
            assert_eq!(value, 0, "{label} must fail closed to a zero skew");
        }
    }

    /// 偏移只做饱和加法：注入不可能把引擎时钟推成回绕后的荒谬值。
    #[test]
    fn skew_application_saturates_instead_of_wrapping() {
        assert_eq!(i64::MAX.saturating_add(5_000), i64::MAX);
        assert_eq!(i64::MIN.saturating_add(-5_000), i64::MIN);
    }
}
