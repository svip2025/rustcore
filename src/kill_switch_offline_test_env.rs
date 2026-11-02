// The local-cancel fixtures require a disabled exchange gateway. Other tests
// legitimately enable it, so readers must hold the same process-wide env lock.
// Field order restores the environment before releasing that lock.
struct OfflineOrderTestEnv {
    _vars: Vec<crate::binance_futures::tests::TestEnvVar>,
    _serial: std::sync::MutexGuard<'static, ()>,
}

fn offline_order_test_env(credential: &str) -> OfflineOrderTestEnv {
    use crate::binance_futures::tests::{env_serial_guard, TestEnvVar};
    let serial = env_serial_guard();
    let vars = vec![
        TestEnvVar::set("ENGINE_LIVE_ENABLED".into(), "0"),
        TestEnvVar::unset(crate::venue::venue_env_name(credential)),
    ];
    OfflineOrderTestEnv {
        _vars: vars,
        _serial: serial,
    }
}

#[test]
fn offline_order_fixture_holds_environment_lock_and_disables_gateway() {
    let env = offline_order_test_env("offline-env-proof");
    assert!(!crate::svc::is_binance_live_enabled());
    assert!(crate::venue::venue_for_credential("offline-env-proof")
        .unwrap()
        .is_none());
    assert!(crate::binance_futures::tests::env_serial_try_guard().is_none());
    drop(env);
}
