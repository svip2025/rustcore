#![cfg(target_os = "linux")]

#[test]
fn sigusr1_reaches_the_production_signal_handler() {
    rustcore::kill_signal::install_sigusr1_handler().expect("install production SIGUSR1 handler");
    assert!(!rustcore::kill_signal::take_pending());
    assert_eq!(unsafe { libc::raise(libc::SIGUSR1) }, 0);
    assert!(
        rustcore::kill_signal::take_pending(),
        "SIGUSR1 must set the handler's atomic handoff flag"
    );
}
