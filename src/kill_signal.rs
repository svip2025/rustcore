use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

static SIGUSR1_PENDING: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn handle_sigusr1(_signal: libc::c_int) {
    // The OS handler performs exactly one lock-free atomic store. Database,
    // logging, and exchange work are deferred to Tokio tasks.
    SIGUSR1_PENDING.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
pub fn install_sigusr1_handler() -> std::io::Result<()> {
    let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    let mask_result = unsafe {
        libc::sigemptyset(set.as_mut_ptr());
        libc::sigaddset(set.as_mut_ptr(), libc::SIGUSR1);
        libc::pthread_sigmask(libc::SIG_UNBLOCK, set.as_ptr(), std::ptr::null_mut())
    };
    if mask_result != 0 {
        return Err(std::io::Error::from_raw_os_error(mask_result));
    }
    let previous = unsafe {
        libc::signal(
            libc::SIGUSR1,
            handle_sigusr1 as *const () as libc::sighandler_t,
        )
    };
    if previous == libc::SIG_ERR {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn install_sigusr1_handler() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "SIGUSR1 is unavailable on this platform",
    ))
}

pub fn spawn_handoff_task(target: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if SIGUSR1_PENDING.swap(false, Ordering::SeqCst) {
                target.store(true, Ordering::SeqCst);
            }
        }
    })
}

pub fn take_pending() -> bool {
    SIGUSR1_PENDING.swap(false, Ordering::SeqCst)
}
