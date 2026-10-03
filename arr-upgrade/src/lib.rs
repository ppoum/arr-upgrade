use std::time::Duration;

use tokio::{select, sync::mpsc::Receiver};
use tokio_util::sync::CancellationToken;

#[expect(clippy::result_unit_err)]
pub async fn try_receive_message<T>(
    receiver: &mut Receiver<T>,
    cancel_token: &CancellationToken,
) -> Result<Option<T>, ()> {
    select! {
        msg = receiver.recv() => Ok(msg),
        _ = cancel_token.cancelled() => Err(())
    }
}

/// Attempts to sleep for the duration, exiting early if the token is cancelled. Returns `Ok(())`
/// when the sleep finishes, or `Err(())` if cancelled early.
#[expect(clippy::result_unit_err)]
pub async fn sleep_or_cancel(
    duration: Duration,
    cancel_token: &CancellationToken,
) -> Result<(), ()> {
    select! {
        _ = tokio::time::sleep(duration) => Ok(()),
        _ = cancel_token.cancelled() => Err(())
    }
}
