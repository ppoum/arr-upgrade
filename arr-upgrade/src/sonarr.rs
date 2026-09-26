use arr_api::sonarr::SonarrClient;
use cron::Schedule;

use crate::config::SonarrSearchGranularity;

pub async fn start_sonarr_handler(
    name: String,
    _client: SonarrClient,
    count: u32,
    schedule: Schedule,
    granularity: SonarrSearchGranularity,
) -> anyhow::Result<()> {
    // NOTE: temporary implementation
    log::info!("Started sonarr-{name}");
    log::info!("TMP: count={count}, schedule={schedule:?}, granularity={granularity:?}");
    Ok(())
}
