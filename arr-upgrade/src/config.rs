use std::{collections::HashMap, io, path::Path};

use anyhow::Context;
use serde::Deserialize;

const DEFAULT_FREQUENCY: &str = "hourly";
const DEFAULT_COUNT: u32 = 5;
const DEFAULT_GRANULARITY: SonarrSearchGranularity = SonarrSearchGranularity::Season;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct ThinRadarrInstance {
    pub url: String,
    pub api_key: String,
    pub frequency: Option<String>,
    pub count: Option<u32>,
}

pub struct RadarrInstance {
    pub url: String,
    pub api_key: String,
    pub frequency: String,
    pub count: u32,
}

#[derive(Debug, Deserialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SonarrSearchGranularity {
    Season,
    Show,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct ThinSonarrInstance {
    pub url: String,
    pub api_key: String,
    pub frequency: Option<String>,
    pub count: Option<u32>,
    pub search_granularity: Option<SonarrSearchGranularity>,
}

pub struct SonarrInstance {
    pub url: String,
    pub api_key: String,
    pub frequency: String,
    pub count: u32,
    pub search_granularity: SonarrSearchGranularity,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    default: DefaultSection,
    #[serde(default)]
    radarr: HashMap<String, ThinRadarrInstance>,
    #[serde(default)]
    sonarr: HashMap<String, ThinSonarrInstance>,
}

/// `[default]` section of the toml config file
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DefaultSection {
    frequency: String,
    count: u32,
    search_granularity: SonarrSearchGranularity,
}

impl Default for DefaultSection {
    fn default() -> Self {
        Self {
            frequency: DEFAULT_FREQUENCY.into(),
            count: DEFAULT_COUNT,
            search_granularity: DEFAULT_GRANULARITY,
        }
    }
}

impl Config {
    pub fn get_radarr_instances(&self) -> HashMap<String, RadarrInstance> {
        self.radarr
            .clone()
            .into_iter()
            .map(|(key, thin)| {
                let full_instance = RadarrInstance {
                    url: thin.url,
                    api_key: thin.api_key,
                    frequency: thin
                        .frequency
                        .unwrap_or_else(|| self.default.frequency.clone()),
                    count: thin.count.unwrap_or(self.default.count),
                };
                (key, full_instance)
            })
            .collect()
    }

    pub fn get_sonarr_instances(&self) -> HashMap<String, SonarrInstance> {
        self.sonarr
            .clone()
            .into_iter()
            .map(|(key, thin)| {
                let full_instance = SonarrInstance {
                    url: thin.url,
                    api_key: thin.api_key,
                    frequency: thin.frequency.unwrap_or(self.default.frequency.clone()),
                    count: thin.count.unwrap_or(self.default.count),
                    search_granularity: thin
                        .search_granularity
                        .unwrap_or(self.default.search_granularity),
                };
                (key, full_instance)
            })
            .collect()
    }
}

pub fn load_config(path: impl AsRef<Path>) -> anyhow::Result<Config> {
    let contents = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            log::error!("Config file not found, is this the first start?");
            log::error!(
                "Generating a default config file, please edit it before restarting the app"
            );
            let default = include_bytes!("../../config.sample.toml");
            std::fs::write(path, default)
                .with_context(|| "failed to write default config to {path}")?;
            std::process::exit(1);
        }
        Err(e) => return Err(e).with_context(|| "failed to read config"),
    };

    toml::from_slice(&contents).with_context(|| "failed to parse config")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::assert_matches;

    use std::io::{self, Write};

    use tempfile::NamedTempFile;

    fn create_config(contents: String) -> io::Result<NamedTempFile> {
        let mut file = tempfile::NamedTempFile::new()?;
        write!(file, "{}", contents)?;
        Ok(file)
    }

    #[test]
    fn config_can_parse_empty() {
        let file = create_config("".to_owned()).unwrap();
        load_config(file.path()).expect("parsing shouldn't fail");
    }

    /// Ensure no regression in config parsing
    #[test]
    fn config_can_parse_multiple_radarr() {
        let file = create_config(
            r#"
        [radarr.one]
        url = "foo"
        api_key = "foo"

        [radarr.two]
        url = "bar"
        api_key = "bar"
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).expect("parsing shouldn't fail");
        let radarr = config.radarr;
        assert_eq!(radarr.len(), 2);
        let one = radarr
            .get("one")
            .expect("radarr instance named one should exist");
        assert_eq!(one.url, "foo");
        assert_eq!(one.api_key, "foo");
        let two = radarr
            .get("two")
            .expect("radarr instance named two should exist");
        assert_eq!(two.url, "bar");
        assert_eq!(two.api_key, "bar");
    }

    /// When instance doesn't define frequency, use default
    #[test]
    fn config_uses_default_frequency() {
        let file = create_config(
            r#"
        [default]
        frequency = "default_freq"

        [radarr.one]
        url = ""
        api_key = ""
        [radarr.two]
        url = ""
        api_key = ""
        frequency = "overwrite_freq"

        [sonarr.one]
        url = ""
        api_key = ""
        [sonarr.two]
        url = ""
        api_key = ""
        frequency = "overwrite_freq"
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();

        let radarr = config.get_radarr_instances();
        let one = radarr.get("one").expect("should have instance named one");
        assert_eq!(one.frequency, "default_freq");
        let two = radarr.get("two").expect("should have instance named two");
        assert_eq!(two.frequency, "overwrite_freq");

        let sonarr = config.get_sonarr_instances();
        let one = sonarr.get("one").expect("should have instance named one");
        assert_eq!(one.frequency, "default_freq");
        let two = sonarr.get("two").expect("should have instance named two");
        assert_eq!(two.frequency, "overwrite_freq");
    }

    /// When both the default section and instance don't define frequency, then use
    /// [DEFAULT_FREQUENCY]
    #[test]
    fn config_uses_default_default_frequency() {
        let file = create_config(
            r#"
        [radarr.one]
        url = ""
        api_key = ""
        [sonarr.one]
        url = ""
        api_key = ""
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();

        let radarr = config.get_radarr_instances();
        let one = radarr.get("one").expect("should have instance named one");
        assert_eq!(one.frequency, DEFAULT_FREQUENCY);

        let sonarr = config.get_sonarr_instances();
        let one = sonarr.get("one").expect("should have instance named one");
        assert_eq!(one.frequency, DEFAULT_FREQUENCY);
    }

    /// When instance doesn't define count, use default
    #[test]
    fn config_uses_default_count() {
        let file = create_config(
            r#"
        [default]
        count = 999

        [radarr.one]
        url = ""
        api_key = ""
        [radarr.two]
        url = ""
        api_key = ""
        count = 111

        [sonarr.one]
        url = ""
        api_key = ""
        [sonarr.two]
        url = ""
        api_key = ""
        count = 111
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();

        let radarr = config.get_radarr_instances();
        let one = radarr.get("one").expect("should have instance named one");
        assert_eq!(one.count, 999);
        let two = radarr.get("two").expect("should have instance named two");
        assert_eq!(two.count, 111);

        let sonarr = config.get_sonarr_instances();
        let one = sonarr.get("one").expect("should have instance named one");
        assert_eq!(one.count, 999);
        let two = sonarr.get("two").expect("should have instance named two");
        assert_eq!(two.count, 111);
    }

    /// When both the default section and instance don't define count, then use [DEFAULT_COUNT]
    #[test]
    fn config_uses_default_default_count() {
        let file = create_config(
            r#"
        [radarr.one]
        url = ""
        api_key = ""
        [sonarr.one]
        url = ""
        api_key = ""
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();

        let radarr = config.get_radarr_instances();
        let one = radarr.get("one").expect("should have instance named one");
        assert_eq!(one.count, DEFAULT_COUNT);

        let sonarr = config.get_sonarr_instances();
        let one = sonarr.get("one").expect("should have instance named one");
        assert_eq!(one.count, DEFAULT_COUNT);
    }

    /// When sonarr instance doesn't define granularity, use default
    #[test]
    fn config_uses_default_granularity() {
        let file = create_config(
            r#"
        [default]
        search_granularity = "show"

        [sonarr.one]
        url = ""
        api_key = ""
        [sonarr.two]
        url = ""
        api_key = ""
        search_granularity = "season"
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        let sonarr = config.get_sonarr_instances();

        let one = sonarr.get("one").expect("should have instance named one");
        assert_matches!(one.search_granularity, SonarrSearchGranularity::Show);
        let two = sonarr.get("two").expect("should have instance named two");
        assert_matches!(two.search_granularity, SonarrSearchGranularity::Season);
    }

    /// When both the default section and instance don't define granularity, then use
    /// [DEFAULT_GRANULARITY]
    #[test]
    fn config_uses_default_default_granularity() {
        let file = create_config(
            r#"
        [sonarr.one]
        url = ""
        api_key = ""
            "#
            .to_owned(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        let sonarr = config.get_sonarr_instances();

        let one = sonarr.get("one").expect("should have instance named one");
        assert_matches!(one.search_granularity, DEFAULT_GRANULARITY);
    }
}
