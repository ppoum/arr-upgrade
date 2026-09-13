use std::{collections::HashMap, path::Path};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct ThinArrInstance {
    pub url: String,
    pub api_key: String,
    pub frequency: Option<String>,
    pub count: Option<u32>,
}

pub struct ArrInstance {
    pub url: String,
    pub api_key: String,
    pub frequency: String,
    pub count: u32,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    default: DefaultSection,
    #[serde(default)]
    radarr: HashMap<String, ThinArrInstance>,
}

/// `[default]` section of the toml config file
#[derive(Debug, Deserialize)]
struct DefaultSection {
    #[serde(default = "config_default_frequency")]
    frequency: String,
    #[serde(default = "config_default_count")]
    count: u32,
}

impl Default for DefaultSection {
    fn default() -> Self {
        Self {
            frequency: config_default_frequency(),
            count: config_default_count(),
        }
    }
}

impl Config {
    pub fn get_radarr_instances(&self) -> HashMap<String, ArrInstance> {
        self.radarr
            .clone()
            .into_iter()
            .map(|(key, thin)| {
                let full_instance = ArrInstance {
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
}

fn config_default_frequency() -> String {
    "hourly".into()
}

fn config_default_count() -> u32 {
    5
}

pub fn load_config(path: impl AsRef<Path>) -> anyhow::Result<Config> {
    let contents = std::fs::read(path).with_context(|| "failed to read config")?;
    toml::from_slice(&contents).with_context(|| "failed to parse config")
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
