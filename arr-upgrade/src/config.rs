use std::{collections::HashMap, path::Path};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ArrService {
    pub url: String,
    pub api_key: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub radarr: HashMap<String, ArrService>,
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
