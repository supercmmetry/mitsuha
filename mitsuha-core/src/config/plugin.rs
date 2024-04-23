use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Plugin {
    pub kind: String,
    pub name: String,

    #[serde(skip)]
    pub config: PluginConfiguration,
}

impl Plugin {
    pub fn with_configuration(mut self) -> anyhow::Result<Self> {
        self.config = PluginConfiguration::new(self.name.as_str())?;

        Ok(self)
    }
}

fn default_spec() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

#[derive(Default, Deserialize, Debug, Clone)]
pub struct PluginConfiguration {
    #[serde(default = "default_spec")]
    pub spec: serde_json::Value,
}

impl PluginConfiguration {
    pub fn new(name: &str) -> anyhow::Result<Self> {
        let config_dir = super::Config::get_config_dir()?;

        let config = config::Config::builder()
            .add_source(config::File::with_name(&format!("{}/plugin.{}", config_dir, name)))
            .build();

        if config.is_err() {
            let mut obj = Self::default();
            obj.spec = default_spec();
            return Ok(obj);
        }

        Ok(config?.try_deserialize()?)
    }

    pub fn get_spec<T>(&self) -> anyhow::Result<T> where for<'de> T: Deserialize<'de> {
        Ok(serde_json::from_value(self.spec.clone())?)
    }
}
