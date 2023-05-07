use serde::{Deserialize, Serialize};

#[derive(strum_macros::Display, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ModuleType {
    #[strum(serialize = "wasm")]
    WASM,

    #[strum(serialize = "service")]
    SERVICE,

    #[strum(serialize = "unknown")]
    UNKNOWN,
}

impl Default for ModuleType {
    fn default() -> Self {
        Self::UNKNOWN
    }
}

impl From<musubi_api::types::ModuleType> for ModuleType {
    fn from(data: musubi_api::types::ModuleType) -> Self {
        match data {
            musubi_api::types::ModuleType::WASM => Self::WASM,
            musubi_api::types::ModuleType::SERVICE => Self::SERVICE,
            musubi_api::types::ModuleType::UNKNOWN => Self::UNKNOWN,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModuleInfo {
    pub name: String,
    pub version: String,
    pub modtype: ModuleType,
}

impl From<musubi_api::types::Dep> for ModuleInfo {
    fn from(data: musubi_api::types::Dep) -> Self {
        Self {
            name: data.name,
            version: data.version,
            modtype: data.modtype.into(),
        }
    }
}

impl ModuleInfo {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/core/moduleinfo"
    }

    pub fn get_identifier(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            Self::get_identifier_type(),
            self.modtype.to_string(),
            self.name,
            self.version
        )
    }

    pub fn equals_identifier_type(s: &String) -> bool {
        return s.starts_with(Self::get_identifier_type());
    }
}

pub trait Module<T> {
    fn get_info(&self) -> ModuleInfo;

    fn get_musubi_spec(&mut self) -> anyhow::Result<musubi_api::types::Spec>;

    fn inner(&self) -> &T;
}
