use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::{
    module::{ModuleInfo, ModuleType},
    types::SharedAsyncMany,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    pub module_info: ModuleInfo,
    pub name: String,
}

impl Symbol {
    pub fn to_exported_function_symbol(&self) -> String {
        format!(
            "__musubi_exported_function__mod__{}__fun__{}",
            musubi_api::utils::sanitize_module_name(self.module_info.name.as_str()),
            self.name.as_str()
        )
    }

    pub fn to_imported_function_symbol(&self) -> String {
        format!(
            "__musubi_imported_function__mod__{}__fun__{}",
            musubi_api::utils::sanitize_module_name(self.module_info.name.as_str()),
            self.name.as_str()
        )
    }

    pub fn parse_musubi_function_symbol(symbol: &str) -> anyhow::Result<(String, String)> {
        let split_1: Vec<&str> = symbol.split("__mod__").into_iter().collect();
        if split_1.len() != 2 {
            return Err(anyhow::anyhow!(
                "expected exactly one __mod__ specifier in symbol: {}",
                symbol
            ));
        }

        let rhs = split_1.get(1).unwrap().clone();

        let split_2: Vec<&str> = rhs.split("__fun__").into_iter().collect();
        if split_2.len() != 2 {
            return Err(anyhow::anyhow!(
                "expected exactly one __fun__ specifier in symbol: {}",
                symbol
            ));
        }

        let unsanitized_module_name = split_2.get(0).unwrap().clone().replace("_", ".");
        let function_name = split_2.get(1).unwrap().clone();

        if !musubi_api::utils::validate_module_name(unsanitized_module_name.as_str()) {
            return Err(anyhow::anyhow!(
                "invalid module name '{}' found in symbol: {}",
                unsanitized_module_name,
                symbol
            ));
        }

        Ok((unsanitized_module_name, function_name.to_string()))
    }

    pub fn from_imported_function_symbol(
        symbol: &str,
        modtype: ModuleType,
        version: &str,
    ) -> anyhow::Result<Self> {
        let (module_name, function_name) = Self::parse_musubi_function_symbol(symbol)?;
        if !musubi_api::utils::verify_imported_symbol(
            symbol,
            musubi_api::utils::sanitize_module_name(module_name.as_str()).as_str(),
        ) {
            return Err(anyhow::anyhow!("invalid musubi import symbol"));
        }

        Ok(Self {
            module_info: ModuleInfo {
                name: module_name,
                version: version.to_string(),
                modtype,
            },
            name: function_name,
        })
    }

    pub fn from_exported_function_symbol(
        symbol: &str,
        modtype: ModuleType,
        version: &str,
    ) -> anyhow::Result<Self> {
        let (module_name, function_name) = Self::parse_musubi_function_symbol(symbol)?;
        if !musubi_api::utils::verify_exported_symbol(
            symbol,
            musubi_api::utils::sanitize_module_name(module_name.as_str()).as_str(),
        ) {
            return Err(anyhow::anyhow!("invalid musubi export symbol"));
        }

        Ok(Self {
            module_info: ModuleInfo {
                name: module_name,
                version: version.to_string(),
                modtype,
            },
            name: function_name,
        })
    }
}

pub type SymbolFunc = SharedAsyncMany<dyn Fn(Vec<u8>) -> BoxFuture<'static, Vec<u8>> + Send + Sync>;
