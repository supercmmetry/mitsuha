use async_trait::async_trait;

use crate::{
    config::{self},
    module::ModuleInfo,
};

use super::Provider;

#[derive(Clone)]
pub struct ApiProvider {
    client: reqwest::Client,
    config: config::provider::ApiProvider,
}

impl ApiProvider {
    pub fn new(client: reqwest::Client, config: config::provider::ApiProvider) -> Self {
        Self { client, config }
    }

    pub fn make_request<T: ToString>(
        &self,
        method: reqwest::Method,
        route: T,
    ) -> reqwest::RequestBuilder {
        self.client.request(
            method,
            self.config.address.clone() + route.to_string().as_str(),
        )
    }
}

#[async_trait(?Send)]
impl Provider for ApiProvider {
    async fn get_raw_module(&self, module_info: &ModuleInfo) -> anyhow::Result<Vec<u8>> {
        let module_name = module_info.name.as_str();
        let version = module_info.version.as_str();

        match module_info.modtype.clone() {
            crate::module::ModuleType::WASM => {
                let response = self
                    .make_request(reqwest::Method::GET, self.config.wasm.get.as_str())
                    .query(&[("module", module_name), ("version", version)])
                    .send()
                    .await?
                    .error_for_status()?;

                Ok(response.bytes().await?.to_vec())
            }
            crate::module::ModuleType::SERVICE => {
                let response = self
                    .make_request(reqwest::Method::GET, self.config.service.get.as_str())
                    .query(&[("module", module_name), ("version", version)])
                    .send()
                    .await?
                    .error_for_status()?;

                Ok(response.bytes().await?.to_vec())
            }
            x => Err(anyhow::anyhow!("cannot provide modules with type: {}", x)),
        }
    }
}
