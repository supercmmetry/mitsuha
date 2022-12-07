use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct WasmRouteSpec {
    pub get: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceRouteSpec {
    pub get: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiProvider {
    pub address: String,
    pub wasm: WasmRouteSpec,
    pub service: ServiceRouteSpec,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Provider {
    pub api: ApiProvider,
}
