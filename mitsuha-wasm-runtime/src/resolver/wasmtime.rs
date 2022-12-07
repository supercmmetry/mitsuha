use std::collections::HashMap;

use mitsuha_core::{
    errors::Error,
    module::{Module, ModuleInfo},
    types,
};
use num_traits::cast::FromPrimitive;

#[derive(Clone)]
pub struct WasmMetadata {
    spec: musubi_api::types::Spec,
}

impl WasmMetadata {
    pub fn new(mut data: &[u8]) -> anyhow::Result<Self> {
        let mut musubi_info_sections: Vec<Vec<u8>> = vec![];
        let mut musubi_header_sections: Vec<Vec<u8>> = vec![];
        let mut parser = wasmparser::Parser::new(0);

        loop {
            match parser.parse(data, true)? {
                wasmparser::Chunk::NeedMoreData(_) => {
                    return Err(anyhow::anyhow!(
                        "unexpected error occured while parsing WASM"
                    ))
                }
                wasmparser::Chunk::Parsed { payload, consumed } => {
                    data = &data[consumed..];

                    match payload {
                        wasmparser::Payload::CustomSection(s) => match s.name() {
                            ".musubi_info" => musubi_info_sections.push(s.data().to_vec()),
                            ".musubi_header" => musubi_header_sections.push(s.data().to_vec()),
                            _ => {}
                        },
                        wasmparser::Payload::End { .. } => {
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        if musubi_info_sections.len() == 0 {
            return Err(anyhow::anyhow!(
                "could not find section .musubi_info in WASM binary"
            ));
        }

        if musubi_info_sections.len() > 1 {
            return Err(anyhow::anyhow!(
                "duplicate .musubi_info sections found in WASM binary"
            ));
        }

        let raw = musubi_info_sections.get(0).unwrap().clone();
        let info: musubi_api::types::Info = serde_json::from_slice(raw.as_slice())?;

        let mut headers: HashMap<musubi_api::types::Header, Vec<u8>> = Default::default();
        for section in musubi_header_sections {
            if let Some(key) = section.get(0) {
                if let Some(header) = musubi_api::types::Header::from_u8(*key) {
                    headers.insert(header, section[1..].to_vec());
                }
            }
        }

        let spec = musubi_api::types::Spec { info, headers };

        Ok(Self { spec })
    }

    pub fn get_musubi_spec(&self) -> musubi_api::types::Spec {
        self.spec.clone()
    }
}

#[derive(Clone)]
pub struct WasmtimeModule {
    inner: wasmtime::Module,
    info: ModuleInfo,
    metadata: WasmMetadata,
}

impl Module<wasmtime::Module> for WasmtimeModule {
    fn get_info(&self) -> ModuleInfo {
        self.info.clone()
    }

    fn inner(&self) -> &wasmtime::Module {
        &self.inner
    }

    fn get_musubi_spec(&mut self) -> anyhow::Result<musubi_api::types::Spec> {
        Ok(self.metadata.get_musubi_spec())
    }
}

impl WasmtimeModule {
    pub fn new(data: Vec<u8>, module_info: ModuleInfo) -> types::Result<Self> {
        let metadata = WasmMetadata::new(data.as_slice()).map_err(|e| Error::WasmError {
            message: "failed to parse musubi metadata".to_string(),
            inner: module_info.clone(),
            source: e,
        })?;

        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::from_binary(&engine, data.as_slice()).map_err(|e| {
            Error::ModuleLoadFailed {
                message: "failed to load wasm module".to_string(),
                inner: module_info.clone(),
                source: e,
            }
        })?;

        Ok(Self {
            metadata,
            inner: module,
            info: module_info,
        })
    }
}
