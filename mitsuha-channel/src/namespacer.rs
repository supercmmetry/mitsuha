use std::sync::Arc;

use mitsuha_core::{
    channel::{ComputeChannel, ComputeInputExt},
    constants::Constants,
    errors::Error,
    types,
};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};

use crate::{context::ChannelContext, WrappedComputeChannel};

use async_trait::async_trait;

pub struct NamespacerChannel {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    id: String,
}

#[async_trait]
impl ComputeChannel for NamespacerChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        mut elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        let mut namespace: Option<String> = None;

        if let Some(extensions) = elem.get_extensions() {
            namespace = extensions
                .get(&Constants::ChannelNamespace.to_string())
                .cloned();
        }

        if let Some(namespace) = namespace {
            match &mut elem {
                ComputeInput::Run { spec } => {
                    spec.handle = Self::get_namespaced_handle(&namespace, &spec.handle);

                    spec.extensions.insert(
                        Constants::ModuleResolverPrefix.to_string(),
                        Self::get_namespaced_handle(&namespace, &String::new()),
                    );
                }
                ComputeInput::Extend { handle, .. } => {
                    *handle = Self::get_namespaced_handle(&namespace, &handle);
                }
                ComputeInput::Status { handle, .. } => {
                    *handle = Self::get_namespaced_handle(&namespace, &handle);
                }
                ComputeInput::Abort { handle, .. } => {
                    *handle = Self::get_namespaced_handle(&namespace, &handle);
                }
                ComputeInput::Store { spec } => {
                    spec.handle = Self::get_namespaced_handle(&namespace, &spec.handle);
                }
                ComputeInput::Load { handle, .. } => {
                    *handle = Self::get_namespaced_handle(&namespace, &handle);
                }
                ComputeInput::Persist { handle, .. } => {
                    *handle = Self::get_namespaced_handle(&namespace, &handle);
                }
                ComputeInput::Clear { handle, .. } => {
                    *handle = Self::get_namespaced_handle(&namespace, &handle);
                }
            }
        }

        match self.next.read().await.clone() {
            Some(chan) => chan.compute(ctx, elem).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl NamespacerChannel {
    pub(super) fn get_namespaced_handle(namespace: &String, handle: &String) -> String {
        format!("namespace/{}/{}", namespace, handle)
    }

    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/namespacer"
    }

    pub fn new() -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            next: Arc::new(tokio::sync::RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use lazy_static::lazy_static;
    use mitsuha_core::{
        channel::ComputeChannel,
        constants::Constants,
        types,
    };
    use mitsuha_core_types::{channel::{ComputeInput, ComputeOutput}, symbol::Symbol, kernel::{JobSpec, StorageSpec}, module::ModuleInfo};
    use tokio::sync::RwLock;

    use crate::context::ChannelContext;

    use super::NamespacerChannel;

    #[derive(Clone)]
    struct TestSinkChannel {
        input: Arc<RwLock<Option<ComputeInput>>>,
    }

    #[async_trait]
    impl ComputeChannel for TestSinkChannel {
        type Context = ChannelContext;

        fn id(&self) -> String {
            "mitsuha/test/channel/testsink".to_string()
        }

        async fn compute(
            &self,
            _ctx: ChannelContext,
            elem: ComputeInput,
        ) -> types::Result<ComputeOutput> {
            *self.input.write().await = Some(elem);

            Ok(ComputeOutput::Completed)
        }

        async fn connect(&self, _next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
            unimplemented!()
        }
    }

    impl TestSinkChannel {
        pub fn new() -> Self {
            TestSinkChannel {
                input: Arc::new(RwLock::new(None)),
            }
        }

        pub async fn get_input(&self) -> Option<ComputeInput> {
            self.input.read().await.clone()
        }
    }

    lazy_static! {
        static ref NAMESPACE: String = "samplens".to_string();
        static ref EXTENSIONS: HashMap<String, String> =
            [(Constants::ChannelNamespace.to_string(), NAMESPACE.clone())]
                .into_iter()
                .collect();
    }

    #[tokio::test]
    async fn test_namespaced_run() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Run {
                    spec: JobSpec {
                        handle: "job/sample".to_string(),
                        symbol: Symbol {
                            module_info: ModuleInfo {
                                name: "mitsuha.test".to_string(),
                                version: "0.1.0".to_string(),
                                modtype: mitsuha_core_types::module::ModuleType::WASM,
                            },
                            name: "run".to_string(),
                        },
                        input_handle: "job/sample/input-1".to_string(),
                        output_handle: "job/sample/output-1".to_string(),
                        ttl: 10,
                        extensions: EXTENSIONS.clone(),
                    },
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Run { spec } => {
                assert_eq!(
                    spec.extensions
                        .get(&Constants::ModuleResolverPrefix.to_string())
                        .unwrap()
                        .clone(),
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &String::new())
                );

                assert_eq!(
                    spec.handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"job/sample".to_string())
                );

                // Other handles in the job should remain unchanged otherwise we might have
                // redundant namespacing issues.

                assert_eq!(spec.input_handle, "job/sample/input-1".to_string());
                assert_eq!(spec.output_handle, "job/sample/output-1".to_string());
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_extend() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Extend {
                    handle: "job/sample".to_string(),
                    ttl: 10,
                    extensions: EXTENSIONS.clone(),
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Extend { handle, .. } => {
                assert_eq!(
                    handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"job/sample".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_status() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Status {
                    handle: "job/sample".to_string(),
                    extensions: EXTENSIONS.clone(),
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Status { handle, .. } => {
                assert_eq!(
                    handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"job/sample".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_abort() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Abort {
                    handle: "job/sample".to_string(),
                    extensions: EXTENSIONS.clone(),
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Abort { handle, .. } => {
                assert_eq!(
                    handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"job/sample".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_store() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Store {
                    spec: StorageSpec {
                        handle: "data-1".to_string(),
                        data: vec![],
                        ttl: 10,
                        extensions: EXTENSIONS.clone(),
                    },
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Store { spec } => {
                assert_eq!(
                    spec.handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"data-1".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_load() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Load {
                    handle: "data-1".to_string(),
                    extensions: EXTENSIONS.clone(),
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Load { handle, .. } => {
                assert_eq!(
                    handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"data-1".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_persist() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Persist {
                    handle: "data-1".to_string(),
                    ttl: 10,
                    extensions: EXTENSIONS.clone(),
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Persist { handle, .. } => {
                assert_eq!(
                    handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"data-1".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }

    #[tokio::test]
    async fn test_namespaced_clear() {
        let channel = NamespacerChannel::new();

        let raw_sink = TestSinkChannel::new();

        let sink: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(raw_sink.clone()));

        channel.connect(sink.clone()).await;

        channel
            .compute(
                Default::default(),
                ComputeInput::Clear {
                    handle: "data-1".to_string(),
                    extensions: EXTENSIONS.clone(),
                },
            )
            .await
            .unwrap();

        let input = raw_sink.get_input().await.unwrap();

        match input {
            ComputeInput::Clear { handle, .. } => {
                assert_eq!(
                    handle,
                    NamespacerChannel::get_namespaced_handle(&NAMESPACE, &"data-1".to_string())
                );
            }
            _ => panic!("invalid compute input"),
        }
    }
}
