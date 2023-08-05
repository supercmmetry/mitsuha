use mitsuha_channel::context::ChannelContext;
use mitsuha_runtime_rpc::{model::channel::channel_proto, proto};

use super::Service;

#[derive(Clone)]
pub struct ChannelService {
    context: ChannelContext,
}

#[tonic::async_trait]
impl proto::channel::channel_server::Channel for ChannelService {
    async fn compute(
        &self,
        request: tonic::Request<proto::channel::ComputeRequest>,
    ) -> tonic::Result<tonic::Response<proto::channel::ComputeResponse>> {
        let ctx = self.context.clone();

        let compute_input = request
            .into_inner()
            .try_into()
            .map_err(|e: anyhow::Error| tonic::Status::internal(e.to_string()))?;

        let compute_output = self
            .context
            .channel_start
            .clone()
            .unwrap()
            .compute(ctx, compute_input)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let compute_response = compute_output
            .try_into()
            .map_err(|e: anyhow::Error| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(compute_response))
    }
}

impl ChannelService {
    pub fn new(channel_context: ChannelContext) -> Box<dyn Service> {
        Box::new(Self {
            context: channel_context,
        })
    }
}

impl Service for ChannelService {
    fn register_rpc(
        &self,
        server: tonic::transport::server::Router,
    ) -> tonic::transport::server::Router {
        let reflector = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(channel_proto::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        server
            .add_service(proto::channel::channel_server::ChannelServer::new(
                self.clone(),
            ))
            .add_service(reflector)
    }
}
