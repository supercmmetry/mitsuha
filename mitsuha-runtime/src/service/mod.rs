use std::sync::Arc;

use mitsuha_channel::context::ChannelContext;
use mitsuha_core::channel::ComputeChannel;

pub mod channel;

pub trait Service {
    fn register_rpc(
        &self,
        server: tonic::transport::server::Router,
    ) -> tonic::transport::server::Router;
}
