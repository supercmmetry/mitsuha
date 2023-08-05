pub mod channel;

pub trait Service {
    fn register_rpc(
        &self,
        server: tonic::transport::server::Router,
    ) -> tonic::transport::server::Router;
}
