use actix_web::dev::{ServiceFactory, ServiceRequest};
use actix_web::{App, HttpServer};
use mitsuha_core::config::Config;

mod prometheus;

macro_rules! register_routes {
    ($app: ident, $b: block) => {
        use actix_web::dev::{ServiceFactory, ServiceRequest};

        pub fn register_routes<T>($app: actix_web::App<T>) -> actix_web::App<T>
        where
            T: ServiceFactory<
                ServiceRequest,
                Config = (),
                Error = actix_web::error::Error,
                InitError = (),
            >,
        {
            $b
        }
    };
}

pub(crate) use register_routes;

pub fn register_all_routes<T>(mut app: App<T>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = actix_web::error::Error, InitError = ()>,
{
    app = prometheus::register_routes(app);

    app
}

pub async fn start_server(config: Config) -> anyhow::Result<()> {
    let server = HttpServer::new(move || {
        let app = App::new()
            .wrap(actix_web::middleware::NormalizePath::default())
            .wrap(actix_cors::Cors::permissive());

        register_all_routes(app)
    })
    .bind((config.api.address.clone(), config.api.http_port as u16))?;

    server.run().await?;

    Ok(())
}
