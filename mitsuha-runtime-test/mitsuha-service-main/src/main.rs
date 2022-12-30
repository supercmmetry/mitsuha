use actix_web::{App, HttpServer, get, post};

#[get("/mitsuha/health")]
async fn mitsuha_health() -> String {
    println!("Received health request!");
    format!("Running")
}

#[post("/api/hello")]
async fn hello() -> String {
    println!("Received data request!");
    format!("Hello world!")
}

#[actix_web::main]
async fn start_server() {
    HttpServer::new(move || {
        App::new()
            .service(mitsuha_health)
            .service(hello)
    })
    .bind(("0.0.0.0", 1728))
    .unwrap()
    .run()
    .await
    .unwrap();
}

fn main() {
    start_server();
}
