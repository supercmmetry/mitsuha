musubi_bindings::musubi_info!("../musubi.json");

use musubi_api::DataBuilder;
use musubi_api::types::{Data, Value};
use musubi_bindings::musubi_export;

#[musubi_export(module = "mitsuha.test.loop")]
fn run(input: Data) -> Data {
    loop {}

    DataBuilder::new()
        .build()
}

#[no_mangle]
extern "C" fn mugen_loop() {
    loop {}
}

fn main() {
}