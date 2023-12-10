musubi_bindings::musubi_info!("../musubi.json");

use musubi_api::DataBuilder;
use musubi_api::types::Data;
use musubi_bindings::musubi_export;

#[allow(unreachable_code)]
#[musubi_export(module = "mitsuha.test.loop")]
fn run(_input: Data) -> Data {
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