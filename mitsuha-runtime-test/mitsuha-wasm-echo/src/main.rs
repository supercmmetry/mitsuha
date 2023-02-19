musubi_bindings::musubi_info!("../musubi.json");

use musubi_api::types::Data;
use musubi_bindings::musubi_export;

#[musubi_export(module = "mitsuha.test.echo")]
fn echo(input: Data) -> Data {
    input
}

fn main() {}
