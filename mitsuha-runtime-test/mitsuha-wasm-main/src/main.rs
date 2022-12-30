extern crate mitsuha_bindings;

musubi_bindings::musubi_info!("../musubi.json");

use musubi_api::types::Data;
use musubi_bindings::musubi_export;

pub mod mitsuha {
    pub mod test {
        pub mod echo {
            use musubi_api::types::Data;

            musubi_bindings::musubi_import!(module = "mitsuha.test.echo", function = "echo");
        }

        pub mod loopy {
            use musubi_api::types::Data;

            musubi_bindings::musubi_import!(module = "mitsuha.test.loop", function = "run");
        }
    }
}

#[musubi_export(module = "mitsuha.test.main")]
fn run(input: Data) -> Data {
    mitsuha::test::echo::echo(input)
}

#[musubi_export(module = "mitsuha.test.main")]
fn run_loop(input: Data) -> Data {
    mitsuha::test::loopy::run(input)
}

fn main() {}
