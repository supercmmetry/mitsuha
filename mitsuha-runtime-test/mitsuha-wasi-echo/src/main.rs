musubi_bindings::musubi_info!("../musubi.json");

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{Read, Write};
use mitsuha_bindings::mitsuha_export;
use musubi_api::types::{Data, Value};
use musubi_bindings::musubi_export;

#[mitsuha_export(module = "mitsuha.test.wasi.echo")]
fn echo(mut input: Data) -> Data {
    let v = input.values().get(0).cloned().unwrap();
    let msg: String;

    match v {
        Value::String(s) => { msg = s; },
        _ => panic!("expected string"),
    }

    std::fs::create_dir("/").unwrap();

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open("/sample.txt")
            .unwrap();

        file.write_all(msg.as_bytes()).unwrap();
        file.flush().unwrap();
    }

    {
        let mut read_msg = String::new();

        let mut file = File::open("/sample.txt").unwrap();

        assert_eq!(file.metadata().unwrap().len(), 12);

        file.read_to_string(&mut read_msg).unwrap();

        *input.values_mut().get_mut(0).unwrap() = Value::String(read_msg);
    }


    input
}

fn main() {}
