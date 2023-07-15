use anyhow::anyhow;

pub fn generate_queue_handle(id: u64) -> String {
    format!("mitsuha/qflow/queue/{}", id)
}

pub fn generate_queue_element_handle(id: u64, elem_idx: u64) -> String {
    format!("{}/elem/{}", generate_queue_handle(id), elem_idx)
}

pub fn generate_queue_offset_handle(id: u64) -> String {
    format!("{}/offset", generate_queue_handle(id))
}

pub fn generate_queue_length_handle(id: u64) -> String {
    format!("{}/length", generate_queue_handle(id))
}

pub fn generate_queue_lock_handle(id: u64) -> String {
    format!("{}/lock", generate_queue_handle(id))
}

pub fn generate_sticky_queue_handle(suffix: String) -> String {
    format!("mitsuha/qflow/sticky/{}", suffix)
}

pub fn generate_sticky_element_handle(id: String, elem_idx: u64) -> String {
    format!("{}/elem/{}", generate_sticky_queue_handle(id), elem_idx)
}

pub fn generate_sticky_element_trigger_handle(elem_id: String) -> String {
    format!(
        "{}/{}",
        generate_sticky_queue_handle("trigger".to_string()),
        elem_id
    )
}

pub fn unwrap_sticky_element_trigger_handle(s: String) -> anyhow::Result<String> {
    let v: Vec<&str> = s
        .split(generate_sticky_queue_handle("trigger".to_string()).as_str())
        .collect();

    if v.len() != 2 {
        return Err(anyhow!(
            "failed to parse sticky element trigger handle: {}",
            s
        ));
    }

    Ok(v[1].to_string())
}

pub fn generate_sticky_queue_offset_handle(node_id: String) -> String {
    format!("{}/offset", generate_sticky_queue_handle(node_id))
}

pub fn generate_sticky_queue_length_handle(node_id: String) -> String {
    format!("{}/length", generate_sticky_queue_handle(node_id))
}

pub fn generate_queue_count_handle() -> String {
    "mitsuha/qflow/queue/count".to_string()
}

pub fn vec_to_u64(data: Vec<u8>) -> anyhow::Result<u64> {
    let result = data.try_into();
    if result.is_err() {
        return Err(anyhow!("failed to parse u64 data"));
    }

    Ok(u64::from_le_bytes(result.unwrap()))
}
