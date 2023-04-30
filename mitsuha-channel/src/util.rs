use rand::{distributions::Alphanumeric, Rng};

pub fn generate_random_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}
