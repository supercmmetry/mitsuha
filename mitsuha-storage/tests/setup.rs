use std::sync::Once;

static LOG_INIT_ONCE: Once = Once::new();

pub fn init_basic_logging() {
    LOG_INIT_ONCE.call_once(|| {
        env_logger::init();
    });
}
