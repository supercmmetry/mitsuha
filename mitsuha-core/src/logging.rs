use crate::config::Config;

pub fn setup_logging() -> anyhow::Result<()> {
    let config_dir = Config::get_config_dir()?;
    let log_config_filename = format!("{}/log4rs.yml", config_dir);

    log4rs::init_file(log_config_filename, Default::default())?;

    Ok(())
}
