use mitsuha_core_types::module::ModuleInfo;

pub trait Module<T> {
    fn get_info(&self) -> ModuleInfo;

    fn get_musubi_spec(&mut self) -> anyhow::Result<musubi_api::types::Spec>;

    fn inner(&self) -> &T;
}
