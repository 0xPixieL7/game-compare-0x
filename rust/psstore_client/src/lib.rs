pub use crate::root::*;

mod root;
pub fn get_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|err| panic!("Missing env. key={key}, error={err}"))
}