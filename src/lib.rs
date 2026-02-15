pub mod daemon;
pub mod runtime;
pub mod types;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/nuefs.rs"));
}
