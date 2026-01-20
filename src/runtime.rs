use std::path::PathBuf;

pub const SOCKET_ENV: &str = "NUEFSD_SOCKET";

pub fn default_socket_path() -> PathBuf {
    if let Some(path) = std::env::var_os(SOCKET_ENV) {
        return PathBuf::from(path);
    }

    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));

    let uid = unsafe { libc::geteuid() };
    base.join(format!("nuefsd-{uid}.sock"))
}

