use std::path::PathBuf;

fn main() {
    let mut socket: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = args.next().map(PathBuf::from);
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            other => {
                eprintln!("nuefsd: unknown argument: {other}");
                print_help();
                std::process::exit(2);
            }
        }
    }

    let socket = socket.unwrap_or_else(default_socket_path);
    if let Err(e) = _nuefs::daemon::server::serve(socket) {
        eprintln!("nuefsd: fatal error: {e}");
        std::process::exit(1);
    }
}

fn default_socket_path() -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));

    let uid = unsafe { libc::geteuid() };
    base.join(format!("nuefsd-{uid}.sock"))
}

fn print_help() {
    eprintln!("Usage: nuefsd [--socket PATH]");
}

