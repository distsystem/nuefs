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

    let socket = socket.unwrap_or_else(_nuefs::runtime::default_socket_path);
    if let Err(e) = _nuefs::daemon::server::serve(socket) {
        eprintln!("nuefsd: fatal error: {e}");
        std::process::exit(1);
    }
}

fn print_help() {
    eprintln!("Usage: nuefsd [--socket PATH]");
}
