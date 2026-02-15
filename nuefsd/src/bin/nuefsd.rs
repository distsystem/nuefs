use std::fs::File;
use std::path::PathBuf;

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn default_log_path() -> String {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        format!("{runtime_dir}/nuefsd.log")
    } else {
        "/tmp/nuefsd.log".to_string()
    }
}

#[tokio::main]
async fn main() {
    let mut socket: Option<PathBuf> = None;
    let mut log_path: Option<String> = None;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = args.next().map(PathBuf::from);
            }
            "--log" => {
                log_path = args.next();
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

    let log_path = log_path.unwrap_or_else(default_log_path);
    let use_stdout = log_path == "-";

    if use_stdout {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .init();
    } else {
        let log_file = match File::create(&log_path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("nuefsd: failed to create log file {log_path}: {e}");
                std::process::exit(1);
            }
        };

        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .with_writer(log_file)
            .with_ansi(false)
            .init();
    }

    // Catch panics from FUSE handler threads and log them
    std::panic::set_hook(Box::new(|info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic payload".to_string()
        };
        // Write to a dedicated file to bypass tracing (which may not flush)
        use std::io::Write;
        let msg = format!(
            "PANIC thread={name} location={location} message={payload}\n"
        );
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/nuefs-panic.log")
        {
            let _ = f.write_all(msg.as_bytes());
        }
        error!(
            thread = name,
            location = location,
            message = payload,
            "PANIC in thread"
        );
    }));

    let socket = socket.unwrap_or_else(nuefs_rs::runtime::default_socket_path);
    info!(socket = %socket.display(), pid = std::process::id(), log = %log_path, "nuefsd starting");
    if let Err(e) = nuefs_rs::daemon::server::serve(socket).await {
        eprintln!("nuefsd: fatal error: {e}");
        std::process::exit(1);
    }
}

fn print_help() {
    eprintln!("Usage: nuefsd [--socket PATH] [--log PATH]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --socket PATH  Unix socket path (default: $XDG_RUNTIME_DIR/nuefsd.sock)");
    eprintln!("  --log PATH     Log file path, use '-' for stdout (default: $XDG_RUNTIME_DIR/nuefsd.log)");
}
