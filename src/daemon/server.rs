use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use thiserror::Error;

use crate::types::{Request, Response};

use super::manager::Manager;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind unix socket {socket}: {source}")]
    Bind { socket: PathBuf, source: std::io::Error },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub fn serve(socket_path: PathBuf) -> Result<(), ServerError> {
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path).map_err(|e| ServerError::Bind {
        socket: socket_path.clone(),
        source: e,
    })?;

    let manager = Arc::new(Mutex::new(Manager::new()));

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("nuefsd: accept error: {e}");
                continue;
            }
        };

        let manager = manager.clone();
        std::thread::spawn(move || {
            if let Err(e) = handle_connection(stream, manager) {
                eprintln!("nuefsd: connection error: {e}");
            }
        });
    }

    Ok(())
}

fn handle_connection(mut stream: UnixStream, manager: Arc<Mutex<Manager>>) -> Result<(), std::io::Error> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let request: Result<Request, _> = serde_json::from_str(&line);
    let response: Response = match request {
        Ok(req) => manager.lock().handle(req),
        Err(e) => Response::Err {
            message: format!("invalid request: {e}"),
        },
    };

    let json = serde_json::to_vec(&response).unwrap_or_else(|e| {
        serde_json::to_vec(&Response::Err {
            message: format!("failed to encode response: {e}"),
        })
        .unwrap_or_else(|_| b"{\"status\":\"err\",\"message\":\"encode failure\"}".to_vec())
    });

    stream.write_all(&json)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

