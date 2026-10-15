//! `leep-mcp`: MCP server over stdio. Stdout carries protocol messages only;
//! every log line goes to stderr.

use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use leep_mcp::{Config, Server};

fn main() -> ExitCode {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("leep-mcp: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("leep-mcp: serving {} over stdio", cfg.server);
    let server = Server::new(cfg);
    let mut out = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("leep-mcp: stdin error: {e}");
                return ExitCode::from(1);
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        if let Some(reply) = server.handle_line(&line) {
            if writeln!(out, "{reply}").and_then(|_| out.flush()).is_err() {
                return ExitCode::from(1);
            }
        }
    }
    ExitCode::SUCCESS
}
