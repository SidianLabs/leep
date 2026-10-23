//! A command's output on stdout, for every Leep binary.
//!
//! Rust ignores SIGPIPE, so once the reader of stdout goes away
//! (`leep log | head -1`) every write fails with `BrokenPipe`, and `println!`
//! panics on that. Restoring the default SIGPIPE disposition instead would
//! also kill a server that writes to a closed socket, so the binaries write
//! through these functions and the [`out!`](crate::out) and
//! [`outln!`](crate::outln) macros, which end the process quietly.

use std::fmt;
use std::io::{self, Write};

/// Exit status once stdout's reader has gone: what a shell reports for a
/// process killed by SIGPIPE (128 + 13), as for `git log | head -1`. Not 0,
/// because the command stopped before it finished.
pub const BROKEN_PIPE_EXIT: i32 = 141;

/// Exit with [`BROKEN_PIPE_EXIT`] if `e` says stdout's reader has gone.
pub fn exit_if_broken_pipe(e: &io::Error) {
    if e.kind() == io::ErrorKind::BrokenPipe {
        // Destructors do not run; every caller is a one-shot command that
        // must survive SIGINT at the same point anyway.
        std::process::exit(BROKEN_PIPE_EXIT);
    }
}

/// `print!` that exits quietly when the reader has gone. Any other error
/// panics, as `print!` does.
pub fn print(args: fmt::Arguments<'_>) {
    let res = io::stdout().lock().write_fmt(args);
    if let Err(e) = res {
        exit_if_broken_pipe(&e);
        panic!("failed printing to stdout: {e}");
    }
}

/// Write `bytes` and flush, exiting quietly when the reader has gone. Other
/// errors (a full disk behind `>`) are the caller's to report.
pub fn write_all(bytes: &[u8]) -> io::Result<()> {
    let res = {
        let mut out = io::stdout().lock();
        out.write_all(bytes).and_then(|()| out.flush())
    };
    if let Err(e) = &res {
        exit_if_broken_pipe(e);
    }
    res
}

/// Stdout as an [`io::Write`] that exits quietly when the reader has gone,
/// for a one-shot command's log writer. Never for a long-running server,
/// which must outlive whatever reads its logs.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stdout;

impl Write for Stdout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        io::stdout().write(buf).inspect_err(exit_if_broken_pipe)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        io::stdout().write_all(buf).inspect_err(exit_if_broken_pipe)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush().inspect_err(exit_if_broken_pipe)
    }
}

/// `print!` through [`print`]: exits with [`BROKEN_PIPE_EXIT`] instead of
/// panicking when stdout's reader has gone.
#[macro_export]
macro_rules! out {
    ($($arg:tt)*) => {
        $crate::stdout::print(::std::format_args!($($arg)*))
    };
}

/// `println!` through [`print`]: exits with [`BROKEN_PIPE_EXIT`] instead of
/// panicking when stdout's reader has gone.
#[macro_export]
macro_rules! outln {
    () => {
        $crate::stdout::print(::std::format_args!("\n"))
    };
    ($($arg:tt)*) => {
        $crate::stdout::print(::std::format_args!("{}\n", ::std::format_args!($($arg)*)))
    };
}
