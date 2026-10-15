//! Checks the binary starts and parses its flags. Having an integration test
//! also makes `cargo test --workspace` build `leep-soak`, which the soak smoke
//! test in leep-e2e runs.

use std::process::Command;

#[test]
fn help_lists_the_main_flags() {
    let out = Command::new(env!("CARGO_BIN_EXE_leep-soak"))
        .arg("--help")
        .output()
        .expect("run leep-soak --help");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8_lossy(&out.stdout);
    for flag in ["--duration", "--agents", "--seed", "--chaos"] {
        assert!(help.contains(flag), "missing {flag} in:\n{help}");
    }
}
