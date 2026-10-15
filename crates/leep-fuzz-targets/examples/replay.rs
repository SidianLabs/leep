//! Run one target on one file: `cargo run -p leep-fuzz-targets --example replay -- <target> <file>`.
//! The way to reproduce a crash artifact on stable, with a normal backtrace.

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(name), Some(file)) = (args.next(), args.next()) else {
        eprintln!("usage: replay <target> <file>");
        eprintln!(
            "targets: {}",
            leep_fuzz_targets::TARGETS
                .iter()
                .map(|t| t.name)
                .collect::<Vec<_>>()
                .join(" ")
        );
        std::process::exit(2);
    };
    let Some(target) = leep_fuzz_targets::target(&name) else {
        eprintln!("no target {name}");
        std::process::exit(2);
    };
    let data = std::fs::read(&file).expect("cannot read input file");
    (target.check)(&data);
    println!("{name}: {} bytes ok", data.len());
}
