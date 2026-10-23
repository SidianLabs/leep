//! `leep keygen`: create an author signing key.

use std::path::PathBuf;

use clap::Args;

use crate::error::Result;
use crate::sign::AuthorKey;
use leep_core::outln;

#[derive(Args, Debug)]
pub struct KeygenArgs {
    /// Where to write the private key (created with mode 0600; never overwritten).
    #[arg(long)]
    pub out: PathBuf,
    /// Name for the keys-file entry (default: $USER).
    #[arg(long)]
    pub name: Option<String>,
}

/// Prints the entry to add to the server's signing keys file on stdout, so
/// it can be piped; the private key never leaves the file.
pub fn run(args: &KeygenArgs) -> Result<i32> {
    let key = AuthorKey::generate()?;
    key.save(&args.out)?;
    let name = args
        .name
        .clone()
        .or_else(|| std::env::var("USER").ok().filter(|u| !u.is_empty()))
        .unwrap_or_else(|| "me".into());
    outln!(
        "{}",
        serde_json::json!({ "name": name, "public_key": key.public_key() })
    );
    eprintln!(
        "wrote private key {} (key id {}).\nAdd the line above to the `keys` array of the server's signing keys file, then commit with `leep commit --sign {}`.",
        args.out.display(),
        key.key_id(),
        args.out.display()
    );
    Ok(0)
}
