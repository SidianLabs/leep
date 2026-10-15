//! Operator-supplied files: OWNERS, the review policy, signing keys and the
//! tokens file. A bad file must be an error message, never a panic.

use leep_derive::owners::parse_owners;
use leep_server::auth::Auth;
use leep_server::review::ReviewPolicy;
use leep_server::signing::SigningKeys;

use crate::lossy;

pub fn owners(data: &[u8]) {
    let text = lossy(data);
    let file = parse_owners(&text);
    for name in &file.owners {
        assert!(!name.is_empty(), "blank names are skipped");
        assert!(!name.contains('#'), "a comment never survives");
        assert!(!name.contains('\n'));
        assert_eq!(name.trim(), name, "names are trimmed");
        assert_ne!(name, "noparent", "the directive is not an owner");
    }
    // Writing the result back out and reading it again changes nothing. A
    // BOM is skipped only at the start of the text, so a name that starts
    // with one (possible after the first line) is the one exception.
    if file.owners.iter().all(|n| !n.contains('\u{feff}')) {
        let mut out: String = file.owners.iter().map(|n| format!("{n}\n")).collect();
        if file.noparent {
            out.push_str("noparent\n");
        }
        assert_eq!(parse_owners(&out), file);
    }
}

pub fn review_policy(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(policy) = ReviewPolicy::parse(text) else {
        return;
    };
    assert!(policy.refs.iter().all(|r| !r.trim().is_empty()));
    for path in ["README.md", "docs/a/b.md", "a", "src/x.rs", ".hidden/./x"] {
        let _ = policy.rules.is_docs(path);
    }
}

pub fn signing_keys(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(keys) = SigningKeys::parse(text) {
        assert!(!keys.is_empty());
    }
}

pub fn auth_tokens(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(auth) = Auth::parse(text) {
        assert!(!auth.is_empty());
        assert!(auth.reviewers().len() <= auth.len());
    }
}
