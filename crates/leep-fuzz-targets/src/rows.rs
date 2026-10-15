//! Derived-data row decoders: code search rows, GC rows and skiplists.

use leep_derive::derived::{SkipEntry, Skiplist};
use leep_derive::gc;
use leep_derive::search::trigram::{extract, BlobRow, Bloom, Probe, BLOOM_BITS};

/// Distinct trigram codes to probe a row with: the codes in `set`, plus
/// neighbours that are probably absent.
fn probe_codes(from: &[u32]) -> Vec<u32> {
    let mut codes: Vec<u32> = from.iter().copied().take(8).collect();
    for c in from.iter().copied().take(4) {
        codes.push(c.wrapping_add(1) & 0xFF_FFFF);
        codes.push(c ^ 0x80_0000);
    }
    codes
}

pub fn blob_trigrams_row(data: &[u8]) {
    // The probe reads rows lazily and must never panic on a corrupt one.
    let junk = Probe::new(vec![0, 1, 0x61_6263, 0xFF_FFFF]);
    let _ = junk.blob(data);
    let _ = junk.tree(data);

    let Ok(row) = BlobRow::decode(data) else {
        return;
    };
    assert_eq!(
        row.encode(),
        data,
        "the decoder accepts only canonical rows"
    );
    let BlobRow::Trigrams(set) = &row else {
        return;
    };
    assert!(set.windows(2).all(|w| w[0] < w[1]), "sorted and distinct");
    assert!(set.iter().all(|&c| c < 1 << 24));
    let probe = Probe::new(probe_codes(set));
    let present = probe
        .blob(data)
        .expect("a row that decodes must probe")
        .expect("an indexed row");
    for c in probe_codes(set) {
        assert_eq!(present.has(c), set.binary_search(&c).is_ok(), "code {c:#x}");
    }
}

pub fn tree_trigrams_row(data: &[u8]) {
    let Ok(bloom) = Bloom::decode(data) else {
        return;
    };
    // Either form is accepted, but the encoder is canonical and lossless.
    let enc = bloom.encode();
    let again = Bloom::decode(&enc).expect("re-encoded filter must decode");
    assert_eq!(again.encode(), enc);
    assert_eq!(again.set_bits(), bloom.set_bits());
    assert!(bloom.set_bits() <= BLOOM_BITS);

    let codes: Vec<u32> = data
        .windows(3)
        .take(16)
        .map(|w| u32::from(w[0]) << 16 | u32::from(w[1]) << 8 | u32::from(w[2]))
        .collect();
    let probe = Probe::new(codes.clone());
    let present = probe.tree(data).expect("a row that decodes must probe");
    for c in codes {
        assert_eq!(present.has(c), bloom.contains(c), "code {c:#x}");
    }
    let mut merged = Bloom::default();
    merged.or_encoded(data).unwrap();
    merged.or_encoded(&enc).unwrap();
    assert_eq!(
        merged.encode(),
        enc,
        "OR-ing a filter with itself changes nothing"
    );
}

/// Blob bytes in, rows out: the encoders against the decoders.
pub fn trigram_codec(data: &[u8]) {
    let row = BlobRow::of(data);
    assert_eq!(BlobRow::decode(&row.encode()).unwrap(), row);

    let codes = extract(data);
    assert!(codes.windows(2).all(|w| w[0] < w[1]));
    if let BlobRow::Trigrams(set) = &row {
        assert_eq!(set, &codes);
    }
    let mut bloom = Bloom::default();
    for &c in &codes {
        bloom.insert(c);
    }
    assert!(
        codes.iter().all(|&c| bloom.contains(c)),
        "no false negatives"
    );
    let enc = bloom.encode();
    let back = Bloom::decode(&enc).unwrap();
    assert_eq!(back.encode(), enc);
    assert!(codes.iter().all(|&c| back.contains(c)));

    let probe = Probe::new(codes.iter().copied().take(32).collect());
    if let BlobRow::Trigrams(_) = &row {
        let p = probe.blob(&row.encode()).unwrap().unwrap();
        assert!(codes.iter().take(32).all(|&c| p.has(c)));
    }
    let p = probe.tree(&enc).unwrap();
    assert!(codes.iter().take(32).all(|&c| p.has(c)));
}

pub fn gc_rows(data: &[u8]) {
    let kinds = [
        gc::BLOB_CLAIM,
        gc::BLOB_CONDEMNED,
        gc::BLOB_RESCUE,
        gc::BLOB_DOOM,
    ];
    let Some((&k, value)) = data.split_first() else {
        return;
    };
    let kind = kinds[k as usize % kinds.len()];
    let Ok((time, state)) = gc::decode_row(kind, value) else {
        return;
    };
    let mut again = time.to_be_bytes().to_vec();
    assert_eq!(state.is_some(), kind == gc::BLOB_DOOM);
    if let Some(s) = state {
        assert!(s <= 2);
        again.push(s);
    }
    assert_eq!(again, value, "a decoded row is exactly its canonical bytes");
}

pub fn skiplist_row(data: &[u8]) {
    let Ok(entries) = Skiplist::decode(data) else {
        return;
    };
    let mut again = Vec::new();
    for SkipEntry { id, generation } in &entries {
        again.extend_from_slice(id.0.as_bytes());
        again.extend_from_slice(&generation.to_be_bytes());
    }
    assert_eq!(again, data);
}
