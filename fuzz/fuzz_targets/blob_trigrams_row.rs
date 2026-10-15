#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| leep_fuzz_targets::check::blob_trigrams_row(data));
