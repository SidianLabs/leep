#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| leep_fuzz_targets::check::search_plan(data));
