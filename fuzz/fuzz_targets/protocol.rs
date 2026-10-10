#![no_main]
use libfuzzer_sys::fuzz_target;

// Exercise the production source without instrumenting unrelated TLS/SQLite code.
#[allow(dead_code)]
#[path = "../../src/protocol.rs"]
mod protocol;

fuzz_target!(|data: &str| {
    if let Ok(message) = protocol::Message::parse(data) {
        if let Ok(encoded) = message.encode() {
            assert_eq!(protocol::Message::parse(&encoded).unwrap(), message);
        }
    }
});
