//! Bytes from the network, decoded as the driver's read loop decodes frames and a follower
//! decodes `AppendEntries` records. Neither may panic, a frame must take its length and a byte
//! more, and a record that decodes must encode back to the same bytes. (One with term or index 0
//! decodes but can't be encoded; Raft refuses it, as its index never follows the last.)

#![no_main]

use libfuzzer_sys::fuzz_target;
use turbofan_cluster::codec::{self, Record};

fuzz_target!(|bytes: &[u8]| {
    let mut rest = bytes;
    while let Ok(Some((message, used))) = codec::decode_frame(rest) {
        assert!(4 < used && used <= rest.len(), "frame took {used} of {}", rest.len());
        // Debug reads every field, data included.
        std::hint::black_box(format!("{message:?}"));
        rest = &rest[used..];
    }

    let mut rest = bytes;
    while let Ok(Some((record, used))) = Record::decode(rest) {
        if record.term == 0 || record.index == 0 {
            break;
        }
        let mut again = Vec::new();
        record.encode_into(&mut again);
        assert_eq!(again, rest[..used], "{record:?} encodes differently");
        rest = &rest[used..];
    }
});
