# turbofan-sbe

SBE ([Simple Binary Encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding))
codecs for Rust: a generator that turns a message schema into Rust code from a `build.rs`, and the
small runtime that the generated code calls. Turbofan's wire protocols are built with it.

## Use

```toml
[dependencies]
turbofan-sbe = { path = "../turbofan-sbe" }

[build-dependencies]
turbofan-sbe = { path = "../turbofan-sbe" }
```

```rust
// build.rs
fn main() {
    println!("cargo::rerun-if-changed=schema/venue.xml");
    let code = turbofan_sbe::codegen::SbeGenerator::load("schema/venue.xml")
        .and_then(|generator| generator.render())
        .unwrap_or_else(|error| panic!("schema/venue.xml: {error}"));
    let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("venue.rs");
    std::fs::write(out, code).unwrap();
}
```

```rust
// src/lib.rs
pub mod venue {
    include!(concat!(env!("OUT_DIR"), "/venue.rs"));
}

use turbofan_sbe::Encode;

let mut out = Vec::new();
venue::NewOrder { id: 7, qty: 100 }.encode_into(&mut out)?;
let (venue::Decoded::NewOrder(order), len) = venue::decode(&out)? else { return Ok(()) };
assert_eq!((order.id(), order.qty(), len), (7, 100, out.len()));
```

## What's generated

For each message, the generator writes a decoder that reads the message where it lies
(`NewOrderRef`, with no copying) and a struct to encode (`NewOrder`). It also writes an enum, set or
struct for each type in the schema, and `decode`, which reads the message header and wraps the
decoder that matches its template ID.

- **Names follow Rust conventions:** `clOrdID` becomes `cl_ord_id`, `ExecutionReport_New` becomes
  `ExecutionReportNew`. The types for a group's entries take the name of what contains them
  (`NewOrderCrossNoSides`).
- **Unknown enum values are kept:** a value the schema doesn't list decodes as `Unknown(raw)`.
  Enums compare by their value on the wire, so `Unknown(b'T')` equals the variant whose value is
  `b'T'`.
- **Supported schemas:** SBE 1.0, with either byte order, `offset`, `sinceVersion`, constant and
  optional fields, nested groups, variable-length data, and types included with
  `<xi:include href="…"/>`.

## Decoding untrusted bytes

A decoder checks the whole message when it wraps it: the root block, every group entry and every
variable-length field must lie inside the buffer, and each block must be long enough for the
version it claims. After that, reading a field can't fail.

Fields newer than the message's version read as absent. A block longer than the schema's, from a
newer version, is read as far as the schema goes. Bad input is an `SbeError`, never a panic.

Encoding checks each data field against its length limit. A field that's too long is an error,
and the buffer is left as it was.

## Tests

`tests/sbe.rs` checks the codecs generated from SBE's example schema (the car) and from
`tests/sbe/versioned.xml`:

- they read and write the same bytes as real-logic's own codecs;
- every truncated message is an error;
- they handle versioning, byte order and unknown enum values.

The generated test codecs are checked in, and a test fails if the generator would now write
something different. After changing the generator, run `SBE_REGENERATE=1 cargo test -p
turbofan-sbe` to rewrite them, and review the diff.
