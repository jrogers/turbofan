//! Generates the wire codecs from `schema/cluster.xml`.

fn main() {
    println!("cargo::rerun-if-changed=schema/cluster.xml");
    let code = turbojet_codegen::SbeGenerator::load("schema/cluster.xml")
        .and_then(|generator| generator.render())
        .unwrap_or_else(|error| panic!("schema/cluster.xml: {error}"));
    let out = std::path::Path::new(&std::env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("wire.rs");
    std::fs::write(out, code).expect("writing the generated codec");
}
