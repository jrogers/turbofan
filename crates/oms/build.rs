//! Generates the order messages' codec from `schema/oms.xml`.

fn main() {
    println!("cargo::rerun-if-changed=schema/oms.xml");
    let code = turbofan_sbe::codegen::SbeGenerator::load("schema/oms.xml")
        .and_then(|generator| generator.render())
        .unwrap_or_else(|error| panic!("schema/oms.xml: {error}"));
    let out = std::path::Path::new(&std::env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("oms.rs");
    std::fs::write(out, code).expect("writing the generated codec");
}
