//! Codecs from SBE (Simple Binary Encoding) message schemas, generated from a `build.rs`.

mod naming;
mod render;
mod schema;

use std::fmt;
use std::path::{Path, PathBuf};

/// Why a codec couldn't be generated from a schema.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// An I/O error, naming the path.
fn io_error(path: &Path) -> impl FnOnce(std::io::Error) -> Error + '_ {
    move |e| Error(format!("{}: {e}", path.display()))
}

/// Generates a codec for an SBE message schema: for each message, a decoder that reads it where it
/// lies (`NewOrderSingleRef`) and a struct to encode (`NewOrderSingle`); an enum, set or composite
/// struct per schema type; and `decode`, which reads a message header and wraps the decoder its
/// template ID names. The code calls this crate's runtime, so the crate it's generated into depends on
/// `turbofan-sbe` as well as building with it.
///
/// From a `build.rs`:
///
/// ```no_run
/// use turbofan_sbe::codegen::SbeGenerator;
///
/// let code = SbeGenerator::load("schemas/venue.xml").unwrap().render().unwrap();
/// let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("venue.rs");
/// std::fs::write(out, code).unwrap();
/// ```
///
/// and in the crate: `mod venue { include!(concat!(env!("OUT_DIR"), "/venue.rs")); }`.
///
/// Names follow Rust's conventions: `clOrdID` → `cl_ord_id`, `ExecutionReport_New` →
/// `ExecutionReportNew`, `FALSE_VALUE` → `FalseValue`. A group entry's types are prefixed with
/// what it's in (`NewOrderCrossNoSides`), since schemas reuse group names. Enums keep a value the
/// schema doesn't list as `Unknown`. Doc comments give each field's schema name and ID, not the
/// schema's descriptions, whose licence is the schema's publisher's.
///
/// SBE 1.0 schemas are supported, with both byte orders, `offset`, `sinceVersion`, constant and
/// optional fields, nested groups and variable-length data, and types included with
/// `<xi:include href="…"/>`.
#[derive(Debug)]
pub struct SbeGenerator {
    schema: schema::Schema,
}

impl SbeGenerator {
    /// Reads the schema at `path`, and the files it includes, relative to it.
    ///
    /// # Errors
    ///
    /// If a file can't be read or isn't XML, or the schema isn't valid SBE 1.0 or can't be
    /// generated as Rust.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(io_error(path))?;
        let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        Self::parse_with(&text, |href| {
            let included: PathBuf = dir.join(href);
            std::fs::read_to_string(&included).map_err(io_error(&included))
        })
    }

    /// Reads a schema from its XML. It can't include other files.
    ///
    /// # Errors
    ///
    /// As [`load`](Self::load).
    pub fn parse(xml: &str) -> Result<Self, Error> {
        Self::parse_with(xml, |href| Err(Error(format!("can't include {href}: the schema was given as text"))))
    }

    fn parse_with(xml: &str, include: impl Fn(&str) -> Result<String, Error>) -> Result<Self, Error> {
        let doc = roxmltree::Document::parse(xml).map_err(|e| Error(format!("schema: {e}")))?;
        let root = doc.root_element();
        let mut reader = schema::Reader::new();
        for node in root.children().filter(|n| n.tag_name().name() == "include") {
            let href = node.attribute("href").ok_or_else(|| Error("an include has no href".into()))?;
            let text = include(href)?;
            let included = roxmltree::Document::parse(&text).map_err(|e| Error(format!("{href}: {e}")))?;
            reader.read_types(included.root_element())?;
        }
        reader.read_types(root)?;
        Ok(Self { schema: reader.finish(root)? })
    }

    /// The code, unformatted, in one file.
    ///
    /// # Errors
    ///
    /// If two schema names would be the same Rust name, or a type would be named after one the
    /// code uses.
    pub fn render(&self) -> Result<String, Error> {
        render::Renderer::new(&self.schema).render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A schema with `types` and `messages` in it, and the header every schema needs.
    fn schema(types: &str, messages: &str) -> String {
        format!(
            "<sbe:messageSchema xmlns:sbe='http://fixprotocol.io/2016/sbe' package='t' id='1' version='0'><types>\
             <composite name='messageHeader'><type name='blockLength' primitiveType='uint16'/>\
             <type name='templateId' primitiveType='uint16'/><type name='schemaId' primitiveType='uint16'/>\
             <type name='version' primitiveType='uint16'/></composite>{types}</types>{messages}</sbe:messageSchema>"
        )
    }

    fn error(xml: &str) -> String {
        match SbeGenerator::parse(xml).and_then(|g| g.render()) {
            Ok(_) => panic!("generated"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn names_follow_rust_conventions() {
        assert_eq!(render::type_name("messageHeader"), "MessageHeader");
        assert_eq!(render::type_name("ExecutionReport_New"), "ExecutionReportNew");
        assert_eq!(render::type_name("FALSE_VALUE"), "FalseValue");
        assert_eq!(render::type_name("UTCTimestampNanos"), "UTCTimestampNanos");
        assert_eq!(render::type_name("3DAY"), "V3day");
    }

    #[test]
    fn fields_are_laid_out_in_order_or_at_their_offset() {
        let xml = schema(
            "",
            "<sbe:message name='M' id='1'><field name='a' id='1' type='uint8'/>\
             <field name='b' id='2' type='uint32' offset='4'/><field name='c' id='3' type='int16'/></sbe:message>",
        );
        let generator = SbeGenerator::parse(&xml).unwrap();
        let fields = &generator.schema.messages[0].block.fields;
        assert_eq!(fields.iter().map(|f| f.offset).collect::<Vec<_>>(), [0, 4, 8]);
        assert_eq!(generator.schema.messages[0].block.block_length, 10);
    }

    #[test]
    fn overlapping_offsets_are_an_error() {
        let xml = schema(
            "",
            "<sbe:message name='M' id='1'><field name='a' id='1' type='uint32'/>\
             <field name='b' id='2' type='uint8' offset='2'/></sbe:message>",
        );
        assert!(error(&xml).contains("overlaps"), "{}", error(&xml));
    }

    #[test]
    fn names_that_clash_in_rust_are_an_error() {
        let xml = schema("<enum name='side' encodingType='char'/><enum name='Side' encodingType='char'/>", "");
        assert!(error(&xml).contains("already a type"), "{}", error(&xml));
        let xml = schema("<enum name='Option' encodingType='char'/>", "");
        assert!(error(&xml).contains("already a type"), "{}", error(&xml));
    }

    #[test]
    fn values_that_arent_literals_of_their_type_are_an_error() {
        let xml =
            schema("<enum name='E' encodingType='uint8'><validValue name='A'>1); panic!(</validValue></enum>", "");
        assert!(error(&xml).contains("isn't a U8 value"), "{}", error(&xml));
        let xml = schema(
            "<enum name='E' encodingType='uint8'><validValue name='A'>1</validValue><validValue name='B'>1</validValue></enum>",
            "",
        );
        assert!(error(&xml).contains("twice"), "{}", error(&xml));
    }

    #[test]
    fn a_composite_cycle_is_an_error() {
        let xml = schema(
            "<composite name='A'><ref name='b' type='B'/></composite><composite name='B'><ref name='a' type='A'/></composite>",
            "",
        );
        assert!(error(&xml).contains("nests too deeply"), "{}", error(&xml));
    }

    fn code(xml: &str) -> String {
        SbeGenerator::parse(xml).and_then(|g| g.render()).unwrap()
    }

    #[test]
    fn a_block_whose_fields_are_all_newer_needs_nothing_from_older_versions() {
        let xml = schema(
            "",
            "<sbe:message name='M' id='1'><field name='a' id='1' type='uint64' sinceVersion='1'/></sbe:message>",
        );
        assert!(code(&xml).contains("let needed = if version >= 1 { 8 } else { 0 };"));
    }

    #[test]
    fn overlapping_composite_members_are_an_error() {
        let xml = schema(
            "<composite name='C'><type name='a' primitiveType='uint64'/><type name='b' primitiveType='uint16' offset='2'/></composite>",
            "",
        );
        assert!(error(&xml).contains("overlaps"), "{}", error(&xml));
    }

    #[test]
    fn literals_must_fit_their_type_and_are_written_as_rust() {
        for (prim, text) in [("uint8", "300"), ("int8", "-129"), ("double", "inf"), ("float", "NaN")] {
            let xml = schema(
                &format!("<type name='T' primitiveType='{prim}' nullValue='{text}'/>"),
                "<sbe:message name='M' id='1'><field name='a' id='1' type='T' presence='optional'/></sbe:message>",
            );
            assert!(error(&xml).contains("isn't a"), "{prim} {text}: {}", error(&xml));
        }
        assert_eq!(schema::Prim::F64.literal("1e5").unwrap(), "100000.0");
        assert_eq!(schema::Prim::I32.literal("+5").unwrap(), "5");
    }

    #[test]
    fn an_optional_enum_is_null_as_its_encoding_type_says() {
        let xml = schema(
            "<type name='side_t' primitiveType='uint8' nullValue='0'/>\
             <enum name='Side' encodingType='side_t'><validValue name='Buy'>1</validValue></enum>",
            "<sbe:message name='M' id='1'><field name='s' id='1' type='Side' presence='optional'/></sbe:message>",
        );
        let code = code(&xml);
        assert!(code.contains("filter(|raw| *raw != 0)") && code.contains("map_or(0, Side::raw)"), "{code}");
    }

    #[test]
    fn a_constant_enums_value_ref_must_be_of_its_enum() {
        let xml = schema(
            "<enum name='Side' encodingType='uint8'><validValue name='Leader'>1</validValue></enum>\
             <enum name='Role' encodingType='uint8'><validValue name='Leader'>2</validValue></enum>",
            "<sbe:message name='M' id='1'><field name='s' id='1' type='Side' presence='constant' valueRef='Role.Leader'/></sbe:message>",
        );
        assert!(error(&xml).contains("isn't a value of Side"), "{}", error(&xml));
    }

    #[test]
    fn secrets_are_masked_in_the_encoder_too() {
        let xml = schema(
            "<composite name='varData'><type name='length' primitiveType='uint32'/><type name='varData' primitiveType='uint8' length='0'/></composite>",
            "<sbe:message name='Hello' id='1'><data name='token' id='1' type='varData'/></sbe:message>",
        );
        let code = code(&xml);
        assert!(!code.contains("#[derive(Debug, Clone, PartialEq)]\npub struct Hello"), "{code}");
        assert_eq!(code.matches(".field(\"token\", &format_args!(\"***\"))").count(), 2, "{code}");
    }

    #[test]
    fn includes_need_a_file() {
        let xml = schema("", "")
            .replace("<types>", "<xi:include xmlns:xi='http://www.w3.org/2001/XInclude' href='common.xml'/><types>");
        assert!(error(&xml).contains("can't include common.xml"), "{}", error(&xml));
    }
}
