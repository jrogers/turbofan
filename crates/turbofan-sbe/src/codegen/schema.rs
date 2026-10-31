//! An SBE message schema, read from its XML and laid out: every field's offset and size worked
//! out, every type reference resolved.

use std::collections::HashMap;

use super::Error;

/// How deeply composites may nest, and groups within groups. Deeper is surely a reference cycle.
const MAX_DEPTH: usize = 16;

/// A schema, ready to render.
#[derive(Debug)]
pub(crate) struct Schema {
    pub package: String,
    pub id: u16,
    pub version: u16,
    pub big_endian: bool,
    pub enums: Vec<Enum>,
    pub sets: Vec<Set>,
    pub composites: Vec<Composite>,
    /// The `messageHeader` composite (or the schema's `headerType`).
    pub header: String,
    pub messages: Vec<Message>,
}

/// A primitive type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Prim {
    Char,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}

impl Prim {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "char" => Self::Char,
            "int8" => Self::I8,
            "int16" => Self::I16,
            "int32" => Self::I32,
            "int64" => Self::I64,
            "uint8" => Self::U8,
            "uint16" => Self::U16,
            "uint32" => Self::U32,
            "uint64" => Self::U64,
            "float" => Self::F32,
            "double" => Self::F64,
            _ => return None,
        })
    }

    pub fn size(self) -> usize {
        match self {
            Self::Char | Self::I8 | Self::U8 => 1,
            Self::I16 | Self::U16 => 2,
            Self::I32 | Self::U32 | Self::F32 => 4,
            Self::I64 | Self::U64 | Self::F64 => 8,
        }
    }

    pub fn rust(self) -> &'static str {
        match self {
            Self::Char | Self::U8 => "u8",
            Self::I8 => "i8",
            Self::I16 => "i16",
            Self::I32 => "i32",
            Self::I64 => "i64",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::F32 => "f32",
            Self::F64 => "f64",
        }
    }

    pub fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    /// The null value SBE gives it when the schema doesn't, as Rust.
    pub fn default_null(self) -> String {
        match self {
            Self::Char => "0".to_string(),
            Self::I8 | Self::I16 | Self::I32 | Self::I64 => format!("{}::MIN", self.rust()),
            _ => format!("{}::MAX", self.rust()),
        }
    }

    /// `text` as a Rust literal of this type, checked, so schema text can't inject code.
    pub fn literal(self, text: &str) -> Result<String, Error> {
        let text = text.trim();
        let ok = match self {
            Self::Char => {
                return match text.as_bytes() {
                    [b] if b.is_ascii() => Ok(format!("b{:?}", char::from(*b))),
                    _ => Err(Error(format!("char value {text:?} isn't one ASCII character"))),
                };
            }
            Self::F32 | Self::F64 => text.parse::<f64>().is_ok(),
            _ => text.parse::<i128>().is_ok(),
        };
        if !ok {
            return Err(Error(format!("{text:?} isn't a {self:?} value")));
        }
        Ok(if self.is_float() && !text.contains('.') { format!("{text}.0") } else { text.to_string() })
    }
}

/// Whether a field is always there, may be null, or is fixed by the schema and not on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Presence {
    Required,
    Optional,
    Constant,
}

/// What a field or composite member holds, resolved.
#[derive(Debug, Clone)]
pub(crate) enum Kind {
    /// A primitive. `null` and `constant` are Rust literals.
    Prim {
        prim: Prim,
        null: String,
        constant: Option<String>,
    },
    /// A char array of `len`, NUL-padded. `constant` is its text.
    Chars {
        len: usize,
        constant: Option<String>,
    },
    /// An array of `len` primitives.
    Array {
        prim: Prim,
        len: usize,
    },
    /// An enum. `null` is a Rust literal of the encoding; `constant` the variant name.
    Enum {
        name: String,
        encoding: Prim,
        null: String,
        constant: Option<String>,
    },
    Set {
        name: String,
        encoding: Prim,
    },
    Composite {
        name: String,
    },
}

/// A field of a block or a composite member, at its offset.
#[derive(Debug, Clone)]
pub(crate) struct Slot {
    pub name: String,
    pub id: Option<u32>,
    pub kind: Kind,
    pub presence: Presence,
    pub offset: usize,
    /// Bytes on the wire: none for a constant.
    pub size: usize,
    pub since: u16,
}

#[derive(Debug)]
pub(crate) struct Enum {
    pub name: String,
    pub encoding: Prim,
    /// Each value's name and its Rust literal.
    pub values: Vec<(String, String)>,
}

#[derive(Debug)]
pub(crate) struct Set {
    pub name: String,
    pub encoding: Prim,
    pub choices: Vec<(String, u32)>,
}

#[derive(Debug)]
pub(crate) struct Composite {
    pub name: String,
    pub members: Vec<Slot>,
    pub size: usize,
}

#[derive(Debug)]
pub(crate) struct Message {
    pub name: String,
    pub id: u16,
    pub block: Block,
}

/// A message's root, or a group entry: fixed fields, then groups, then data.
#[derive(Debug)]
pub(crate) struct Block {
    pub fields: Vec<Slot>,
    pub block_length: usize,
    pub groups: Vec<Group>,
    pub data: Vec<Data>,
}

#[derive(Debug)]
pub(crate) struct Group {
    pub name: String,
    pub id: Option<u32>,
    /// The dimension composite, with `blockLength` and `numInGroup`.
    pub dimension: String,
    pub since: u16,
    pub block: Block,
}

#[derive(Debug)]
pub(crate) struct Data {
    pub name: String,
    pub id: Option<u32>,
    pub length: Prim,
    /// The schema's `maxValue` for the length, as a Rust literal.
    pub max: Option<String>,
    pub since: u16,
}

/// A type as the schema declares it, before resolving.
#[derive(Debug, Clone)]
enum Declared {
    Encoded {
        prim: Prim,
        length: Option<usize>,
        presence: Presence,
        null: Option<String>,
        max: Option<String>,
        text: Option<String>,
        value_ref: Option<String>,
    },
    Composite(Vec<(String, Member)>),
    Enum,
    Set,
}

/// A composite member: a type declared inline, or a `ref` to a named one.
#[derive(Debug, Clone)]
enum Member {
    Inline(Declared, Option<usize>),
    Ref(String, Option<usize>),
}

/// Reads schemas: the types first, from every document (included ones too), then the messages.
pub(crate) struct Reader {
    declared: HashMap<String, Declared>,
    enums: Vec<Enum>,
    sets: Vec<Set>,
    /// Composite names, in declaration order, to lay out once everything's read.
    composites: Vec<String>,
}

fn attr<'a>(node: roxmltree::Node<'a, '_>, name: &str) -> Option<&'a str> {
    node.attribute(name)
}

fn required<'a>(node: roxmltree::Node<'a, '_>, name: &str) -> Result<&'a str, Error> {
    node.attribute(name).ok_or_else(|| {
        Error(format!("<{} {}> has no {name}", node.tag_name().name(), attr(node, "name").unwrap_or("")))
    })
}

fn number<T: std::str::FromStr>(node: roxmltree::Node<'_, '_>, name: &str) -> Result<Option<T>, Error> {
    attr(node, name)
        .map(|text| text.trim().parse().map_err(|_| Error(format!("{name}={text:?} isn't a number"))))
        .transpose()
}

fn presence(text: Option<&str>) -> Result<Option<Presence>, Error> {
    Ok(match text {
        None => None,
        Some("required") => Some(Presence::Required),
        Some("optional") => Some(Presence::Optional),
        Some("constant") => Some(Presence::Constant),
        Some(other) => return Err(Error(format!("unknown presence {other:?}"))),
    })
}

fn element_children<'a, 'i>(node: roxmltree::Node<'a, 'i>) -> impl Iterator<Item = roxmltree::Node<'a, 'i>> {
    node.children().filter(roxmltree::Node::is_element)
}

impl Reader {
    pub(crate) fn new() -> Self {
        Self { declared: HashMap::new(), enums: Vec::new(), sets: Vec::new(), composites: Vec::new() }
    }

    /// Reads the `<types>` in a document: the schema itself or one it includes.
    pub(crate) fn read_types(&mut self, root: roxmltree::Node<'_, '_>) -> Result<(), Error> {
        let types = root.descendants().filter(|n| n.tag_name().name() == "types");
        for types in types {
            for node in element_children(types) {
                let name = required(node, "name")?.to_string();
                let declared = self.declare(node)?;
                if self.declared.insert(name.clone(), declared).is_some() {
                    return Err(Error(format!("type {name} is declared twice")));
                }
            }
        }
        Ok(())
    }

    /// A type's declaration. Enums and sets are recorded as they're met, inline ones too.
    fn declare(&mut self, node: roxmltree::Node<'_, '_>) -> Result<Declared, Error> {
        match node.tag_name().name() {
            "type" => encoded(node),
            "enum" => {
                self.enums.push(self.read_enum(node)?);
                Ok(Declared::Enum)
            }
            "set" => {
                self.sets.push(self.read_set(node)?);
                Ok(Declared::Set)
            }
            "composite" => {
                let mut members = Vec::new();
                for member in element_children(node) {
                    let name = required(member, "name")?.to_string();
                    let offset = number(member, "offset")?;
                    let declared = if member.tag_name().name() == "ref" {
                        Member::Ref(required(member, "type")?.to_string(), offset)
                    } else {
                        let declared = self.declare(member)?;
                        if !matches!(declared, Declared::Encoded { .. }) {
                            self.lift(&name, declared.clone())?;
                        }
                        Member::Inline(declared, offset)
                    };
                    members.push((name, declared));
                }
                self.composites.push(required(node, "name")?.to_string());
                Ok(Declared::Composite(members))
            }
            other => Err(Error(format!("unknown type element <{other}>"))),
        }
    }

    /// Declares an enum, set or composite found inside a composite under its own name, so it's
    /// generated as a type of its own.
    fn lift(&mut self, name: &str, declared: Declared) -> Result<(), Error> {
        if self.declared.insert(name.to_string(), declared).is_some() {
            return Err(Error(format!("type {name} is declared twice")));
        }
        Ok(())
    }

    /// An enum's encoding: a primitive, or a named encoded type.
    fn encoding(&self, node: roxmltree::Node<'_, '_>) -> Result<Prim, Error> {
        let name = required(node, "encodingType")?;
        Prim::parse(name)
            .or_else(|| match self.declared.get(name) {
                Some(Declared::Encoded { prim, .. }) => Some(*prim),
                _ => None,
            })
            .ok_or_else(|| Error(format!("unknown encodingType {name}")))
    }

    fn read_enum(&self, node: roxmltree::Node<'_, '_>) -> Result<Enum, Error> {
        let name = required(node, "name")?.to_string();
        let encoding = self.encoding(node)?;
        let mut values: Vec<(String, String)> = Vec::new();
        for value in element_children(node) {
            let literal = encoding.literal(value.text().unwrap_or(""))?;
            if values.iter().any(|(_, l)| *l == literal) {
                return Err(Error(format!("enum {name} has the value {literal} twice")));
            }
            values.push((required(value, "name")?.to_string(), literal));
        }
        Ok(Enum { name, encoding, values })
    }

    fn read_set(&self, node: roxmltree::Node<'_, '_>) -> Result<Set, Error> {
        let name = required(node, "name")?.to_string();
        let encoding = self.encoding(node)?;
        let bits = u32::try_from(encoding.size() * 8).expect("a primitive is at most 64 bits");
        let mut choices = Vec::new();
        for choice in element_children(node) {
            let text = choice.text().unwrap_or("").trim();
            let bit: u32 = text.parse().map_err(|_| Error(format!("set {name}: bit {text:?} isn't a number")))?;
            if bit >= bits {
                return Err(Error(format!("set {name}: bit {bit} doesn't fit its {bits}-bit encoding")));
            }
            choices.push((required(choice, "name")?.to_string(), bit));
        }
        Ok(Set { name, encoding, choices })
    }

    /// Reads the messages and lays everything out.
    pub(crate) fn finish(self, root: roxmltree::Node<'_, '_>) -> Result<Schema, Error> {
        let byte_order = attr(root, "byteOrder").unwrap_or("littleEndian");
        let big_endian = match byte_order {
            "littleEndian" => false,
            "bigEndian" => true,
            other => return Err(Error(format!("unknown byteOrder {other:?}"))),
        };
        let mut composites = Vec::new();
        for name in &self.composites {
            // A data field's type (a length and varData) isn't a fixed-size composite.
            let Some(Declared::Composite(members)) = self.declared.get(name) else { continue };
            if !members.iter().any(|(member, _)| member == "varData") {
                composites.push(self.composite(name, 0)?);
            }
        }
        let mut messages = Vec::new();
        for node in element_children(root).filter(|n| n.tag_name().name() == "message") {
            let name = required(node, "name")?.to_string();
            let id = number(node, "id")?.ok_or_else(|| Error(format!("message {name} has no id")))?;
            let block = self.block(node, 0).map_err(|e| Error(format!("message {name}: {e}")))?;
            messages.push(Message { name, id, block });
        }
        let header = attr(root, "headerType").unwrap_or("messageHeader").to_string();
        self.check_header(&composites, &header)?;
        let (enums, sets) = (self.enums, self.sets);
        Ok(Schema {
            package: attr(root, "package").unwrap_or("").to_string(),
            id: number(root, "id")?.ok_or_else(|| Error("the schema has no id".into()))?,
            version: number(root, "version")?.unwrap_or(0),
            big_endian,
            enums,
            sets,
            composites,
            header,
            messages,
        })
    }

    /// The header must have the four members `decode` reads, each a `uint16`.
    fn check_header(&self, composites: &[Composite], header: &str) -> Result<(), Error> {
        let composite = composites
            .iter()
            .find(|c| c.name == header)
            .ok_or_else(|| Error(format!("the schema has no {header} composite")))?;
        for member in ["blockLength", "templateId", "schemaId", "version"] {
            let slot = composite.members.iter().find(|m| m.name == member);
            if !slot.is_some_and(|s| matches!(s.kind, Kind::Prim { prim: Prim::U16, constant: None, .. })) {
                return Err(Error(format!("{header}.{member} must be a uint16")));
            }
        }
        Ok(())
    }

    fn composite(&self, name: &str, depth: usize) -> Result<Composite, Error> {
        if depth > MAX_DEPTH {
            return Err(Error(format!("composite {name} nests too deeply (a cycle?)")));
        }
        let Some(Declared::Composite(members)) = self.declared.get(name) else {
            return Err(Error(format!("{name} isn't a composite")));
        };
        let mut slots = Vec::new();
        let mut at = 0;
        for (member, declared) in members {
            let ((kind, presence), offset) = match declared {
                Member::Inline(inner @ Declared::Encoded { .. }, offset) => {
                    (self.resolve_declared(member, inner, None, None, depth)?, offset)
                }
                // An enum, set or composite declared inline is lifted under the member's name.
                Member::Inline(_, offset) => (self.resolve(member, None, None, depth + 1)?, offset),
                Member::Ref(type_name, offset) => (self.resolve(type_name, None, None, depth + 1)?, offset),
            };
            let slot = self.slot(member.clone(), None, kind, presence, offset.unwrap_or(at), 0, depth)?;
            at = slot.offset + slot.size;
            slots.push(slot);
        }
        Ok(Composite { name: name.to_string(), members: slots, size: at })
    }

    #[expect(clippy::too_many_arguments, reason = "a slot is made of all of these")]
    fn slot(
        &self,
        name: String,
        id: Option<u32>,
        kind: Kind,
        presence: Presence,
        offset: usize,
        since: u16,
        depth: usize,
    ) -> Result<Slot, Error> {
        let size = if presence == Presence::Constant {
            0
        } else {
            match &kind {
                Kind::Prim { prim, .. } => prim.size(),
                Kind::Chars { len, .. } => *len,
                Kind::Array { prim, len } => prim.size() * len,
                Kind::Enum { encoding, .. } | Kind::Set { encoding, .. } => encoding.size(),
                Kind::Composite { name } => self.composite(name, depth + 1)?.size,
            }
        };
        Ok(Slot { name, id, kind, presence, offset, size, since })
    }

    /// Resolves a field's `type`, with the field's own presence and valueRef, if any.
    fn resolve(
        &self,
        type_name: &str,
        presence: Option<Presence>,
        value_ref: Option<&str>,
        depth: usize,
    ) -> Result<(Kind, Presence), Error> {
        if let Some(prim) = Prim::parse(type_name) {
            let declared = Declared::Encoded {
                prim,
                length: None,
                presence: Presence::Required,
                null: None,
                max: None,
                text: None,
                value_ref: None,
            };
            return self.resolve_declared(type_name, &declared, presence, value_ref, depth);
        }
        let declared = self.declared.get(type_name).ok_or_else(|| Error(format!("unknown type {type_name}")))?;
        self.resolve_declared(type_name, declared, presence, value_ref, depth)
    }

    fn resolve_declared(
        &self,
        name: &str,
        declared: &Declared,
        presence: Option<Presence>,
        value_ref: Option<&str>,
        depth: usize,
    ) -> Result<(Kind, Presence), Error> {
        match declared {
            Declared::Encoded { .. } => self.encoded_kind(declared, presence, value_ref),
            Declared::Enum => {
                let e = self.enums.iter().find(|e| e.name == name).expect("declared enums are recorded");
                let presence = presence.unwrap_or(Presence::Required);
                let constant = match value_ref {
                    Some(r) => Some(self.value_ref(r)?.0),
                    None if presence == Presence::Constant => {
                        return Err(Error(format!("constant {name} has no valueRef")));
                    }
                    None => None,
                };
                let null = e.encoding.default_null();
                Ok((Kind::Enum { name: name.to_string(), encoding: e.encoding, null, constant }, presence))
            }
            Declared::Set => {
                let s = self.sets.iter().find(|s| s.name == name).expect("declared sets are recorded");
                Ok((Kind::Set { name: name.to_string(), encoding: s.encoding }, Presence::Required))
            }
            Declared::Composite(_) => {
                if depth > MAX_DEPTH {
                    return Err(Error(format!("composite {name} nests too deeply (a cycle?)")));
                }
                Ok((Kind::Composite { name: name.to_string() }, Presence::Required))
            }
        }
    }

    fn encoded_kind(
        &self,
        declared: &Declared,
        presence: Option<Presence>,
        field_ref: Option<&str>,
    ) -> Result<(Kind, Presence), Error> {
        let Declared::Encoded { prim, length, presence: own, null, text, value_ref, .. } = declared else {
            unreachable!("only encoded types come here");
        };
        let presence = presence.unwrap_or(*own);
        let constant = if presence == Presence::Constant {
            match field_ref.or(value_ref.as_deref()) {
                Some(r) => Some(self.value_ref(r)?.1),
                None => Some(text.clone().unwrap_or_default()),
            }
        } else {
            None
        };
        if *prim == Prim::Char && (length.is_some_and(|l| l != 1) || constant.as_ref().is_some_and(|c| c.len() != 1)) {
            return Ok((Kind::Chars { len: length.unwrap_or(0), constant }, presence));
        }
        if let Some(len) = length.filter(|&l| l != 1) {
            return Ok((Kind::Array { prim: *prim, len }, presence));
        }
        let null = null.as_deref().map(|n| prim.literal(n)).transpose()?.unwrap_or_else(|| prim.default_null());
        let constant = constant.map(|c| prim.literal(&c)).transpose()?;
        Ok((Kind::Prim { prim: *prim, null, constant }, presence))
    }

    /// `Enum.Value`: the variant's name, and its value's text.
    fn value_ref(&self, value_ref: &str) -> Result<(String, String), Error> {
        let (enum_name, value) =
            value_ref.split_once('.').ok_or_else(|| Error(format!("valueRef {value_ref:?} isn't Enum.Value")))?;
        let e = self.enums.iter().find(|e| e.name == enum_name);
        let literal = e.and_then(|e| e.values.iter().find(|(n, _)| n == value)).map(|(_, l)| l.clone());
        let literal = literal.ok_or_else(|| Error(format!("valueRef {value_ref} names no enum value")))?;
        // The text, for a field whose type is a primitive: b'C' → C, 9 → 9.
        let text = literal.strip_prefix("b'").and_then(|l| l.strip_suffix('\'')).unwrap_or(&literal).to_string();
        Ok((value.to_string(), text))
    }

    fn block(&self, node: roxmltree::Node<'_, '_>, depth: usize) -> Result<Block, Error> {
        if depth > MAX_DEPTH {
            return Err(Error("groups nest too deeply".into()));
        }
        let mut block = Block { fields: Vec::new(), block_length: 0, groups: Vec::new(), data: Vec::new() };
        let mut at = 0;
        for child in element_children(node) {
            match child.tag_name().name() {
                "field" => {
                    let slot = self.field(child, at)?;
                    at = slot.offset + slot.size;
                    block.fields.push(slot);
                }
                "group" => block.groups.push(self.group(child, depth)?),
                "data" => block.data.push(self.data(child)?),
                other => return Err(Error(format!("unknown element <{other}>"))),
            }
        }
        block.block_length = number(node, "blockLength")?.unwrap_or(0).max(at);
        Ok(block)
    }

    fn field(&self, node: roxmltree::Node<'_, '_>, at: usize) -> Result<Slot, Error> {
        let name = required(node, "name")?.to_string();
        let resolved =
            self.resolve(required(node, "type")?, presence(attr(node, "presence"))?, attr(node, "valueRef"), 0);
        let (kind, presence) = resolved.map_err(|e| Error(format!("field {name}: {e}")))?;
        let offset = number(node, "offset")?.unwrap_or(at);
        if offset < at {
            return Err(Error(format!("field {name}: offset {offset} overlaps the field before")));
        }
        let since = number(node, "sinceVersion")?.unwrap_or(0);
        self.slot(name, number(node, "id")?, kind, presence, offset, since, 0)
    }

    fn group(&self, node: roxmltree::Node<'_, '_>, depth: usize) -> Result<Group, Error> {
        let name = required(node, "name")?.to_string();
        let dimension = attr(node, "dimensionType").unwrap_or("groupSizeEncoding").to_string();
        let composite = self.composite(&dimension, 0)?;
        for member in ["blockLength", "numInGroup"] {
            let slot = composite.members.iter().find(|m| m.name == member);
            let unsigned = |p: &Prim| matches!(p, Prim::U8 | Prim::U16 | Prim::U32);
            if !slot.is_some_and(|s| matches!(&s.kind, Kind::Prim { prim, constant: None, .. } if unsigned(prim))) {
                return Err(Error(format!("group {name}: {dimension}.{member} must be an unsigned integer")));
            }
        }
        let block = self.block(node, depth + 1).map_err(|e| Error(format!("group {name}: {e}")))?;
        let since = number(node, "sinceVersion")?.unwrap_or(0);
        Ok(Group { name, id: number(node, "id")?, dimension, since, block })
    }

    fn data(&self, node: roxmltree::Node<'_, '_>) -> Result<Data, Error> {
        let name = required(node, "name")?.to_string();
        let type_name = required(node, "type")?;
        let Some(Declared::Composite(members)) = self.declared.get(type_name) else {
            return Err(Error(format!("data {name}: {type_name} isn't a composite")));
        };
        let length = members.iter().find_map(|(member, declared)| match declared {
            Member::Inline(Declared::Encoded { prim, max, .. }, _) if member == "length" => Some((*prim, max)),
            _ => None,
        });
        let (length, max) = length
            .filter(|(p, _)| matches!(p, Prim::U8 | Prim::U16 | Prim::U32))
            .ok_or_else(|| Error(format!("data {name}: {type_name} needs an unsigned length")))?;
        let max = max.as_deref().map(|m| length.literal(m)).transpose()?;
        let since = number(node, "sinceVersion")?.unwrap_or(0);
        Ok(Data { name, id: number(node, "id")?, length, max, since })
    }
}

fn encoded(node: roxmltree::Node<'_, '_>) -> Result<Declared, Error> {
    let name = required(node, "name")?;
    let prim_name = required(node, "primitiveType")?;
    let prim =
        Prim::parse(prim_name).ok_or_else(|| Error(format!("type {name}: unknown primitiveType {prim_name}")))?;
    Ok(Declared::Encoded {
        prim,
        length: number(node, "length")?,
        presence: presence(attr(node, "presence"))?.unwrap_or(Presence::Required),
        null: attr(node, "nullValue").map(str::to_string),
        max: attr(node, "maxValue").map(str::to_string),
        text: node.text().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()),
        value_ref: attr(node, "valueRef").map(str::to_string),
    })
}
